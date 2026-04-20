# Single-Pipeline Merged Index: TPC-H Analysis

## Overview

The VLDB experiments compare four storage structures on TPC-H:

1. Traditional single-table indexes (B-trees, one per table)
2. Materialized views (pre-computed join results)
3. **Single merged index** — one MI covering the most I/O-dominant pipeline
4. Clustered + hash index combination

The existing Calcite tests in `MergedIndexTpchPlanTest` demonstrate **multi-MI
cascade**: every pipeline in the query gets its own merged index, collapsing the
entire plan into a single `EnumerableMergedIndexScan`. That is the subject of the
next paper.

The **single-MI** approach is the focus of the current paper: pick exactly ONE
pipeline, build ONE merged index for it, and leave all remaining joins as regular
query-time operators.

### Key framing: candidates are conceptual

MI candidates are defined by **tables and sort key**, not by plan shape. Any set
of tables whose join/aggregation operations require compatible sort orders can be
stored in one merged index. This is independent of how many pipelines the current
Volcano plan happens to produce. Two candidates may overlap (share tables). The
experimental evaluation determines which candidate yields the best trade-off.

The test file `MergedIndexSinglePipelineTpchPlanTest` selects a pipeline by
matching the set of leaf table qualified names — NOT by pipeline index — making
the selection robust to planner changes.

---

## Candidate MIs per Query

### Q12 — Shipping Mode / Order Priority (ORDERS, LINEITEM)

| Field | Content |
|-------|---------|
| Candidate | MI(ORDERS, LINEITEM) by `o_orderkey` |
| What MI absorbs | O-L join; both table scans; pre-sort on orderkey |
| What remains at query time | `EnumerableFilter` (shipmode + date), `EnumerableSortedAggregate` (GROUP BY shipmode) |
| Re-sort needed | No — no further join after the MI |
| Note | Q12 has only two tables; single-MI and multi-MI outputs are identical |
| Calcite status | `tpchQ12` already produces this plan; no new test variant needed |

---

### Q3 — Shipping Priority (CUSTOMER, ORDERS, LINEITEM)

#### Candidate A: MI(ORDERS, LINEITEM) by `o_orderkey`

| Field | Content |
|-------|---------|
| What MI absorbs | Inner O-L join + both table scans + pre-sort on orderkey |
| What remains at query time | Merge-join with CUSTOMER on `o_custkey = c_custkey`; SortedAggregate; ORDER BY revenue DESC LIMIT 10 |
| Re-sort needed | Yes — MI output is ordered by orderkey; CUSTOMER join needs custkey order |
| Re-sort alternative | Hash join for CUSTOMER avoids the re-sort if CUSTOMER fits in memory |
| Calcite test | `tpchQ3OrdersLineitem` pass-1-only output; `tpchQ3OlMI` (placeholder) |

#### Candidate B: MI(CUSTOMER, ORDERS) by `o_custkey`

| Field | Content |
|-------|---------|
| What MI absorbs | C-O join + both table scans + pre-sort on custkey |
| What remains at query time | Merge-join with LINEITEM on `o_orderkey = l_orderkey`; SortedAggregate; ORDER BY revenue DESC LIMIT 10 |
| Re-sort needed | Yes — MI output is ordered by custkey; LINEITEM join needs orderkey order |
| Calcite test | `tpchQ3CoMI` (placeholder) |

Note: a three-table MI(CUSTOMER, ORDERS, LINEITEM) requires a hierarchical key
structure (`o_orderkey` structured as `(custkey, local_id)`) that TPC-H surrogate
keys do not provide. Two separate two-table MIs are the correct model here.

---

### Q9 — Product Type / Profit (ORDERS, LINEITEM, PART, PARTSUPP, SUPPLIER, NATION)

#### Candidate A: MI(ORDERS, LINEITEM) by `o_orderkey`

| Field | Content |
|-------|---------|
| What MI absorbs | O-L leaf join + both table scans; highest I/O savings (LINEITEM = SF×6M rows) |
| What remains at query time | 4 joins: PART (partkey), PARTSUPP (partkey, suppkey), SUPPLIER (suppkey), NATION (nationkey) |
| Re-sort needed | Yes — MI output ordered by orderkey; next join (PART) needs partkey order |
| Re-sort alternative | Hash-join PART first using `p_name LIKE '%green%'` filter, then probe PARTSUPP hash table |
| Calcite test | `tpchQ9` pass-1-only output; `tpchQ9OlMI` (placeholder) |

#### Candidate B: MI(LINEITEM, PARTSUPP) by `(l_partkey, l_suppkey)`

| Field | Content |
|-------|---------|
| What MI absorbs | L-PS compound-key join + both table scans; pre-sort on (partkey, suppkey) |
| What remains at query time | Joins: ORDERS (orderkey), PART (partkey), SUPPLIER (suppkey), NATION (nationkey) |
| Re-sort needed | Depends on join order; orderkey join likely needs a re-sort |
| SQL rewrite | LINEITEM ⋈ PARTSUPP as inner join with `ps_partkey = l_partkey AND ps_suppkey = l_suppkey` (partkey first to match PARTSUPP PK order) |
| Calcite test | `tpchQ9LpsMI` (placeholder) |

#### Candidate C: MI(LINEITEM, PART) by `l_partkey`

| Field | Content |
|-------|---------|
| What MI absorbs | L-PART join + both table scans; enables pushing `p_name LIKE '%green%'` into the MI |
| What remains at query time | Joins: ORDERS (orderkey), PARTSUPP (partkey, suppkey), SUPPLIER (suppkey), NATION (nationkey) |
| Note | PART filter absorbed at maintenance time → query-time plan is filter-free |
| Calcite test | Not yet planned |

#### Candidate D: MI(SUPPLIER, NATION) by `s_nationkey`

| Field | Content |
|-------|---------|
| What MI absorbs | S-N join + both table scans; SUPPLIER (SF×10K) and NATION (25 rows) are small |
| What remains at query time | All other joins unchanged; marginal I/O benefit |
| Calcite test | Not yet planned |

Candidates A and B both include LINEITEM — they are alternatives, not compatible
choices. The experimental evaluation determines which reduces I/O most after
accounting for re-sort cost.

---

### Q5 — Local Supplier Volume (CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION)

TBD — candidates to be identified after SQL rewrite analysis.

Key open questions:

- Does Calcite's Volcano planner produce merge-join plans for Q5's star topology,
  or hash joins? MI benefit requires merge joins along the candidate pipeline.
- If the planner chooses hash join for CUSTOMER (custkey join), the MI output
  cannot feed a merge join and needs a separate sort anyway.

Calcite test: `tpchQ5` (placeholder).

---

### Q7 — Volume Shipping (SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION×2)

TBD — potential "MI not helpful" honest evaluation case.

The NATION self-join (`n1.n_name` and `n2.n_name`) may break interesting-ordering
chains and force hash joins or extra sorts regardless of MI use. Worth documenting
as a bound on MI applicability.

Calcite test: `tpchQ7` (placeholder).

---

## Test File

`MergedIndexSinglePipelineTpchPlanTest` (same package as `MergedIndexTpchPlanTest`):

- `singleMIPlan(sql, config, tables)` — helper that runs Phase 1 (Volcano),
  discovers pipelines, selects the pipeline whose leaf table set matches
  `tables`, creates ONE `MergedIndex`, runs one HEP pass. Not yet implemented
  (throws `UnsupportedOperationException`).
- One `@Test` method per candidate above. All currently empty (no assertions).
- Visualization via `TpchPlanTestUtil`; output to
  `test-dot-output/single-mi/<query>/`.

---

## int-ord-plans/ Deliverables

Expected directory structure for the leanstore integration:

```
calcite-integration-info/int-ord-plans/
  q12/
    plan.dot          ← AFTER plan: single MI = full multi-MI for Q12
    maintenance.dot   ← maintenance plan for MI(ORDERS, LINEITEM)
    README.md         ← tables, key, what's absorbed, what's query-time
  q3-ol/
    plan.dot          ← AFTER pass-1-only plan (Candidate A)
    maintenance.dot   ← maintenance plan for MI(ORDERS, LINEITEM)
    README.md
  q9-ol/
    plan.dot          ← AFTER pass-1-only plan (Candidate A)
    maintenance.dot   ← maintenance plan for MI(ORDERS, LINEITEM)
    README.md
```

Definitions:

- `plan.dot` — query-time plan with only the selected MI substituted (not full
  cascade). For Q12 this equals the existing output; for Q3/Q9 it is the
  intermediate state after the first HEP pass.
- `maintenance.dot` — incremental maintenance plan for the selected MI, showing
  delta join semantics over the two base tables.
- `README.md` — per-query prose: tables, join key, operators absorbed into the MI
  at maintenance time, operators remaining at query time, re-sort trade-off.

---

## Status

| Query | Candidate | Calcite test | DOTs | Notes |
|-------|-----------|--------------|------|-------|
| Q12 | MI(ORDERS, LINEITEM) | `tpchQ12` (passes) | `q12/` | multi-MI = single-MI |
| Q3 | Candidate A: MI(ORDERS, LINEITEM) | `tpchQ3OlMI` (placeholder) | — | pass-1-only plan exists in `tpchQ3OrdersLineitem` |
| Q3 | Candidate B: MI(CUSTOMER, ORDERS) | `tpchQ3CoMI` (placeholder) | — | SQL rewrite needed |
| Q9 | Candidate A: MI(ORDERS, LINEITEM) | `tpchQ9OlMI` (placeholder) | — | pass-1-only plan exists in `tpchQ9` |
| Q9 | Candidate B: MI(LINEITEM, PARTSUPP) | `tpchQ9LpsMI` (placeholder) | — | SQL rewrite needed |
| Q9 | Candidate C: MI(LINEITEM, PART) | — | — | Not yet planned |
| Q9 | Candidate D: MI(SUPPLIER, NATION) | — | — | Not yet planned |
| Q5 | TBD | `tpchQ5` (placeholder) | — | Analysis needed |
| Q7 | TBD | `tpchQ7` (placeholder) | — | Honest "not helpful" candidate |
