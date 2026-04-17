# Single-Pipeline Merged Index: TPC-H Analysis

## Overview

The VLDB revision experiments compare four storage structures on TPC-H:

1. Traditional single-table indexes (B-trees, one per table)
2. Materialized views (pre-computed join results)
3. **Single merged index** — one MI covering the most I/O-dominant pipeline
4. Clustered + hash index combination

The existing Calcite tests demonstrate **multi-MI cascade**: every pipeline in the
query gets its own merged index, collapsing the entire plan into a single
`EnumerableMergedIndexScan`. That is the subject of the next paper.

The **single-MI** approach is the focus of the current paper (matching the geo
benchmark): pick exactly ONE pipeline, build ONE merged index for it, and leave all
remaining joins as regular query-time operators. This document analyzes which
pipeline to pick for each TPC-H query and what the resulting plan looks like.

**Selection criterion**: maximize I/O reduction with one MI. For TPC-H, LINEITEM is
the largest table (SF×6M rows) and the ORDERS ⋈ LINEITEM join on `o_orderkey =
l_orderkey` dominates I/O in every query that touches both tables. This makes
`MI(ORDERS, LINEITEM)` by `orderkey` the obvious single-MI choice for all Tier 1
queries.

---

## Per-Query Analysis

### Q12 — Shipping Mode / Order Priority (2 tables)

| Field | Content |
|-------|---------|
| Selected pipeline | ORDERS ⋈ LINEITEM on `o_orderkey = l_orderkey` |
| MI composition | `MergedIndex(ORDERS, LINEITEM)` by `orderkey` |
| What MI absorbs | The join itself; both table scans; pre-sorting on orderkey |
| What remains at query time | `EnumerableFilter` (shipmode + date), `EnumerableSortedAggregate` (GROUP BY shipmode) |
| Why this pipeline | Q12 has only two tables — this is the only pipeline |
| Calcite evidence | `tpchQ12`, single HEP pass (multi-MI = single-MI for 2-table query) |
| Status | Plan produced; DOT files in `plus/test-dot-output/q12/` |

**Query-time plan** (from `root-pipeline-query-plan.dot`):

```
EnumerableSortedAggregate(group=[shipmode], HIGH_LINE_COUNT, LOW_LINE_COUNT)
  EnumerableFilter(shipmode IN ('MAIL','SHIP') AND date conditions)
    EnumerableMergedIndexScan(MI[0], source=view([0]))
```

**Maintenance plan** (from `maintenance.dot`): `LogicalJoin(o_orderkey = l_orderkey)`
over `LogicalPipelineOutputScan(ORDERS)` and `LogicalDelta(LINEITEM)` (plus symmetric
delta), wrapped in `LogicalProject` for the pre-computed row shape.

**Note**: For Q12, single-MI and multi-MI outputs are identical. No additional test
variant is needed; `tpchQ12` already produces the target plan.

---

### Q3 — Shipping Priority (3 tables: CUSTOMER, ORDERS, LINEITEM)

| Field | Content |
|-------|---------|
| Selected pipeline | ORDERS ⋈ LINEITEM on `o_orderkey = l_orderkey` (inner/leaf pipeline) |
| MI composition | `MergedIndex(ORDERS, LINEITEM)` by `orderkey` |
| What MI absorbs | Inner join + both table scans + pre-sort on orderkey |
| What remains at query time | Merge join with CUSTOMER on `o_custkey = c_custkey` (needs sort on custkey or hash join), `EnumerableSortedAggregate`, `ORDER BY revenue DESC LIMIT 10` |
| Why this pipeline | LINEITEM+ORDERS dominate I/O; CUSTOMER is small (SF×150K rows) |
| Calcite evidence | `tpchQ3OrdersLineitem`, after HEP pass 1 only (stop before pass 2) |
| Status | Plan produced (pass 1 output); variant test not yet written |

**Single-MI query-time plan** (after pass 1 only):

```
EnumerableLimit(10)
  EnumerableSort(revenue DESC)
    EnumerableSortedAggregate(group=[o_orderkey, o_custkey, o_shippriority], revenue)
      EnumerableMergeJoin(o_custkey = c_custkey)
        EnumerableMergedIndexScan(MI[0], source=ORDERS)   ← leaf MI scan
        EnumerableMergedIndexScan(MI[0], source=LINEITEM)  ← leaf MI scan
        [right] EnumerableSort(custkey)
          EnumerableTableScan(CUSTOMER)
```

**Re-sort question**: The MI output is ordered by `orderkey`. The CUSTOMER join needs
`custkey` order. Options: (a) sort the MI output by custkey before the join — adds
one sort at query time; (b) use hash join for CUSTOMER — avoids the sort but loses
merge-join streaming. Document in per-query README.

**Maintenance plan**: same as `maintenance-0.dot` from `tpchQ3OrdersLineitem` —
`LogicalJoin(o_orderkey=l_orderkey)` with delta semantics, plus pre-aggregation on
`orderkey` for the `L_REVENUE` sum.

---

### Q9 — Product Type / Profit (6 tables: ORDERS, LINEITEM, PART, PARTSUPP, SUPPLIER, NATION)

| Field | Content |
|-------|---------|
| Selected pipeline | ORDERS ⋈ LINEITEM on `o_orderkey = l_orderkey` (leaf/deepest pipeline) |
| MI composition | `MergedIndex(ORDERS, LINEITEM)` by `orderkey` |
| What MI absorbs | Leaf join + both table scans + pre-sort on orderkey |
| What remains at query time | 4 additional joins: PART (partkey), PARTSUPP (partkey, suppkey), SUPPLIER (suppkey), NATION (nationkey); filter `p_name LIKE '%green%'`; aggregate; `ORDER BY n_name, o_year DESC` |
| Why this pipeline | LINEITEM+ORDERS dominate I/O; remaining tables are much smaller |
| Calcite evidence | `tpchQ9`, after HEP pass 1 only (leaf-5 pipeline; stop before passes 2–5) |
| Status | Plan produced (pass 1 output); variant test not yet written |

**Single-MI query-time plan** (after pass 1 only, schematic):

```
EnumerableAggregate(group=[n_name, o_year], SUM_PROFIT)
  EnumerableFilter(p_name LIKE '%green%')
    EnumerableMergeJoin(s_nationkey = n_nationkey)
      EnumerableMergeJoin(l_suppkey = s_suppkey)
        EnumerableMergeJoin(ps_partkey=l_partkey AND ps_suppkey=l_suppkey)
          EnumerableMergeJoin(l_partkey = p_partkey)
            EnumerableMergedIndexScan(MI[0], source=ORDERS)   ← MI scan
            EnumerableMergedIndexScan(MI[0], source=LINEITEM)  ← MI scan
            [right] EnumerableSort(partkey)
              EnumerableTableScan(PART)
          [right] EnumerableSort(partkey, suppkey)
            EnumerableTableScan(PARTSUPP)
        [right] EnumerableSort(suppkey)
          EnumerableTableScan(SUPPLIER)
      [right] EnumerableSort(nationkey)
        EnumerableTableScan(NATION)
```

**Re-sort question**: MI output is ordered by `orderkey`. The next join (PART) needs
`partkey` order. A sort on `partkey` is unavoidable at query time — the MI does not
eliminate this sort. Single-MI benefit is confined to the O-L join; the rest of the
plan is unchanged from the no-MI baseline.

**Maintenance plan**: same as `leaf-5-index-creation-plan.dot` from `tpchQ9` —
`LogicalMergeJoin(o_orderkey = l_orderkey)` with delta semantics over ORDERS and
LINEITEM base tables.

---

### Q5 — Local Supplier Volume (6 tables: CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION)

| Field | Content |
|-------|---------|
| Selected pipeline | ORDERS ⋈ LINEITEM on `o_orderkey = l_orderkey` (likely leaf pipeline) |
| MI composition | `MergedIndex(ORDERS, LINEITEM)` by `orderkey` |
| What MI absorbs | Leaf join + both scans (same as Q9 leaf) |
| What remains at query time | Joins with CUSTOMER (custkey), SUPPLIER (suppkey), NATION (nationkey), REGION (regionkey); filter `r_name = 'ASIA'`; aggregate; ORDER BY revenue DESC |
| Why this pipeline | Same rationale as Q9 — LINEITEM+ORDERS dominate; MI gives largest raw I/O savings |
| Calcite evidence | No existing test — needs new `tpchQ5` test method |
| Status | Not started |

**Open questions**:

- Does Calcite's Volcano planner produce a merge-join plan for Q5 (star topology)?
  Q5 has no natural interesting-ordering chain beyond orderkey; hash joins may dominate.
- If the planner chooses hash join for CUSTOMER (custkey join), the MI output cannot
  feed a merge join and needs a separate sort anyway.
- Analysis needed before claiming MI benefit for Q5.

---

### Q7 — Volume Shipping (5+ tables, NATION self-join)

| Field | Content |
|-------|---------|
| Selected pipeline | Unclear — NATION appears twice (n1, n2); no single dominant pipeline |
| MI composition | Potentially `MergedIndex(ORDERS, LINEITEM)` by orderkey if the OL join is present |
| What MI absorbs | TBD |
| What remains at query time | TBD — NATION self-join complicates ordering |
| Why this pipeline | TBD — candidate for "MI not helpful" honest evaluation |
| Calcite evidence | No existing test |
| Status | Analysis only — not started |

**Hypothesis**: Q7 may be a case where a merged index offers limited benefit because
the NATION self-join breaks interesting-ordering chains and forces a hash join or
extra sort regardless. Worth documenting as an honest bound on MI applicability.

---

## int-ord-plans/ Deliverables

Expected directory structure for the leanstore integration:

```
calcite-integration-info/int-ord-plans/
  q12/
    plan.dot          ← AFTER plan: single MI substituted (= full multi-MI for Q12)
    maintenance.dot   ← maintenance plan for MI(ORDERS, LINEITEM)
    README.md         ← tables, key, what's absorbed, what's query-time
  q3/
    plan.dot          ← AFTER pass-1-only plan
    maintenance.dot   ← maintenance plan for MI(ORDERS, LINEITEM)
    README.md
  q9/
    plan.dot          ← AFTER pass-1-only plan
    maintenance.dot   ← maintenance plan for MI(ORDERS, LINEITEM)
    README.md
```

Definitions:

- `plan.dot` — the query-time plan with only the selected pipeline's MI substituted
  (not full cascade). For Q12 this equals the existing output; for Q3/Q9 it is the
  intermediate state after HEP pass 1.
- `maintenance.dot` — the incremental maintenance plan for the one selected MI,
  showing delta join semantics over ORDERS and LINEITEM base tables.
- `README.md` — per-query prose: tables, join key, which operators are absorbed into
  the MI at maintenance time, which operators remain at query time, and the re-sort
  trade-off where applicable.

---

## Implementation Tasks

| Priority | Task | File/Method | Notes |
|----------|------|-------------|-------|
| Done | Q12 single-MI plan | `tpchQ12` | multi-MI = single-MI; no changes needed |
| Next | Q3 single-MI variant | Add `tpchQ3SingleMI` to `MergedIndexTpchPlanTest` | Stop after HEP pass 1; export DOTs |
| Next | Q9 single-MI variant | Add `tpchQ9SingleMI` to `MergedIndexTpchPlanTest` | Stop after HEP pass 1 (leaf-5 only) |
| Next | Export DOTs | Copy from `test-dot-output/` to `calcite-integration-info/int-ord-plans/` | After variant tests pass |
| Later | Q5 analysis | New `tpchQ5` test | Verify Volcano produces merge-join plan |
| Later | Q7 analysis | Documentation only | Honest "MI not helpful" candidate |

---

## LeanStore Operator Implications

From `TPCH_experiments.md`: "the only operator directly reading from merged indexes
is PremergedJoin, but this may no longer be the case with int-ord-plans."

For single-MI, the PremergedJoin output feeds further query-time operators:

| Query | MI output order | Next join key | Re-sort needed? |
|-------|-----------------|---------------|-----------------|
| Q12 | orderkey | — (no further join) | No |
| Q3 | orderkey | custkey (CUSTOMER join) | Yes — sort on custkey, or use hash join |
| Q9 | orderkey | partkey (PART join) | Yes — sort on partkey unavoidable |

The re-sort cost is O(N_OL × log N_OL) where N_OL is the ORDERS ⋈ LINEITEM result
size. At scale factor 1 this is ~6M rows. Whether this cost is acceptable relative
to the I/O savings of the MI is an empirical question for the leanstore experiments.

**Alternative for Q3**: if CUSTOMER is small enough, a broadcast hash join avoids
the re-sort entirely. PremergedJoin streams orderkey-sorted rows; CUSTOMER fits in
memory. This is the likely production implementation.

**Alternative for Q9**: PART filter (`p_name LIKE '%green%'`) reduces the effective
join size before PARTSUPP. If PART is hash-joined first (probe side), the MI output
can be filtered and then joined to a hash table of matching (partkey, suppkey) pairs
from PARTSUPP. No re-sort needed if all remaining joins use hash join.
