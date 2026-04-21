# Single-Pipeline Merged Index: TPC-H Analysis

## Overview

The VLDB experiments compare four storage structures on TPC-H:

1. Traditional single-table indexes (B-trees, one per table) + merge join
2. Materialized views (pre-computed join results)
3. **Single merged index** — one MI covering one pipeline of the query
4. Traditional single-table indexes (B-trees, one per table) + hash join

The existing Calcite tests in `MergedIndexTpchPlanTest` demonstrate **multi-MI
cascade**: every pipeline in the query gets its own merged index, collapsing the
entire plan into a single `EnumerableMergedIndexScan`. That is the subject of the
next paper.

The **single-MI** approach is the focus of the current paper: pick exactly ONE
pipeline, build ONE merged index for it, and leave all remaining joins as regular
query-time operators.

### Key framing: candidates are conceptual

Single MI candidates are defined by **tables and sort key**, not by the current
multi-MI plan shape. A choice of MI may dictate the plan shape. Any set of tables
whose join/aggregation operations require compatible sort orders can be stored in
one merged index — including single-table indexed views (one row type, sorted on a
query-relevant key). This is independent of how many pipelines the current Volcano
plan happens to produce. Two candidates may overlap (share tables). The
experimental evaluation determines which candidate yields the best trade-off.

The `SingleMIPipelineIdentifier` algorithm automates candidate enumeration from
the logical plan, replacing the earlier approach of purely manual SQL rewrites.

---

## Pipeline Identification Algorithm

Implemented in `testkit/.../SingleMIPipelineIdentifier.java`. Given a logical
plan, the algorithm:

1. **Enumerate sort requirements** — Walk the plan tree. For each sort-based
   operator (Join, Aggregate, Sort), trace key columns to base tables via
   `RelMetadataQuery.getColumnOrigins()`. Each produces a `(table, collation)`
   requirement. All operators are treated uniformly — joins are not special.

2. **Build column equivalence classes** — Extract all equi-join conditions, build
   union-find of `(table, column)` pairs. Two columns in different tables belong
   to the same class when equi-joined.

3. **Prefix consolidation** — Group requirements by table. Merge collations
   sharing a prefix relationship. **Compound-key reordering**: multi-column keys
   from joins or GROUP BY can be reordered to maximize prefix matches (ORDER BY
   keys cannot). Both original and reordered versions are kept as options.

4. **Find globally-consistent subsets** — Enumerate table combinations. Map each
   collation to equivalence-class space. Check if all form a valid prefix chain
   (one subsumes all others as prefixes). No separate connectivity check is
   needed — a valid prefix chain implicitly ensures tables share a logical sort
   key.

5. **Rank candidates** — Sort by table count (descending). Cardinality-based
   ranking is future work.

### Verified results

**Q12** (2 tables): `{ORDERS, LINEITEM}` by orderkey

**Q3** (3 tables, 2 candidates):

- `{CUSTOMER, ORDERS}` by custkey
- `{ORDERS, LINEITEM}` by orderkey
- No 3-table candidate — custkey and orderkey are independent keys with no
  prefix relationship

**Q9** (6 tables, multi-table candidates):

- `{LINEITEM, PART, PARTSUPP}` by (partkey, suppkey) — **3 tables** (best)
- `{ORDERS, LINEITEM}` by orderkey — 2 tables
- `{LINEITEM, SUPPLIER}` by (suppkey, partkey) — 2 tables (reordered compound key)
- `{SUPPLIER, NATION}` by nationkey — 2 tables
- `{PART, PARTSUPP}` by partkey — 2 tables
- Cannot extend `{L, P, PS}` with SUPPLIER: L's sort `[partkey, suppkey]` does
  not provide `[suppkey]` order alone
- With key reordering to `[suppkey, partkey]`: trades PART for SUPPLIER in the
  3-table candidate

---

## Candidate MI Analysis

For each candidate MI, the analysis reasons about:

1. **Plan shape required** — what join order the query plan must take
2. **What the MI absorbs** — which joins, sorts, aggregations move to maintenance
   time
3. **What remains at query time** — remaining joins, filters, projections; re-sort
   cost
4. **Storage cost** — row count, row width, index size estimate
5. **Update cost** — maintenance overhead per base-table insert/delete

The pipelines from the current multi-MI plans (in `MergedIndexTpchPlanTest`) are
valid candidates, but not the only ones. For example, Q12 admits both the
two-table join pipeline (ORDERS ⋈ LINEITEM by orderkey) and a single-table
indexed view (LINEITEM sorted by shipmode, used directly for the GROUP BY).

Per-query analysis: TBD — to be filled as each candidate is implemented in tests.

---

## int-ord-plans/ Deliverables

Expected directory structure for the leanstore integration:

```
calcite-integration-info/int-ord-plans/
  q12/
    plan.dot          ← query-time plan with selected MI substituted
    maintenance.dot   ← maintenance plan for the selected MI
    README.md         ← candidate chosen, analysis summary
  q3/
    plan.dot
    maintenance.dot
    README.md
  q9/
    plan.dot
    maintenance.dot
    README.md
```

- `plan.dot` — query-time plan with only the selected MI substituted; remaining
  joins intact (not full cascade)
- `maintenance.dot` — incremental maintenance plan for the selected MI
- `README.md` — candidate chosen, plan shape, operators absorbed, query-time
  remainder, re-sort cost

---

## Running Tests

```bash
# Single-MI pipeline identification + substitution tests
./gradlew :plus:cleanTest :plus:test --tests "*.MergedIndexSinglePipelineTpchPlanTest" --info

# Multi-MI tests (regression check)
./gradlew :plus:cleanTest :plus:test --tests "*.MergedIndexTpchPlanTest" --info
```

---

## Test Files

**`MergedIndexSinglePipelineTpchPlanTest`** (same package as
`MergedIndexTpchPlanTest`):

- `testIdentifyQ12()`, `testIdentifyQ3()`, `testIdentifyQ9()` — pipeline
  identification tests (working, all pass)
- `tpchQ12OlMI()`, `tpchQ3OlMI()`, `tpchQ3CoMI()`, `tpchQ9OlMI()`,
  `tpchQ9LpsMI()` — MI substitution tests (placeholders)
- `tpchQ5()`, `tpchQ7()` — TBD

**`SingleMIPipelineIdentifier`** (`testkit/`):

- 5-step algorithm: enumerate → equivalence classes → consolidate → find subsets
  → rank
- Key inner classes: `SortRequirement`, `Candidate`, `TableCol`, `UnionFind`

**`TpchPlanTestUtil`** — shared DOT visualization helpers

---

## Status

| Query | Identification | Candidates found | MI substitution test | DOTs |
|-------|---------------|-----------------|---------------------|------|
| Q12 | ✅ Done | `{O,L}` by orderkey | placeholder | — |
| Q3 | ✅ Done | `{C,O}` by custkey; `{O,L}` by orderkey | placeholder | — |
| Q9 | ✅ Done | `{L,P,PS}` 3-table + four 2-table | placeholder | — |
| Q5 | — | — | placeholder | — |
| Q7 | — | — | placeholder | — |
