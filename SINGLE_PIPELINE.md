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

Pipeline selection is done **manually through SQL rewrites**, not automatically.
Each test method rewrites the SQL query to force the desired join order (the same
technique used in `MergedIndexTpchPlanTest`), ensuring the target pipeline appears
in the plan. Then a single HEP pass substitutes only that pipeline's MI.

---

## Candidate MI Analysis — TBD

For each candidate MI, the analysis should reason about:

1. **Plan shape required** — what join order and operator sequence the query plan
   must take for the MI to be applicable
2. **What the MI absorbs** — which joins, sorts, and aggregations move to
   maintenance time
3. **What remains at query time** — remaining joins, filters, projections; whether
   a re-sort is needed before the next join and at what cost
4. **Storage cost** — row count, row width, index size estimate
5. **Update cost** — maintenance overhead per base-table insert/delete

The pipelines from the current multi-MI plans (in `MergedIndexTpchPlanTest`) are
valid candidates, but not the only ones. For example, Q12 admits both the
two-table join pipeline (ORDERS ⋈ LINEITEM by orderkey) and a single-table indexed
view (LINEITEM sorted by shipmode, used directly for the GROUP BY).

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

## Test File

`MergedIndexSinglePipelineTpchPlanTest` (same package as `MergedIndexTpchPlanTest`):

- `singleMIPlan(sql, config, pipelineIndex)` — runs Phase 1 (Volcano), discovers
  pipelines, creates ONE `MergedIndex` for the pipeline at `pipelineIndex` in
  `flatten()`, runs one HEP pass. Not yet implemented.
- One `@Test` method per candidate. All currently empty (no assertions).
- Visualization via `TpchPlanTestUtil`; output to `test-dot-output/single-mi/<query>/`.

---

## Status

| Query | Candidate | Calcite test | DOTs |
|-------|-----------|--------------|------|
| Q12 | TBD | `tpchQ12OlMI` (placeholder) | — |
| Q3 | TBD | `tpchQ3OlMI`, `tpchQ3CoMI` (placeholders) | — |
| Q9 | TBD | `tpchQ9OlMI`, `tpchQ9LpsMI` (placeholders) | — |
| Q5 | TBD | `tpchQ5` (placeholder) | — |
| Q7 | TBD | `tpchQ7` (placeholder) | — |
