# Session Progress: Merged Index Feature

## Status

| Item                                                                    | Status  |
|-------------------------------------------------------------------------|---------|
| `CLAUDE.md` + `main.md`                                                 | Done    |
| `core/.../materialize/MergedIndex.java` (+ `sources` field, `of()` factory) | Done |
| `core/.../materialize/MergedIndexRegistry.java` (`findFor(List<Object>, ...)`) | Done |
| `core/.../adapter/enumerable/EnumerableMergedIndexScan.java`            | Done    |
| `core/.../adapter/enumerable/EnumerableMergedIndexJoin.java`            | Deleted (per-source arch) |
| `core/.../adapter/enumerable/PipelineToMergedIndexScanRule.java` (Sort-boundary) | Done |
| `core/.../adapter/enumerable/EnumerableRules.java` (constant)           | Done    |
| `core/.../adapter/enumerable/EnumerableMergedIndexDeltaScan.java` (NEW) | Obsolete (Option B) |
| `core/.../adapter/enumerable/DeltaToMergedIndexDeltaScanRule.java` (NEW)| Obsolete (Option B) |
| `core/.../adapter/enumerable/PipelineToMergedIndexScanRuleTest.java`    | Done ✓  |
| TPC-H Q3 (deleted — incorrect CUSTOMER+ORDERS example)                  | Removed |
| TPC-H Q12 (2-table: ORDERS ⋈ LINEITEM, full substitution)              | Done ✓  |
| TPC-H Q3-OL full 3-table substitution — `tpchQ3OrdersLineitem()`        | Done ✓  |
| TPC-H Q9 (6-table, all leaf joins substituted)                          | Done ✓  |
| Pipeline overhaul: sort-boundary-based discovery                         | Done ✓  |
| Rule generalization: accept SortedAggregate inputs                       | Done ✓  |
| `inputAlreadySorted` direction check fix                                 | Done ✓  |
| `flattenPipelines`: `p.mergedIndex != null` (not source count)           | Done ✓  |
| `MergedIndexTestUtil` — shared test helpers extracted to `testkit`        | Done ✓  |
| `TaggedRowSchema` — tagged interleaved row metadata (Subtask 1)          | Done ✓  |
| Pipeline discovery moved to `Pipeline.java` (production code)            | Done ✓  |
| Single-source indexed views (Q12, Q9)                                    | Done ✓  |
| Q9 sort direction fix (`propagateOrderByDirection`)                      | Done ✓  |
| Index Creation Plan Capture (Subtask 2) — Q12 wired into HEP loop        | Done ✓  |

## Terminology

- **Bottom side** / earlier = input side (table scans, leaf pipelines). Smaller field indices.
- **Top side** / later = output side (final result, root pipeline).
- `injectSortsBeforeSortBasedOps` processes bottom-up (starts from input side) for proper
  recognition of sorted inputs by later operators.

## Commands

```bash
# Run TPC-H plan tests
./gradlew :plus:cleanTest :plus:test --tests "*.MergedIndexTpchPlanTest" --info

# Run core rule test
./gradlew :core:test --tests "*.PipelineToMergedIndexScanRuleTest"
```

Search for `=== Q12 BEFORE`, `=== Q12 AFTER`, `=== Q3 OL AFTER`, `=== Q9 AFTER` in output.

### Sample AFTER output (with indexed views)

**Q12** (2-table + indexed view — MergeJoin absorbed):

```text
EnumerableSort(l_shipmode)                ← ORDER BY (no-op)
  EnumerableSortedAggregate(l_shipmode)
    MIScan(ivMI)                           ← indexed view on l_shipmode
```

**Q3-OL** root query plan (outer pipeline — no indexed view, LimitSort not a boundary):

```text
EnumerableLimitSort
  EnumerableProject
    EnumerableMergeJoin(custkey)                  ← STAYS
      MIScan(MI_outer, src=inner_view, group=G2)  ← replaces Sort→(inner result)
      MIScan(MI_outer, src=CUSTOMER, group=G2)    ← replaces Sort→Scan(CUSTOMER)
```

**Q9** (6-table + indexed view — entire plan collapses):

```text
MIScan(ivMI)                               ← single scan, entire plan collapsed
```

ORDER BY is redundant after GROUP BY and was removed. The final MIScan absorbs
all 5 joins + filter + aggregate, sorted by the GROUP BY key (n_name, o_year DESC).

---

## Test Plan Summaries

Full DOT diagrams in `plus/test-dot-output/`. Plans are accurate as of the last test run.

### Query time vs. maintenance time

The BEFORE plan (Phase 1, pre-HEP) IS the maintenance plan for each merged index.
At update time, when a base table row is inserted/deleted/updated, the affected
pipeline segment re-executes for the changed key and updates the merged index.
This is 1-to-1 cost: one base-table change → one merged index entry change,
the same as a traditional single-table index.

For nested merged indexes (Q3-OL, Q9), updates cascade level-by-level: a base
table change triggers the inner maintenance plan, whose output delta triggers the
outer maintenance plan, and so on. Each individual step is still 1-to-1; there
are depth-many cascading steps total.

---

### Q12 — 2-table + indexed view (`tpchQ12`)

Key: `o_orderkey = l_orderkey`. 2 pipelines: join + indexed view on l_shipmode.

```text
BEFORE                                     AFTER (indexed view absorbs MergeJoin)
EnumerableSort(l_shipmode)                 EnumerableSort(l_shipmode)   ← ORDER BY
  EnumerableSortedAggregate                  EnumerableSortedAggregate
    EnumerableSort(l_shipmode)                 MIScan(ivMI)             ← indexed view
      EnumerableMergeJoin(orderkey)
        EnumerableSort → Scan(ORDERS)
        EnumerableSort → Scan(LINEITEM)
```

---

### Q3-OL — 3-table (`tpchQ3OrdersLineitem`)

Keys: `l_orderkey = o_orderkey` (inner), `o_custkey = c_custkey` (outer). Two pipelines.

```text
BEFORE (full plan)
EnumerableLimitSort
  EnumerableProject
    EnumerableMergeJoin(custkey)          ← outer pipeline
      EnumerableSort(custkey)
        EnumerableMergeJoin(orderkey)     ← inner pipeline
          EnumerableSort
            EnumerableAggregate → Scan(LINEITEM)
          EnumerableSort → Scan(ORDERS)
      EnumerableSort → Scan(CUSTOMER)
```

```text
AFTER — Root query plan (outer pipeline only)
EnumerableLimitSort
  EnumerableProject
    EnumerableMergeJoin(custkey)                     ← STAYS
      MIScan(MI_outer, src=inner_view, group=G2)    ← replaces Sort→(inner result)
      MIScan(MI_outer, src=CUSTOMER, group=G2)      ← replaces Sort→Scan(CUSTOMER)
```

```text
AFTER — Index creation plan (inner pipeline, populates MI_inner)
EnumerableMergeJoin(orderkey)
  EnumerableSortedAggregate → TableScan(LINEITEM)
  TableScan(ORDERS)
```

---

### Q9 — 6-table + indexed view (`tpchQ9`)

Keys: orderkey → partkey → (partkey,suppkey) → suppkey → nationkey.
6 pipelines: 5 join + 1 indexed view on (n_name ASC, o_year DESC).

After `propagateOrderByDirection`, the GROUP BY sort direction changes from
`(n_name ASC, o_year ASC)` to `(n_name ASC, o_year DESC)` matching the ORDER BY.
Both ORDER BY and GROUP BY sorts are boundary sorts → two indexed view levels.

```text
BEFORE (after sort-direction fix)
EnumerableAggregate(n_name, o_year)
  EnumerableSort(n_name ASC, o_year DESC)    ← GROUP BY (boundary sort)
    EnumerableProject → Filter → 5 nested MergeJoins...
```

```text
AFTER — Entire plan collapses to indexed view scan
EnumerableMergedIndexScan(ivMI)               ← indexed view, sorted by n_name, o_year DESC
```

Inner pipelines (index creation plans, 5 levels):
- L1: MI(OL) by orderkey — MergeJoin(ORDERS, LINEITEM)
- L2: MI(OLP) by partkey — MergeJoin(OL_view, PART)
- L3: MI(OLPS) by (partkey,suppkey) — MergeJoin(OLP_view, PARTSUPP)
- L4: MI(OLPPS) by suppkey — MergeJoin(OLPS_view, SUPPLIER)
- L5: MI(OLPPSN) by nationkey — MergeJoin(OLPPS_view, NATION)

---

## Maintenance & Index Creation (design notes)

Archived Option B maintenance plan content → `TRASH-option-b.md`.

### Index creation plan

For non-root pipelines, the BEFORE plan IS the index creation plan. It populates the
MI from base tables (or inner MI views). Store as `physicalPlan` field on `Pipeline`.

### Maintenance plan

Same as index creation but processes deltas instead of full data. Future work — reconcile
with existing `deriveIncrementalPlan()` and delta scan infrastructure.

---

## What Was Done (2026-03-10 to 2026-03-24)

### Indexed Views Feature (Single-Source Pipelines)
- `Pipeline.buildTree()` moved to production code; now identifies both multi-source (join) and single-source (indexed view) pipelines.
- `Pipeline.flatten()` now includes pipelines with `sources.size() >= 1` (was `>= 2`).
- Multi-stage HEP planner: each pipeline level registered incrementally, one per HEP pass (leaf-to-root).
- Q12 now demonstrates indexed view absorption: join pipeline (ORDERS ⋈ LINEITEM) feeds single-source indexed view (GROUP BY l_shipmode); final result is one MIScan.

### Index Creation Plan Capture (Subtask 2)
- Added `indexCreationPlan` field to `MergedIndex` with getter/setter.
- Javadoc added: "Physical execution plan for this pipeline. Reads from this MI (via MIScans), executes pipeline operators (join, aggregate, etc.), and produces output rows that become a source in the parent pipeline."
- Wired into Q12 multi-stage HEP loop (commit `128b1fb53`): after each HEP pass except root, `pipeline.root` captured as index creation plan.
- Wired into Q3-OL and Q9 HEP loops (same pattern as Q12) — both already present in the code.
- Test verification: all intermediate pipelines have non-null `indexCreationPlan` fields (except root).
- Q3-OL root pipeline assertion added: exactly 2 `EnumerableMergedIndexScan` nodes in final plan
  (inner pipeline MIScans absorbed into outer `view([0])` reference; not separate nodes in root plan).
- `MergedIndexTestUtil`: no deprecated `buildPipelineTree`/`flattenPipelines` methods found; all tests
  already use `Pipeline.buildTree()` and `pipelineTree.flatten()` directly.

### Q12, Q3-OL, Q9 Javadoc & Plan Updates
- `tpchQ12()`: restructured to show 2-pipeline discovery (join + indexed view); affirms indexed view absorption.
- `tpchQ3OrdersLineitem()`: multi-stage HEP loop with per-pipeline registration; intermediate plans written to DOT.
- `tpchQ9()`: sort direction fix via `propagateOrderByDirection()`; 6 pipelines (5 joins + 1 indexed view); entire plan collapses to single MIScan.
- All tests use `Pipeline.buildTree()` and `pipelineTree.flatten()` (not custom test utils).

### Terminology & Design Notes
- "Bottom side" = input side (leaf pipelines, smaller indices).
- "Top side" = output side (root pipeline, final result).
- Two-pipeline test output structure added to SESSION_PROGRESS.md for Q12, Q3-OL, Q9.

### Documentation Migration (2026-03-24)
- Migrated stable reference content from SESSION_PROGRESS.md to CLAUDE.md:
  - "Calcite Planner Architecture: Volcano vs HEP" (revised with current single-pass BOTTOM_UP approach)
  - "Architecture: Sort-Boundary-Based Pipeline Replacement" (documents current operand pattern)
  - "Flow Chart A/B/C" (planning workflow and merged index concept)
  - "Multi-stage HEP for nested pipelines" (replaced outdated two-pass section)
- Updated Q9 plan documentation: removed redundant ORDER BY sort note; clarified indexed view scan as final step.
- Cleaned up SESSION_PROGRESS.md to keep only ephemeral session content (status table, commands, test summaries).
- Added detailed cost model Javadoc to `EnumerableMergedIndexScan.computeSelfCost()` and expanded
  `MergedIndexScanGroup` class-level Javadoc explaining why the class exists and the IO sharing design decision.
  Fixed pre-existing checkstyle violations in `MergedIndexRegistry.java` (`instanceof` line-wrap).

## Next Steps

### [Short-term] (Next Session)

1. ~~**EnumerableMergedIndexScan.java — Document cost model with scan group sharing**~~ Done.

2. ~~**isBoundarySort() Javadoc — Add future-work note on LimitSort**~~ Done:
   `Pipeline.java` lines 100–106 already document `EnumerableLimitSort` exclusion
   and the future-work note for top-K indexed views.

3. ~~**Cleanup: Remove stale DOT artifacts**~~ Done:
   No `leaf-4` or `branch-1` files exist in `plus/test-dot-output/`; no stale
   references in `MergedIndexTpchPlanTest.java`.

4. **Maintenance plans for indexed views** — single-source pipelines currently skip `setMaintenancePlan`.
   Design: single delta branch (not union of two). Reconcile with existing delta infrastructure.

### [Medium-term] (Future Sessions)

1. **Additional TPC-H queries** — Q5 (hierarchical keys), Q6 (baseline, no MI), Q14 (complex filters).

2. **Functional dependency metadata** — `RelMdFunctionalDependencies` for automatic
   ORDERKEY→CUSTKEY recognition; enable 3-table merged indexes without manual registration.

3. **JOB (Join Order Benchmark)** — generalization beyond TPC-H.

### [Long-term]

1. **Window functions and DISTINCT** — extend sort injection to handle `OVER`, `INTERSECT`, `EXCEPT`.

2. **End-to-end execution** — real sequential B-tree scan; LeanStore or in-memory storage integration.

3. **PATH B: Native merged index support** — tables report collation via `getStatistic()`;
   rule matches bare `EnumerableTableScan` with collation traits (no explicit Sorts).
