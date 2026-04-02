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
| `core/.../adapter/enumerable/EnumerableMergedIndexDeltaScan.java` (NEW) | Done ✓ (per-source with sourceIndex+scanGroup) |
| `core/.../adapter/enumerable/DeltaToMergedIndexDeltaScanRule.java` (NEW)| Done ✓ (updated to pass through sourceIndex/scanGroup) |
| `core/.../rel/logical/LogicalPipelineOutputScan.java` (NEW)             | Done ✓ |
| `core/.../adapter/enumerable/LogicalTableScanToMergedIndexRule.java` (NEW) | Done ✓ |
| `core/.../adapter/enumerable/PipelineOutputScanRule.java` (NEW)         | Done ✓ |
| `core/.../adapter/enumerable/PipelineToMergedIndexScanRuleTest.java`    | Done ✓  |
| `core/.../materialize/MaintenancePlanConverter.java` (NEW)              | Done ✓  |
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

## Maintenance & Index Creation

- **Index creation plan**: For non-root pipelines, stored as `pipeline.indexCreationPlan` (the BEFORE physical plan).
- **Maintenance plan**: `deriveMaintenancePlan()` produces a `LogicalDelta`-wrapped logical plan per pipeline. Physical conversion via `convertToPhysicalMaintenancePlan()` runs two HEP passes.
- **Physical maintenance plan**: `convertToPhysicalMaintenancePlan()` runs two HEP passes.
  Both full-scan and delta branches use pipeline output type. Source subtrees identified
  by pipeline boundary sorts, not join inputs.
  - **Fully physical**: all operators are enumerable after three-pass conversion.
    `EnumerableMergedIndexScan` / `EnumerableMergedIndexDeltaScan` at the leaves;
    `EnumerableMergeJoin`, `EnumerableUnion`, `EnumerableProject`, `EnumerableFilter`,
    etc. above them.
  - **Shared scan group**: all leaf scans (full and delta) within one pipeline's
    maintenance plan share the same `MergedIndexScanGroup` — a single MI stores all
    sources of that pipeline interleaved by the shared sort key.
  - Example physical maintenance plan for a 2-source pipeline (MI over ORDERS+LINEITEM):
    ```
    EnumerableUnion
    ├─ EnumerableMergeJoin
    │  ├─ EnumerableMergedIndexScan(MI, ORDERS, group=G)
    │  └─ EnumerableMergedIndexDeltaScan(MI, LINEITEM, group=G)
    └─ EnumerableMergeJoin
       ├─ EnumerableMergedIndexDeltaScan(MI, ORDERS, group=G)
       └─ EnumerableMergedIndexScan(MI, LINEITEM, group=G)
    ```
    All 4 scans share group G (same pipeline, same MI).

---

## What Was Done (2026-03-10 to 2026-03-25)

All code below is stable and tested. Per-commit details omitted; see git log.

- **Sort-boundary pipeline architecture**: `Pipeline.buildTree()` discovers pipelines
  from `EnumerableSort` boundaries; `flatten()` returns post-order list including
  single-source indexed views (`sources.size() >= 1`).
- **PipelineToMergedIndexScanRule**: matches `EnumerableSort` operands; multi-stage
  HEP (one pass per nesting level, BOTTOM_UP) replaces pipelines leaf-to-root.
- **TPC-H tests**: Q12 (2-table + indexed view), Q3-OL (3-table nested), Q9 (6-table,
  5 nested joins + indexed view). All pass with correct BEFORE/AFTER plans and DOT output.
- **Index creation plan capture**: `pipeline.indexCreationPlan` set after each HEP pass
  (except root). Wired into Q12, Q3-OL, Q9 test loops.
- **Incremental maintenance plan**: `deriveMaintenancePlan()` wraps `pipeline.logicalRoot`
  in `LogicalDelta`, applies 6 `StreamRules` via HEP. Fixed `SetOp.deriveRowType()`
  fast-path for type equivalence. `scopeLogicalRoot()` replaces child views with
  `LogicalValues.createEmpty` placeholders. DOT visualization with per-operator colors.
- **`scopeLogicalRoot` fix (2026-04-01)**: Removed `!child.sources.isEmpty()` filter so
  ALL child pipelines (both leaf table-scan and non-leaf MI-view) are replaced with
  `LogicalPipelineOutputScan` placeholders before delta push-down. Previously only
  non-leaf children were scoped, leaving leaf pipelines' `logicalRoot` exposed and
  potentially allowing delta propagation to cross pipeline boundaries.
- **Physical maintenance plan conversion (2026-04-01)**: Converts logical maintenance
  plans to fully physical (enumerable) plans.
  - `LogicalPipelineOutputScan` (new logical node): placeholder for child pipeline output.
    Carries `Pipeline` reference. Used in `scopeLogicalRoot` for ALL source pipelines
    (leaf and non-leaf) — replaces old `LogicalValues.createEmpty` approach.
  - `EnumerableMergedIndexDeltaScan`: repurposed as per-source with `sourceIndex` +
    `scanGroup` fields, matching `EnumerableMergedIndexScan` design.
  - Conversion rules: `PipelineOutputScanRule` (new), `DeltaToMergedIndexDeltaScanRule`
    (updated to pass through sourceIndex/scanGroup), `LogicalTableScanToMergedIndexRule` (new).
  - `convertToPhysicalMaintenancePlan()`: three passes — (1) HEP: `PipelineOutputScanRule`
    converts placeholders to `EnumerableMergedIndexScan`, (2) HEP: `DeltaToMergedIndexDeltaScanRule`
    folds `LogicalDelta(scan)` to `EnumerableMergedIndexDeltaScan`, (3) Volcano: standard
    `ENUMERABLE_*` rules convert remaining LogicalJoin, LogicalUnion, LogicalProject,
    LogicalFilter to enumerable counterparts. Volcano reuses the Phase 1 planner extracted
    from `afterPass2.getCluster().getPlanner()` — HEP nodes share the original cluster.
  - Key insight: MI stores output of each source pipeline. Both full-scan and delta
    branches use pipeline output type; difference is which rows flow, not the type.
  - Delta and full scans share the same `MergedIndexScanGroup` within a pipeline —
    all sources in one pipeline are interleaved in a single MI, so one group covers all.
  - Applied to Q12, Q3-OL, Q9 in `MergedIndexTpchPlanTest`. All operators fully enumerable.
- **Documentation**: stable reference content migrated to `CLAUDE.md`; cost model
  Javadoc expanded on `EnumerableMergedIndexScan` and `MergedIndexScanGroup`.
- **MaintenancePlanConverter** (2026-04-02): extracted `deriveMaintenancePlan`, `scopeLogicalRoot`,
  `replaceChildBoundaries`, `convertToPhysical`, and `IVM_RULES` from test to
  `core/src/main/java/org/apache/calcite/materialize/MaintenancePlanConverter.java`. Pure code
  motion, no logic changes.

## Next Steps

### Short-term (next session)

1. ~~**Logical → Physical maintenance plan conversion (leaf scans)**~~ — **Done** ✓
   Leaf scans converted to `EnumerableMergedIndexScan` / `EnumerableMergedIndexDeltaScan`
   via two HEP passes in `convertToPhysicalMaintenancePlan()`. Applied to Q12, Q3-OL, Q9.

2. ~~**Convert remaining logical operators to enumerable**~~ — **Done** ✓
   Pass 3 Volcano in `convertToPhysicalMaintenancePlan()` converts LogicalJoin, LogicalUnion,
   LogicalProject, LogicalFilter to `EnumerableMergeJoin`, `EnumerableUnion`, etc.
   Uses Phase 1 VolcanoPlanner extracted from `afterPass2.getCluster().getPlanner()`.
   All 3 tests pass; physical maintenance plans are now fully enumerable.

3. ~~**Move `convertToPhysicalMaintenancePlan` to production code**~~ — **Done** ✓
   Extracted to `core/materialize/MaintenancePlanConverter.java`. Contains `deriveMaintenancePlan`,
   `scopeLogicalRoot`, `replaceChildBoundaries`, `convertToPhysical`, and `IVM_RULES`.

### Medium-term

- ~~Maintenance-time assembly operator design (how the executor uses the physical plan)~~ (obsolete along with [option B](./TRASH-option-b.md))
- Additional TPC-H queries (Q5, Q7, Q10) for more operator combinations

### Long-term

- Window functions, DISTINCT, set operators in sort-based pipelines
- End-to-end execution prototype
- Functional dependency-based index matching
- ~~PATH B: native merged index support (tables report collation via `getStatistic()`)~~ (Not necessary for story telling in research paper.)
