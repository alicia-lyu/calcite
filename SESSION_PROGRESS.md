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

## Maintenance & Index Creation

- **Index creation plan**: For non-root pipelines, stored as `pipeline.indexCreationPlan` (the BEFORE physical plan).
- **Maintenance plan**: Logical-only so far — `deriveMaintenancePlan()` produces a `LogicalDelta`-wrapped plan per pipeline. Physical conversion is the next step (see Next Steps).

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
- **Documentation**: stable reference content migrated to `CLAUDE.md`; cost model
  Javadoc expanded on `EnumerableMergedIndexScan` and `MergedIndexScanGroup`.

## Next Steps

### Short-term (next session)

1. **Logical → Physical maintenance plan conversion** — Convert the logical
   maintenance plan (from `deriveMaintenancePlan`) into a physical (enumerable)
   plan that can actually execute.

   **Current state:** Each non-root pipeline has a logical maintenance plan containing:
   - `LogicalDelta(LogicalTableScan(T))` for each base table T in the pipeline
   - `LogicalValues.createEmpty` placeholders where child pipeline views were scoped out
   - Standard logical operators (LogicalJoin, LogicalProject, LogicalAggregate) above

   **The key challenge — merged index delta representation:**
   A merged index stores rows from multiple tables interleaved by a shared key.
   When a row changes in source table A, the delta must flow through the assembly
   pipeline (joins with other sources B, C, …) to produce the output delta for the
   parent index. The physical plan needs operators that:
   - Scan the *unchanged* sources from the existing merged index (not full table scans)
   - Process the *delta* from the changed source
   - Assemble the output delta via the pipeline's join/aggregate operators

   This is distinct from simple `DeltaTableScan` (single-table change stream) because
   the "unchanged" side of each join reads from the *merged index* (pre-sorted,
   co-located), not from independent table scans.

   **Approach options:**
   - (a) New `EnumerableDeltaMergedIndexScan` that reads unchanged sources from MI
   - (b) Reuse Calcite's stream infrastructure (`Chi` / `StreamRules`) with MI-aware physical rules
   - (c) Volcano conversion pass with delta-aware cost model

   **Files:** `MergedIndexTpchPlanTest.java` (test harness), `Pipeline.java`
   (`scopeLogicalRoot`, `captureLogicalRoots`), possibly new physical nodes in
   `adapter/enumerable/`. Reference: `deriveMaintenancePlan()` in test file.

### Medium-term

2. Maintenance-time assembly operator design (how the executor uses the physical plan)
3. Additional TPC-H queries (Q5, Q7, Q10) for more operator combinations
4. PATH B: native merged index support (tables report collation via `getStatistic()`)

### Long-term

5. Window functions, DISTINCT, set operators in sort-based pipelines
6. End-to-end execution prototype
7. Functional dependency-based index matching
