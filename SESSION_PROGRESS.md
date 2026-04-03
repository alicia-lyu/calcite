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

See `CLAUDE.md` (§ "Architecture: Sort-Boundary-Based Pipeline Replacement") for the
full design. Key production classes:

- `MaintenancePlanConverter`: `deriveMaintenancePlan`, `scopeLogicalRoot`,
  `replaceChildBoundaries`, `convertToPhysical`, `IVM_RULES`.
- Physical maintenance plan (2-source example):
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

## What Was Done

### 2026-03-10 to 2026-03-25

- Sort-boundary pipeline architecture, `PipelineToMergedIndexScanRule`, TPC-H tests
  (Q12, Q3-OL, Q9), index creation plan capture, incremental maintenance plan
  (`deriveMaintenancePlan` + `SetOp.deriveRowType` fast-path fix).

### 2026-04-01 to 2026-04-02

- `scopeLogicalRoot` fix: ALL child pipelines (leaf + non-leaf) replaced with
  `LogicalPipelineOutputScan` placeholders before delta push-down.
- Physical maintenance plan conversion: `convertToPhysicalMaintenancePlan()` — three
  passes (HEP×2 + Volcano) converts all operators to fully enumerable. Applied to
  Q12, Q3-OL, Q9.
- `MaintenancePlanConverter` extracted to production code
  (`core/materialize/MaintenancePlanConverter.java`). Pure code motion, no logic changes.

### 2026-04-03

- Created `CALCITE_LEANSTORE_INTEGRATION.md`: feasibility study for Calcite→LeanStore
  bridge (plan export + C++ interpreter, 8 milestones, template classification).
- Created `RESEARCH_QUESTIONS.md`: formal execution model for maintenance (B-tree delta
  cache, LSM compaction-as-maintenance, propagation tags), 6 analytically tractable
  efficiency questions (Q1–Q3 tractable from existing plans).

---

## Next Steps

### Short-term (next session)

1. **Treat `LimitSort` as pipeline boundary**: Currently only `EnumerableSort` is
   recognized as a pipeline boundary. `EnumerableLimitSort` (ORDER BY + LIMIT) should
   also split pipelines — it imposes a sort order that a merged index can satisfy.
   Affects `Pipeline.buildTree()` and `PipelineToMergedIndexScanRule`.

2. **Omit projections in colored DOT output**: `EnumerableProject` nodes add visual
   clutter without conveying pipeline structure. Amend the DOT generation method to
   skip projection operators, connecting their inputs directly to their parents.

3. **Hoist variable-predicate filters to root query plan**: Filters with variable
   predicates (e.g., `l_shipdate < ?`, `c_mktsegment = 'BUILDING'`) should NOT be
   absorbed into merged indexes — doing so makes the index unusable for other queries
   with different predicate values. Move such filters above the pipeline boundary so
   they appear only in the root query plan. Ensure the columns needed for predicate
   evaluation are projected through (add projection operators if the pipeline output
   would otherwise drop them).

4. **Additional TPC-H queries** (Q5, Q7, Q10): add to `MergedIndexTpchPlanTest`. Each
   exercises different operator combinations (multi-way join, date range filter,
   aggregation variants). Goal: confirm pipeline discovery generalizes beyond Q9.

### Medium-term

- **End-to-end execution prototype with LeanStore**: see `CALCITE_LEANSTORE_INTEGRATION.md`
  for full 8-milestone plan. M1–M2 (Calcite plan serialization to JSON/protobuf) and
  M3–M5 (hard-coded Q12/Q3-OL execution in C++) can proceed in parallel.
- **Maintenance efficiency analysis**: see `RESEARCH_QUESTIONS.md`. Q1–Q3 (1-to-1 update
  cost, cascade depth, space overhead) are analytically tractable from existing plans
  without new implementation.

### Long-term

- Window functions, DISTINCT, set operators in sort-based pipelines.
- Functional dependency-based index matching (FD: `o_orderkey → o_custkey` enabling
  3-table merged index without manual key chain).
