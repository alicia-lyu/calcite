# Session Progress: Merged Index Feature

## Status

| Item                                                                    | Status  |
|-------------------------------------------------------------------------|---------|
| `CLAUDE.md` + `main.md`                                                 | Done    |
| `core/.../materialize/MergedIndex.java` (+ `sources` field, `of()` factory) | Done |
| `core/.../materialize/MergedIndexRegistry.java` (`findFor(List<Object>, ...)`) | Done |
| `core/.../adapter/enumerable/EnumerableMergedIndexScan.java`            | Done    |
| `core/.../adapter/enumerable/EnumerableMergedIndexJoin.java` (NEW)      | Obsolete (Option B) |
| `core/.../adapter/enumerable/PipelineToMergedIndexScanRule.java` (generalized) | Done |
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

### Sample AFTER output (Transparent Per-Source MI Scans)

**Q12** (2-table, single root pipeline — MergeJoin stays):

```text
EnumerableSort(l_shipmode)
  EnumerableSortedAggregate(...)
    EnumerableMergeJoin(orderkey)           ← STAYS
      MIScan(MI, src=ORDERS, group=G1)     ← replaces Sort→Scan(ORDERS)
      MIScan(MI, src=LINEITEM, group=G1)   ← replaces Sort→Scan(LINEITEM)
```

**Q3-OL** root query plan (outer pipeline only — inner is index creation plan):

```text
EnumerableLimitSort
  EnumerableProject
    EnumerableMergeJoin(custkey)                  ← STAYS
      MIScan(MI_outer, src=inner_view, group=G2)  ← replaces Sort→(inner result)
      MIScan(MI_outer, src=CUSTOMER, group=G2)    ← replaces Sort→Scan(CUSTOMER)
```

**Q9** root query plan (outer pipeline only):

```text
EnumerableSort(n_name, o_year DESC)
  EnumerableAggregate(n_name, o_year)
    EnumerableProject
      EnumerableFilter(p_name LIKE ...)
        EnumerableMergeJoin(nationkey)                    ← STAYS
          MIScan(MI_outer, src=inner_view, group=G5)
          MIScan(MI_outer, src=NATION, group=G5)
```

---

## Flow Chart A — Calcite's Normal Planning Workflow

```text
SQL String
    │
    ▼
[SqlParser]                         → SqlNode  (AST)
    │
    ▼
[SqlValidator]                      → SqlNode  (type-annotated)
    │
    ▼
[SqlToRelConverter]                 → LogicalPlan  (convention = NONE)
    │                                 e.g.  LogicalProject
    │                                         └─ LogicalJoin
    │                                              ├─ LogicalTableScan(A)
    │                                              └─ LogicalTableScan(B)
    │
    ▼  Volcano planner (EnumerableRules)
[Phase 1: logical → physical]       → Physical Pipeline  (convention = ENUMERABLE)
    ENUMERABLE_TABLE_SCAN_RULE          e.g.  EnumerableProject
    ENUMERABLE_MERGE_JOIN_RULE                  └─ EnumerableMergeJoin
    ENUMERABLE_SORT_RULE                             ├─ EnumerableSort
    ENUMERABLE_PROJECT_RULE                          │    └─ EnumerableTableScan(A)
                                                     └─ EnumerableSort
                                                          └─ EnumerableTableScan(B)
    │
    ▼  (code generation via Janino)
[EnumerableRel.implement()]         → Java bytecode → execution
```

---

## Flow Chart B — Merged Index Substitution (Transparent Per-Source MI Scans)

```text
          ┌─────────────────────────────────────────────┐
          │  (same physical pipeline from Flow A)       │
          │   EnumerableMergeJoin                       │
          │     ├─ EnumerableSort → EnumerableTableScan │
          │     └─ EnumerableSort → EnumerableTableScan │
          └───────────────┬─────────────────────────────┘
                          │
    ┌─────────────────────┼──────────────────────────┐
    │  Between phases:    │                           │
    │  walk plan tree, discover pipelines             │
    │  register MergedIndex per pipeline              │
    │  create MergedIndexScanGroup per pipeline       │
    └─────────────────────┼──────────────────────────┘
                          │
                          ▼  HEP planner
          [PipelineToMergedIndexScanRule]
          Matches: EnumerableSort at pipeline boundary
          Checks:  Sort's input is part of a registered MI
          Fires:   Sort(input_chain) → MIScan(MI, srcIdx, group)
                          │
                          ▼
          [Merged Index Plan — operators stay, Sorts replaced]
          EnumerableMergeJoin                    ← STAYS
            ├─ MIScan(MI, src=A, group=G1)      ← replaces Sort→Scan(A)
            └─ MIScan(MI, src=B, group=G1)      ← replaces Sort→Scan(B)
               G1 = shared physical scan object
               (one sequential scan; MergeJoin assembles on-the-fly)
```

**Pipeline categories:**

|              | Root pipeline | Other pipelines      |
|--------------|---------------|----------------------|
| Data flow    | Query plan    | Index creation plan  |
| Delta flow   | N/A           | Maintenance plan     |

---

## Flow Chart C — Merged Index Concept (Why It Matters)

```
TRADITIONAL QUERY EXECUTION                  WITH MERGED INDEX
────────────────────────────────────         ──────────────────────────────────
At query time:                               At update time (like a B-tree):
  Scan(ORDERS)  ──sort(orderkey)──┐            Insert into merged index:
  Scan(LINEITEM)──sort(orderkey)──┴►MergeJoin  k=1, tag=ORDERS,  row=(...)
                                               k=1, tag=LINEITEM, row=(...)
  Cost: O(N log N) sorts + O(N) join           k=2, tag=ORDERS,  row=(...)
                                               k=2, tag=LINEITEM, row=(...)

                                               At query time:
                                               Scan merged index (one pass)
                                               → assemble join on the fly

                                               Cost: O(N) sequential read only
                                               Space: same as two separate indexes
                                               Update: 1 base insert → 1 index insert
```

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

### Q12 — 2-table (`tpchQ12`)

Key: `o_orderkey = l_orderkey`. Single root pipeline. One HEP pass.

```text
BEFORE                                     AFTER (Query plan — MergeJoin stays)
EnumerableSort(l_shipmode)                 EnumerableSort(l_shipmode)
  EnumerableSortedAggregate                  EnumerableSortedAggregate
    EnumerableMergeJoin(orderkey)              EnumerableMergeJoin(orderkey)  ← STAYS
      EnumerableSort → Scan(ORDERS)              MIScan(MI, src=ORDERS, G1)
      EnumerableSort → Scan(LINEITEM)            MIScan(MI, src=LINEITEM, G1)
```

Index creation: N/A (root pipeline — no MI to populate, sources read directly).

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

### Q9 — 6-table (`tpchQ9`)

Keys: orderkey → partkey → (partkey,suppkey) → suppkey → nationkey.
5 pipelines, 4 inner (index creation) + 1 root (query plan).

```text
BEFORE (full plan)
EnumerableSort(n_name, o_year DESC)
  EnumerableAggregate(n_name, o_year)
    EnumerableFilter(p_name LIKE ...)
      EnumerableMergeJoin(nationkey)         ← root pipeline
        EnumerableSort
          EnumerableMergeJoin(suppkey)
            EnumerableSort
              EnumerableMergeJoin(partkey,suppkey)
                EnumerableSort → Scan(PARTSUPP)
                EnumerableSort
                  EnumerableMergeJoin(partkey)
                    EnumerableSort → Scan(PART)
                    EnumerableSort
                      EnumerableMergeJoin(orderkey)
                        EnumerableSort → Scan(ORDERS)
                        EnumerableSort → Scan(LINEITEM)
            EnumerableSort → Scan(SUPPLIER)
        EnumerableSort → Scan(NATION)
```

```text
AFTER — Root query plan (outermost pipeline only)
EnumerableSort(n_name, o_year DESC)
  EnumerableAggregate(n_name, o_year)
    EnumerableProject
      EnumerableFilter(p_name LIKE ...)
        EnumerableMergeJoin(nationkey)                   ← STAYS
          MIScan(MI_outer, src=inner_view, group=G5)
          MIScan(MI_outer, src=NATION, group=G5)
```

Inner pipelines (index creation plans, 4 levels):
- L1: MI(OL) by orderkey — MergeJoin(ORDERS, LINEITEM)
- L2: MI(OLP) by partkey — MergeJoin(OL_view, PART)
- L3: MI(OLPS) by (partkey,suppkey) — MergeJoin(OLP_view, PARTSUPP)
- L4: MI(OLPPS) by suppkey — MergeJoin(OLPS_view, SUPPLIER)

Note: `EnumerableFilter(p_name LIKE '%green%')` stays in root query plan.

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

## Next Steps

### Completed

1. ~~**MergedIndex ↔ Pipeline deduplication**~~ **DONE** (commit `33779964c`)
2. ~~**Subtask 0: Assembly subtree identification**~~ **DONE** (commit `f8c8981f8`) — moved to test utils
3. ~~**Subtask 1: Tagged interleaved row type**~~ **DONE** (commit `c3a048e11`)
   - `TaggedRowSchema` in `materialize/TaggedRowSchema.java`.
   - Wired into `MergedIndex.getTaggedRowSchema()`.
4. ~~**Test helpers extraction**~~ **DONE** (commit `cbd4908bb`)
   - `MergedIndexTestUtil` in `testkit/`.

### Short Term (next session) — Transparent Per-Source MI Scans

1. **Subtask 0 (revised): Per-source MI scan operator** (files: `EnumerableMergedIndexScan.java`, new `MergedIndexScanGroup.java`)
   - Add `sourceIndex` field to `EnumerableMergedIndexScan` — designates which source's row type this scan produces.
   - Create `MergedIndexScanGroup` class — shared meta object referenced by all leaf scans in one assembly subtree.
   - `implement()`: scan MI, filter by source tag, return source-native rows.
   - Collation: MI's shared collation remapped to source's field indices.

2. **Subtask 1 (revised): PipelineToMergedIndexScanRule — Sort boundary matching** (file: `PipelineToMergedIndexScanRule.java`)
   - Rule matches `EnumerableSort` at pipeline boundaries (current implementation should be close).
   - Replace: `Sort(input_chain)` → `MIScan(MI, sourceIndex, scanGroup)`.
   - Parent operators (MergeJoin, SortedAggregate, etc.) remain untouched.

3. **Subtask 3: Update test expectations** (files: `PipelineToMergedIndexScanRuleTest.java`, `MergedIndexTpchPlanTest.java`)
   - AFTER plans: MergeJoin stays, leaf scans replace Sort→TableScan.
   - Assembly subtree validation in `MergedIndexTestUtil`.

### Following Sessions

1. **Subtask 2: Index creation plan** (file: `Pipeline.java`)
   - For non-root pipelines, BEFORE plan = index creation plan.
   - Store as `physicalPlan` field on Pipeline.

2. **Subtask 4: Cost model with scan group sharing** (file: `EnumerableMergedIndexScan.java`)
   - N leaf scans sharing one physical scan: combined IO = one sequential scan, not N.
   - `MergedIndexScanGroup` enables cost sharing.

3. **End-to-end test with actual row production** — Q12, Q3-OL, Q9.

### Medium Term

1. **Direction-agnostic sort injection** — propagate downstream direction requirements
   to eliminate redundant sorts (e.g., Q9 GROUP BY ASC vs ORDER BY DESC).

2. **`extractCollation` specificity** — choose most specific collation when both MergeJoin
   inputs have collations.

3. **Additional TPC-H queries** — Q5 (hierarchical keys), Q6 (baseline, no MI).

4. **Realistic cost model** — adapt Calcite's cost model for merged index access.

### Long Term

1. **PATH B: Native merged index support** — tables report collation via `getStatistic()`,
   rule matches bare `EnumerableTableScan` with collation traits.

2. **Functional dependency metadata** — `RelMdFunctionalDependencies` for automatic
   ORDERKEY→CUSTKEY recognition.

3. **JOB (Join Order Benchmark)** — generalization beyond TPC-H.

4. **`implement()` stub** — real sequential B-tree scan; LeanStore integration.
