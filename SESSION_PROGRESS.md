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

After `propagateOrderByDirection`, both ORDER BY and GROUP BY sorts have
(n_name ASC, o_year DESC). Both are boundary sorts → two indexed view
levels. The final MIScan absorbs all 5 joins + filter + aggregate.

---

## Calcite Planner Architecture: Volcano vs HEP

Calcite has **two independent planner engines**. Understanding both is essential for
this project because we use them in sequence.

### Volcano (Phase 1) — Cost-Based Optimizer

The classic Cascade/Volcano framework:
- Explores equivalent plans via **rules** that fire on pattern matches
- Maintains a **memo** of equivalent expressions grouped by properties (traits like
  sort order, convention/physical-operator-set)
- Multiple rules can fire on the same node, producing **alternatives**
- The planner picks the **cheapest** plan using the cost model

**In our pipeline**: Phase 1 uses Volcano to convert the logical plan (LogicalJoin,
LogicalTableScan, etc.) to a physical plan (EnumerableMergeJoin, EnumerableSort,
EnumerableTableScan, etc.). It chooses between hash join vs merge join, hash aggregate
vs sorted aggregate, etc. — all based on cost.

### HEP (Phase 2) — Heuristic/Deterministic Rewriter

A simpler, **deterministic rewrite engine** (no cost comparison):
- Applies rules as **unconditional rewrites** — if a rule matches, the replacement happens
- Processes the plan in a configurable traversal order:
  - `BOTTOM_UP`: leaves first, then ancestors (what we use — inner Sorts fire before outer)
  - `TOP_DOWN`: root first, then descendants
  - `DEPTH_FIRST`: avoids re-processing after each transformation
  - `ARBITRARY`: default, efficient, order doesn't matter
- Think of HEP as **"find-and-replace for plan trees"**

**In our pipeline**: Phase 2 uses HEP to apply `PipelineToMergedIndexScanRule`. This is
a deterministic rewrite — if a Sort's input matches a registered MI, replace it. No cost
comparison needed (MI scan is always preferred). HEP is ideal because we want unconditional
substitution and need control over traversal order.

### What Is a "Rule"?

A **rule** is a pattern-match-and-transform unit with two parts:
1. **Operand pattern**: What plan node to match (e.g., `EnumerableSort` with no FETCH/OFFSET)
2. **`onMatch(call)`**: Inspect the node, optionally call `call.transformTo(replacement)`

In Volcano, multiple rules can fire on the same node → produces alternatives → cost picks
winner. In HEP, rules fire and replace unconditionally.

### Why BOTTOM_UP Matters for Nested Pipelines

For nested pipelines (Q3-OL, Q9), inner boundary Sorts must be replaced by MIScans
**before** outer Sorts fire. Otherwise, the outer Sort can't recognize its inner subtree
as an MI view. `BOTTOM_UP` guarantees this ordering:

```text
Step 1: Inner Sort(orderkey) over ORDERS    → MIScan(innerMI, 0)  ← leaf, fires first
Step 2: Inner Sort(orderkey) over LINEITEM  → MIScan(innerMI, 1)  ← leaf, fires second
Step 3: Outer Sort(custkey) over inner result → sees MIScans → recognizes innerMI → MIScan(outerMI, 0)
Step 4: Outer Sort(custkey) over CUSTOMER   → MIScan(outerMI, 1)
```

Previously we used **N sequential HepPlanner instances** (one per nesting level) to
achieve this ordering. With `BOTTOM_UP`, a single HEP pass suffices.

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
EnumerableSort(n_name ASC, o_year DESC)        ← ORDER BY (boundary)
  EnumerableAggregate(n_name, o_year)
    EnumerableSort(n_name ASC, o_year DESC)    ← GROUP BY (boundary, direction fixed)
      EnumerableProject → Filter → 5 nested MergeJoins...
```

```text
AFTER — Entire plan collapses to a single scan
MIScan(ivMI)
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

## Next Steps

### Completed

1. ~~**MergedIndex ↔ Pipeline deduplication**~~ **DONE** (commit `33779964c`)
2. ~~**Subtask 0: Assembly subtree identification**~~ **DONE** (commit `f8c8981f8`) — moved to test utils
3. ~~**Subtask 1: Tagged interleaved row type**~~ **DONE** (commit `c3a048e11`)
   - `TaggedRowSchema` in `materialize/TaggedRowSchema.java`.
   - Wired into `MergedIndex.getTaggedRowSchema()`.
4. ~~**Test helpers extraction**~~ **DONE** (commit `cbd4908bb`)
   - `MergedIndexTestUtil` in `testkit/`.

### Short Term (completed) — Transparent Per-Source MI Scans

1. ~~**Subtask 0 (revised): Per-source MI scan operator**~~ **DONE** (commit `84f1589a4`)
   - `EnumerableMergedIndexScan`: added `sourceIndex`, `scanGroup`, source-native row type.
   - New `MergedIndexScanGroup` class in `materialize/`.
   - Old `create(cluster, mi, rowType)` kept as deprecated overload.
   - `implement()`: stub returning empty enumerable (execution out of scope).

2. ~~**Subtask 1 (revised): PipelineToMergedIndexScanRule — Sort boundary matching**~~ **DONE** (commits `69911832f`, `82c5ffbec`)
   - Rule rewritten to match `EnumerableSort` (not `EnumerableMergeJoin`).
   - `findForSource()`: structural matching (leaf table name + mergedIndex identity),
     per-source boundary collation, HepRelVertex-aware traversal.
   - Incremental MI registration: one level per HEP pass (leaf-to-root).
   - Scan groups discovered via plan tree search for sibling MIScans.
   - `EnumerableMergedIndexJoin` deleted (parent MergeJoin stays in plan).
   - `@Execution(SAME_THREAD)` on core tests (registry race condition fix).

### Short Term (next session)

1. **Subtask 2: Index creation plan capture** (file: `Pipeline.java`, `MergedIndex.java`)
   - After each non-root HEP pass, the subtree rooted at `pipeline.root` has
     MIScan leaves — this IS the index creation plan.
   - Store as `indexCreationPlan` field on MergedIndex.
   - Extract from post-HEP plan in the multi-stage loop.

2. **Subtask 4: Cost model with scan group sharing** (file: `EnumerableMergedIndexScan.java`)
   - N leaf scans sharing one physical scan: combined IO = one sequential scan, not N.
   - `MergedIndexScanGroup` enables cost sharing.

3. **Remove deprecated `create(cluster, mi, rowType)`** if no callers remain outside delta tests.

4. **End-to-end test with actual row production** — Q12, Q3-OL, Q9.

5. **Maintenance plans for indexed views** — single-source pipelines need
   a different maintenance strategy (single delta branch, not union of two).
   Currently skipped (no `setMaintenancePlan` for indexed view pipelines).

### Medium Term

1. **`extractCollation` specificity** — choose most specific collation when both MergeJoin
   inputs have collations.

2. **Additional TPC-H queries** — Q5 (hierarchical keys), Q6 (baseline, no MI).

3. **Realistic cost model** — adapt Calcite's cost model for merged index access.

### Long Term

1. **PATH B: Native merged index support** — tables report collation via `getStatistic()`,
   rule matches bare `EnumerableTableScan` with collation traits.

2. **Functional dependency metadata** — `RelMdFunctionalDependencies` for automatic
   ORDERKEY→CUSTKEY recognition.

3. **JOB (Join Order Benchmark)** — generalization beyond TPC-H.

4. **`implement()` stub** — real sequential B-tree scan; LeanStore integration.
