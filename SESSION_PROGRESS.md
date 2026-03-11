# Session Progress: Merged Index Feature

## Status

| Item                                                                    | Status  |
|-------------------------------------------------------------------------|---------|
| `CLAUDE.md` + `main.md`                                                 | Done    |
| `core/.../materialize/MergedIndex.java` (+ `sources` field, `of()` factory) | Done |
| `core/.../materialize/MergedIndexRegistry.java` (`findFor(List<Object>, ...)`) | Done |
| `core/.../adapter/enumerable/EnumerableMergedIndexScan.java`            | Done    |
| `core/.../adapter/enumerable/EnumerableMergedIndexJoin.java` (NEW)      | Done    |
| `core/.../adapter/enumerable/PipelineToMergedIndexScanRule.java` (generalized) | Done |
| `core/.../adapter/enumerable/EnumerableRules.java` (constant)           | Done    |
| `core/.../adapter/enumerable/EnumerableMergedIndexDeltaScan.java` (NEW) | Done    |
| `core/.../adapter/enumerable/DeltaToMergedIndexDeltaScanRule.java` (NEW)| Done    |
| `core/.../adapter/enumerable/PipelineToMergedIndexScanRuleTest.java`    | Done ✓  |
| TPC-H Q3 (deleted — incorrect CUSTOMER+ORDERS example)                  | Removed |
| TPC-H Q12 (2-table: ORDERS ⋈ LINEITEM, full substitution)              | Done ✓  |
| TPC-H Q3-OL full 3-table substitution — `tpchQ3OrdersLineitem()`        | Done ✓  |
| TPC-H Q9 (6-table, all leaf joins substituted)                          | Done ✓  |
| Pipeline overhaul: sort-boundary-based discovery                         | Done ✓  |
| Rule generalization: accept SortedAggregate inputs                       | Done ✓  |
| `inputAlreadySorted` direction check fix                                 | Done ✓  |
| `flattenPipelines`: `p.mergedIndex != null` (not source count)           | Done ✓  |

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

### Sample AFTER output

**Q12** (full 2-table substitution):

```
EnumerableSort(...)
  EnumerableAggregate(...)
    EnumerableMergedIndexScan(
      tables=[[[TPCH, ORDERS]:O_ORDERKEY, [TPCH, LINEITEM]:L_ORDERKEY]],
      collation=[[0]])
```

**Q3-OL** (full 3-table substitution — both inner and outer replaced):

```
EnumerableLimitSort(...)
  EnumerableProject(...)
    EnumerableMergedIndexJoin(sources=[[view([0]), [TPCH, CUSTOMER]:C_CUSTKEY]], joinType=[INNER], collation=[[3]])
      EnumerableMergedIndexScan(tables=[[view([0]), [TPCH, CUSTOMER]:C_CUSTKEY]], collation=[[3]])
```

**Q9** (full 6-table substitution):

```
EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])
  EnumerableAggregate(group=[{0, 1}], SUM_PROFIT=[SUM($2)])
    EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
      EnumerableProject(...)
        EnumerableFilter(condition=[LIKE($26, '%green%')])
          EnumerableMergedIndexJoin(...)
            EnumerableMergedIndexScan(...)
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

## Flow Chart B — Merged Index Substitution on Top of Normal Planning

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
    │  walk plan tree     │                           │
    │  extract RelOptTable refs + collation           │
    │  MergedIndexRegistry.register(                  │
    │    new MergedIndex(tables, collation, rc))      │
    └─────────────────────┼──────────────────────────┘
                          │
                          ▼  HEP planner
          [PipelineToMergedIndexScanRule]
          Matches: EnumerableMergeJoin
                     ├─ EnumerableSort → EnumerableTableScan(A)
                     └─ EnumerableSort → EnumerableTableScan(B)
          Checks:  MergedIndexRegistry.findFor(tables, collation) != empty
          Fires:   call.transformTo(EnumerableMergedIndexScan)
                          │
                          ▼
          [Merged Index Plan]
          EnumerableProject
            └─ EnumerableMergedIndexScan
               tables=[A, B], collation=[k ASC]
               (one sequential pass; join assembled on-the-fly)
```

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

### Q12 — 2-table full substitution (`tpchQ12`)

Key: `o_orderkey = l_orderkey`. One HEP pass. One level, no cascade.

```text
BEFORE                                     AFTER (Query plan only)
EnumerableSort                             EnumerableSort
  EnumerableAggregate                        EnumerableAggregate
    EnumerableMergeJoin(orderkey)              EnumerableMergedIndexScan
      EnumerableSort → Scan(ORDERS)              [ORDERS]:O_ORDERKEY
      EnumerableSort → Scan(LINEITEM)            [LINEITEM]:L_ORDERKEY
```

After: query plan + maintenance plan

```text
QUERY-TIME PLAN (AFTER)                    MAINTENANCE PLAN (from BEFORE)
EnumerableSort                             On ORDERS insert(o_orderkey=k):
  EnumerableAggregate                        insert ORDERS record at key k
    EnumerableMergedIndexScan                  into MI(ORDERS+LINEITEM)
      [ORDERS]:O_ORDERKEY                On LINEITEM insert(l_orderkey=k):
      [LINEITEM]:L_ORDERKEY               insert LINEITEM record at key k
                                            into MI(ORDERS+LINEITEM)
```

Maintenance plan structure (the replaced pipeline from BEFORE):
```text
  MergeJoin(orderkey)   ← re-run for delta key k to produce merged index entry
    Sort → Scan(ORDERS)
    Sort → Scan(LINEITEM)
```

---

### Q3-OL — 3-table full substitution (`tpchQ3OrdersLineitem`)

Keys: `l_orderkey = o_orderkey` (inner), `o_custkey = c_custkey` (outer). Two HEP passes. Two-level cascade.

```text
BEFORE                                     AFTER (Query plan only)
EnumerableLimitSort                        EnumerableLimitSort
  EnumerableProject                          EnumerableProject
    EnumerableMergeJoin(custkey) ←outer        EnumerableMergedIndexJoin(custkey, INNER)
      EnumerableSort(custkey)                    EnumerableMergedIndexScan
        EnumerableMergeJoin(orderkey) ←inner       [view(OL)]:O_CUSTKEY
          EnumerableSort                            [CUSTOMER]:C_CUSTKEY
            EnumerableAggregate → Scan(LINEITEM)
          EnumerableSort → Scan(ORDERS)
      EnumerableSort → Scan(CUSTOMER)
```

After: query plan + maintenance plan

```text
QUERY-TIME PLAN (AFTER)                    MAINTENANCE PLANS (from BEFORE)
EnumerableLimitSort                        Level 1 — MI(OL) by orderkey:
  EnumerableProject                          On LINEITEM insert(l_orderkey=k):
    EnumerableMergedIndexJoin(custkey)         re-aggregate LINEITEM for key k,
      EnumerableMergedIndexScan                update MI(OL) at k
        [view(OL)]:O_CUSTKEY               On ORDERS insert(o_orderkey=k):
        [CUSTOMER]:C_CUSTKEY                 insert ORDERS record at k in MI(OL)

                                           Level 2 — MI(OL+CUSTOMER) by custkey:
                                             On MI(OL) delta at (orderkey, custkey=c):
                                               update MI(OL+CUSTOMER) at custkey c
                                             On CUSTOMER insert(c_custkey=c):
                                               insert CUSTOMER record at c
```

Maintenance plan structure (the two replaced pipelines from BEFORE):
```text
  Inner: MergeJoin(orderkey)              Outer: MergeJoin(custkey)
           Sort(Agg(LINEITEM))                     Sort(MI(OL) view)
           Sort → Scan(ORDERS)                     Sort → Scan(CUSTOMER)
```

---

### Q3 (deleted)

The `tpchQ3` test was an incorrect example: it registered a merged index for
CUSTOMER ⋈ ORDERS by custkey, but the natural inner pipeline for TPC-H Q3 is
ORDERS ⋈ LINEITEM by orderkey. The correct 3-table test is `tpchQ3OrdersLineitem`
(Q3-OL), which tests inner (ORDERS+LINEITEM by orderkey) + outer (+CUSTOMER by custkey).

---

### Q9 — 6-table full substitution (`tpchQ9`)

Keys: orderkey → partkey → (partkey,suppkey) → suppkey → nationkey. Five HEP passes.
`findAllPipelines` discovers 5 nested `Pipeline` objects post-order; `MergedIndex.of()`
builds OL → OLP → OLPS → OLPPS → OLPPSS+NATION bottom-up. Five-level cascade.

```text
BEFORE                                     AFTER
EnumerableSort(n_name, o_year DESC)        EnumerableSort(n_name, o_year DESC) ← ORDER BY only
  EnumerableAggregate(n_name, o_year)        EnumerableAggregate(n_name, o_year)
    EnumerableFilter(p_name LIKE ...)          EnumerableProject
      EnumerableMergeJoin(nationkey)             EnumerableFilter(p_name LIKE ...)
        EnumerableSort                             EnumerableMergedIndexJoin(nationkey, INNER)
          EnumerableMergeJoin(suppkey)               EnumerableMergedIndexScan
            EnumerableSort                             [view(OLPPS)]:N_NATIONKEY
              EnumerableMergeJoin(partkey,suppkey)     [NATION]:N_NATIONKEY
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
QUERY-TIME PLAN (AFTER)                    MAINTENANCE PLANS (5 levels from BEFORE)
EnumerableSort(n_name, o_year DESC)        L1 OL(orderkey):   ORDERS/LINEITEM delta
  EnumerableAggregate(n_name, o_year)      L2 OLP(partkey):   OL/PART delta
    EnumerableProject                      L3 OLPS(pk,sk):    OLP/PARTSUPP delta
      EnumerableFilter(p_name LIKE ...)    L4 OLPPS(suppkey): OLPS/SUPPLIER delta
        EnumerableMergedIndexJoin          L5 final(natkey):  OLPPS/NATION delta
          EnumerableMergedIndexScan
            [view(OLPPS)]:S_NATIONKEY      Each level: 1 delta in → 1 MI entry updated
            [NATION]:N_NATIONKEY           Cascade depth = 5 for a LINEITEM base change
```

Note: `EnumerableFilter(p_name LIKE '%green%')` remains because PART is absorbed into
the merged index but the filter cannot be pushed below the assembled join result.

---

## Maintenance Plan Generation (implemented 2026-03-10)

Obsolete.

`MergedIndex.maintenancePlan` stores the incremental IVM plan derived by
`deriveIncrementalPlan(Join)` in `MergedIndexTpchPlanTest`.

### Two-phase maintenance model

A merged index does **not** store a pre-computed join — it stores records from each
source table independently, interleaved by sort key. This distinction drives two
fundamentally different maintenance phases:

#### Phase 1 — base table Δ → inner MI (no join needed)

Each source contributes independently. One base-table insert/delete triggers exactly
one MI record update, with no join against any other source. For example, in Q3-OL:

- `ORDERS` insert at orderkey=k → insert ORDERS record into MI(OL)\[k\]
- `LINEITEM` insert at orderkey=k → re-aggregate LINEITEM for key k → update Agg
  record in MI(OL)\[k\]

The semi-naive formula `Δ(A ⋈ B) = (Δ(A) ⋈ B) ∪ (A ⋈ Δ(B))` does **not** apply
here. Branch 2 (ORDERS delta) needs no join with Agg(LINEITEM); ORDERS records are
simply inserted into the MI slot for key k. The formula overcounts by joining even
for direct-insertion paths.

#### Phase 2 — inner MI Δ → outer MI (join/propagation required)

When a change in the inner MI must propagate to the outer MI, the key level changes
(e.g., orderkey → custkey). Before this step, the inner MI just stores multiple types of records, now we need to assemble them together. It usually involves a join but may also involve additional operators, as defined by the pipeline for the current merged index.
For example, in Q3-OL,
when a lineitem insertion triggers an additional joined record, we need to produce it and insert it directly into the outer MI.
Note that a join-like lookup in the outer merged index is still NOT required.

**The BEFORE plan defines both phases:**

- Phase 1 updates leaf merged indexes that correspond to leaf pipelines.
- Phase 2 updates non-leaf merged indexes that correspond to inner pipelines.

### Current `deriveIncrementalPlan` — union of independent deltas (2026-03-10)

A merged index stores records from each source **independently**, interleaved by
sort key. No join between sources is needed at maintenance time — each source inserts
its records directly at the appropriate sort key. The maintenance plan is therefore:

```text
LogicalUnion(all=true)
  LogicalDelta(sortedInputs[0])    ← new records from source 0
  LogicalDelta(sortedInputs[1])    ← new records from source 1
  ...
```

For nested pipelines (Q3-OL outer, Q9 levels 1–4), the left sorted input wraps
the entire inner pipeline (e.g., `Sort(inner_join_result)`). `LogicalDelta` over
this node means "run the inner pipeline for changed keys and emit the assembled
delta" — the Phase 2 propagation is defined by the inner pipeline's own operators.
No additional join node is added at the outer MI level.

The implementation bypasses `HepPlanner + StreamRules` because
`DeltaJoinTransposeRule.onMatch()` calls `HepRuleCall.transformTo()` which runs
`verifyTypeEquivalence` — this fails because TPC-H schema uses `JavaType(String)` while
the newly created `LogicalJoin`s re-derive their row types as `VARCHAR` (SQL type system).
By constructing the plan directly, we avoid the type mismatch entirely.

### `EnumerableMergedIndexDeltaScan` and `DeltaToMergedIndexDeltaScanRule` (2026-03-10)

`EnumerableMergedIndexDeltaScan` — physical delta-scan operator analogous to
`EnumerableMergedIndexScan`, slightly higher cost. Registered as
`ENUMERABLE_DELTA_TO_MERGED_INDEX_DELTA_SCAN_RULE` in `EnumerableRules` (opt-in).

`DeltaToMergedIndexDeltaScanRule` matches `LogicalDelta(EnumerableMergedIndexScan)`
and replaces it with `EnumerableMergedIndexDeltaScan`. Tested in `tpchQ3OrdersLineitem`
by constructing a synthetic `LogicalDelta(innerScan)` and verifying the rule fires.

### Tag-based lazy propagation (future design)

Each merged-index record carries a 1-byte `propagated` flag. On base-table insert:

1. Insert into MI with `propagated=false`.
2. Background worker finds untagged records, runs the phase-1 source pipeline for
   that key (re-agg LINEITEM, etc.), propagates delta to next-level MI via phase-2
   join lookup.
3. Mark source record as `propagated=true`.

This avoids storing full delta records and keeps update cost O(1) amortized per
cascade level.

---

## Next Steps

### Short Term (next session)

1. ~~**MergedIndex ↔ Pipeline deduplication**~~ **DONE** (commit `33779964c`)

2. **Subtask 0: Assembly subtree identification** (files: `Pipeline.java`, test in `MergedIndexTpchPlanTest.java`)
   - Implement `findAssemblySubtree(Pipeline)` — LCA-based algorithm to find the minimal
     connected subgraph of operators between pipeline root and boundary Sorts.
   - Add test validation: Q12 → subtree = {MergeJoin}; Q3-OL → subtree = {MergeJoin} per level.
   - See `overhaul-03-10.md` §Subtask 0 for algorithm pseudo-code and examples.

### Following Sessions

3. **Subtask 1: Tagged interleaved row type** (new file or in `EnumerableMergedIndexScan.java`)
   - Define how the raw scan outputs tagged records from heterogeneous source tables.
   - Evaluate wide union vs. generic tagged row vs. per-source typed enumerables.

4. **Subtask 3: `EnumerableMergedIndexAssemble` operator** (new file: `EnumerableMergedIndexAssemble.java`)
   - Implement Algorithm 1 (N-way inner join: buffer per source, Cartesian product on key change).
   - Assembly strategy parameterized by absorbed operator types from Subtask 0.

5. **Subtask 2: `EnumerableMergedIndexScan.implement()`** (file: `EnumerableMergedIndexScan.java`)
   - Produce interleaved tagged stream from source enumerables merged by sort key.

6. **Subtask 4: Update `PipelineToMergedIndexScanRule`** (file: `PipelineToMergedIndexScanRule.java`)
   - Rule produces `Assemble(Scan)` replacing the Assembly subtree.
   - `EnumerableMergedIndexJoin` may become unnecessary.

7. **Subtask 5: Index creation mode** (file: `Pipeline.java`)
   - `physicalPlan` field on Pipeline; after HEP substitution, extract and store subtree.
   - End-to-end: `while (hasNext) { parentMI.add(physicalPlan.next()) }`.

8. **End-to-end test with actual row production** — Q12, Q3-OL, Q9.

### Medium Term

1. **Direction-agnostic sort injection** (file: `MergedIndexTpchPlanTest.java`,
   method `injectSortsBeforeSortBasedOps`)
   - Sort-based operators (Aggregate GROUP BY, Join) don't inherently require ASC or DESC.
     Currently `injectSortsBeforeSortBasedOps` always creates ASC sorts
     (`new RelFieldCollation(idx)` defaults to ASC).
   - Future: look downstream at parent operator's required direction and proactively match.
   - Concrete example: Q9's GROUP BY creates `Sort(n_name ASC, o_year ASC)` but ORDER BY
     needs `(n_name ASC, o_year DESC)`. With direction propagation, the injected sort would
     use DESC for o_year, eliminating the redundant post-aggregate sort.

2. **`extractCollation` specificity** (file: `PipelineToMergedIndexScanRule.java`)
   - When both MergeJoin inputs have collations, choose the most specific one. Both sides
     are compatible but not necessarily identical.

3. **Additional TPC-H queries** — show all order-based query plans can use merged indexes.
   - Q5: CUSTOMER ⋈ ORDERS ⋈ LINEITEM ⋈ SUPPLIER ⋈ NATION ⋈ REGION — hierarchical keys.
   - Q6: single-table aggregate, no join — baseline showing no MI applies.

4. **Realistic cost model** — explore Calcite's cost model for index/MV access, adapt
   for merged index access and full query+maintenance plan costs.

### Long Term

1. **PATH B: Native merged index support** — `PipelineToMergedIndexScanRule` matching
   bare `EnumerableTableScan` with collation traits (no explicit `EnumerableSort`).

2. **Functional dependency metadata** — `RelMdFunctionalDependencies` for automatic
   ORDERKEY→CUSTKEY recognition.

3. **JOB (Join Order Benchmark)** — generalization beyond TPC-H.

4. **`implement()` stub** — real sequential B-tree scan implementation; LeanStore integration.
