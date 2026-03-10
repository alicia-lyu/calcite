# Session Progress: Merged Index Feature

## Status

| Item                                                                    | Status  |
|-------------------------------------------------------------------------|---------|
| `CLAUDE.md` + `main.md`                                                 | Done    |
| `core/.../materialize/MergedIndex.java` (+ `sources` field, `of()` factory) | Done |
| `core/.../materialize/MergedIndexRegistry.java` (`findFor(List<Object>, ...)`) | Done |
| `core/.../adapter/enumerable/EnumerableMergedIndexScan.java`            | Done    |
| `core/.../adapter/enumerable/EnumerableMergedIndexJoin.java` (NEW)      | Done    |
| `core/.../adapter/enumerable/PipelineToMergedIndexScanRule.java` (outer pipeline support) | Done |
| `core/.../adapter/enumerable/EnumerableRules.java` (constant)           | Done    |
| `core/.../adapter/enumerable/PipelineToMergedIndexScanRuleTest.java`    | Done ✓  |
| TPC-H Q3 (CUSTOMER ⋈ ORDERS ⋈ LINEITEM, partial substitution)          | Done ✓      |
| TPC-H Q12 (2-table: ORDERS ⋈ LINEITEM, full substitution)              | Done ✓      |
| TPC-H Q3-OL full 3-table substitution — `tpchQ3OrdersLineitem()`        | Done ✓      |
| TPC-H Q9 (6-table, all leaf joins substituted)                          | Done ✓      |

## Commands

```bash
# Run TPC-H plan tests
./gradlew :plus:cleanTest :plus:test --tests "*.MergedIndexTpchPlanTest" --info

# Run core rule test
./gradlew :core:test --tests "*.PipelineToMergedIndexScanRuleTest"
```

Search for `=== Q3 BEFORE`, `=== Q3 AFTER`, `=== Q12 BEFORE`, `=== Q12 AFTER` in output.

### Sample AFTER output

**Q3** (partial substitution — inner join replaced, outer join remains):

```
EnumerableLimitSort(...)
  EnumerableAggregate(...)
    EnumerableMergeJoin(condition=[=($8, $17)])          ← outer join REMAINS
      EnumerableSort(sort0=[$8])
        EnumerableMergedIndexScan(
          tables=[[[TPCH, CUSTOMER]:C_CUSTKEY, [TPCH, ORDERS]:O_CUSTKEY]],
          collation=[[0]])
      EnumerableSort(sort0=[$0])
        EnumerableTableScan(table=[[TPCH, LINEITEM]])
```

**Q12** (full substitution):

```
EnumerableSort(...)
  EnumerableAggregate(...)
    EnumerableMergedIndexScan(
      tables=[[[TPCH, ORDERS]:O_ORDERKEY, [TPCH, LINEITEM]:L_ORDERKEY]],
      collation=[[0]])
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

### Incorrect example: Q3 — 3-table partial substitution (`tpchQ3`)

Key: `c_custkey = o_custkey` (leaf replaced), `o_orderkey = l_orderkey` (outer, stays at query time). One HEP pass.

```text
BEFORE                                     AFTER (Query plan only)
EnumerableLimitSort                        EnumerableLimitSort
  EnumerableAggregate                        EnumerableAggregate
    EnumerableMergeJoin(orderkey) ←outer       EnumerableMergeJoin(orderkey) ← REMAINS
      EnumerableSort(orderkey)                   EnumerableSort(orderkey)
        EnumerableMergeJoin(custkey) ←leaf           EnumerableMergedIndexScan
          EnumerableSort → Scan(CUSTOMER)               [CUSTOMER]:C_CUSTKEY
          EnumerableSort → Scan(ORDERS)                 [ORDERS]:O_CUSTKEY
      EnumerableSort → Scan(LINEITEM)            EnumerableSort → Scan(LINEITEM)
```

After: query plan + maintenance plan

The outer join has no registered merged index — it remains in the query-time plan,
so no maintenance plan is generated for it.

```text
QUERY-TIME PLAN (AFTER)                    MAINTENANCE PLAN for MI(CUSTOMER+ORDERS)
EnumerableLimitSort                        On CUSTOMER insert(c_custkey=k):
  EnumerableAggregate                        insert CUSTOMER record at key k
    EnumerableMergeJoin(orderkey) ←stays   On ORDERS insert(o_custkey=k):
      EnumerableSort(orderkey)               insert ORDERS record at key k
        EnumerableMergedIndexScan            into MI(CUSTOMER+ORDERS)
          [CUSTOMER]:C_CUSTKEY
          [ORDERS]:O_CUSTKEY             No maintenance plan for outer join
      EnumerableSort → Scan(LINEITEM)      (LINEITEM ⋈ result stays query-time)
```

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

`MergedIndex.maintenancePlan` stores the incremental IVM plan derived by
`deriveIncrementalPlan(Join)` in `MergedIndexTpchPlanTest`.

### Two-phase maintenance model

A merged index does **not** store a pre-computed join — it stores records from each
source table independently, interleaved by sort key. This distinction drives two
fundamentally different maintenance phases:

#### Phase 1 — base table Δ → inner MI (no join needed)

Each source contributes independently. One base-table insert/delete triggers exactly
one MI record update, with no join against any other source:

- `ORDERS` insert at orderkey=k → insert ORDERS record into MI(OL)\[k\]
- `LINEITEM` insert at orderkey=k → re-aggregate LINEITEM for key k → update Agg
  record in MI(OL)\[k\]

The semi-naive formula `Δ(A ⋈ B) = (Δ(A) ⋈ B) ∪ (A ⋈ Δ(B))` does **not** apply
here. Branch 2 (ORDERS delta) needs no join with Agg(LINEITEM); ORDERS records are
simply inserted into the MI slot for key k. The formula overcounts by joining even
for direct-insertion paths.

#### Phase 2 — inner MI Δ → outer MI (join/propagation required)

When a change in the inner MI must propagate to the outer MI, the key level changes
(e.g., orderkey → custkey). At this level, a join-like lookup IS required: to update
MI(OL+CUSTOMER)\[custkey=c\], the system must correlate the inner MI delta at
(orderkey=k, custkey=c) with the CUSTOMER record at custkey=c. Between levels there
may also be additional operators (aggregation, projection) beyond a simple join.

**The BEFORE plan defines both phases:**

- Phase 1 sources: each sub-pipeline feeding into the join input (e.g., Agg(LINEITEM),
  Sort(ORDERS))
- Phase 2 operator: the join (and any operators) connecting levels

### Current `deriveIncrementalPlan` — approximation

The implementation directly constructs a `LogicalUnion` of two `LogicalJoin` branches,
wrapping one side in `LogicalDelta`:

```text
LogicalUnion
  LogicalJoin(orderkey)
    LogicalDelta(Sort(Agg(LINEITEM)))   ← correct for LINEITEM delta (phase 1, re-agg)
    Sort(ORDERS)
  LogicalJoin(orderkey)
    Sort(Agg(LINEITEM))
    LogicalDelta(Sort(ORDERS))          ← incorrect: ORDERS delta needs NO join
```

Branch 2 is an over-approximation. For phase 1 (ORDERS → inner MI), the correct plan
is just `insert Δ(ORDERS) into MI(OL)` with no join. The current formula is accurate
only for phase 2 (inter-level propagation).

The implementation bypasses `HepPlanner + StreamRules` because
`DeltaJoinTransposeRule.onMatch()` calls `HepRuleCall.transformTo()` which runs
`verifyTypeEquivalence` — this fails because TPC-H schema uses `JavaType(String)` while
the newly created `LogicalJoin`s re-derive their row types as `VARCHAR` (SQL type system).
By constructing the plan directly, we avoid the type mismatch entirely.

### Unresolved gap: full maintenance plan accuracy

The deeper issue is that the entire maintenance model needs to distinguish the two
phases. An accurate maintenance plan for phase 1 would skip the join for direct-
insertion sources and only propagate through each source's own sub-pipeline (Agg,
Sort, etc.). The current `deriveIncrementalPlan` treats all branches as joins, which
is correct only for phase 2.

For nested pipelines (Q3-OL outer, Q9 levels 1-4), the leaf of the maintenance plan
is `LogicalDelta(EnumerableMergedIndexScan)`. No `StreamRule` exists for this node,
so it is left as an unresolved leaf. A new physical operator
`EnumerableMergedIndexDeltaScan` is needed, along with a rule converting
`LogicalDelta(EnumerableMergedIndexScan) → EnumerableMergedIndexDeltaScan`.

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

1. **Fix `deriveIncrementalPlan` for phase-1 accuracy**
   (file: `MergedIndexTpchPlanTest.java`, method `deriveIncrementalPlan`)
   - Branch 2 (`left ⋈ Δ(right)`) is wrong for direct-insert sources — a bare
     `Sort → TableScan` with no aggregation on top needs no join; only emit
     `LogicalDelta(right)`.
   - Fix: inspect the right-hand source; if it is `Sort → TableScan` (no Agg), emit
     `LogicalDelta(right)` alone; if it is `Sort → Agg → Scan`, keep the join branch.
   - Update test assertions: `containsString("LogicalJoin")` will need refinement to
     match the corrected shape.

2. **Generate maintenance plan DOT files**
   (file: `MergedIndexTpchPlanTest.java`)
   - After deriving each `maintenancePlan`, call the existing `writeDot` helper, e.g.,
     `writeDot("q12_maintenance.dot", maintenancePlan)`.
   - Output goes to `plus/test-dot-output/` alongside the existing BEFORE/AFTER query
     plan DOTs.

3. **`EnumerableMergedIndexDeltaScan` operator**
   (new file: `adapter/enumerable/EnumerableMergedIndexDeltaScan.java`; new StreamRule)
   - `deriveIncrementalPlan()` leaves `LogicalDelta(EnumerableMergedIndexScan)` as an
     unresolved leaf for nested pipelines (Q3-OL outer, Q9 levels 1–4).
   - Implement a physical operator analogous to `EnumerableMergedIndexScan` and a rule
     converting `LogicalDelta(EnumerableMergedIndexScan) → EnumerableMergedIndexDeltaScan`.

### Medium Term

- **More TPC-H queries** — Q5, Q7, Q10 have similar multi-table join patterns; add tests
  exercising the outer pipeline substitution path.
- **`extractTestSource` cleanup** — `collectPipelines` in the test directly casts `join.getLeft()`
  to `EnumerableSort` (line 518); add an `unwrap`-style guard in case a future Calcite version
  wraps inputs differently.

### Long Term

1. **PATH B: Native merged index support** — add a second operand pattern to
   `PipelineToMergedIndexScanRule` matching `EnumerableMergeJoin` over bare
   `EnumerableTableScan` nodes that already carry the correct collation trait (no explicit
   `EnumerableSort`). Requires modifying `TpchSchema` to report collations via
   `getStatistic() → Statistics.of(n, keys, collations)`.

2. **Functional dependency metadata** — investigate `RelMdFunctionalDependencies` to
   expose ORDERKEY→CUSTKEY automatically, enabling 3-table merged index recognition
   without manual registration.

3. **JOB (Join Order Benchmark)** — representative JOB queries to show generalization
   beyond TPC-H star schemas.

4. **`implement()` stub** — `EnumerableMergedIndexScan.implement()` returns an empty
   enumerable stub. A real implementation would drive a sequential B-tree scan over
   interleaved records, assembling joins and aggregations on-the-fly.
   Explore hooking into the LeanStore repo.
