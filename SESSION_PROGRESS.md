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

## Next Steps

### Short Term (next session)

- **Maintenance plan generation**: explore how Calcite support incremental view maintenance. We will rely on this machinery to generate maintenance plans as shown above. Right now our code only generates query plans. I can think of the following tricky points:
  - The view delta is inserted not into a materialized view strictly speaking, but into a merged index that stores a materialized view.
  - Merged index delta might cascade into the next level of merged index maintenance. Calcite likely only defined base table update triggers, so we may need to define merged index delta triggers.
  - Merge index delta is also unique in that there is typically a step of query processing to generate the delta (e.g., assembling joined records). But without this actual step we should be able to determine the size of the delta. For now, we can always process this step and leave this async triggering for the future (please still explore and write notes about it).
  - To generate delta from merged index A to merged index B, the naive way is, upon insertions into A, we process the required step (e.g., join) and store this delta in a log. The better way is to tag each record in A whether they are propagated to B. Note that, for a joined record to B, as long as one of the participant is not propagated, this joined record will be propagated. With this tag, we only need to keep track of the delta size + sort key instead of the actual delta records in the log. Upon propagation, we process the required step, update the tag, and assemble the delta to B. However, the implementation of the latter might be too complex for this session, so we can start with the naive way, only explore the latter a bit, and write notes about it.

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
