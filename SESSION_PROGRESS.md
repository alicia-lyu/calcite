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

### Q12 — 2-table full substitution (`tpchQ12`)

Key: `o_orderkey = l_orderkey`. One HEP pass.

```text
BEFORE                                     AFTER
EnumerableSort                             EnumerableSort
  EnumerableAggregate                        EnumerableAggregate
    EnumerableMergeJoin(orderkey)              EnumerableMergedIndexScan
      EnumerableSort → Scan(ORDERS)              [ORDERS]:O_ORDERKEY
      EnumerableSort → Scan(LINEITEM)            [LINEITEM]:L_ORDERKEY
```

### Q3 — 3-table partial substitution (`tpchQ3`)

Key: `c_custkey = o_custkey` (leaf), `o_orderkey = l_orderkey` (outer, stays). One HEP pass.

```text
BEFORE                                     AFTER
EnumerableLimitSort                        EnumerableLimitSort
  EnumerableAggregate                        EnumerableAggregate
    EnumerableMergeJoin(orderkey) ←outer       EnumerableMergeJoin(orderkey) ← REMAINS
      EnumerableSort(orderkey)                   EnumerableSort(orderkey)
        EnumerableMergeJoin(custkey) ←leaf           EnumerableMergedIndexScan
          EnumerableSort → Scan(CUSTOMER)               [CUSTOMER]:C_CUSTKEY
          EnumerableSort → Scan(ORDERS)                 [ORDERS]:O_CUSTKEY
      EnumerableSort → Scan(LINEITEM)            EnumerableSort → Scan(LINEITEM)
```

### Q3-OL — 3-table full substitution (`tpchQ3OrdersLineitem`)

Keys: `l_orderkey = o_orderkey` (inner), `o_custkey = c_custkey` (outer). Two HEP passes.

```text
BEFORE                                     AFTER
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

### Q9 — 6-table full substitution (`tpchQ9`)

Keys: orderkey → partkey → (partkey,suppkey) → suppkey → nationkey. Five HEP passes.
`findAllPipelines` discovers 5 nested `Pipeline` objects post-order; `MergedIndex.of()`
builds OL → OLP → OLPS → OLPPS → OLPPSS+NATION bottom-up.

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

Note: `EnumerableFilter(p_name LIKE '%green%')` remains because PART is absorbed into
the merged index but the filter cannot be pushed below the assembled join result.

---

## Next Steps

### Short Term (next session)

- **More TPC-H queries** — Q5, Q7, Q10 have similar multi-table join patterns; add tests
  exercising the outer pipeline substitution path.

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
