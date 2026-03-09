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

## Q9 Reference Plans

CHANGES PENDING, refer to MergedIndexTpchPlanTest.java for up-to-date plans.

---

## Research Notes

### Join Order: Why `tpchQ3OrdersLineitem` Uses a Manually Rewritten SQL

`tpchQ3OrdersLineitem` tests the scenario where ORDERS ⋈ LINEITEM is the leaf join
and CUSTOMER is the outer join — the reverse of Q3. Using the same SQL as Q3
(`FROM customer JOIN orders JOIN lineitem`) always produces the same left-deep tree
`(CUSTOMER ⋈ ORDERS) ⋈ LINEITEM`, so the leaf join is always CUSTOMER ⋈ ORDERS on
`custkey` and the assertion on `O_ORDERKEY` fails.

Calcite's Volcano planner preserves SQL join order because no join-reordering rules
(`JoinCommuteRule`, `JoinAssociateRule`, etc.) are registered. Adding them would not
reliably produce the desired order — with no real statistics at scale 0.01 the planner
picks arbitrarily among equal-cost permutations — and would conflate "what the planner
chose" with "what the test intends to demonstrate". The manual SQL rewrite is therefore
intentional test design, not a workaround.

### Hierarchical Merged Indexes: When They Apply and Why Q3OL Does Not Qualify

**Hierarchical merged indexes** (paper §3.2) store tables with hierarchically structured keys
as one merged index. The prototypical example is geography:

- Nation: key = `(nationkey)`
- State: key = `(nationkey, statekey)` — `statekey` is a LOCAL identifier within a nation
- County: key = `(nationkey, statekey, countykey)` — `countykey` is LOCAL within a state

A single merged index sorted on `(nationkey, statekey, countykey)` satisfies all three merge
join requirements because each shorter key is a **prefix** of the longer one. This works because
`statekey` is scoped within its nation — it is NOT a globally unique surrogate.

**Q3OL does NOT qualify.** `o_orderkey` and `o_custkey` are independent global surrogate keys.
Even though `o_orderkey → o_custkey` (FK), `orderkey` is NOT structured as `(custkey, local_id)`.
A sort by `(custkey, orderkey)` would co-locate ORDERS by customer, but `orderkey` values within
a customer group are arbitrary integers — there is no prefix-chain structure. Therefore:

- The outer join (CUSTOMER ⋈ inner_result on `custkey`) and inner join (LINEITEM ⋈ ORDERS on
  `orderkey`) have **incompatible sort keys** — one cannot be a prefix of the other.
- Q3OL requires **two separate merged indexes**: inner (LINEITEM+ORDERS by `orderkey`) and outer
  (inner_view+CUSTOMER by `custkey`).
- The outer merged index stores the **pre-computed inner join result** + CUSTOMER, sorted by
  `custkey`, built at maintenance time. At query time, only the outer scan is needed.

**FD and collation equivalence**: `sort(orderkey)` ≡ `sort(orderkey, orderdate)` because
`o_orderkey → o_orderdate` (each order has a unique date). `MergedIndex.satisfies()` currently
uses exact prefix matching; FD-based equivalence is deferred as future work.

### Q9 Maintenance Plan

The Q9 merged-index DOT shows two tiers:

- **Query tier**: only the final `Agg` (one sequential scan of `MgIdxGroup`)
- **Maintenance tier**: all dashed-arrow resorts — index creation and incremental update

Current implementation covers query plans only. Maintenance plan work would model
each dashed edge as an incremental-update rule triggered by base-table inserts/deletes.

---

## Next Steps

### Short Term (next session)


### Medium Term

- Add 2–3 more TPC-H queries exercising the outer pipeline substitution path.

### Long Term

1. **PATH B: Native merged index support** — extend `PipelineToMergedIndexScanRule`
   with a second operand pattern matching `EnumerableMergeJoin` over bare
   `EnumerableTableScan` nodes that already carry the correct collation trait (no
   explicit `EnumerableSort` needed when tables advertise collations via `getStatistic()`).
   See "Architectural Note: Two Production Paths" above.

2. **3-table Q3 merged index** — register a 3-table `MergedIndex` for
   (CUSTOMER, ORDERS, LINEITEM) stored by custkey and write a test that replaces
   the entire outer join pipeline in one substitution. Two sub-options:
   (a) extend `PipelineToMergedIndexScanRule` to match a 3-table chain, or
   (b) two sequential HEP passes (inner leaf first, then outer leaf).

3. **Functional dependency metadata** — investigate whether `RelMdFunctionalDependencies`
   can expose ORDERKEY→CUSTKEY so the planner can recognize 3-table merged index
   opportunities automatically rather than requiring manual registration.

4. **JOB (Join Order Benchmark)** — add tests for representative JOB queries to
   show merged index substitution generalizes beyond TPC-H star schemas.

5. **`implement()` stub** — `EnumerableMergedIndexScan.implement()` returns an empty
   enumerable. A real implementation drives a sequential B-tree scan over interleaved
   records and assembles join outputs and computes aggregations on-the-fly.
   Explore feasibility to hook my leanstore repo here.
