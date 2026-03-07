# Session Progress: Merged Index Feature

## Status

| Item                                                                    | Status  |
|-------------------------------------------------------------------------|---------|
| `CLAUDE.md` + `main.md`                                                 | Done    |
| `core/.../materialize/MergedIndex.java`                                 | Done    |
| `core/.../materialize/MergedIndexRegistry.java`                         | Done    |
| `core/.../adapter/enumerable/EnumerableMergedIndexScan.java`            | Done    |
| `core/.../adapter/enumerable/PipelineToMergedIndexScanRule.java`        | Done    |
| `core/.../adapter/enumerable/EnumerableRules.java` (constant)           | Done    |
| Compilation fix (`.replace` vs `.replaceIf`)                            | Done    |
| `core/.../adapter/enumerable/PipelineToMergedIndexScanRuleTest.java`    | Done ✓  |
| `plus/.../adapter/tpch/MergedIndexTpchPlanTest.java`                    | Done ✓  |
| `explainTerms` display fix (table names + key column, not raw toString) | Done ✓  |
| TPC-H Q3 (3-table: CUSTOMER ⋈ ORDERS ⋈ LINEITEM, partial substitution) | Done ✓  |
| TPC-H Q12 (2-table: ORDERS ⋈ LINEITEM, full substitution)              | Done ✓  |
| TPC-H Q3 variant: ORDERS⋈LINEITEM leaf, CUSTOMER outer — `tpchQ3OrdersLineitem()` | Done ✓ |
| TPC-H Q9 (6-table, all leaf joins substituted) — `tpchQ9()`            | Done ✓  |
| Calcite interesting-ordering research documented in SESSION_PROGRESS    | Done ✓  |

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

These represent the target ideal plans for TPC-H Q9, not necessarily what Calcite
currently produces (the planner picks its own join order and cost-based choices).
Paste into https://dreampuf.github.io/GraphvizOnline/ to visualize.

### Q9 Ideal Order-Based Plan (BEFORE merged indexes)

Two independent merge-join pipelines joined at the end on (suppkey, partkey):

```DOT
digraph G {
    rankdir=BT;
    node [shape=rect, fontname="Helvetica,Arial,sans-serif", fontsize=12, style=filled, fillcolor="#f9f9f9"];
    edge [fontname="Helvetica,Arial,sans-serif", fontsize=10];
    label = "TPC-H Q9 — ideal order-based plan";
    labelloc = "t";

    ScanN   [label="Scan: Nation",   shape=folder, fillcolor="#e2e3e5"];
    ScanS   [label="Scan: Supplier", shape=folder, fillcolor="#e2e3e5"];
    ScanPS  [label="Scan: PartSupp", shape=folder, fillcolor="#e2e3e5"];
    ScanO   [label="Scan: Orders",   shape=folder, fillcolor="#e2e3e5"];
    ScanL   [label="Scan: Lineitem", shape=folder, fillcolor="#e2e3e5"];
    ScanP   [label="Scan: Part",     shape=folder, fillcolor="#e2e3e5"];

    SortPS   [label="Sort: (suppkey)"];
    MJ1      [label="Merge Join: (suppkey)",   fillcolor="#cfe2ff"];
    SortS    [label="Sort: (nationkey)"];
    MJ2      [label="Merge Join: (nationkey)", fillcolor="#cfe2ff"];
    MJ3      [label="Merge Join: (orderkey)\nextract(year from o_orderdate) as o_year", fillcolor="#cfe2ff"];
    SortL    [label="Sort: (partkey)"];
    SelectP  [label="Filter: p_name LIKE '%[COLOR]%'", fillcolor="#f8d7da"];
    MJ4      [label="Merge Semi Join: (partkey)", fillcolor="#cfe2ff"];
    SortSub2 [label="Sort: (suppkey, partkey)", penwidth=2];
    MJ_Final [label="Merge Join: (suppkey, partkey)\nn_name, o_year, amount", fillcolor="#cfe2ff", penwidth=2];
    SortFinal [label="Sort: (nation, o_year)"];
    Agg      [label="Groupby: (nation, o_year)\nSUM(amount)", fillcolor="#fff3cd"];

    ScanS  -> MJ1;  ScanPS -> SortPS -> MJ1;
    MJ1    -> SortS -> MJ2;  ScanN -> MJ2;
    ScanO  -> MJ3;  ScanL -> MJ3;
    MJ3    -> SortL -> MJ4;  ScanP -> SelectP -> MJ4;
    MJ4    -> SortSub2;
    MJ2    -> MJ_Final [headlabel="Sorted on\n(nk,sk,pk)"];
    SortSub2 -> MJ_Final;
    MJ_Final -> SortFinal -> Agg;
}
```

### Q9 With Merged Indexes (AFTER)

Each sort replaced by a merged index. Dashed arrows = update-time resort (maintenance).
**At query time only `Agg` runs** (scanning `MgIdxGroup`); everything else is maintenance.

```DOT
digraph G {
    rankdir=BT;
    node [shape=rect, fontname="Helvetica,Arial,sans-serif", fontsize=12, style=filled, fillcolor="#f9f9f9"];
    edge [fontname="Helvetica,Arial,sans-serif", fontsize=10];
    label = "TPC-H Q9 — merged-index plan (dashed = update time)";
    labelloc = "t";

    MgIdxSPS  [label="MergedIdx: Supplier+PartSupp\n(suppkey,partkey)"];
    MJ1       [label="Merge Join 1: (suppkey)\nps_supplycost", fillcolor="#cfe2ff"];
    MgIdxOL   [label="MergedIdx: Orders+Lineitem\n(orderkey)"];
    MJ3       [label="Merge Join 3: (orderkey)\nl_extendedprice,l_discount,l_quantity,o_year", fillcolor="#cfe2ff"];
    MgIdxPOL  [label="MergedIdx: Part+MJ3 Result\n(partkey,suppkey,orderkey)"];
    MJ4       [label="Merge Semi Join 4: (partkey)\nFilter: LIKE '%[COLOR]%'", fillcolor="#cfe2ff"];
    MgIdxAll  [label="MergedIdx: Nation+MJ1 Result+MJ4 Result\n(nationkey,suppkey,partkey)"];
    MJ_Final  [label="Merge Join 5: (suppkey,partkey)\nn_name,o_year,amount", fillcolor="#cfe2ff", penwidth=2];
    MgIdxGroup [label="MergedIdx: MJ5 Result\n(nation,o_year)"];
    Agg       [label="Groupby: (nation,o_year)\nSUM(amount)", fillcolor="#fff3cd"];

    MgIdxSPS -> MJ1;
    MJ1      -> MgIdxAll  [style=dashed, label="resort by (nk,sk,pk)"];
    MgIdxOL  -> MJ3;
    MJ3      -> MgIdxPOL  [style=dashed, label="resort by (pk,sk)"];
    MgIdxPOL -> MJ4;
    MJ4      -> MgIdxAll  [style=dashed, label="resort by (sk,pk,ok)"];
    MgIdxAll -> MJ_Final;
    MJ_Final -> MgIdxGroup [style=dashed, label="resort by (nation,o_year)"];
    MgIdxGroup -> Agg;
}
```

---

## Research Notes

### Functional Dependencies and 3-Table Q3

`ORDERS.o_orderkey → ORDERS.o_custkey` (PK) combined with `LINEITEM.l_orderkey`
referencing `ORDERS.o_orderkey` (FK) means a merged index on (ORDERS, LINEITEM) by
orderkey implicitly groups lineitem rows by custkey. This could enable a 3-table
merged index (CUSTOMER + ORDERS + LINEITEM) stored by custkey if the optimizer
recognizes the functional dependency.

- **Calcite API**: `RelMdFunctionalDependencies`, `UniqueKeys`, and
  `RelOptTable.toRel()` may expose FK→PK paths. Worth investigating.
- If not natively available, assert the FD manually in the test by registering a
  3-table `MergedIndex` and extending `PipelineToMergedIndexScanRule` to match
  3-table pipelines (Sort→MergeJoin(Sort→Scan, Sort→Scan) pattern).

### Q9 Maintenance Plan

The Q9 merged-index DOT shows two tiers:
- **Query tier**: only the final `Agg` (one sequential scan of `MgIdxGroup`)
- **Maintenance tier**: all dashed-arrow resorts — index creation and incremental update

Current implementation covers query plans only. Maintenance plan work would model
each dashed edge as an incremental-update rule triggered by base-table inserts/deletes.

---

## Next Steps

### Short Term (next session)

1. ✓ Tests now write DOT files to `plus/test-dot-output/` (gitignored). Run the TPC-H tests and open the generated `.dot` files with the VSCode Graphviz extension.
2. Verify implemented queries, esp. Q9.
3. Work out two or three more TPC-H queries that teach Claude to generate and test plans for any complex queries.

### Medium Term

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