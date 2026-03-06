# Session Progress: Merged Index Feature

## Status

Note to Claude: include full relative paths here next time.

| Item                                                                    | Status  |
|-------------------------------------------------------------------------|---------|
| `CLAUDE.md` + `main.tex`                                                | Done    |
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

## Known Lessons / Gotchas

- `ENUMERABLE_SORT_RULE` converts existing `LogicalSort` nodes — does NOT create
  sorts from scratch. Inject `LogicalSort` nodes manually before joins when you want
  explicit sorts in the enumerable plan.
- Use `HepPlanner` for phase 2 (not Volcano's `Programs.ofRules`) when applying a
  single transformation rule to an already-physical plan.

---

## Architectural Note: Two Production Paths for Merged Index Optimization

There are two distinct scenarios, and the current rule only handles one:

```
PATH A — Substitution (current PoC, what the test demonstrates)
────────────────────────────────────────────────────────────────
Tables do NOT report collation (getStatistic() = Statistics.UNKNOWN).
At planning time, Volcano adds EnumerableSort nodes before the join.
PipelineToMergedIndexScanRule matches:
  EnumerableMergeJoin
    ├─ EnumerableSort → EnumerableTableScan(A)   ← explicit sort present
    └─ EnumerableSort → EnumerableTableScan(B)
Then replaces the whole pipeline with EnumerableMergedIndexScan.

injectSortsBeforeJoin() in the test simulates this path:
it forces LogicalSort nodes into the plan so ENUMERABLE_SORT_RULE
has something to convert. Without injection, the Volcano planner
cannot satisfy the merge-join's collation requirement.

PATH B — Native (production-correct design)
────────────────────────────────────────────────────────────────
Tables backed by a merged index report their collation via
  getStatistic().getCollations()  →  Statistics.of(n, keys, collations)
This makes EnumerableTableScan carry the collation in its trait set.
Volcano's trait enforcement checks fromTrait.satisfies(toTrait) first;
if the scan already has the required collation, RelCollationTraitDef
.convert() is NEVER called → no EnumerableSort node is added.

The plan becomes:
  EnumerableMergeJoin
    ├─ EnumerableTableScan(A)[collation=[0]]   ← sort-free scan
    └─ EnumerableTableScan(B)[collation=[0]]
PipelineToMergedIndexScanRule currently does NOT match this pattern
because it requires explicit EnumerableSort nodes.

The fix: add a second operand pattern to the rule that matches
EnumerableMergeJoin over bare table scans with the right collation trait.
```

---

## Calcite Interesting Orderings Research

### How Calcite implements interesting orderings (verified from source)

Calcite implements Selinger's interesting ordering technique via two complementary
mechanisms in `EnumerableMergeJoin` (no extra configuration needed):

- **`deriveTraits()` + `DeriveMode.BOTH`** (lines 311–354): propagates collation
  *upward* — if an input is sorted on the join key, the join output is tagged with
  that collation. Downstream operators can consume it without an extra sort.
- **`passThroughTraits()`** (lines 228–309): propagates collation *downward* — if a
  downstream operator (e.g., `EnumerableSortedAggregate`) requires sorted input, this
  pushes that requirement through the join to its inputs.
- **`EnumerableSortedAggregate.passThroughTraits()`** (lines 74–108): similarly pushes
  sort requirements down from the aggregate to its input.
- **`RelCollationTraitDef.convert()`** (lines 64–84): inserts a `LogicalSort` *only
  when* the required collation is NOT already satisfied.

### Config requirements for order-based testing

- `traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)` and
  `ENUMERABLE_SORTED_AGGREGATE_RULE` must both be registered.
- **`ENUMERABLE_AGGREGATE_RULE` must also be present** — without it, the Volcano planner
  fails with `CannotPlanException` because `EnumerableSortedAggregate.passThroughTraits`
  cannot be resolved when the sort requirement propagates through a `LogicalProject`
  (there is no enforcer rule to insert a sort through the project conversion chain).
  The planner picks between hash and sorted aggregate by cost; to observe sorted
  aggregate in a plan, the input must already be sorted on the group keys.

### `injectSortsBeforeJoin` improvements

- Guards against **empty key lists** (non-equi joins, cross joins): skips sort injection
  rather than crashing with `IndexOutOfBoundsException`.
- Builds **multi-column collations** from all equi-join keys (e.g., PARTSUPP joined on
  both suppkey and partkey), not just the first key.

### Q9 SQL form

Use explicit `JOIN … ON …` syntax (not comma-separated FROM with WHERE) so that
`splitJoinCondition` can extract equi-join keys from each join node. The LIKE filter
(`p.p_name LIKE '%green%'`) goes in a WHERE clause and requires `ENUMERABLE_FILTER_RULE`
in the rule set.

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