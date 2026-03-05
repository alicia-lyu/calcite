# Session Progress: Merged Index Feature

## Status

Note to Claude: include full relative paths here next time.

| Item                                     | Status  |
|------------------------------------------|---------|
| CLAUDE.md + main.tex                     | Done    |
| MergedIndex.java                         | Done    |
| MergedIndexRegistry.java                 | Done    |
| EnumerableMergedIndexScan.java           | Done    |
| PipelineToMergedIndexScanRule.java       | Done    |
| EnumerableRules.java (constant)          | Done    |
| Compilation fix (.replace vs .replaceIf) | Done    |
| PipelineToMergedIndexScanRuleTest.java   | Done ✓  |
| TPC-H plan observation test              | Done ✓  |

## Commands

```
./gradlew :plus:cleanTest :plus:test --tests "*.MergedIndexTpchPlanTest" --info
```

And search for `=== BEFORE (order-based pipeline) ===`.

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

## Next steps

`EnumerableMergedIndexScan`'s infomation is too crowded. Emulate `EnumerableTableScan` and only include table names (no columns), but, on top of that, include the sort key of each relation.

EnumerableMergedIndexScan(tables=[[RelOptTableImpl{schema=org.apache.calcite.prepare.CalciteCatalogReader@40996771, names= [TPCH, ORDERS], table=org.apache.calcite.adapter.tpch.TpchSchema$TpchQueryableTable@58295ffd, rowType=RecordType(JavaType(class java.lang.Long) O_ORDERKEY, JavaType(class java.lang.Long) O_CUSTKEY, JavaType(class java.lang.String) O_ORDERSTATUS, JavaType(class java.lang.Double) O_TOTALPRICE, JavaType(class java.sql.Date) O_ORDERDATE, JavaType(class java.lang.String) O_ORDERPRIORITY, JavaType(class java.lang.String) O_CLERK, JavaType(class java.lang.Integer) O_SHIPPRIORITY, JavaType(class java.lang.String) O_COMMENT)}, RelOptTableImpl{schema=org.apache.calcite.prepare.CalciteCatalogReader@40996771, names= [TPCH, LINEITEM], table=org.apache.calcite.adapter.tpch.TpchSchema$TpchQueryableTable@6315f28e, rowType=RecordType(JavaType(class java.lang.Long) L_ORDERKEY, JavaType(class java.lang.Long) L_PARTKEY, JavaType(class java.lang.Long) L_SUPPKEY, JavaType(class java.lang.Integer) L_LINENUMBER, JavaType(class java.lang.Double) L_QUANTITY, JavaType(class java.lang.Double) L_EXTENDEDPRICE, JavaType(class java.lang.Double) L_DISCOUNT, JavaType(class java.lang.Double) L_TAX, JavaType(class java.lang.String) L_RETURNFLAG, JavaType(class java.lang.String) L_LINESTATUS, JavaType(class java.sql.Date) L_SHIPDATE, JavaType(class java.sql.Date) L_COMMITDATE, JavaType(class java.sql.Date) L_RECEIPTDATE, JavaType(class java.lang.String) L_SHIPINSTRUCT, JavaType(class java.lang.String) L_SHIPMODE, JavaType(class java.lang.String) L_COMMENT)}]], collation=[[0]])

Implement the plan for other TPC-H queries, right now only Q3 is implemented. Why do you simplify Q3? Please keep to the original structure of the query as much as possible.