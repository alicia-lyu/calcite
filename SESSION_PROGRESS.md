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

## Next steps

Current output for Q3-before (with merge joins):

```
EnumerableLimitSort(sort0=[$1], sort1=[$2], dir0=[DESC], dir1=[ASC], fetch=[10])
      EnumerableProject(L_ORDERKEY=[$0], REVENUE=[$3], O_ORDERDATE=[$1], O_SHIPPRIORITY=[$2])
        EnumerableAggregate(group=[{0, 1, 2}], REVENUE=[SUM($3)])
          EnumerableProject(L_ORDERKEY=[$17], O_ORDERDATE=[$12], O_SHIPPRIORITY=[$15], $f3=[*($22, -(1, $23))])
            EnumerableMergeJoin(condition=[=($8, $17)], joinType=[inner])
              EnumerableSort(sort0=[$8], dir0=[ASC])
                EnumerableMergeJoin(condition=[=($0, $9)], joinType=[inner])
                  EnumerableSort(sort0=[$0], dir0=[ASC])
                    EnumerableTableScan(table=[[TPCH, CUSTOMER]])
                  EnumerableSort(sort0=[$1], dir0=[ASC])
                    EnumerableTableScan(table=[[TPCH, ORDERS]])
              EnumerableSort(sort0=[$0], dir0=[ASC])
                EnumerableTableScan(table=[[TPCH, LINEITEM]])
```

Current output for Q3-after (with merged index scan):

```
EnumerableLimitSort(sort0=[$1], sort1=[$2], dir0=[DESC], dir1=[ASC], fetch=[10])
      EnumerableProject(L_ORDERKEY=[$0], REVENUE=[$3], O_ORDERDATE=[$1], O_SHIPPRIORITY=[$2])
        EnumerableAggregate(group=[{0, 1, 2}], REVENUE=[SUM($3)])
          EnumerableProject(L_ORDERKEY=[$17], O_ORDERDATE=[$12], O_SHIPPRIORITY=[$15], $f3=[*($22, -(1, $23))])
            EnumerableMergeJoin(condition=[=($8, $17)], joinType=[inner])
              EnumerableSort(sort0=[$8], dir0=[ASC])
                EnumerableMergedIndexScan(tables=[[[TPCH, CUSTOMER]:C_CUSTKEY, [TPCH, ORDERS]:O_CUSTKEY]], collation=[[0]])
              EnumerableSort(sort0=[$0], dir0=[ASC])
                EnumerableTableScan(table=[[TPCH, LINEITEM]])
```

Our code made the choice to substitute the join between orders and customers on `custkey`. Their result is joined with lineitem on `orderkey` on the fly (not as a merged index). 
Even if we limit ourselves to one merged index for this query (please see after Q9 for two merged indexes for this query), this is not the best way to use merged indexes.
This implementation is not as good as the following, which groups the join and groupby on orderkey into a single scan over a merged index. The current implementation has the groupby at the top and didn't conduct aggregate pushdown. Can Calcite support this kind of transformation? If not, we need to force this choice in our test class, manually if necessary, since we are only working on TPC-H queries and, in the future, JOB queries.

```tex
\subsection{Execution of Complex Queries}\label{subsec:complex_queries}

In the execution plan of a complex query, some joins and aggregations may share a sort order within the context of a larger plan.
Though the full scope of complex queries is left to future work, we briefly discuss how execution of such a query pipeline fits into the execution of a more complex query.

Consider, for example, Q3 in the TPC-H Benchmark~\cite{tpch2017spec}, in which \texttt{orderkey} appears both in join predicates and in grouping attributes.
In many possible execution plans, e.g., the one shown in Figure~\ref{fig:tpch_q3_plan}, the merge join and the grouping operator share the same sort order on \texttt{orderkey}.
In a database with a merged index storing the primary indexes of the Orders and Lineitem tables interleaved by \texttt{orderkey},
these two operators require only a single-pass scan over the merged index.
In fact, this single-pass scan covers the part of the execution plan marked by a dashed red line in Figure~\ref{fig:tpch_q3_plan}.
The rest of the query, such as the selection on \texttt{mktsegment} and the final ordering, can be executed as the system usually processes queries without merged indexes.

\begin{figure}[htbp]
    \resizebox{\linewidth}{!}{
    \begin{tikzpicture}[thick, >=stealth, node distance=1cm]
        \node (scan_lineitem) {Lineitem};
        \node[anchor=south, above=0.3 of scan_lineitem] (select_lineitem) {$\sigma_{\texttt{l\_shipdate} > \text{date '[DATE]'}}$};
            \draw (scan_lineitem) -- (select_lineitem);
        \node[anchor=south, above=0.3 of select_lineitem] (sort_lineitem) {$\text{Sort}_{\texttt{l\_orderkey}}$};
            \draw (select_lineitem) -- (sort_lineitem);
        \node[anchor=south, above=0.3 of sort_lineitem] (group_lineitem) {$\gamma_{\texttt{l\_orderkey}}$};
            \draw (sort_lineitem) -- (group_lineitem);
        \node[anchor=south, above=0.3 of group_lineitem] (proj_lineitem) {$\pi_{\begin{subarray}{l}
            \texttt{l\_orderkey},\\
            \texttt{sum(l\_extendedprice}\\
            \quad\texttt{*(1-l\_discount)) as}\\
            \quad\texttt{revenue}
            \end{subarray}
        }$};
        \draw (group_lineitem) -- (proj_lineitem);

        \node (scan_order)[right=2 of select_lineitem] {Orders};
        \node[anchor=south, above=0.3 of scan_order] (select_order) {$\sigma_{\texttt{o\_orderdate} < \text{date `[DATE]'}}$};
            \draw (scan_order) -- (select_order);
        \node[anchor=south, above=0.3 of select_order] (sort_order) {$\text{Sort}_{\texttt{o\_orderkey}}$};
            \draw (select_order) -- (sort_order);

        \node[anchor=south, above=0.3 of proj_lineitem, xshift=1cm] (mergejoin) {$\text{Merge Join}_{\texttt{o\_orderkey}}$};
            \draw (proj_lineitem) -- (mergejoin);
            \draw (sort_order) -- (mergejoin);

        \node (scan_cust)[right=2 of mergejoin, yshift=-1cm] {Customer};
        \node (select_cust)[anchor=south, above=0.3 of scan_cust] {$\sigma_{\texttt{mktsegment} = \text{`[SEGMENT]'}}$};
            \draw (scan_cust) -- (select_cust);
        
        \node (semi_join)[above=0.3 of mergejoin, xshift=1cm] {$\ltimes_{\texttt{custkey}}$};
            \draw (mergejoin) -- (semi_join);
            \draw (select_cust) -- (semi_join);

        \node (ordering)[above=0.3 of semi_join] {$\text{Sort}_{\texttt{revenue desc,o\_orderdate}}$};
            \draw (semi_join) -- (ordering);

        \node (final_proj)[above=0.3 of ordering] {$\pi_{\texttt{o\_orderkey,revenue,o\_orderdate,o\_shippriority}}$};
            \draw (ordering) -- (final_proj);
        
        % the part that can be executed via a single-pass scan over a merged index
        \draw[thick, dashed, red, rounded corners] 
        (scan_lineitem.south east) -- (scan_lineitem.south west) -- 
        (select_lineitem.south west) -- (proj_lineitem.north west) --
        (mergejoin.north west) -- (mergejoin.north east) --
        (select_order.north east) -- (select_order.south east) --
        (scan_order.south east) -- cycle;
        
    \end{tikzpicture}
    }
    \caption{A possible execution plan for TPC-H Q3. \textnormal{
        The part marked by the dashed red line can be executed via a single-pass scan over a merged index storing the primary indexes of the Orders and Lineitem tables interleaved by \texttt{orderkey}.
    }}\label{fig:tpch_q3_plan}
    \Description{A query plan for TPC-H Q3. It shows a series of operations: scans on Lineitem, Orders, and Customer tables; selections on dates and market segment; sorts on order keys; a grouping on the line items; a merge join between orders and line items; a semi-join with customers; and a final sort and projection. A red dashed box highlights the part of the plan involving the join and aggregation of Orders and Lineitem, which can be optimized with a merged index.}
\end{figure}

In summary, the execution of a complex query can be seen as a combination of operators that can be executed via merged indexes and operators that cannot, assuming that the query optimizer can correctly bundle the right operators.
The former are executed via single-pass scans over merged indexes, while the latter are executed as the system usually processes queries without merged indexes.
Our ongoing work investigates complex queries where multiple pipelines of multi-table joins and aggregations, each with a different sort order, coexist---analogous to multiple sets of interesting orderings in the query plan---and how to execute them with multiple merged indexes.
```

Another optimization for this case specifically, lineitem can simply be extended with `custkey` and included in the merged index with order and customer. This is because (orderkey, lineitem) has a functional dependency on custkey (there must be a unique customer for this lineitem).
Implementation of this Q3 plan requires recognizing functional dependencies---is this supported by Calcite? Please make a note of this in this file and leave it for future implementation.

More generally, i.e., when functional dependencies are not present, consider the following query, TPC-H Q9:

```SQL
select nation, o_year, sum(amount) as sum_profit
from (
select n_name as nation, extract(year from o_orderdate) as o_year,
l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
From part, supplier, lineitem, partsupp, orders, nation
where s_suppkey = l_suppkey and ps_suppkey = l_suppkey
and ps_partkey = l_partkey and p_partkey = l_partkey
and o_orderkey = l_orderkey and s_nationkey = n_nationkey
and p_name like '%[COLOR]%'
) as profit
group by nation, o_year
order by nation, o_year desc;
```

A possible plan with order-based operators is shown below.

```DOT
digraph G {
    // Bottom-to-top data flow
    rankdir=BT;
    node [shape=rect, fontname="Helvetica,Arial,sans-serif", fontsize=12, style=filled, fillcolor="#f9f9f9"];
    edge [fontname="Helvetica,Arial,sans-serif", fontsize=10];
    label = "TPC-H Q9";
    labelloc = "t";

    // --- Subtree 1: Supplier & Nation Hierarchy ---
    ScanN   [label="Scan: Nation", shape=folder, fillcolor="#e2e3e5"];
    ScanS   [label="Scan: Supplier", shape=folder, fillcolor="#e2e3e5"];
    ScanPS  [label="Scan: PartSupp", shape=folder, fillcolor="#e2e3e5"];
    
    SortPS  [label="Sort: (suppkey)"];
    MJ1     [label="Merge Join: (suppkey)", fillcolor="#cfe2ff"];
    SortS   [label="Sort: (nationkey)"];
    MJ2     [label="Merge Join: (nationkey)", fillcolor="#cfe2ff"];

    // --- Subtree 2: Order, Lineitem & Part Hierarchy ---
    ScanO   [label="Scan: Orders", shape=folder, fillcolor="#e2e3e5"];
    ScanL   [label="Scan: Lineitem", shape=folder, fillcolor="#e2e3e5"];
    ScanP   [label="Scan: Part", shape=folder, fillcolor="#e2e3e5"];
    
    MJ3     [label="Merge Join: (orderkey)\nextract(year from o_orderdate) as o_year", fillcolor="#cfe2ff"];
    SortL   [label="Sort: (partkey)"];
    SelectP [label="Filter: p_name like '%[COLOR]%'", fillcolor="#f8d7da"];
    MJ4     [label="Merge Semi Join: (partkey)", fillcolor="#cfe2ff"];
    SortSub2 [label="Sort: (suppkey, partkey)", penwidth=2];

    // --- Global Operations ---
    MJ_Final [label="Merge Join: (suppkey, partkey)\n\
        n_name, o_year,\n\
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\
        ", fillcolor="#cfe2ff", penwidth=2];
    Sort_Final [label="Sort: (nationkey, o_year)"]
    Agg      [label="Groupby: (nationkey, o_year)\nn_name, o_year, SUM(amount)", fillcolor="#fff3cd"];

    // --- Edges (Flowing Upwards) ---
    // Subtree 1
    ScanS  -> MJ1;
    ScanPS -> SortPS;
    SortPS -> MJ1;
    MJ1    -> SortS;
    SortS  -> MJ2;
    ScanN  -> MJ2;

    // Subtree 2
    ScanO  -> MJ3;
    ScanL  -> MJ3;
    MJ3    -> SortL;
    ScanP  -> SelectP;
    SelectP -> MJ4;
    SortL  -> MJ4;
    MJ4    -> SortSub2;

    // Final
    MJ2      -> MJ_Final [headlabel="Sorted on\n(nk, sk, pk)"];
    SortSub2 -> MJ_Final;
    MJ_Final -> Sort_Final -> Agg;
}
```

Substituting the sorts in this plan with merged indexes, merging those with the same sort key in one b-tree, we get the following plan:

```DOT
digraph G {
    // Bottom-to-top data flow
    rankdir=BT;
    node [shape=rect, fontname="Helvetica,Arial,sans-serif", fontsize=12, style=filled, fillcolor="#f9f9f9"];
    edge [fontname="Helvetica,Arial,sans-serif", fontsize=10];
    label = "TPC-H Q9: All included non-key columns are marked out. All keys are by default included in the result of any operator.";
    labelloc = "t";
    

    MgIdxSPS [label="Merged Index: Supplier + PartSupp\n(suppkey, partkey)"]
    MJ1     [label="Merge Join 1: (suppkey)\nps_supplycost", fillcolor="#cfe2ff"];
    
    MJ3     [label="Merge Join 3: (orderkey)\nl_extendedprice, l_discount, l_quantity,\nextract(year from o_orderdate) as o_year", fillcolor="#cfe2ff"];
    MgIdxOL [label="Merged Index: Orders + Lineitem\n(orderkey, line number)"];
    MgIdxPOL [label="Merged Index: Part + Merge Join 3 Result\n(partkey, suppkey, orderkey, line number)"];
    MJ4     [label="Merge Semi Join 4: (partkey)\nFilter: p_name like '%[COLOR]%'\nl_extendedprice, l_discount, l_quantity,o_year", fillcolor="#cfe2ff"];

    // --- Global Operations ---
    MgIdxAll [label="Merged Index: Nation + Merge Join 1\nResult +Merge Join 4 Result\n(nationkey, suppkey, partkey)"]
    MJ_Final [label="Merge Join 5: (suppkey, partkey)\n\
        n_name, o_year, l_extendedprice\n\
         * (1 - l_discount) - ps_supplycost * l_quantity as amount\
        ", fillcolor="#cfe2ff", penwidth=2];
    MgIdxGroup [label="Merged Index: Merge Join 5 Result\n(nationkey, o_year)"]
    Agg      [label="Groupby: (nationkey, o_year)\nn_name, o_year, SUM(amount)", fillcolor="#fff3cd"];

    // --- Edges (Flowing Upwards) ---
    MgIdxSPS  -> MJ1;
    MJ1    -> MgIdxAll [style="dashed", headlabel="Resort by\nnationkey, suppkey, partkey"]

    // Subtree 2
    MgIdxOL  -> MJ3;
    MJ3 -> MgIdxPOL [style="dashed", label="Resort by\n(partkey, suppkey)"];
    MgIdxPOL -> MJ4;
    MJ4 -> MgIdxAll  [style="dashed", label="Resort by\n(suppkey, partkey, orderkey)"];

    // Final
    MgIdxAll -> MJ_Final;
    MJ_Final -> MgIdxGroup [style="dashed", label="Resort by\n(nationkey, o_year)"];
    MgIdxGroup -> Agg;
}
```

Technically, the only operations needed at query time is those after the final scan of `MgIdxGroup`. Namely, any dashed arrows represent a break between query time and update time. A resort is needed at every dashed arrow, which is shifted to update time (with index creation and maintenance). The rest of the plan is mainly maintenance plan for this final merged index and many intermediate merged indexes. For now, we are only working on query plans, not maintenance plans, so the required code should be moderate. However, update this file to save the plan for maintenance plans.

Back to Q3, the merge join result between order and lineitem is finally sorted on custkey and join with customer. This should create another merged index like Q9. Next step is to implement tests for both Q3 and Q9 and ensure their plans are same as or very close to the ones above.