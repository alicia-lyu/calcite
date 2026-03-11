# Overhaul Mar 10

I noticed a series of issues in the current code. Some false assumptions have been carried over for several sessions now. Therefore, the code requires an overhaul.

## Inject sorts

**Done.** Renamed `injectSortsBeforeJoin` → `injectSortsBeforeSortBasedOps`. It starts from the bottom side to allow proper recognition of sorted inputs of later operators.

Now handles:

- **Sort node**: recurses into input; drops the Sort when no FETCH/OFFSET and `inputAlreadySorted` confirms the input is already sorted. Note: `inputAlreadySorted` now checks both field index AND direction, so Q9's `ORDER BY n_name ASC, o_year DESC` is NOT dropped (Aggregate output is `[n_name ASC, o_year ASC]` — direction mismatch on `o_year`).
- **Aggregate node**: injects `LogicalSort` on group keys before the aggregate input when not already sorted.
- **Join node**: guards each injection with `inputAlreadySorted` to skip wrapping an already-sorted input.

Enhanced `inputAlreadySorted` to drill through single-input operators (Aggregate, Project, Filter, etc.) to reach an authoritative `Sort` node, enabling detection of e.g. `Agg(Sort([k]))` as already sorted on `[k]`.

**Not yet handled** (future work):

- Sort-based operators which don't specify directions (ASC or DESC) can be executed with any directions, if later operator requires a certain direction(s), proactively use that direction(s).
- **Window functions** (`OVER`): require sorting on PARTITION BY + ORDER BY keys.
- **DISTINCT / set operators** (INTERSECT, EXCEPT): sort-based implementations need a sort on all output columns.
- **Merge-sort joins with non-equi conditions**: currently skipped (cross/non-equi guard), but some could benefit from partial key injection.

## Pipeline & Pipeline Identification

Every query can be seen as a tree of pipelines as shown below. A pipeline is defined by the following properties:

1. Its lower boundary (where inputs come in) can only be sort operators that share the compatible sort order (same sort key or hierarchical prefix chain, see Section "Embedding Interesting Orderings for Multi-table Joins" in `./main.tex`). Data flow that are already sorted can be seen as having an implicit sort operator on top.
2. The upper boundary (where it emits data) can only be 1 sort operator. Note that the next pipeline must require a different sort order; otherwise, they would belong to the same pipeline.
3. Each branch of the tree that is the pipeline might terminate at different locations, so the inputs come in at different times.
4. Inside each pipeline, the data flow retain a certain sort order. Inputs are sorted when they come in. All operations inside preserves order.

![Pipeline identification](img/Screenshot%202026-03-10%20at%207.58.51 PM.png)

Therefore, the first mistake in your code is definition of a pipeline which is centered on a merge join. It should be something like this. I don't remember some Java syntax and didn't check some definitions, please correct them yourself.

```java
private static class Pipeline {
    final RelNode root; // root of this pipeline, also upper boundary (incl.)
    List<Pipeline> sources; // also lower boundary of this pipeline, not part of the pipeline. If a source pipeline `s` doesn't share the sort order here, s.root.parent is a sort operator, it can also be easily inferred using this.sharedCollation. 
    // For ease of coding, a single RelNode can be defined as a pipeline as well, so the bottom pipeline's sources are just table scans, and they should still be wrapped as pipelines. Note that there doesn't need to be a merged index for these pipelines, but you can just use mergedIndex to refer to the table---your choice.
    final RelCollation sharedCollation; // shared sort order
    final double rowCount;
    MergedIndex mergedIndex; // We need to carefully define the correspondence between pipelines and merged indexes, as there can be an off-by-1 error. Let's define it as the merged index that simply store and interleave records from sources. The subsequent operators until `root` are not executed yet.
    final RelNode physicalPlan;
}
```

Furthermore, identification of pipelines in `MergedIndexTpchPlanTest.java` is wrong. Right now, you are not searching for sort operators and identify boundaries. You are naively using each join as 1 pipeline.
I've drafted the pseudo code for function  `findAllPipelines(RelNode root)`:

```java
private static Pipeline findAllPipelines(RelNode node, Pipeline currPipeline = new Pipeline()) {
    while (node) {
        if (node is sort operator) {
            Pipeline newPipeline = new Pipeline(node.input);
            currPipeline.sources.add(newPipeline);
            findAllPipelines(node, newPipeline); // complete sources for newPipeline
        } else if (node has multiple inputs) {
            currPipeline.sources.add(findAllPipelines(node.input1));
            currPipeline.sources.add(findAllPipelines(node.input2));
        } else {
            currPipeline.sources.add(findAllPipelines(node.input));
        }
    }
    return currPipeline; // includes all child pipelines in its member
}
```

Note that the boundary here depends entirely on whether an operator is a sort operator. I believe the code currently doesn't check `inputAlreadySorted` before injecting sort operators. For successful identification of boundaries, you must decide whether `inputAlreadySorted`.

Also note that the code I provided you with above didn't explicit use join for pipeline. Sort is what defines a pipeline, join is only one important use case.

**Done.** Pipeline class and discovery overhauled:
- `Pipeline` now has `root`, `sources` (child pipelines), `sharedCollation`, `boundaryCollation`, `rowCount`, `mergedIndex`. No `join` field.
- `buildPipelineTree` / `buildPipeline` / `collectChildPipelines`: top-down recursion cutting at `EnumerableSort` boundaries.
- `sharedCollation` derived from first boundary Sort's collation (not root node's output collation).
- `flattenPipelines`: post-order flattening, only non-trivial pipelines (≥ 2 sources).
- `resolveSources` / `resolveSourceCollations`: map child pipelines to `RelOptTable` or `MergedIndex`.
- `tpchQ3` test deleted — it was an incorrect example registering CUSTOMER ⋈ ORDERS instead of ORDERS ⋈ LINEITEM.
- `PipelineToMergedIndexScanRule` generalized: `extractSource` now accepts sorted non-Sort inputs (e.g. `SortedAggregate`); `extractCollation` falls back to trait-set collation when no explicit Sort is present.

## Pipeline Conversion

The execution of a pipeline can be broken down into the following cases:

The inputs/sources of a pipeline can be either data flow, e.g. full table scans, or delta flow, e.g., table inserts. This section only considers data flow.

A tree of pipelines breaks down a query into multiple stages, each pipeline is one stage. Following the pull-based Cascade framework, from top to bottom (downstream operator calling `next` on upstream operator), here is an example:

1. Final stage: query-time computation. Let the pipeline be `p1`. `p1.mergedIndex` in effect stores all records from `p1.sources`, so there is no real need to read those sources; they only signify the boundary of `p1`. We just scan this merged index and complete the steps between `p1.root` and `p1.sources`.
2. Index creation final stage: Let the pipeline be `p2`. This pipeline is one source of `p1`. `p2.mergedIndex`in effect stores all records from `p2.sources`. We just scan this merged index and complete the steps of `p2.root` and `p2.sources`, producing result record one by one with `while (true) {p1.mergedIndex.add(p2.physicalPlan.next())}`---pull-based/Cascade, namely the `physicalPlan` is a enumerable.
3. Index creation initial stage:  Similar to index creation final stage.

The only difference between query plan and index creation plan is that the former does not flow into yet another pipeline (with a merged index storing the result). A query plan produces the full query result as requested in a database (flow into the user).

Regardless of query plan or index creation plan, we all need to produce the result of a pipeline based on the merged index (the sources of the pipeline is not actually read, more to signal the lower boundary of the current pipeline). 

We need to create `physicalPlan` for each pipeline, essentially converting the logical plan between `root` and `sources` to a physical plan with `mergedIndex` as the input. It should be similar to the standard conversion, consisting of a series/tree of pull-based operators. The only exception is at the very upstream of the physical plan, because this operator must process a data flow interleaving different types of records, either to join them, join+aggregate them, or aggregate+join them---in short a kind of record assembly. Two example algorithms are Algorithm 1 and 2 in `./main.tex`. Your whole `PipelineToMergedIndexScanRule.java` must be overhauled. 

How to provide a universal implementation for this bottom operator intaking interleaving records (regardless of whether it's join or join+aggregate), I don't have a clear idea yet, I think we need to first define such a stream. Explore and plan this part coarsely. Aside from this, plan the other parts (mainly the structure of cascading pipelines' physical plans) concretely.

## Maintenance plans

Maintenance plans are basically index creation plans that process delta instead of full data. TODO