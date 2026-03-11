# Overhaul Mar 10

I noticed a series of issues in the current code. Some false assumptions have been carried over for several sessions now. Therefore, the code requires an overhaul.

## Inject sorts

**Done.** Renamed `injectSortsBeforeJoin` → `injectSortsBeforeSortBasedOps`. Now handles:

- **Sort node**: recurses into input; drops the Sort when no FETCH/OFFSET and `inputAlreadySorted` confirms the input is already sorted (e.g. Q9's ORDER BY is dropped because the Aggregate output is sorted on GROUP BY keys).
- **Aggregate node**: injects `LogicalSort` on group keys before the aggregate input when not already sorted.
- **Join node**: guards each injection with `inputAlreadySorted` to skip wrapping an already-sorted input.

Enhanced `inputAlreadySorted` to drill through single-input operators (Aggregate, Project, Filter, etc.) to reach an authoritative `Sort` node, enabling detection of e.g. `Agg(Sort([k]))` as already sorted on `[k]`.

**Not yet handled** (future work):
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

Therefore, the first mistake in your code is definition of a pipeline. It should be something like this. I don't remember some Java syntax and didn't check some definitions, please correct them yourself.

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

## Pipeline Conversion

Such a tree of pipelines breaks down a query into multiple stages, each pipeline is one stage. Following the pull-based Cascade framework, from upstream to downstream, the following stages are an example:

1. Final stage: query-time computation. Let the pipeline be `p1`. `p1.mergedIndex` in effect stores all records from `p1.sources`, so there is no real need to read those sources; they only signify the boundary of `p1`. We just scan this merged index and complete the steps between `p1.root` and `p1.sources`.
2. Index creation final stage: Let the pipeline be `p2`. This pipeline is one source of `p1`. `p2.mergedIndex`in effect stores all records from `p2.sources`. We just scan this merged index and complete the steps of `p2.root` and `p2.sources` in this way: `while (true) {p1.mergedIndex.add(p2.physicalPlan.next())}`---pull-based/Cascade.
3. Index creation initial stage:  Let the pipeline be `p3`. This pipeline is one source of `p2`. `p3.mergedIndex`in effect stores all records from `p3.sources`. We just scan this merged index and complete the steps of `p3.root` and `p3.sources` in this way: `while (true) {p2.mergedIndex.add(p3.physicalPlan.next())}`---pull-based/Cascade.

As foreshadowed, we need to create `physicalPlan` for each pipeline, essentially converting the logical plan in `root` to a physical plan. It should be similar to the standard conversion, consisting of a series/tree of pull-based operators. The only exception is at the very upstream of the physical plan, because this operator must process a data flow interleaving different types of records, either to join them, join+aggregate them, or aggregate+join them---in short a kind of record assembly. Two example algorithms are Algorithm 1 and 2 in `./main.tex`. Your whole `PipelineToMergedIndexScanRule.java` must be overhauled.

## Maintenance plans

Maintenance plans are basically index creation plans that process delta instead of full data. TODO