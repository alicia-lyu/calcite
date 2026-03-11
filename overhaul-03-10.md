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

### Architecture Decision: Option B — Separate Scan + Assembly Operators

For each pipeline, the AFTER query plan should look like:

```text
remaining operators (Aggregate, Filter, Project, ...)
  EnumerableMergedIndexAssemble(sources=N, keyIndices=[...])
    EnumerableMergedIndexScan(raw interleaved stream)
```

The scan outputs **tagged interleaved records** (one row per source record, with a
source-tag column). The assembly operator above it buffers per source and emits
Cartesian products on key change (Algorithm 1 from `main.tex`).

### Subtask 0: Identify the Assembly Subtree

**Goal**: Given a pipeline, identify which operators should be absorbed into
`EnumerableMergedIndexAssemble`. This is a tree search problem.

A pipeline's internal tree spans from `root` down to the boundary Sorts (exclusive).
Not all operators belong to Assembly — operators above the Assembly subtree are
"remaining operators" that execute normally on the Assembly's joined output.

**Assembly subtree** = the minimal connected subgraph that includes all nodes directly
consuming boundary Sorts as inputs.

**Definitions**:
- **Boundary Sort**: an `EnumerableSort` separating this pipeline from a child pipeline.
- **Boundary consumer**: a node with a boundary Sort as a direct input.
- **Assembly subtree root** = Lowest Common Ancestor (LCA) of all boundary consumers.
- **Assembly subtree** = all nodes on paths from LCA down to boundary consumers (inclusive).

**Examples**:

Q12 (Assembly = just the MergeJoin):
```text
Pipeline root = EnumerableSort(l_shipmode)
  EnumerableSortedAggregate        ← remaining operator
    EnumerableMergeJoin(orderkey)  ← boundary consumer of BOTH Sorts → LCA → Assembly root
      Sort(orderkey) → ORDERS      ← boundary Sort (source 0)
      Sort(orderkey) → LINEITEM    ← boundary Sort (source 1)

Assembly subtree = {MergeJoin}
```

Hypothetical — SortedAggregate between boundary Sort and join (no intermediate Sort):
```text
Pipeline root = MergeJoin(k)            ← boundary consumer of Sort→B
  SortedAggregate(k)                    ← boundary consumer of Sort→A
    Sort(k) → Scan(A)                   ← boundary Sort (source 0)
  Sort(k) → Scan(B)                     ← boundary Sort (source 1)

Assembly subtree = {MergeJoin, SortedAggregate}
```

This arises when `injectSortsBeforeSortBasedOps` skips injecting a Sort because both
operators share the same key. Assembly must handle both join AND aggregation.

3-way join same key, no intermediate re-sort:
```text
Pipeline root = MergeJoin_outer(k)
  MergeJoin_inner(k)
    Sort(k) → Scan(A)
    Sort(k) → Scan(B)
  Sort(k) → Scan(C)

Assembly subtree = {MergeJoin_outer, MergeJoin_inner}
```

Note: current pipeline discovery does NOT produce multi-join pipelines because
`injectSortsBeforeSortBasedOps` always injects Sorts before every join. But an
optimizer that avoids redundant sorts could produce this case.

**Algorithm** (pseudo-code):
```java
Set<RelNode> findAssemblySubtree(Pipeline pipeline) {
    Set<RelNode> boundarySorts = collectBoundarySorts(pipeline);
    Map<RelNode, Integer> descendantCount = new HashMap<>();
    markDescendants(pipeline.root, boundarySorts, descendantCount);
    RelNode lca = findLCA(pipeline.root, boundarySorts.size(), descendantCount);
    Set<RelNode> subtree = new HashSet<>();
    collectSubtree(lca, boundarySorts, subtree);
    return subtree;
}

// Post-order: count how many boundary Sorts are reachable from each node
int markDescendants(RelNode node, Set<RelNode> boundarySorts,
                    Map<RelNode, Integer> counts) {
    int count = 0;
    for (RelNode input : node.getInputs()) {
        if (boundarySorts.contains(input)) {
            count++;  // this node directly consumes a boundary Sort
        } else {
            count += markDescendants(input, boundarySorts, counts);
        }
    }
    counts.put(node, count);
    return count;
}

// Walk down from LCA, including only nodes on paths to boundary Sorts
void collectSubtree(RelNode node, Set<RelNode> boundarySorts,
                    Set<RelNode> result) {
    result.add(node);
    for (RelNode input : node.getInputs()) {
        if (boundarySorts.contains(input)) continue;
        if (counts.get(input) > 0) {
            collectSubtree(input, boundarySorts, result);
        }
    }
}
```

**What the Assembly subtree determines**:
- Subtree = {MergeJoin} → Algorithm 1 (N-way join, Cartesian product per key group)
- Subtree = {MergeJoin, SortedAggregate} → Algorithm 2 (join + aggregate fusion)
- Subtree = {MergeJoin_outer, MergeJoin_inner} → extended Algorithm 1 (3+ sources)

### Subtask 1: Tagged Interleaved Row Type

The raw scan output needs a schema for records from heterogeneous source tables. Options:

- **Wide union row**: all columns from all sources concatenated + `sourceTag` int.
  Simple but wasteful. For hierarchical keys, must either nest or replicate parent rows
  (replication = implicit join, defeating modularization).
- **Generic tagged row**: `(sortKey Object[], sourceTag int, payload Object[])`.
  Compact but loses column-level type safety.
- **Per-source typed rows via Enumerable union**: each source produces its own typed
  enumerable; the scan merges and tags. Assembly knows each source's schema.

No conclusion yet — explore during implementation.

### Subtask 2: `EnumerableMergedIndexScan.implement()` — Interleaved Stream

The scan obtains source enumerables (via table scans or inner MI scans),
merge-interleaves them by sort key, and tags each record with source index. PoC can
use a runtime helper method rather than full Janino codegen.

### Subtask 3: `EnumerableMergedIndexAssemble` Operator

New physical operator. Algorithm 1 logic (N-way inner join):
```text
buffers = new List<List<Object[]>>[sourceCount]
currentKey = null

for each record in input:
  key = extractKey(record)
  tag = record.sourceTag

  if key != currentKey:
    if currentKey != null: emit cartesianProduct(buffers)
    clear all buffers
    currentKey = key

  buffers[tag].add(extractPayload(record, tag))

// flush last key group
emit cartesianProduct(buffers)
```

The assembly strategy is parameterized by the absorbed operator types (from Subtask 0):
- Join-only → Algorithm 1 (Cartesian product per key group)
- Join + aggregate → Algorithm 2 (aggregate during assembly)
- Multi-level join → extended Algorithm 1 (3+ source buffers)

### Subtask 4: Update `PipelineToMergedIndexScanRule`

The rule produces `Assemble(Scan)` replacing the Assembly subtree. Both inner and outer
pipelines use the same pattern. `EnumerableMergedIndexJoin` may become unnecessary
(Assemble handles both cases uniformly).

Rule's match pattern stays the same (match MergeJoin), but replacement now considers
the Assembly subtree: replace LCA with `Assemble(Scan)`, leave operators above LCA.

### Subtask 5: Index Creation Mode

For non-final pipelines, the physicalPlan's output feeds into the parent merged index:
`while (hasNext) { parentMI.add(physicalPlan.next()) }`.

Needs `physicalPlan` field on `Pipeline.java`. After HEP substitution, extract the
relevant subtree and store on the Pipeline.

### Suggested Implementation Order

1. Subtask 0 — Assembly subtree identification + test validation
2. Subtask 1 — Tagged row type (foundation for scan and assembly)
3. Subtask 3 — Assembly operator (Algorithm 1 implementation)
4. Subtask 2 — Scan.implement with interleaved output
5. Subtask 4 — Rule update (wires Assemble(Scan) into the plan)
6. Subtask 5 — Index creation (physicalPlan field, end-to-end pipeline execution)
7. Test with Q12, Q3-OL, Q9 — verify actual row production

### Future Work: Maintenance Mode

Same as index creation with `DeltaScan` replacing `Scan`. Reconcile with existing
`deriveIncrementalPlan()` and `maintenancePlan` field.

## Maintenance plans

Maintenance plans are basically index creation plans that process delta instead of full data. TODO