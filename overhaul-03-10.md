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

### Architecture Decision: Transparent Per-Source MI Scans

Each boundary Sort in a pipeline is replaced by an `EnumerableMergedIndexScan(MI, sourceIndex)`.
Original operators (MergeJoin, SortedAggregate, etc.) remain in the plan **unchanged**.

**Key principles:**

1. **Sort defines the substitution**, not MergeJoin. Each boundary `Sort + input chain` → `MIScan(MI, sourceIndex)`.
2. **Shared meta object**: Multiple MI scan leaves in one assembly subtree reference a shared `MergedIndexScanGroup` representing the single physical linear scan. Plan stays a tree; shared reference makes one-scan reality explicit.
3. **Assembly subtree stays conceptually** — identifies which leaf scans coordinate. No operator collapse. Code moves to test utils.
4. **Nested MIs are opaque**: The outer pipeline sees the inner view as an opaque source in MI_outer. Inner operators are visible only in the index creation plan.
5. **Buffer management**: Co-locality from the shared meta object is a storage-level concern discussed in the paper, not implemented in Calcite.

**Pipeline categories:**

|              | Root pipeline | Other pipelines      |
|--------------|---------------|----------------------|
| Data flow    | Query plan    | Index creation plan  |
| Delta flow   | N/A           | Maintenance plan     |

**Example AFTER plans:**

Q12 (2-table, single root pipeline):
```text
EnumerableSort(l_shipmode)
  EnumerableSortedAggregate(...)
    EnumerableMergeJoin(orderkey)           ← STAYS
      MIScan(MI, src=ORDERS, group=G1)     ← replaces Sort→Scan(ORDERS)
      MIScan(MI, src=LINEITEM, group=G1)   ← replaces Sort→Scan(LINEITEM)
                                             G1 = shared physical scan
```

Q3-OL root query plan (outer pipeline only):
```text
EnumerableLimitSort
  EnumerableProject
    EnumerableMergeJoin(custkey)                  ← STAYS
      MIScan(MI_outer, src=inner_view, group=G2)  ← replaces Sort→(inner pipeline result)
      MIScan(MI_outer, src=CUSTOMER, group=G2)    ← replaces Sort→Scan(CUSTOMER)
```

Q3-OL inner pipeline (index creation plan, populates MI_inner):
```text
EnumerableMergeJoin(orderkey)
  EnumerableSortedAggregate → TableScan(lineitem)
  TableScan(Orders)
```

Note: the leaf pipeline's mergedIndex is null — its sources are actually read.

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

**Done.** Implemented as `TaggedRowSchema` in `materialize/TaggedRowSchema.java` (commit `c3a048e11`).

Chose the **generic tagged `Object[]`** approach with domain tags:

```text
[ domainTag0, keyVal0, domainTag1, keyVal1, ..., 'I', sourceId, p0, p1, ..., pN ]
  ╰──────────── key fields with tags ──────╯  ╰─ index ID ─╯  ╰── payload ──╯
```

- Domain tags (1 byte each): domain 0 = index identifier, domains 1..K = key fields.
- Index identifier: domain tag 0 + 1-byte source ID (0..sourceCount-1).
- Payload: non-key columns in original order, variable length per source.

`TaggedRowSchema` tracks both **slot-based positions** (for `Object[]` runtime access)
and **byte widths** (for cost estimation via `RelMdSize.averageTypeValueSize`).

Key APIs: `toTaggedRow(sourceTag, sourceRow)`, `getKeyValue(taggedRow, keyIndex)`,
`getSourceId(taggedRow)`, `taggedRowSlotCount(sourceTag)`.

Scoped to **identical keys** (all sources share the same key columns). Hierarchical
keys are future work.

Open question from user: the physical MI implementation should already store records
in a similar tagged format (byte strings). Explore whether Calcite needs to handle
type conversion between physical bytes and TaggedRow, or can assume TaggedRow directly.

### Subtask 0 (revised): Per-Source MI Scan Operator — **DONE**

Reworked `EnumerableMergedIndexScan`:
- Added `sourceIndex` field — designates which source's row type this scan produces (source-native, not tagged/joined).
- Created `MergedIndexScanGroup` class — shared meta object referenced by all leaf scans in one assembly subtree. Represents the single physical linear scan.
- `implement()`: stub returning empty enumerable. Calcite is used purely for plan generation and cost modeling; real execution happens in a separate storage system with actual B-trees/LSM-trees.
- Collation: source's `boundaryCollation` (already in source's field indices).
- Old `create(cluster, mi, rowType)` kept as deprecated overload for backward compat.

### Subtask 1 (revised): PipelineToMergedIndexScanRule — match Sort boundaries

Rewrite the rule to match individual `EnumerableSort` nodes (not `EnumerableMergeJoin`).
- Match pattern: `EnumerableSort` whose input chain resolves to a source in a registered MI.
- Replace: `Sort(input_chain)` → `MergedIndexScan(MI, sourceIndex, scanGroup)`
- Parent operators (MergeJoin, SortedAggregate, etc.) remain **untouched**.
- Need to create one `MergedIndexScanGroup` per pipeline and share it across all Sort replacements in that pipeline. Consider: rule fires per-Sort, so the group must be created on first match and reused. Could attach to the `MergedIndex` or use a rule-level map.
- The old join-matching logic and `EnumerableMergedIndexJoin` become obsolete.

### Subtask 2: Index Creation Plan

For non-root pipelines, the BEFORE plan IS the index creation plan.
- Store as `physicalPlan` field on `Pipeline`.
- Populates MI from base tables (or inner MI views).
- Leaf pipelines have `mergedIndex = null` — their sources are actually read.

### Subtask 3: Update Tests

- All AFTER plan expectations change: MergeJoin stays, leaf scans replace Sort→TableScan.
- `PipelineToMergedIndexScanRuleTest` needs rewrite for new Sort-matching rule.
- `MergedIndexTpchPlanTest` plan assertions update to expect MIScan leaves under existing MergeJoin/SortedAgg.
- Remove or deprecate `EnumerableMergedIndexJoin`-related assertions.

### Subtask 4: Cost Model

- N leaf scans sharing one physical scan: combined IO = one sequential scan, not N.
- `MergedIndexScanGroup` enables cost sharing across sibling scans.

### Execution Scope

Calcite is a query optimization framework, not a database system. It has no storage
engine. All `implement()` methods on MI operators are stubs returning empty enumerables.
Real execution (B-tree scans, tagged row filtering, record assembly) happens in a
separate storage system. Calcite's role is purely **plan generation and cost modeling**.

`TaggedRowSchema` and `MergedIndex.scanData()` exist for PoC testing of row-level
semantics in unit tests, not as production execution paths. The plan tests (Q12, Q3-OL,
Q9) validate plan structure only.

### Suggested Implementation Order

1. ~~Subtask 0 — Assembly subtree identification~~ **DONE** (moved to test utils)
2. ~~Subtask 1 — Tagged row type~~ **DONE** (TaggedRowSchema)
3. ~~Subtask 0 (revised) — Per-source MI scan operator + MergedIndexScanGroup~~ **DONE**
4. Subtask 1 (revised) — Rule update: Sort→MIScan substitution
5. Subtask 3 — Update test expectations
6. Subtask 2 — Index creation plan (physicalPlan field)
7. Subtask 4 — Cost model with scan group sharing

### Test infrastructure: `MergedIndexTestUtil` (commit `cbd4908bb`)

Shared test helpers extracted from `MergedIndexTpchPlanTest` to
`testkit/src/main/java/org/apache/calcite/test/MergedIndexTestUtil.java`
so both `core` and `plus` test suites can reuse them. Includes:
`injectSortsBeforeSortBasedOps`, `inputAlreadySorted`, `buildPipelineTree`,
`flattenPipelines`, `findAllJoins`, `countOccurrences`, `safeGetCollation`.

### Future Work: Maintenance Mode

Same as index creation with `DeltaScan` replacing `Scan`. Reconcile with existing
`deriveIncrementalPlan()` and `maintenancePlan` field.

## Maintenance plans

Maintenance plans are basically index creation plans that process delta instead of full data. TODO