# Research Context: Multi-Table Merged Indexes in Apache Calcite

## Research Objective

Demonstrate that a query plan produced by a cost-based optimizer focused on
**order-based algorithms** (merge join, in-sort & in-stream grouping) immediately
permits a plan for **incremental maintenance** of query results, materialized views,
and their indexes — by replacing each sort operation with a B-tree index
("merged index"). The Calcite codebase is used as a platform to implement and
demonstrate this substitution via the planner rule infrastructure.

## Background

This project extends the prior work documented in `main.md` (a summary of `main.tex`):
  "Storing and Indexing Multiple Tables by Interesting Orderings"
  Wenhui Lyu, Goetz Graefe — University of Wisconsin–Madison / Google (VLDB 2026)

The prior paper covers single-pipeline merged indexes. This Calcite implementation
is a new study extending to multi-pipeline queries. `main.md` provides essential
background on merged index concepts, but the current project's contributions go
beyond what that paper describes.

### Merged Indexes (the key concept)

A **merged index** is a B-tree (or LSM-tree) that stores records from multiple
tables interleaved by a **shared sort key**. For example, a merged index on
(Customer, Orders, Invoice) sorted by `custkey` physically co-locates each
customer record with all its orders and invoices.

Key properties:
- **Partial pre-computation**: sorting/clustering happens at update time (like
  a traditional index), not at query time (like a materialized view)
- **1-to-1 update cost**: one base-table insert/delete triggers one index update
  (not 1-to-N like materialized views)
- **Single-pass query execution**: a range scan over the merged index can execute
  an entire join+grouping pipeline by assembling records on the fly
- **Space-efficient**: stores the same data as individual single-table indexes
  plus minimal overhead (domain tags, ~1 byte each)

### Interesting Orderings (Selinger 1979)

Selinger's interesting ordering technique identifies when multiple operators in a
query plan share a sort order, allowing sort-based operators (merge join, sorted
aggregation) to reuse sorted output from earlier operators. For a 3-way merge
join on `custkey`, only 3 sorts are needed (not 4) because the first join's
output is already sorted.

This technique naturally divides complex queries into **order-based pipelines**:
groups of operators (joins, aggregations) that all share a common key/sort order.

### How Calcite Implements Interesting Orderings (verified from source)

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

### The Core Insight This Implementation Demonstrates

An order-based query plan pipeline:

  Sort(A) ──→ MergeJoin ──→ SortedAgg
  Sort(B) ──→/

is structurally identical to an incremental maintenance plan when each Sort is
replaced by a B-tree (merged) index. Both sorts here uses the merged index that stores A and B interleaved
by their shared key, so one sequential scan replaces the entire pipeline.

**For each pipeline, only ONE scan over the merged index is needed** — not one
scan per table. The scan internally assembles joins and computes aggregations.

## Grand Picture of the Project

Apache Calcite is a SQL query optimization framework. It is NOT a database
system — it has no storage engine. It provides:

### Directly relevant to the research
- `core/src/main/java/org/apache/calcite/rel/` — relational algebra operators
  - `core/` — abstract base operators (Sort, Join, Aggregate, etc.)
  - `logical/` — logical (pre-physical) operator variants
  - `rules/` — ~150 planner rewrite rules
  - `metadata/` — cost and statistics metadata providers
- `core/src/main/java/org/apache/calcite/adapter/enumerable/` — physical
  (executable) operator implementations: EnumerableSort, EnumerableMergeJoin,
  EnumerableSortedAggregate, EnumerableTableScan, etc.
- `core/src/main/java/org/apache/calcite/plan/` — Volcano and HEP planners,
  RelOptCost, RelTrait, RelCollation (sort order traits)
- `core/src/main/java/org/apache/calcite/materialize/` — materialized view and
  lattice infrastructure (where MergedIndex abstractions belong)

### Less relevant to the research (infrastructure/adapters)
- `core/src/main/java/org/apache/calcite/sql/` — SQL parsing and validation
- `core/src/main/java/org/apache/calcite/sql2rel/` — SQL→relational conversion
- Adapter modules: `cassandra/`, `druid/`, `elasticsearch/`, `mongodb/`, etc.
- `server/`, `babel/`, `pig/`, `spark/`, etc. — non-core adapters

## Technical Notes 

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

### Q9 Interesting Ordering Constraints

Original TPC-H Q9 uses 5 distinct join keys over 6 tables:

| Join | Key |
|------|-----|
| LINEITEM ⋈ ORDERS | `l_orderkey = o_orderkey` |
| LINEITEM ⋈ PARTSUPP | `l_partkey = ps_partkey AND l_suppkey = ps_suppkey` (compound) |
| LINEITEM ⋈ SUPPLIER | `l_suppkey = s_suppkey` |
| SUPPLIER ⋈ NATION | `s_nationkey = n_nationkey` |
| LINEITEM ⋈ PART | `l_partkey = p_partkey` |

These keys form at most **two partial interesting-ordering chains**:
1. `orderkey` → ORDERS ⋈ LINEITEM (one leaf merged index)
2. `(partkey, suppkey)` → PARTSUPP; `suppkey` prefix → SUPPLIER (chain; nationkey needs a fresh sort)

PART always requires its own sort on `partkey` — an unavoidable disruption because partkey
conflicts with both the orderkey and suppkey chains.

**Rewritten join order** in the test (`tpchQ9`) minimizes extra sorts by fixing a
left-deep tree: ORDERS ⋈ LINEITEM → PART → PARTSUPP → SUPPLIER → NATION.
- `injectSortsBeforeJoin` injects Sort(orderkey) at the leaf → **one merged index** (ORDERS, LINEITEM)
- Sort(partkey) before PART (semi-join filter applied early)
- Sort(partkey, suppkey) before PARTSUPP — matches PARTSUPP PK `(partkey, suppkey)`
- Sort(suppkey) before SUPPLIER — (partkey, suppkey) prefix does NOT give suppkey alone, so re-sort needed
- Sort(nationkey) before NATION

The PARTSUPP condition must be written `ps_partkey = l_partkey AND ps_suppkey = l_suppkey`
(partkey first) to match PARTSUPP's PK order and get the correct compound collation from
`splitJoinCondition`.

### Design Decisions

**Join order in `tpchQ3OrdersLineitem`** — uses manually rewritten SQL to force
ORDERS ⋈ LINEITEM as the leaf join (reverse of Q3). Calcite's Volcano planner preserves
SQL join order because no join-reorder rules are registered; adding them would not
reliably produce the desired order at scale 0.01. The manual rewrite is intentional
test design, not a workaround.

**Hierarchical vs. independent keys** — hierarchical merged indexes (§3.2) apply when
join keys form a prefix chain (e.g., `(nationkey)` ⊂ `(nationkey, statekey)`). Q3-OL
does **not** qualify: `o_orderkey` and `o_custkey` are independent global surrogates.
Even though `o_orderkey → o_custkey` (FK), orderkey is NOT structured as
`(custkey, local_id)`. Q3-OL therefore requires two separate merged indexes: inner
(LINEITEM+ORDERS by orderkey), outer (inner_view+CUSTOMER by custkey). `MergedIndex
.satisfies()` uses exact prefix matching; FD-based equivalence is future work.

**Q9 two-tier plan** — the AFTER plan has a query tier (one `EnumerableMergedIndexScan`
of the indexed view on nationkey and o_year DESC) and a maintenance tier (the 5 intermediate
join pipelines, absorbed into nested merged indexes at update time). `EnumerableFilter(p_name
LIKE '%green%')` is applied after the indexed view scan because the PART filter cannot be
pushed below the assembled join result.

## Key Files for the Merged Index Feature

| File | Purpose |
|------|---------|
| `adapter/enumerable/EnumerableTableScan.java` | Template for new scan node; line ~84 explicitly anticipates an IndexScan in `passThrough` |
| `adapter/enumerable/EnumerableMergeJoin.java` | Physical merge join — part of the pipeline being replaced |
| `adapter/enumerable/EnumerableSortedAggregate.java` | Physical sorted aggregation — part of the pipeline |
| `adapter/enumerable/EnumerableSort.java` | Physical sort — part of the pipeline |
| `adapter/enumerable/EnumerableRules.java` | Registry of enumerable rules; add new rule constant here |
| `rel/RelCollation.java`, `RelCollationImpl.java` | Sort order trait |
| `rel/RelCollationTraitDef.java` | Trait definition for collation |
| `rel/metadata/RelMdCollation.java` | Metadata handler for collation |
| `plan/RelOptCost.java`, `plan/volcano/VolcanoCost.java` | Cost model |
| `rel/rules/SortRemoveRule.java` | Pattern for `RelRule<Config>` + `@Value.Immutable` |
| `materialize/MaterializationService.java` | Convention for materialize/ package |
| `materialize/Lattice.java` | Example of multi-table structure in materialize/ |

## New Files Added by This Feature

| File | Purpose |
|------|---------|
| `materialize/MergedIndex.java` | Descriptor for one merged index; `sources` field holds `RelOptTable` or inner `MergedIndex` |
| `materialize/MergedIndexRegistry.java` | Static singleton registry; `findFor(List<Object>, RelCollation)` with name/identity matching |
| `adapter/enumerable/EnumerableMergedIndexScan.java` | Leaf scan replacing the entire inner pipeline |
| `adapter/enumerable/EnumerableMergedIndexJoin.java` | Query-time assembly operator for outer pipeline (wraps a scan) |
| `adapter/enumerable/PipelineToMergedIndexScanRule.java` | Planner rule: inner pipeline → scan; outer pipeline → join+scan |
| `core/src/test/java/org/apache/calcite/adapter/enumerable/PipelineToMergedIndexScanRuleTest.java` | Unit tests for the rule (2-table, simple schema) |
| `materialize/TaggedRowSchema.java` | Tagged interleaved row metadata: slot positions, byte widths, `toTaggedRow()` conversion |
| `materialize/Pipeline.java` | Order-based pipeline descriptor (root, sources, collation) |
| `plus/src/test/java/org/apache/calcite/adapter/tpch/MergedIndexTpchPlanTest.java` | TPC-H plan tests: Q3 (partial), Q12 (full), Q3-OL (full 3-table), Q9 (6-table) |
| `testkit/src/main/java/org/apache/calcite/test/MergedIndexTestUtil.java` | Shared test helpers: sort injection, pipeline discovery, tree search |

## Implementation Notes

"Bottom side"/earlier refers to input side. "Top side"/later refers to output side. Use smaller number for bottom side.

### Calcite Planner Architecture: Volcano vs HEP

Calcite has **two independent planner engines**. Understanding both is essential for
this project because we use them in sequence.

#### Volcano (Phase 1) — Cost-Based Optimizer

The classic Cascade/Volcano framework:
- Explores equivalent plans via **rules** that fire on pattern matches
- Maintains a **memo** of equivalent expressions grouped by properties (traits like
  sort order, convention/physical-operator-set)
- Multiple rules can fire on the same node, producing **alternatives**
- The planner picks the **cheapest** plan using the cost model

**In our pipeline**: Phase 1 uses Volcano to convert the logical plan (LogicalJoin,
LogicalTableScan, etc.) to a physical plan (EnumerableMergeJoin, EnumerableSort,
EnumerableTableScan, etc.). It chooses between hash join vs merge join, hash aggregate
vs sorted aggregate, etc. — all based on cost.

#### HEP (Phase 2) — Heuristic/Deterministic Rewriter

A simpler, **deterministic rewrite engine** (no cost comparison):
- Applies rules as **unconditional rewrites** — if a rule matches, the replacement happens
- Processes the plan in a configurable traversal order:
  - `BOTTOM_UP`: leaves first, then ancestors (what we use — inner Sorts fire before outer)
  - `TOP_DOWN`: root first, then descendants
  - `DEPTH_FIRST`: avoids re-processing after each transformation
  - `ARBITRARY`: default, efficient, order doesn't matter
- Think of HEP as **"find-and-replace for plan trees"**

**In our pipeline**: Phase 2 uses HEP to apply `PipelineToMergedIndexScanRule`. This is
a deterministic rewrite — if a Sort's input matches a registered MI, replace it. No cost
comparison needed (MI scan is always preferred). HEP is ideal because we want unconditional
substitution and need control over traversal order.

#### What Is a "Rule"?

A **rule** is a pattern-match-and-transform unit with two parts:
1. **Operand pattern**: What plan node to match (e.g., `EnumerableSort` with no FETCH/OFFSET)
2. **`onMatch(call)`**: Inspect the node, optionally call `call.transformTo(replacement)`

In Volcano, multiple rules can fire on the same node → produces alternatives → cost picks
winner. In HEP, rules fire and replace unconditionally.

#### Why BOTTOM_UP Matters for Nested Pipelines

For nested pipelines (Q3-OL, Q9), inner boundary Sorts must be replaced by MIScans
**before** outer Sorts fire. Otherwise, the outer Sort can't recognize its inner subtree
as an MI view. `BOTTOM_UP` guarantees this ordering:

```text
Step 1: Inner Sort(orderkey) over ORDERS    → MIScan(innerMI, 0)  ← leaf, fires first
Step 2: Inner Sort(orderkey) over LINEITEM  → MIScan(innerMI, 1)  ← leaf, fires second
Step 3: Outer Sort(custkey) over inner result → sees MIScans → recognizes innerMI → MIScan(outerMI, 0)
Step 4: Outer Sort(custkey) over CUSTOMER   → MIScan(outerMI, 1)
```

Previously we used **N sequential HepPlanner instances** (one per nesting level) to
achieve this ordering. With `BOTTOM_UP`, a single HEP pass suffices.

### Architecture: Sort-Boundary-Based Pipeline Replacement

**Current design** (as of 2026-03-23): `PipelineToMergedIndexScanRule` is centered on
`EnumerableSort` nodes at pipeline boundaries, not on `MergeJoin` nodes.

**Why**: A Sort boundary marks the transition between two pipelines. When a Sort's input
contains a registered merged index, the entire subtree (including joins, aggregations,
and filters) gets absorbed into that index at update time. The query-time plan replaces
the Sort with a direct scan (`EnumerableMergedIndexScan`).

**Two cases**:

1. **Inner pipeline** — both sources are base tables (`RelOptTable`):
   ```text
   EnumerableSort(key)
     EnumerableMergeJoin(key)
       EnumerableSort → (aggregate/project chain →) EnumerableTableScan [A]
       EnumerableSort → EnumerableTableScan [B]
   ```
   Replaced by: `EnumerableMergedIndexScan(innerMI, sourceIdx, group)` (per source).
   Join assembly + aggregation are pre-computed at maintenance time; no assembly at query time.

2. **Outer pipeline** — source is an inner `MergedIndex` view plus a base table:
   ```text
   EnumerableSort(key)
     EnumerableMergeJoin(key)
       EnumerableSort → EnumerableMergedIndexScan [inner view]
       EnumerableSort → EnumerableTableScan [C]
   ```
   Replaced by: `EnumerableMergedIndexScan(outerMI, sourceIdx, group)` (per source).
   The parent `MergeJoin` stays in the plan; it assembles outer records at query time.

**Matching strategy** (structural, not identity-based):
- For each registered `MergedIndex`, check if the Sort's input contains a leaf table
  matching one of the MI's sources.
- Use qualified table name (schema + table) for matching because HEP copies non-leaf
  RelNodes, destroying identity.
- Leaf `EnumerableTableScan` nodes survive HEP's node-copying because they have no
  children (not wrapped in `HepRelVertex`).

### Two Production Paths for Merged Index Optimization

There are two distinct scenarios; the current rule only handles PATH A:

### MergedIndex.of() — mixed sources factory

Use `MergedIndex.of(List<Object>, ...)` when any source is an inner `MergedIndex`
view. The `sources` field holds `RelOptTable` (base table) or `MergedIndex` (inner
view). The flat `tables` field expands all views to base tables for display.

```java
// Inner pipeline (base tables only):
new MergedIndex(List.of(tableA, tableB), collations, collation, rowCount)

// Outer pipeline (inner view + base table):
MergedIndex.of(List.of(innerMergedIndex, tableC), collations, collation, rowCount)
```

### HepRelVertex unwrapping in PipelineToMergedIndexScanRule

In HEP, all nodes are wrapped in `HepRelVertex`. `join.getLeft()` returns a
`HepRelVertex`, not the actual `EnumerableSort`. The rule uses a private helper:

```java
private static RelNode unwrap(RelNode node) {
  return node instanceof HepRelVertex
      ? ((HepRelVertex) node).getCurrentRel() : node;
}
```

Every access to `join.getLeft()`, `join.getRight()`, and `sort.getInput()` must
call `unwrap()` first to get the actual rel node.

### Multi-stage HEP for nested pipelines (Q3-OL, Q9)

When an outer pipeline depends on an inner pipeline, use **BOTTOM_UP traversal** in
HEP to ensure leaf pipelines are replaced first. Register merged indexes incrementally
(leaf-to-root) across multiple HEP passes:

```java
// Pass 1: Replace inner Sort(orderkey) → MIScan(innerMI, 0 and 1)
HepPlanner pass1 = new HepPlanner(hepProgram);
pass1.setRoot(phase1Plan);
RelNode afterPass1 = pass1.findBestExp();

// Between passes: register inner MI, extract indexCreationPlan
MergedIndex innerMI = new MergedIndex(innerPipeline);
MergedIndexRegistry.register(innerMI);

// Pass 2: Replace outer Sort(custkey) → MIScan(outerMI, 0 and 1)
HepPlanner pass2 = new HepPlanner(hepProgram);
pass2.setRoot(afterPass1);
RelNode phase2Plan = pass2.findBestExp();

// Register outer MI, extract indexCreationPlan
MergedIndex outerMI = new MergedIndex(outerPipeline);
MergedIndexRegistry.register(outerMI);
```

**Why multiple passes**: a single pass with BOTTOM_UP can replace multiple pipeline
levels, but for complex queries (Q9 with 6 tables), explicit per-level registration
makes the index creation plan structure clear. Each pass writes `pipeline.indexCreationPlan`
after HEP completes.

### Cost model for EnumerableMergedIndexScan

The scan is always cheaper than the pipeline it replaces because:

- Sorts (O(N log N)) are eliminated — data is pre-sorted in the index
- N table scans collapse into one sequential pass
- Cost: `rowCount=ΣTᵢ, cpu=ΣTᵢ*0.1, io=ΣTᵢ`

### Rule registration

`PipelineToMergedIndexScanRule` is registered as a constant in `EnumerableRules`
but NOT added to `ENUMERABLE_RULES` list — callers opt in explicitly (e.g., tests).

### Critical design point

The merged index stores records from ALL participating tables interleaved, so
**the entire pipeline (sort + merge join + sorted agg) collapses into ONE scan**.
This is not just replacing the sort leaves — the join and aggregation are also
eliminated and performed internally by the scan operator.

### Two Production Paths for Merged Index Optimization

There are two distinct scenarios; the current rule only handles PATH A:

```text
PATH A — Substitution (current PoC, what the tests demonstrate)
────────────────────────────────────────────────────────────────
Tables do NOT report collation (getStatistic() = Statistics.UNKNOWN).
Volcano adds EnumerableSort nodes before the join at planning time.
PipelineToMergedIndexScanRule matches:
  EnumerableMergeJoin
    ├─ EnumerableSort → EnumerableTableScan(A)   ← explicit sort present
    └─ EnumerableSort → EnumerableTableScan(B)
Then replaces the whole pipeline with EnumerableMergedIndexScan.

injectSortsBeforeJoin() in the test simulates this path: it forces
LogicalSort nodes into the plan so ENUMERABLE_SORT_RULE has something
to convert. Without injection, Volcano cannot satisfy the merge-join's
collation requirement (AbstractConverter nodes are left unresolved).

PATH B — Native (production-correct design, not yet implemented)
────────────────────────────────────────────────────────────────
Tables backed by a merged index report collation via
  getStatistic().getCollations() → Statistics.of(n, keys, collations)
EnumerableTableScan carries the collation in its trait set.
Volcano's trait enforcement skips RelCollationTraitDef.convert() →
no EnumerableSort is added. The plan becomes:
  EnumerableMergeJoin
    ├─ EnumerableTableScan(A)[collation=[0]]   ← sort-free scan
    └─ EnumerableTableScan(B)[collation=[0]]
Fix needed: add a second operand pattern to PipelineToMergedIndexScanRule
matching EnumerableMergeJoin over bare scans with the correct collation trait.
```

### Config requirements for order-based testing

- `traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)` and
  `ENUMERABLE_SORTED_AGGREGATE_RULE` must both be registered.
- **`ENUMERABLE_AGGREGATE_RULE` must also be present** — without it, the Volcano
  planner throws `CannotPlanException` because `EnumerableSortedAggregate
  .passThroughTraits` cannot resolve when sort requirements propagate through a
  `LogicalProject`. The planner picks between hash and sorted aggregate by cost.
- **`ENUMERABLE_FILTER_RULE`** is required for queries with WHERE predicates (e.g.,
  Q9's `p_name LIKE '%green%'`).

### Test infrastructure patterns

**`injectSortsBeforeJoin(RelNode)`** — recursive helper used in tests. Wraps each
`Join` input in a `LogicalSort` so `ENUMERABLE_SORT_RULE` has nodes to convert.

- Uses `RelOptUtil.splitJoinCondition` to extract per-join equi-keys automatically.
- Guards against empty key lists (non-equi / cross joins): skips injection rather
  than crashing with `IndexOutOfBoundsException`.
- Builds multi-column collations from all equi-join keys (e.g., PARTSUPP on both
  suppkey and partkey), not just the first key.

**`findLeafMergeJoin(RelNode)`** — locates the `EnumerableMergeJoin` whose both
inputs are `Sort → TableScan`. Used in Q3 to target the inner (leaf) join for
partial substitution while leaving the outer join intact.

**Q9 SQL form** — use explicit `JOIN … ON …` syntax (not comma-separated FROM with
WHERE) so that `splitJoinCondition` can extract equi-join keys from each join node.

**Phase 2 planner** — always use `HepPlanner` (not Volcano `Programs.ofRules`) when
applying `PipelineToMergedIndexScanRule` to an already-physical plan. Volcano cannot
build a complete optimal plan from a single transformation rule.

### Filter Hoisting: Widen-Then-Narrow Technique

Predicates nested below Projects or SortedAggregates reference wide row types (all upstream
output columns). Direct hoisting above the boundary Sort invalidates these references.
**Solution**: a pre-pass that widens intermediate Projects to preserve filter field indices,
then hoists the filter, then optionally narrows the root if the row type changed.

**Three-stage algorithm** (`hoistFiltersAboveBoundaries` in `MergedIndexTestUtil`):

1. **Widen pass** (`widenProjectsForFilters`):
   - Post-order walk: for each Project on an ancestor path of a filter, collect field indices
     that the filter references but the Project would drop.
   - Append `RexInputRef` projections for those indices, preserving field positions.
   - **Join handling**: if a Project feeds a filter with right-side refs, shift indices by
     `(widened_left_count - original_left_count)`.
   - **Aggregate/SetOp**: do NOT propagate `needed` upward — widening stops at these operators
     (their output cardinality / structure differs from input).

2. **Commute**: `Filter.passThroughTraits()` and project/aggregate rewrite rules allow filters
   to bubble upward through widened operators.

3. **Narrow** (optional): if the root node's row type changed (widening added columns), wrap
   in a final `Project` that selects only the original columns, restoring the output type.

**Outcome for Q9**: `p_name LIKE '%green%'` filter sits directly between SortedAggregate and
the top boundary Sort. The P5 indexed view is filter-free and reusable across queries with
different PART predicates (e.g., `LIKE '%red%'`).

**Pipeline discovery**: filter hoisting does NOT change pipeline boundaries. Boundary Sorts are
defined structurally by their position in the plan tree. Filters moved above a boundary Sort
are orthogonal to pipeline source matching.

### Future work: Sort-based operator enhancements

Current `injectSortsBeforeSortBasedOps` always creates ASC sorts. Future improvements:

- **Sort direction alignment**: Sort-based operators (merge join, sorted aggregation,
  DISTINCT) don't inherently require a direction. When a later operator specifies
  direction requirements (e.g., `ORDER BY o_year DESC`), proactively use that
  direction in the injected sort to avoid redundant re-sorts.
- **Window functions** (`OVER`): require sorting on PARTITION BY + ORDER BY keys.
  Need to generalize `injectSortsBeforeSortBasedOps` to recognize window operators
  and inject multi-key sorts on partition + order columns.
- **DISTINCT / set operators** (INTERSECT, EXCEPT): sort-based implementations need
  a sort on all output columns. Current helper only handles equi-join keys.
- **Merge-sort joins with non-equi conditions**: currently skipped due to cross-join
  guard. Some could benefit from partial key injection if the non-equi condition is
  independent of the equi key.

## Style of coding and documentation

- Prepend `[MergedIndex]` to all commit messages.
- If staged changes are large and consist different objectives, break them into multiple commits with clear messages. Even consider breaking changes in a single file into multiple commits if they are logically distinct.

## Design Considerations

This project is a new research study extending the prior paper (`main.md`). The
implementation demonstrates feasibility while design policy questions (materialization
decisions, cost trade-offs, coverage scope) are the primary research contribution.

- When encountering a design choice with multiple valid approaches, document it in
  `DESIGN_OPTIONS.md` with: the question, alternatives, trade-offs, current default, and
  references to relevant code/paper sections.
- Distinguish **engineering decisions** (how to implement) from **design policy** (what to
  implement / when to apply). Policy decisions belong in `DESIGN_OPTIONS.md`.
- When a default is chosen for engineering progress, note it as "current default" so it can
  be revisited in research discussions.

## Session Discipline

User instructions across the files. Some are explicitly marked by `lwh` (my initials), some are not so just use your best judgment. They should be deleted once the instruction is executed. If a problem is encountered, follow up with your comment while keeping mine.

Commit early and often with meaningful commit messages, not necessarily when a feature is fully implemented, not necessarily when the program is bug-free. Don't wait for my explicit request.
Minor changes by me can also be staged and commited along with your changes, they are usually new notes for you.
After any fully implemented feature with all tests passed, it should be properly tagged, like a snapshot.

At the end of every session (or whenever asked to wrap up / update notes):

1. **Refresh `## Next Steps` in `SESSION_PROGRESS.md`** — replace the old next-steps
   section with specific, actionable items based on what was just implemented.
   Group these into short-term (next session) and medium-term (later sessions) buckets.
   Short-term next step should name the file/class/rule to change and describe the concrete goal.

2. Take note of any confusion that led to plan reiterations, dead-ends, design changes, and implementation bugs. Especially record useful user inputs.

3. If a session ends with half-implemented code or a plan that is not fully tested, add a "Work in Progress" note to the next steps with a clear description of what is incomplete and what the next session should focus on to finish it. Be specific and comprehensive with the conversation history of this session.

4. **Compact verbose notes** — move long plan output dumps, old TODO prose, and
   exploration logs out of `SESSION_PROGRESS.md`. Keep only concise lessons and
   reference diagrams (DOT, ASCII). Use sub-sections with clear headings. Old next-steps may include notes that should be reorgnized elsewhere in @SESSION_PROGRESS.md, in the Javadoc of the test methods, or in CLAUDE.md.

5. **Expected plans near test code** — paste plan output snippets (BEFORE/AFTER
   structure) as Javadoc comments in the test method they belong to, not in
   `SESSION_PROGRESS.md`. Reference the full DOT diagrams in `SESSION_PROGRESS.md`
   from the Javadoc with a short note.

6. **Commit** all documentation and code changes together after the cleanup.
