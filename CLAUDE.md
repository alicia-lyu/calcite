# Research Context: Multi-Table Merged Indexes in Apache Calcite

## Research Objective

Demonstrate that a query plan produced by a cost-based optimizer focused on
**order-based algorithms** (merge join, in-sort & in-stream grouping) immediately
permits a plan for **incremental maintenance** of query results, materialized views,
and their indexes — by replacing each sort operation with a B-tree index
("merged index"). The Calcite codebase is used as a platform to implement and
demonstrate this substitution via the planner rule infrastructure.

## Background

This project accompanies the VLDB 2026 paper:
  "Storing and Indexing Multiple Tables by Interesting Orderings"
  Wenhui Lyu, Goetz Graefe — University of Wisconsin–Madison / Google

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
| `materialize/MergedIndex.java` | Descriptor for one merged index (tables + shared collation) |
| `materialize/MergedIndexRegistry.java` | Static singleton registry mapping table sets to indexes |
| `adapter/enumerable/EnumerableMergedIndexScan.java` | Physical operator replacing the entire pipeline |
| `adapter/enumerable/PipelineToMergedIndexScanRule.java` | Planner rule matching the pipeline pattern |
| `core/src/test/java/org/apache/calcite/adapter/enumerable/PipelineToMergedIndexScanRuleTest.java` | Unit tests for the rule (2-table, simple schema) |
| `plus/src/test/java/org/apache/calcite/adapter/tpch/MergedIndexTpchPlanTest.java` | TPC-H plan tests: Q3 (partial), Q12 (full), Q3-OL variant, Q9 (6-table) |

## Implementation Notes

### Pattern matched by PipelineToMergedIndexScanRule

```
EnumerableMergeJoin
  EnumerableSort
    EnumerableTableScan [table A]
  EnumerableSort
    EnumerableTableScan [table B]
```

Optionally wrapped by `EnumerableSortedAggregate`.

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

```
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

## Session Discipline

User instructions across the files are explicitly marked by `lwh` (my initials). It should be deleted once the instruction is executed. If a problem is encountered, follow up with your comment while keeping mine.

At the end of every session (or whenever asked to wrap up / update notes):

1. **Refresh `## Next Steps` in `SESSION_PROGRESS.md`** — replace the old next-steps
   section with specific, actionable items based on what was just implemented. 
   Group these into short-term (next session) and medium-term (later sessions) buckets.
   Short-term next step should name the file/class/rule to change and describe the concrete goal.

2. **Compact verbose notes** — move long plan output dumps, old TODO prose, and
   exploration logs out of `SESSION_PROGRESS.md`. Keep only concise lessons and
   reference diagrams (DOT, ASCII). Use sub-sections with clear headings. Old next-steps may include notes that should be reorgnized elsewhere in @SESSION_PROGRESS.md, in the Javadoc of the test methods, or in CLAUDE.md.

3. **Expected plans near test code** — paste plan output snippets (BEFORE/AFTER
   structure) as Javadoc comments in the test method they belong to, not in
   `SESSION_PROGRESS.md`. Reference the full DOT diagrams in `SESSION_PROGRESS.md`
   from the Javadoc with a short note.

4. **Commit** all documentation and code changes together after the cleanup.
