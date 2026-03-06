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

## Session Discipline

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
