# Research Context: Multi-Pipeline Merged Indexes in Apache Calcite

## Research Objective and Active Scope

This fork is a research artifact demonstrating rule-based decomposition of queries
into order-sharing pipelines, substitution with merged-index (MI) scans, and
construction of index-creation and maintenance plan trees. It extends the
single-pipeline work in the [published paper](https://www.vldb.org/pvldb/vol19/p3621-lyu.pdf).
See [main.md](main.md) for background.

**Direction confirmed September 2026:** resume multiple-pipeline research. Calcite
provides reproducible reference plans for the paper artifact; actual execution
will be implemented manually in C++ in LeanStore later. Those implementations may
adapt the reference plans, as happened with Q3/Q5/Q10. Record the adaptations and
validate their semantics rather than requiring literal operator-for-operator
translation. Automatic plan export/interpreter integration and an MI-aware
cost-based optimizer are deferred.

The current implementation demonstrates plan construction for three examples.
It does not yet establish complete TPC-H semantics, executable cascade maintenance,
or optimal physical design. The four future milestones require separate planning;
the September documentation sweep does not select new execution algorithms.

## Reading Guide

- [SESSION_PROGRESS.md](SESSION_PROGRESS.md): verified status, query coverage,
  known gaps, test commands, and next planning tasks. Start here each session.
- [DESIGN_OPTIONS.md](DESIGN_OPTIONS.md): implemented mechanisms, experimental
  defaults, and unresolved policy choices.
- [RESEARCH_QUESTIONS.md](RESEARCH_QUESTIONS.md): maintenance-efficiency questions
  and hypotheses requiring analysis or execution evidence.
- [CALCITE_LEANSTORE_INTEGRATION.md](CALCITE_LEANSTORE_INTEGRATION.md): manual
  handoff and the deferred automatic-bridge proposal.
- [SINGLE_PIPELINE.md](SINGLE_PIPELINE.md): historical single-MI work from the
  published-paper phase; it is not the active next-steps list.

## Core Concepts

A merged index interleaves records from multiple sources using compatible sort
keys. Interesting orderings allow joins and sorted aggregations to reuse those
orders. One physical scan can then support assembly of an order-sharing pipeline.
In this fork, a source can itself be a preceding pipeline's output.

Distinguish three operations:

1. Store a source record in an MI. For a raw base-record MI, one source insertion
   adds one corresponding entry to that index.
2. Assemble a pipeline's output from its sources. Joins and aggregates execute
   here; the output need not have the same cardinality as the changed input.
3. Store that output in a downstream MI under another order. This creates a
   materialized dependency and can require multiple writes per base-table change.

The single-pipeline 1-to-1 storage property does not imply 1-to-1 maintenance
through a cascade. Fanout depends on the changed relation, matching rows,
aggregations, and downstream storage. Physical scan sharing is an execution
objective represented by metadata, not demonstrated by the Java scan stubs.

State the maintenance claim precisely. An update to one MI can be 1-to-1 with
that MI's stored entry or state, just like an ordinary index update. A cascade
across MIs is not globally 1-to-1: the delta emitted by `MI_leaf` can fan out,
change an aggregate, or become multiple updates to `MI_root`. Keep "per-MI
update" and "across-MI cascade" separate in all future notes.

Likewise, "one scan" is valid when referring to the root MI scan used to enter
the remaining query execution. It is an overstatement only if it is used to imply
that the Java `EnumerableMergedIndexScan` already implements storage execution,
or that no operators remain above the root MI scan.

## Current Planner Architecture

### Phase 1: construct an order-based physical plan

The TPC-H examples use Volcano with a restricted rule set, manually shaped SQL
join orders, and `MergedIndexTestUtil.injectSortsBeforeSortBasedOps`. The tests
register merge-join conversion but no general join-reordering search. Both hash
and sorted aggregate conversions are available. Calling this an MI-aware
cost-based optimizer would overstate the implementation.

Calcite's collation traits describe sort order. `EnumerableMergeJoin` derives and
passes through compatible collations; `EnumerableSortedAggregate` can request
sorted input. Trait propagation alone does not prove the final SQL ORDER BY is
satisfied, especially across a hash aggregate.

The helpers hoist filters on the logical plan before Volcano and split physical
`EnumerableLimitSort` into `EnumerableLimit(EnumerableSort(...))` before pipeline
discovery. This makes the sort a replaceable boundary while preserving LIMIT.
Filter hoisting widens projects to retain predicate columns and narrows the result
when necessary. Widening stops at aggregate/set-operation boundaries; semantic
validation beyond the existing examples remains necessary.

### Phase 2: discover pipelines and substitute registered indexes

`Pipeline.buildTree` cuts at sort boundaries; `flatten` returns relevant pipelines
in post-order. The tests construct `new MergedIndex(pipeline)`, register indexes
incrementally from leaves toward the root, and run a separate HEP pass per level.
Do not replace this description with the older claim of one universal HEP pass.

`PipelineToMergedIndexScanRule` matches a boundary Sort and replaces it with an
`EnumerableMergedIndexScan` for one source. Parent joins and aggregates remain
explicit at that stage:

```text
MergeJoin                              MergeJoin
  Sort -> source A       becomes         MIScan(MI, source 0, group G)
  Sort -> source B                       MIScan(MI, source 1, group G)
```

A later substitution can replace a sort over an assembled child pipeline with
a scan of that child's output stored in the parent MI. This moves the child
assembly into an index-creation/maintenance plan. It does not mean the per-source
scan operator itself implements arbitrary joins or aggregates.

### Descriptors, matching, and execution limits

- `MergedIndex` wraps a `Pipeline`; `getSources()` returns pipeline sources.
  The old mixed `List<Object>` constructor/factory descriptions are obsolete.
- `MergedIndexRegistry.findForSource` checks source boundary collation and matches
  by node identity, leaf table qualified name, or nested MI identity. Qualified
  names alone do not distinguish repeated occurrences of a table; self-join and
  subtree-equivalence safety need further validation.
- HEP wraps nodes in `HepRelVertex`. Unwrap nodes when inspecting their actual
  operator type or children in rules.
- `MergedIndexScanGroup` associates logical source scans with an intended shared
  physical scan. It does not implement that scan or coordinate storage readers.
- Scan cost estimates divide total rows by source count for row/CPU estimates
  and charge full index I/O to each scan. HEP substitution does not consult these
  costs. There is no evidence that every substitution is always cheaper.
- MI rules are opt-in; they are not enabled for all Enumerable planning.
- `EnumerableMergedIndexScan.implement()` and the delta scan's implementation
  return empty enumerables. These operators are plan representations here.

The current path requires explicit sort boundaries. Native use of already-sorted
storage without those boundaries is deferred, not a prerequisite of the current
artifact scope.

### Index creation and maintenance

The non-root pipeline's `indexCreationPlan` reads its MI sources, assembles output,
and produces rows for the parent MI. The root's execution is the query plan.
`MaintenancePlanConverter` scopes logical roots to child-pipeline boundaries,
uses StreamRules to derive delta trees, and converts them to Enumerable operators.
Both logical derivation and physical conversion exist.

Physical conversion is not execution validation. Signed changes, aggregate
replacement/retraction, concurrent or batched changes, old/new input visibility,
and downstream update application still require an execution contract. In
particular, a StreamRules delta aggregate is not by itself a general solution
for maintaining aggregate results under insertions and deletions.

`Pipeline.captureLogicalRoots` associates logical and physical subtrees using
traversal order with a fallback for split LIMIT sorts. New query shapes must
validate these associations rather than assume they generalize.

### Plan intent not fully recoverable from DOT

DOT files are useful for recovering operator shape, substitution points, and
stored plan trees. They do not fully recover why a plan was shaped that way.
Preserve these design notes unless the code itself changes the invariant:

- **Volcano then HEP:** Phase 1 builds an order-based Enumerable plan. Phase 2
  applies deterministic HEP substitutions after MI registration. HEP is used
  because the substitutions are a research artifact choice, not a planner cost
  alternative.
- **Sort boundaries:** a boundary Sort marks the transition between pipelines.
  Replacing the boundary with an MI scan means the boundary's input has become
  stored or maintained under that MI's order.
- **Nested order:** inner pipeline substitutions must be available before a
  parent pipeline can treat the child output as an MI source. The tests currently
  make this explicit with leaf-to-root registration and repeated HEP passes.
- **Root scan:** a fully substituted root can be represented as one root-MI scan
  followed by any remaining query operators. That is the intended "one scan"
  query-time property for the stored root pipeline.
- **Maintenance tier:** non-root pipeline plans are not discarded. They become
  index-creation and maintenance plans that populate the parent MI.

## Query-Specific Lessons

- **Q3:** the current example preaggregates LINEITEM by orderkey, joins ORDERS,
  then CUSTOMER, and adds a revenue/date sort. It omits the real Q3 predicates.
  See the status document before treating it as benchmark SQL.
- **Q3 join order:** `tpchQ3OrdersLineitem` manually shapes SQL so ORDERS joins
  LINEITEM as the leaf pipeline before CUSTOMER. This is intentional test design:
  the registered rule set does not include general join reordering, and relying
  on Volcano to rediscover this exact shape would make the artifact brittle.
- **Functional dependencies:** `orderkey -> custkey` does not make an orderkey
  scan custkey-sorted. LeanStore's COL design explicitly extends keys to
  `(custkey, orderkey, linenumber)`; the Calcite examples do not model this
  physical design. This is an implementation limitation, not an impossibility
  of three-table MIs.
- **Q9:** the example fixes the join sequence ORDERS–LINEITEM–PART–PARTSUPP–
  SUPPLIER–NATION. It is not proven to minimize sorts or indexes. `(partkey,
  suppkey)` supplies partkey order, not suppkey order. The helper aligns a sort
  with nation/year ordering but removes ORDER BY above a hash aggregate;
  the final required order is not guaranteed.
- **Q9 interesting-order constraints:** the original query uses orderkey,
  partkey, `(partkey, suppkey)`, suppkey, and nationkey. PART breaks the
  orderkey and suppkey chains. The test writes the PARTSUPP condition as
  `ps_partkey = l_partkey AND ps_suppkey = l_suppkey` so
  `splitJoinCondition` extracts the compound key in PARTSUPP primary-key order.
- **Q9 query tier vs. maintenance tier:** the intended fully substituted shape
  is a query tier entered through one indexed-view/root-MI scan, plus maintenance
  tiers for the intermediate joins. A PART filter above the root scan is a
  query-time remainder unless that predicate is deliberately baked into an MI.
- **Q12:** shipmode grouping remains at query time. The second MI stores
  intermediate joined/projected rows, not the final grouped answer.

## Source Map

Paths below are relative to `core/src/main/java/org/apache/calcite/` unless noted.

| Component | Purpose |
|-----------|---------|
| `materialize/Pipeline.java` | Pipeline discovery, boundaries, logical-root capture |
| `materialize/MergedIndex.java` | Pipeline-backed MI descriptor and stored plans |
| `materialize/MergedIndexRegistry.java` | Registration and source matching |
| `materialize/MergedIndexScanGroup.java` | Shared-scan metadata |
| `materialize/TaggedRowSchema.java` | Tagged-row schema metadata |
| `materialize/MaintenancePlanConverter.java` | Logical delta derivation and physical conversion |
| `adapter/enumerable/PipelineToMergedIndexScanRule.java` | Sort-boundary substitution |
| `adapter/enumerable/EnumerableMergedIndexScan.java` | Per-source plan scan |
| `adapter/enumerable/EnumerableMergedIndexDeltaScan.java` | Per-source delta plan scan |
| `adapter/enumerable/PipelineOutputScanRule.java` | Convert pipeline placeholders to MI scans |
| `adapter/enumerable/DeltaToMergedIndexDeltaScanRule.java` | Convert delta MI scans |
| `rel/logical/LogicalPipelineOutputScan.java` | Logical pipeline-source placeholder |

Plan orchestration and DOT output live in `plus/src/test/java/org/apache/calcite/
adapter/tpch/MergedIndexTpchPlanTest.java` and `TpchPlanTestUtil.java`. Shared
sort/filter helpers live in `testkit/src/main/java/org/apache/calcite/test/
MergedIndexTestUtil.java`; single-MI candidate enumeration is in that directory's
`SingleMIPipelineIdentifier.java`. Core structural tests are in
`core/src/test/java/org/apache/calcite/adapter/enumerable/
PipelineToMergedIndexScanRuleTest.java`.

## Testing and Terminology

Run focused tests for the affected feature; see the commands in
[SESSION_PROGRESS.md](SESSION_PROGRESS.md). The static MI registry requires
isolation/clearing between tests; the TPC-H MI classes use same-thread execution.
Do not describe passing plan assertions as query-result parity.

Use leaf/branch/root for pipeline positions. Bottom/earlier means the input side;
top/later means the output side. Use smaller numbers for earlier pipeline levels
when introducing new descriptions; generated filenames may retain older numbering.

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
   reference diagrams (DOT, ASCII). Use sub-sections with clear headings. Old next-steps may include notes that should be reorganized elsewhere in @SESSION_PROGRESS.md, in the Javadoc of the test methods, or in CLAUDE.md.

5. **Expected plans near test code** — paste plan output snippets (BEFORE/AFTER
   structure) as Javadoc comments in the test method they belong to, not in
   `SESSION_PROGRESS.md`. Reference the full DOT diagrams in `SESSION_PROGRESS.md`
   from the Javadoc with a short note.

6. **Commit** all documentation and code changes together after the cleanup.
