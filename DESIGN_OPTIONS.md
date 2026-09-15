# Design Options for Multi-Table Merged Indexes

This document separates implemented mechanics from research policy choices.
Current code is a plan-generation artifact; it does not yet prove executable
query performance or maintenance cost.

## Settled Mechanics

| Decision | Current choice |
|----------|----------------|
| Planning architecture | Volcano first, then HEP substitution |
| Pipeline detection | Sort-boundary discovery in `Pipeline.buildTree` |
| MI descriptor | `MergedIndex` wraps a `Pipeline` |
| Rule trigger | Explicit sort boundaries, opt-in rule |
| Nested registration | Leaf-to-root registration with repeated HEP passes |
| Execution in Calcite | Stub scans return empty enumerables |
| LeanStore path | Manual C++ plans first |
| Automatic bridge | Deferred |
| Cost-based MI choice | Deferred |

## Open Decisions

### 1. Materialization Policy

Should every discovered pipeline be stored as a physical MI?

- All materialized: simplest query-time story, highest write and space cost.
- Selective materialization: stores only valuable pipeline outputs.
- Cost-based choice: uses query frequency, update frequency, fanout, and storage
  estimates.

Current default in tests: materialize all registered pipelines for the selected
example. This is a test policy, not an optimizer result.

### 2. Filter Placement

Should filters be included in an MI creation plan or left above the MI scan?

- Unfiltered MI: more reusable across predicates, larger stored structure.
- Filtered MI: smaller and faster for one predicate, less reusable.

Current default in helpers: hoist filters toward the root pipeline so child MIs
are predicate-agnostic when possible. This must be checked per query because
hoisting can require widening projections and preserving predicate columns.

### 3. Hierarchical and Functional-Dependency Keys

When can one MI serve several logical keys?

- Prefix-chain support is valid when keys are lexicographic prefixes.
- Functional dependencies such as `orderkey -> custkey` do not automatically
  make an orderkey scan custkey-sorted.
- A physical design can extend keys, for example `(custkey, orderkey,
  linenumber)`, but Calcite must model that design explicitly before matching it.

Current default: exact collation/prefix matching, no FD-derived key rewriting.

### 4. Shared Physical Scan Costing

Multiple logical `EnumerableMergedIndexScan` nodes can describe one intended
physical pass over an MI. How should this be costed?

- Independent costing: each logical scan pays its own estimate.
- Shared-scan costing: first scan pays I/O; later scans pay mainly CPU.
- Amortized costing: split I/O across a scan group.

Current default: independent local estimates. HEP substitution does not consult
these costs, so the estimates are descriptive only.

### 5. Maintenance Semantics

The converter can build logical and physical maintenance-plan trees. The storage
and transaction contract remains open.

Open choices:

- eager versus batched propagation
- signed delta representation
- aggregate replacement and retraction
- old/new row visibility
- downstream write ordering
- how a scan group maps to one storage cursor

State 1-to-1 maintenance with its scope. Updating one MI entry or state can be a
1-to-1 index-maintenance step. A cascade across MIs is a sequence of such steps
and can still fan out depending on join multiplicity, aggregation, and key
changes between parent and child MIs.

### 6. Final Ordering

Injected sorts are mostly driven by join and aggregate requirements. ORDER BY
requirements need separate validation after MI substitution, especially when a
hash aggregate remains in the root plan.

Current known issue: the Q9 root plan can feed an `EnumerableAggregate`; that
operator does not guarantee the requested final order.

### 7. Tagged-Row Representation

How should interleaved records be represented when execution is implemented?

- Java `Object[]` plus domain tags is useful for plan debugging.
- Byte-string keys and typed payloads match LeanStore/RocksDB more closely.
- A hybrid representation may help tests without requiring a full storage engine.

Current Calcite code only carries metadata. LeanStore owns the real physical
representation.

## Current Artifact Decisions

For the next phase, treat the current repository as:

- a reproducible plan-shape artifact for multiple pipelines
- a source of DOT plans and maintenance-plan hypotheses
- a record of plan intent that DOT files cannot fully reconstruct
- a guide for manual LeanStore C++ implementations

Do not treat it as:

- a finished full TPC-H optimizer
- an executable MI implementation in Java
- proof that all substitutions are cheaper
- proof that all across-MI cascades have single-row fanout
