# Calcite to LeanStore Handoff

## Current Direction

The active plan is manual LeanStore C++ implementations first. Calcite remains a
planning artifact: it shows pipeline boundaries, merged-index substitutions, and
maintenance-plan structure. LeanStore remains the execution system: it owns the
real B-tree/LSM storage, typed records, scanners, joins, filters, and aggregates.

Automatic Calcite-to-LeanStore integration is deferred. Earlier ideas about JSON
plan export, generated template instantiations, and a runtime C++ interpreter are
still plausible future work, but they are not the current deliverable.

## What Calcite Provides Today

Calcite can produce structural reference plans for a small set of examples:

- Q3-style nested pipelines
- Q9 nested multi-pipeline plan
- Q12 ORDERS/LINEITEM plus grouped root plan

The relevant plan nodes include:

- `EnumerableMergedIndexScan`
- `EnumerableMergedIndexDeltaScan`
- `MergedIndexScanGroup`
- `Pipeline.indexCreationPlan`
- `MaintenancePlanConverter` logical and physical maintenance plans

These are not executable scans. Their Java enumerable implementations are empty.
Use the plans as design evidence and regression artifacts, not as a runtime
interface.

## What LeanStore Provides Today

LeanStore already has manual merged-index query implementations, including Q3,
Q5, and Q10. Those plans use physical designs that may go beyond the current
Calcite model, such as custkey-extended order/lineitem layouts.

Relevant LeanStore concepts for future manual work:

- `LeanStoreMergedAdapter<Records...>` for heterogeneous storage
- merged scanners over folded keys
- `PremergedJoin` and related join-state machinery
- hand-written TPC-H query templates under `../leanstore/frontend/tpch/`

## Manual Handoff Workflow

For each query selected for LeanStore work:

1. Generate or inspect the Calcite reference plan and DOT output.
2. Identify the order-sharing pipelines and the intended MI key for each one.
3. Decide whether LeanStore should implement the same structure or adapt it.
4. Write the C++ plan manually in the style of existing Q3/Q5/Q10 code.
5. Document the adaptation and the correctness oracle.

The correctness oracle should be explicit: compare against a known SQL result,
an existing LeanStore baseline, or a small deterministic test fixture. Plan-shape
tests alone are not enough.

## Adaptation Is Expected

The Calcite plan is a reference, not a binding ABI. Manual LeanStore code can
change:

- key layouts
- which predicates are applied before or after assembly
- whether an aggregate is maintained or recomputed
- how multiple logical scans share one physical pass
- whether a child pipeline is materialized as a parent source

When the C++ plan differs from Calcite, document why the change is semantically
safe and what it should improve.

## Deferred Automatic Bridge

The postponed bridge would likely include:

- plan serialization from Calcite
- a stable schema for MI descriptors, source indexes, collations, expressions,
  aggregates, and maintenance plans
- generated LeanStore template registration for the selected MI shapes
- a C++ operator interpreter or generated C++ query code
- end-to-end result tests comparing SQL, Calcite plans, and LeanStore execution

This should wait until several manual plans make the required runtime contract
clear. Building it now risks encoding the wrong abstraction.

## Open Gaps Before Execution Claims

- Q3 and Q12 test SQL need semantic corrections.
- Q9 final ordering needs inspection because the current root plan can pass
  through a hash aggregate.
- The current Calcite registry matching does not robustly distinguish repeated
  self-join table occurrences.
- Maintenance plans need execution semantics for signed deltas, aggregate
  retractions, old/new visibility, batching, and transaction ordering.
- Scan-group metadata does not yet implement one shared physical scan.

## Near-Term Deliverable

The immediate deliverable is documentation alignment plus a clean planning
backlog. Implementation resumes after the query semantics and manual LeanStore
targets are selected.
