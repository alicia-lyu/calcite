# Session Progress: Multi-Pipeline Merged Indexes

This file is the current entry point for resuming work. It supersedes older
notes that described the single-pipeline paper phase or earlier integration
plans.

## Current State

The implementation is a Calcite planning artifact. It demonstrates how a query
can be decomposed into order-sharing pipelines, how registered merged indexes
(MIs) can replace sort boundaries, and how index-creation and maintenance plan
trees can be captured for those pipelines.

It is not an executable benchmark system. `EnumerableMergedIndexScan` and
`EnumerableMergedIndexDeltaScan` are plan nodes whose `implement()` methods
return empty enumerables. Query execution will be written manually in LeanStore
first, following the current Q3/Q5/Q10 style there. Automatic Calcite-to-LeanStore
plan export and an MI-aware cost-based optimizer are deferred.

## Verified Implementation Pieces

| Area | Status |
|------|--------|
| `Pipeline` sort-boundary discovery | Implemented |
| `MergedIndex` pipeline descriptor | Implemented |
| `MergedIndexRegistry` source and collation matching | Implemented |
| `PipelineToMergedIndexScanRule` HEP substitution | Implemented |
| Shared scan metadata with `MergedIndexScanGroup` | Implemented |
| Tagged-row metadata with `TaggedRowSchema` | Implemented |
| Logical maintenance derivation with `MaintenancePlanConverter` | Implemented |
| Physical maintenance-plan conversion to Enumerable | Implemented |
| TPC-H multi-MI structural examples | Q3, Q9, Q12 only |
| TPC-H full workload coverage | Not implemented |
| Query-result parity tests | Not implemented |
| LeanStore automatic bridge | Deferred |

Focused tests were last rerun during the September 2026 doc sweep:

```bash
./gradlew :plus:cleanTest :core:cleanTest :plus:test --tests '*.MergedIndexTpchPlanTest' :core:test --tests '*.PipelineToMergedIndexScanRuleTest' --offline
```

Result: build successful. These tests validate plan structure, DOT generation,
and rule behavior. They do not validate SQL result equivalence or LeanStore
execution.

## TPC-H Coverage

| Query | Current MI plan artifact | Notes |
|-------|--------------------------|-------|
| Q1 | None | No multi-pipeline artifact |
| Q2 | None | No multi-pipeline artifact |
| Q3 | Partial | Structural Q3-style plan exists, but current test SQL omits the real market-segment and date predicates |
| Q4 | None | No multi-pipeline artifact |
| Q5 | None in Calcite multi-MI tests | LeanStore has a manual implementation path |
| Q6 | None | No multi-pipeline artifact |
| Q7 | None | No multi-pipeline artifact |
| Q8 | None | No multi-pipeline artifact |
| Q9 | Partial | Six-table structural example exists; final ordering and full semantics need validation |
| Q10 | None in Calcite multi-MI tests | LeanStore has a manual implementation path |
| Q11 | None | No multi-pipeline artifact |
| Q12 | Partial | Two-level structural example exists, but high-line-count logic currently omits `2-HIGH` |
| Q13 | None | No multi-pipeline artifact |
| Q14 | None | No multi-pipeline artifact |
| Q15 | None | No multi-pipeline artifact |
| Q16 | None | No multi-pipeline artifact |
| Q17 | None | No multi-pipeline artifact |
| Q18 | None | No multi-pipeline artifact |
| Q19 | None | No multi-pipeline artifact |
| Q20 | None | No multi-pipeline artifact |
| Q21 | None | No multi-pipeline artifact |
| Q22 | None | No multi-pipeline artifact |

Ordinary Calcite TPC-H examples exist elsewhere in the tests, but they are not
multi-MI artifacts and should not be counted as implemented MI plans.

## Query Artifacts

DOT files are written under `plus/test-dot-output/`.

### Q3

The current Q3-style plan preaggregates LINEITEM by orderkey, joins ORDERS by
orderkey, then joins CUSTOMER by custkey and applies a revenue/date ordering.
It is useful for demonstrating nested pipelines, but it is not a faithful TPC-H
Q3 semantic test because the important predicates are missing.

LeanStore's manual Q3 implementation has already adapted the physical design by
using a custkey-extended COL index. That is stronger than what the Calcite plan
currently models.

### Q9

The current Q9 example fixes a join sequence:

```text
ORDERS -> LINEITEM -> PART -> PARTSUPP -> SUPPLIER -> NATION
```

It builds nested pipelines around orderkey, partkey, `(partkey, suppkey)`,
suppkey, and nationkey. This is a useful stress test for nested registration and
maintenance-plan capture. It is not proof that the chosen join sequence, final
ordering, or filter placement is optimal.

Known issue: the saved root query plan currently contains an MI scan feeding a
filter and `EnumerableAggregate`. A hash aggregate does not guarantee the final
`ORDER BY n_name, o_year DESC`.

### Q12

The current Q12 example demonstrates an ORDERS/LINEITEM pipeline and a grouped
root plan. The test should be corrected before using it as evidence for TPC-H
semantics: TPC-H Q12 counts high-priority orders for both `1-URGENT` and
`2-HIGH`, while the current test only handles `1-URGENT`.

## Maintenance Notes

The maintenance converter can derive scoped logical delta plans and convert them
to Enumerable plans. Treat those plans as reference structures. They are not an
execution contract yet.

Do not claim blanket 1-to-1 cascade cost. One source-record update adds one
entry to a raw-source MI, but a downstream pipeline can emit multiple rows if
the changed record joins with many rows, changes an aggregate state, or feeds a
parent MI with a different key.

Open execution details include signed deltas, aggregate retractions, old/new
row visibility, batching, downstream application order, and transaction rules.

## LeanStore Handoff

The next LeanStore phase is manual C++ plans first. Use Calcite as a reference
for order-sharing structure, pipeline boundaries, and maintenance hypotheses.
Adapt the implementation when LeanStore's physical indexes make a better plan
possible, as in Q3/Q5/Q10.

Record each adaptation explicitly:

- which Calcite pipeline it corresponds to
- which LeanStore index and key layout it uses
- which operators remain at query time
- which predicates or aggregates are applied manually
- what correctness check will compare it against

## Next Planning Steps

1. Correct Q3 and Q12 SQL semantics in the structural tests and regenerate DOT
   artifacts.
2. Inspect the Q9 root plan and decide how final ordering should be enforced or
   represented.
3. Build a query-by-query manual-plan worksheet for TPC-H Q1-Q22, starting from
   which queries plausibly need multiple pipelines.
4. Map selected queries to LeanStore manual C++ implementation work, using Q3,
   Q5, and Q10 as style references.

The September 2026 sweep only updates documentation. It does not choose new
query execution algorithms.
