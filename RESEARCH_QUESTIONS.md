# Research Questions: Merged Index Maintenance Efficiency

## Status

This file records hypotheses and analysis questions. It is not a results section.
The current Calcite code builds maintenance-plan structures, but it does not
execute them or measure I/O. Claims below should be validated analytically,
through LeanStore experiments, or both before they are used in a paper.

## Core Claim to Test

A merged index can reduce maintenance and query I/O when the records needed for
one pipeline are physically co-located by the pipeline key. For a delta at key
`k`, the executor should be able to seek to `k`, scan the local key range, assemble
the affected joined rows or aggregate changes, and update downstream state.

This is a physical execution property. The Calcite maintenance plan describes
which sources participate and where delta scans appear; it does not by itself
prove that only one storage pass is used.

## Terms

- Source-record write: inserting, deleting, or updating one base-table record in
  an MI that stores raw source records.
- Pipeline output delta: rows or aggregate-state changes produced after assembly.
- Cascade step: applying one pipeline output delta to a downstream MI or query
  result.

A source-record write can be 1-to-1 for the MI currently being updated, while the
pipeline output delta sent to a parent MI can still have fanout. Keep per-MI
update counts separate from across-MI cascade counts.

## Q1: Per-Update I/O for One B-tree Pipeline

Question: for an eager B-tree implementation, how many pages does one base-table
change touch in one pipeline?

For a selective FK/PK pipeline such as ORDERS/LINEITEM on `orderkey`, the expected
read work is:

```text
one seek + scan all records at orderkey k
```

At TPC-H SF=1 this is usually one ORDERS row plus a small number of LINEITEM
rows. The write work then depends on what the pipeline emits:

- raw-source MI entry update
- joined-row delta
- aggregate-state replacement
- parent MI entry update

When an experiment reports "one scan," specify whether it means one scan of the
root MI followed by remaining query execution, or one local scan inside a
maintenance step.

Needed evidence:

- page count per key under LeanStore layout
- average and tail records per key
- comparison with separate-index lookups or a materialized-view maintenance path

## Q2: Fanout Across Nested Pipelines

Question: when a child pipeline feeds a parent MI, how large can the downstream
delta become?

For one changed row in source `Ti`:

```text
output_delta_size = product of matching row counts from the other sources
```

FK/PK joins often have fanout 1 when the changed row is on the many side and
joins to a single parent. Changes on the parent side can fan out to many child
rows. Many-to-many joins can also produce larger deltas.

For Q3-style ORDERS/LINEITEM then CUSTOMER, the answer depends on which table
changed:

- LINEITEM change: likely one order-level delta, then one customer-level update.
- ORDERS change: can affect all lineitems for that order.
- CUSTOMER change: can affect all relevant orders for that customer.

Needed evidence:

- per-table fanout formulas for Q3, Q9, and Q12
- average and tail fanout under TPC-H data
- whether each parent MI stores raw records, joined tuples, or aggregate states

## Q3: Aggregation Maintenance

Question: should aggregates be recomputed by scanning the affected key range, or
maintained with auxiliary state?

Range recomputation is attractive when the affected group is small and physically
co-located. Auxiliary state is attractive when the group is large or the grouping
key has low cardinality.

Q12 is the cautionary case. `l_shipmode` has few values, so a global shipmode
group is large. If the maintained structure is keyed by orderkey and the
shipmode aggregate remains a query-time operation, update cost is different from
maintaining the final shipmode result directly.

Needed evidence:

- which aggregates are maintained at each pipeline level
- group cardinalities for maintained keys
- insert/delete behavior and aggregate retraction logic

## Q4: B-tree Versus LSM Maintenance

Question: when should a pipeline use B-tree storage versus LSM storage?

B-trees make the eager "seek to key, scan local range, write result" model easier
to reason about. LSMs make writes cheaper but may defer physical co-location
until compaction.

Open LSM questions:

- whether read-time access only merges records in memory or can force range
  compaction
- how pending joined results are represented across levels
- whether deferred propagation reduces write amplification enough to offset read
  complexity
- how to bound stale reads if propagation is lazy

## Q5: Comparison With Traditional IVM

Question: what exactly is saved relative to a materialized-view cascade?

Possible savings:

- fewer separately stored intermediate results
- local partner discovery from one co-located key range
- fewer random lookups across independent structures
- no final materialized query result when the final answer is produced at read
  time

These savings are workload-dependent. The "one fewer level" idea is a hypothesis
to count per query, not a universal law.

Needed evidence:

- store count per cascade for each query
- reads and writes per updated table
- whether parent-table updates fan out
- whether final result materialization is required by the benchmark scenario

## Q6: Query Plan Readiness for TPC-H

Question: which TPC-H queries are ready to become manual LeanStore plans?

Current answer: only Q3/Q5/Q10 have existing LeanStore manual work, and Calcite
multi-MI artifacts cover only Q3-style, Q9, and Q12 examples. The remaining
queries need query-by-query planning before implementation.

For each query, record:

- SQL semantics and predicates
- candidate order-sharing pipelines
- expected MI key layout
- operators left at query time
- correctness oracle
- known adaptation from Calcite to LeanStore

## Summary

The next paper claim should be built from concrete per-query accounting:

- local key-range scan cost
- fanout per updated table
- maintained state size
- query-time operators that remain
- LeanStore measurements

The current Calcite repository supplies plan structure and hypotheses. It does
not close those research questions by itself.
