# TRASH — Option B Architecture (Archived)

Moved here on 2026-03-19 during architecture shift to "Transparent Per-Source MI Scans."
Provenance headers indicate original file and line numbers.

---

## From `overhaul-03-10.md` — "Architecture Decision: Option B" (lines 85–97)

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

---

## From `overhaul-03-10.md` — Subtask 2 old form (lines 231–234)

### Subtask 2: `EnumerableMergedIndexScan.implement()` — Interleaved Stream

The scan obtains source enumerables (via MI scans).
The physical implementation of merged index should already use a record structure similar to the interleaved tagged rows---basically byte strings which lay out the object array contiguously. One row of a type is emitted at a time. Explore whether we need to handle the type conversion between the physical bytes and TaggedRow in Calcite, or we could simply assume that we can get TaggedRow from a merged index. Plan for concrete changes to `EnumerableMergedIndexScan.implement()` and relevant methods.

---

## From `overhaul-03-10.md` — Subtask 3: Assembly Operator (lines 236–262)

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

---

## From `overhaul-03-10.md` — Subtask 4 old form (lines 264–271)

### Subtask 4: Update `PipelineToMergedIndexScanRule`

The rule produces `Assemble(Scan)` replacing the Assembly subtree. Both inner and outer
pipelines use the same pattern. `EnumerableMergedIndexJoin` may become unnecessary
(Assemble handles both cases uniformly).

Rule's match pattern stays the same (match MergeJoin), but replacement now considers
the Assembly subtree: replace LCA with `Assemble(Scan)`, leave operators above LCA.

---

## From `overhaul-03-10.md` — Old Suggested Implementation Order (lines 282–289)

### Suggested Implementation Order

1. ~~Subtask 0 — Assembly subtree identification + test validation~~ **DONE**
2. ~~Subtask 1 — Tagged row type (foundation for scan and assembly)~~ **DONE**
3. Subtask 3 — Assembly operator (Algorithm 1 implementation)
4. Subtask 2 — Scan.implement with interleaved output
5. Subtask 4 — Rule update (wires Assemble(Scan) into the plan)
6. Subtask 5 — Index creation (physicalPlan field, end-to-end pipeline execution)
7. Test with Q12, Q3-OL, Q9 — verify actual row production

---

## From `SESSION_PROGRESS.md` — Sample AFTER output (lines 49–78)

**Q12** (full 2-table substitution):

```
EnumerableSort(...)
  EnumerableAggregate(...)
    EnumerableMergedIndexScan(
      tables=[[[TPCH, ORDERS]:O_ORDERKEY, [TPCH, LINEITEM]:L_ORDERKEY]],
      collation=[[0]])
```

**Q3-OL** (full 3-table substitution — both inner and outer replaced):

```
EnumerableLimitSort(...)
  EnumerableProject(...)
    EnumerableMergedIndexJoin(sources=[[view([0]), [TPCH, CUSTOMER]:C_CUSTKEY]], joinType=[INNER], collation=[[3]])
      EnumerableMergedIndexScan(tables=[[view([0]), [TPCH, CUSTOMER]:C_CUSTKEY]], collation=[[3]])
```

**Q9** (full 6-table substitution):

```
EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])
  EnumerableAggregate(group=[{0, 1}], SUM_PROFIT=[SUM($2)])
    EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
      EnumerableProject(...)
        EnumerableFilter(condition=[LIKE($26, '%green%')])
          EnumerableMergedIndexJoin(...)
            EnumerableMergedIndexScan(...)
```

---

## From `SESSION_PROGRESS.md` — Flow Chart B (lines 115–147)

### Flow Chart B — Merged Index Substitution on Top of Normal Planning

```text
          ┌─────────────────────────────────────────────┐
          │  (same physical pipeline from Flow A)       │
          │   EnumerableMergeJoin                       │
          │     ├─ EnumerableSort → EnumerableTableScan │
          │     └─ EnumerableSort → EnumerableTableScan │
          └───────────────┬─────────────────────────────┘
                          │
    ┌─────────────────────┼──────────────────────────┐
    │  Between phases:    │                           │
    │  walk plan tree     │                           │
    │  extract RelOptTable refs + collation           │
    │  MergedIndexRegistry.register(                  │
    │    new MergedIndex(tables, collation, rc))      │
    └─────────────────────┼──────────────────────────┘
                          │
                          ▼  HEP planner
          [PipelineToMergedIndexScanRule]
          Matches: EnumerableMergeJoin
                     ├─ EnumerableSort → EnumerableTableScan(A)
                     └─ EnumerableSort → EnumerableTableScan(B)
          Checks:  MergedIndexRegistry.findFor(tables, collation) != empty
          Fires:   call.transformTo(EnumerableMergedIndexScan)
                          │
                          ▼
          [Merged Index Plan]
          EnumerableProject
            └─ EnumerableMergedIndexScan
               tables=[A, B], collation=[k ASC]
               (one sequential pass; join assembled on-the-fly)
```

---

## From `SESSION_PROGRESS.md` — Q12/Q3-OL/Q9 AFTER plan diagrams (lines 196–318)

### Q12 — 2-table full substitution (`tpchQ12`)

Key: `o_orderkey = l_orderkey`. One HEP pass. One level, no cascade.

```text
BEFORE                                     AFTER (Query plan only)
EnumerableSort                             EnumerableSort
  EnumerableAggregate                        EnumerableAggregate
    EnumerableMergeJoin(orderkey)              EnumerableMergedIndexScan
      EnumerableSort → Scan(ORDERS)              [ORDERS]:O_ORDERKEY
      EnumerableSort → Scan(LINEITEM)            [LINEITEM]:L_ORDERKEY
```

After: query plan + maintenance plan

```text
QUERY-TIME PLAN (AFTER)                    MAINTENANCE PLAN (from BEFORE)
EnumerableSort                             On ORDERS insert(o_orderkey=k):
  EnumerableAggregate                        insert ORDERS record at key k
    EnumerableMergedIndexScan                  into MI(ORDERS+LINEITEM)
      [ORDERS]:O_ORDERKEY                On LINEITEM insert(l_orderkey=k):
      [LINEITEM]:L_ORDERKEY               insert LINEITEM record at key k
                                            into MI(ORDERS+LINEITEM)
```

Maintenance plan structure (the replaced pipeline from BEFORE):
```text
  MergeJoin(orderkey)   ← re-run for delta key k to produce merged index entry
    Sort → Scan(ORDERS)
    Sort → Scan(LINEITEM)
```

---

### Q3-OL — 3-table full substitution (`tpchQ3OrdersLineitem`)

Keys: `l_orderkey = o_orderkey` (inner), `o_custkey = c_custkey` (outer). Two HEP passes. Two-level cascade.

```text
BEFORE                                     AFTER (Query plan only)
EnumerableLimitSort                        EnumerableLimitSort
  EnumerableProject                          EnumerableProject
    EnumerableMergeJoin(custkey) ←outer        EnumerableMergedIndexJoin(custkey, INNER)
      EnumerableSort(custkey)                    EnumerableMergedIndexScan
        EnumerableMergeJoin(orderkey) ←inner       [view(OL)]:O_CUSTKEY
          EnumerableSort                            [CUSTOMER]:C_CUSTKEY
            EnumerableAggregate → Scan(LINEITEM)
          EnumerableSort → Scan(ORDERS)
      EnumerableSort → Scan(CUSTOMER)
```

After: query plan + maintenance plan

```text
QUERY-TIME PLAN (AFTER)                    MAINTENANCE PLANS (from BEFORE)
EnumerableLimitSort                        Level 1 — MI(OL) by orderkey:
  EnumerableProject                          On LINEITEM insert(l_orderkey=k):
    EnumerableMergedIndexJoin(custkey)         re-aggregate LINEITEM for key k,
      EnumerableMergedIndexScan                update MI(OL) at k
        [view(OL)]:O_CUSTKEY               On ORDERS insert(o_orderkey=k):
        [CUSTOMER]:C_CUSTKEY                 insert ORDERS record at k in MI(OL)

                                           Level 2 — MI(OL+CUSTOMER) by custkey:
                                             On MI(OL) delta at (orderkey, custkey=c):
                                               update MI(OL+CUSTOMER) at custkey c
                                             On CUSTOMER insert(c_custkey=c):
                                               insert CUSTOMER record at c
```

Maintenance plan structure (the two replaced pipelines from BEFORE):
```text
  Inner: MergeJoin(orderkey)              Outer: MergeJoin(custkey)
           Sort(Agg(LINEITEM))                     Sort(MI(OL) view)
           Sort → Scan(ORDERS)                     Sort → Scan(CUSTOMER)
```

---

### Q3 (deleted)

The `tpchQ3` test was an incorrect example: it registered a merged index for
CUSTOMER ⋈ ORDERS by custkey, but the natural inner pipeline for TPC-H Q3 is
ORDERS ⋈ LINEITEM by orderkey. The correct 3-table test is `tpchQ3OrdersLineitem`
(Q3-OL), which tests inner (ORDERS+LINEITEM by orderkey) + outer (+CUSTOMER by custkey).

---

### Q9 — 6-table full substitution (`tpchQ9`)

Keys: orderkey → partkey → (partkey,suppkey) → suppkey → nationkey. Five HEP passes.
`findAllPipelines` discovers 5 nested `Pipeline` objects post-order; `MergedIndex.of()`
builds OL → OLP → OLPS → OLPPS → OLPPSS+NATION bottom-up. Five-level cascade.

```text
BEFORE                                     AFTER
EnumerableSort(n_name, o_year DESC)        EnumerableSort(n_name, o_year DESC) ← ORDER BY only
  EnumerableAggregate(n_name, o_year)        EnumerableAggregate(n_name, o_year)
    EnumerableFilter(p_name LIKE ...)          EnumerableProject
      EnumerableMergeJoin(nationkey)             EnumerableFilter(p_name LIKE ...)
        EnumerableSort                             EnumerableMergedIndexJoin(nationkey, INNER)
          EnumerableMergeJoin(suppkey)               EnumerableMergedIndexScan
            EnumerableSort                             [view(OLPPS)]:N_NATIONKEY
              EnumerableMergeJoin(partkey,suppkey)     [NATION]:N_NATIONKEY
                EnumerableSort → Scan(PARTSUPP)
                EnumerableSort
                  EnumerableMergeJoin(partkey)
                    EnumerableSort → Scan(PART)
                    EnumerableSort
                      EnumerableMergeJoin(orderkey)
                        EnumerableSort → Scan(ORDERS)
                        EnumerableSort → Scan(LINEITEM)
            EnumerableSort → Scan(SUPPLIER)
        EnumerableSort → Scan(NATION)
```

```text
QUERY-TIME PLAN (AFTER)                    MAINTENANCE PLANS (5 levels from BEFORE)
EnumerableSort(n_name, o_year DESC)        L1 OL(orderkey):   ORDERS/LINEITEM delta
  EnumerableAggregate(n_name, o_year)      L2 OLP(partkey):   OL/PART delta
    EnumerableProject                      L3 OLPS(pk,sk):    OLP/PARTSUPP delta
      EnumerableFilter(p_name LIKE ...)    L4 OLPPS(suppkey): OLPS/SUPPLIER delta
        EnumerableMergedIndexJoin          L5 final(natkey):  OLPPS/NATION delta
          EnumerableMergedIndexScan
            [view(OLPPS)]:S_NATIONKEY      Each level: 1 delta in → 1 MI entry updated
            [NATION]:N_NATIONKEY           Cascade depth = 5 for a LINEITEM base change
```

Note: `EnumerableFilter(p_name LIKE '%green%')` remains because PART is absorbed into
the merged index but the filter cannot be pushed below the assembled join result.

---

## From `SESSION_PROGRESS.md` — Maintenance Plan Generation (lines 325–398)

### Maintenance Plan Generation (implemented 2026-03-10)

Obsolete.

`MergedIndex.maintenancePlan` stores the incremental IVM plan derived by
`deriveIncrementalPlan(Join)` in `MergedIndexTpchPlanTest`.

#### Two-phase maintenance model

A merged index does **not** store a pre-computed join — it stores records from each
source table independently, interleaved by sort key. This distinction drives two
fundamentally different maintenance phases:

**Phase 1 — base table Δ → inner MI (no join needed)**

Each source contributes independently. One base-table insert/delete triggers exactly
one MI record update, with no join against any other source. For example, in Q3-OL:

- `ORDERS` insert at orderkey=k → insert ORDERS record into MI(OL)\[k\]
- `LINEITEM` insert at orderkey=k → re-aggregate LINEITEM for key k → update Agg
  record in MI(OL)\[k\]

The semi-naive formula `Δ(A ⋈ B) = (Δ(A) ⋈ B) ∪ (A ⋈ Δ(B))` does **not** apply
here. Branch 2 (ORDERS delta) needs no join with Agg(LINEITEM); ORDERS records are
simply inserted into the MI slot for key k. The formula overcounts by joining even
for direct-insertion paths.

**Phase 2 — inner MI Δ → outer MI (join/propagation required)**

When a change in the inner MI must propagate to the outer MI, the key level changes
(e.g., orderkey → custkey). Before this step, the inner MI just stores multiple types of records, now we need to assemble them together. It usually involves a join but may also involve additional operators, as defined by the pipeline for the current merged index.
For example, in Q3-OL,
when a lineitem insertion triggers an additional joined record, we need to produce it and insert it directly into the outer MI.
Note that a join-like lookup in the outer merged index is still NOT required.

**The BEFORE plan defines both phases:**

- Phase 1 updates leaf merged indexes that correspond to leaf pipelines.
- Phase 2 updates non-leaf merged indexes that correspond to inner pipelines.

#### Current `deriveIncrementalPlan` — union of independent deltas (2026-03-10)

A merged index stores records from each source **independently**, interleaved by
sort key. No join between sources is needed at maintenance time — each source inserts
its records directly at the appropriate sort key. The maintenance plan is therefore:

```text
LogicalUnion(all=true)
  LogicalDelta(sortedInputs[0])    ← new records from source 0
  LogicalDelta(sortedInputs[1])    ← new records from source 1
  ...
```

For nested pipelines (Q3-OL outer, Q9 levels 1–4), the left sorted input wraps
the entire inner pipeline (e.g., `Sort(inner_join_result)`). `LogicalDelta` over
this node means "run the inner pipeline for changed keys and emit the assembled
delta" — the Phase 2 propagation is defined by the inner pipeline's own operators.
No additional join node is added at the outer MI level.

The implementation bypasses `HepPlanner + StreamRules` because
`DeltaJoinTransposeRule.onMatch()` calls `HepRuleCall.transformTo()` which runs
`verifyTypeEquivalence` — this fails because TPC-H schema uses `JavaType(String)` while
the newly created `LogicalJoin`s re-derive their row types as `VARCHAR` (SQL type system).
By constructing the plan directly, we avoid the type mismatch entirely.

#### `EnumerableMergedIndexDeltaScan` and `DeltaToMergedIndexDeltaScanRule` (2026-03-10)

`EnumerableMergedIndexDeltaScan` — physical delta-scan operator analogous to
`EnumerableMergedIndexScan`, slightly higher cost. Registered as
`ENUMERABLE_DELTA_TO_MERGED_INDEX_DELTA_SCAN_RULE` in `EnumerableRules` (opt-in).

`DeltaToMergedIndexDeltaScanRule` matches `LogicalDelta(EnumerableMergedIndexScan)`
and replaces it with `EnumerableMergedIndexDeltaScan`. Tested in `tpchQ3OrdersLineitem`
by constructing a synthetic `LogicalDelta(innerScan)` and verifying the rule fires.

#### Tag-based lazy propagation (future design)

Each merged-index record carries a 1-byte `propagated` flag. On base-table insert:

1. Insert into MI with `propagated=false`.
2. Background worker finds untagged records, runs the phase-1 source pipeline for
   that key (re-agg LINEITEM, etc.), propagates delta to next-level MI via phase-2
   join lookup.
3. Mark source record as `propagated=true`.

This avoids storing full delta records and keeps update cost O(1) amortized per
cascade level.

---

## From `SESSION_PROGRESS.md` — Old Next Steps (lines 430–449)

### Short Term (next session)

1. **Subtask 3: `EnumerableMergedIndexAssemble` operator** (new file: `EnumerableMergedIndexAssemble.java`)
   - Implement Algorithm 1 (N-way inner join: buffer per source, Cartesian product on key change).
   - Assembly strategy parameterized by absorbed operator types from Subtask 0.

2. **Subtask 2: `EnumerableMergedIndexScan.implement()`** (file: `EnumerableMergedIndexScan.java`)
   - The physical MI should already store records in tagged interleaved format (byte strings
     laying out the Object[] contiguously). Explore whether type conversion between physical
     bytes and TaggedRow is needed in Calcite, or we can assume TaggedRow directly from MI.

### Following Sessions

3. **Subtask 4: Update `PipelineToMergedIndexScanRule`** (file: `PipelineToMergedIndexScanRule.java`)
   - Rule produces `Assemble(Scan)` replacing the Assembly subtree.
   - `EnumerableMergedIndexJoin` may become unnecessary.

4. **Subtask 5: Index creation mode** (file: `Pipeline.java`)
   - `physicalPlan` field on Pipeline; after HEP substitution, extract and store subtree.
   - End-to-end: `while (hasNext) { parentMI.add(physicalPlan.next()) }`.

5. **End-to-end test with actual row production** — Q12, Q3-OL, Q9.
