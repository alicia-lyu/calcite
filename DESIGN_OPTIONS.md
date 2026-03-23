# Design Options for Multi-Table Merged Indexes

This document catalogs open and settled design decisions for the merged index
implementation in Calcite. Open decisions are research-relevant policy choices;
settled decisions are engineering choices already committed to.

---

## Open Decisions

### 1. Materialization Policy for Pipelines

**Question**: Should every pipeline's merged index be materialized
(persisted), or should some be recomputed at query time from their preceding pipeline

**Alternatives**:

- **All materialized** — every MI is a physical B-tree. Fastest queries, highest
  space usage, most update overhead for deeply nested pipelines.
- **Selective materialization** — only leaf MIs are materialized; intermediate
  views are recomputed at query time from leaf scans. Reduces space and update
  fan-out but adds query-time join work.
- **Cost-based** — the optimizer chooses per-MI based on update frequency, query
  frequency, and fan-out. Most flexible but requires accurate statistics.

**Current default**: All materialized.

**Prior paper reference** (`main.md`): §6 (Spectrum of Pre-computation, Table 4).

**Code reference**: `Pipeline.java:78` (`mergedIndex` field),
`overhaul-03-10.md` pipeline categories table.

---

### 2. Filter Handling When Creating Merged Indexes

**Question**: When an order-based pipeline contains filters (e.g., Q9's
`p_name LIKE '%green%'`), should the filter be included or deleted when
creating the merged index?

Note: `EnumerableMergedIndexScan` is just a special `TableScan` — nothing is
"pushed down" into it. The question is whether the MI creation plan (which
replaces the pipeline) retains or drops the filter. Index creation plans and
materialized view plans typically delete filters by default, producing an
unfiltered index. Filters in the root pipeline then stay in the query plan above the scan.
If inner pipelines drop their filters, those filters need to be moved to the
final pipeline, provided that the filtered column is still included at that
point.

A useful heuristic may be to omit predicates whose parameters can change across
queries (e.g., `p_name LIKE '%green%'` vs. `'%blue%'`), preserving MI
reusability. Predicates on stable attributes (e.g., status flags, type codes)
may be safe to bake into the MI. Existing research on partial indexes and
parameterized views is likely relevant — further investigation needed.

**Alternatives**:

- **Delete filter (unfiltered MI)** — the MI stores all records; the filter
  remains in the query plan above the scan. MI is maximally reusable across
  queries with different predicates. This filter needs to be moved to the final pipeline.
  We also need to ensure that the selected column stay in that final pipeline.
- **Retain filter (filtered MI)** — the MI only stores records matching the
  predicate. Smaller MI, but limits reuse (query-specific). Only appropriate
  for stable predicates unlikely to change.

**Current default**: Delete filter (unfiltered MI); filter stays in query plan.

**Prior paper reference** (`main.md`): §4.3 (complex queries).

**Code reference**: `CLAUDE.md:166-169` (Q9 two-tier plan).

---

### 3. Hierarchical vs. Independent Keys

**Question**: When can a single MI serve a prefix-chain of join keys (§3.2) vs.
requiring separate MIs per key?

**Alternatives**:

- **Independent keys only** — each MI serves exactly one shared key. Separate MIs
  needed for `orderkey` and `custkey` even if one is functionally dependent on
  the other. Simple matching logic.
- **Prefix-chain support** — a single MI sorted on `(k1, k2, k3)` serves joins
  on any prefix `(k1)`, `(k1, k2)`, etc. Requires hierarchical key detection
  in `MergedIndex.satisfies()`.
- **FD-aware prefix chains** — extends prefix-chain support with functional
  dependency reasoning (see decision #4).

**Current default**: Independent keys only; `MergedIndex.satisfies()` uses exact
prefix matching.

**Prior paper reference** (`main.md`): §3.2 (Hierarchical Join Keys).

**Code reference**: `MergedIndex.java:174` (`satisfies()`),
`CLAUDE.md:158-164`.

---

### 4. Functional Dependency Exploitation

**Question**: Can FDs (e.g., `o_orderkey -> o_custkey`) allow a single 3-table MI
instead of two nested MIs?

This is equivalent to minimizing pipeline count. For Q3, the sort on custkey can be pushed down to the sorts on orderkey, and this will require extending the lineitem rows with custkey.

**Current default**: Not exploited (two nested MIs).

**Prior paper reference** (`main.md`): §3.4 (Multi-Table Aggregations, FD example).

**Code reference**: `CLAUDE.md:105-117` (Functional Dependencies and 3-Table Q3),
`MergedIndex.java` TODO at `satisfies()`.

---

### 5. Cost Model for Shared Physical Scan

**Question**: How should the optimizer cost N logical MI scans that share one
physical sequential scan of the same merged index?

**Alternatives**:

- **Independent costing** — each `EnumerableMergedIndexScan` is costed as a
  separate sequential scan. Overstates I/O when multiple scans share the same
  physical pass.
- **Shared-scan costing** — the first scan pays full I/O; subsequent scans of the
  same MI pay only CPU (buffer hit). Requires scan-sharing detection in the cost
  model.
- **Amortized costing** — total I/O divided equally among all scans of the same
  MI. Simpler but less accurate for plans where not all scans execute.

**Current default**: Independent costing.

**Prior paper reference** (`main.md`): §6 (pre-computation spectrum implies shared maintenance).

**Code reference**: `overhaul-03-10.md` Subtask 4,
`EnumerableMergedIndexScan.java` (`computeSelfCost`).

---

### 6. Maintenance Plan Structure

**Question**: For cascading updates through nested MIs, what is the update
propagation model?

**Alternatives**:

- **Eager cascade** — a base-table update immediately propagates through all
  dependent MIs. Simple semantics, potentially high write amplification for
  deep nesting.
- **Lazy (tag-based) propagation** — updates are tagged and deferred; dependent
  MIs are refreshed on next read or at batch boundaries. Lower write
  amplification but stale reads possible. Propagate when compacting LSM?
- **Hybrid** — leaf MIs updated eagerly (1-to-1 cost); outer MIs updated lazily
  or on demand.

**Current default**: eager all the way.

**Prior paper reference** (`main.md`): §5 (Index Maintenance).

**Code reference**: `TRASH-option-b.md` (two-phase model, lazy propagation
design).

---

### 7. Sort Direction Propagation

**Question**: Should injected sorts match downstream direction requirements to
avoid redundant re-sorts?

**Alternatives**:

- **Always ASC** — simplest. Downstream operators that need DESC (e.g., Q9
  `ORDER BY o_year DESC`) require an additional sort.
- **Direction-aware injection** — `injectSortsBeforeSortBasedOps` inspects
  downstream consumers and injects sorts with the required direction. Eliminates
  redundant re-sorts but adds complexity to sort injection.
- **Bidirectional MI storage** — the MI stores records in both directions (or
  supports reverse iteration). Eliminates the problem at the storage level.

**Current default**: Always ASC (`new RelFieldCollation(idx)` defaults to ASC).

**Prior paper reference** (`main.md`): §3.1 (sort order in merged indexes).

**Code reference**: `overhaul-03-10.md:17-19`,
`MergedIndexTestUtil.java` (`injectSortsBeforeSortBasedOps`).

---

### 8. Physical Representation of Tagged Rows

**Question**: How should interleaved records be represented in the scan operator?

**Alternatives**:

- **Object[] with domain tags** — Java-native, easy to debug, works with
  Calcite's `Enumerable<Object[]>` interface. Not representative of real B-tree
  storage.
- **Byte strings** — closer to actual B-tree/LSM storage format described in
  §3.3. Requires serialization/deserialization, but demonstrates the paper's
  record structure faithfully.
- **Hybrid** — byte-string keys with Object[] payloads. Demonstrates key
  comparison semantics without full serialization overhead.

**Current default**: Object[] with domain tags.

**Prior paper reference** (`main.md`): §3.3 (Record Structure).

**Code reference**: `TaggedRowSchema.java`.

---

## Settled Decisions

These are engineering choices already committed to. Included for completeness.

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Architecture | Transparent per-source MI scans (Option B) | Simpler planner integration; each source scanned independently |
| Pipeline identification | Sort-boundary-based | More robust than join-centered; aligns with interesting ordering theory |
| PoC path | PATH A (substitution) | PATH B (native collation reporting) deferred; substitution sufficient for paper |
| Rule registration | Opt-in explicit (not in `ENUMERABLE_RULES`) | Avoids interfering with unrelated Calcite tests |
| Multi-pass HEP | Separate `HepPlanner` per nesting level | Single-pass unreliable for dependent pipeline replacements |
| Aggregate views in MI | MI stores whatever `p.sources` produces | Obsolete from prior paper (`main.md`); aggregates are just part of the pipeline, not a separate MI design choice |
