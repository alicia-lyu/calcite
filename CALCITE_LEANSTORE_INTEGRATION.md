# Calcite ↔ LeanStore Integration: Feasibility & Milestones

## 1. Motivation

Currently, every query in the LeanStore merged-index system requires manually written C++ template code. As query scope expands to full TPC-H and JOB (Join Order Benchmark), manual coding doesn't scale. The goal: use Calcite as the plan generator (query optimization, cost-based join ordering, merged-index substitution) and LeanStore as the executor (B-tree/LSM merged index scans, premerged joins).

## 2. LeanStore Project Summary (Relevant to Integration)

GitHub: https://github.com/alicia-lyu/leanstore

### 2.1 Architecture
- High-performance OLTP storage engine for many-core CPUs and NVMe SSDs
- Two storage backends: LeanStore B-tree (`BTreeVI`, `BTreeLL`) and RocksDB LSM
- Build system: CMake, C++17

### 2.2 Merged Index Adapter (`LeanStoreMergedAdapter.hpp`)
- Variadic C++ template: `LeanStoreMergedAdapter<Records...>` stores heterogeneous records in one B-tree
- Key operations: `insert()`, `lookup1()`, `erase()`, `update1()`, `getScanner<JK, JR>()`
- Key folding: records stored as lexicographically-comparable byte sequences via `foldKey()`/`unfoldKey()`
- Type discrimination: `toType()` static method reconstructs variant types from binary payloads by size

### 2.3 Merged Scanner (`LeanStoreMergedScanner.hpp`)
- Navigation: `next()`, `prev()`, `seek(key)`, `seekForPrev(key)`, `seekTyped<RecordType>()`
- Returns `std::variant<Record1, Record2, ...>` — compile-time type safety, runtime polymorphism
- Adaptive positioning: when seeking, landing on exact record type not guaranteed — iterates forward to find desired type

### 2.4 PremergedJoin (`premerged_join.hpp`)
- Single-pass multi-table assembly over one merged scanner
- Adaptive seek strategy: chooses between sequential scan, direct seek, or tentative filter based on key distance
- Join state machine (`join_state.hpp`): per-type record queues, cartesian product assembly at key boundaries
- Result queue with consumer callback — pipeline-style processing

### 2.5 Type System (`Types.hpp`)
- Core types: `Integer` (s32), `UInteger` (u32), `Timestamp` (s64), `Numeric` (double), `Varchar<N>`
- All types support fold/unfold for lexicographic byte ordering in B-tree keys
- Record structs have nested `Key` struct, `maxFoldLength()`, `foldKey()`, `unfoldKey()`, `generateRandomRecord()`

### 2.6 TPC-H Support (`tpch_tables.hpp`, `tpch_workload.hpp`)
- All 8 TPC-H tables defined as C++ record types with proper key structures
- Static type IDs (PART=0, SUPPLIER=1, PARTSUPP=2, CUSTOMER=3, ORDERS=4, LINEITEM=5, NATION=6, REGION=7)
- Data loading infrastructure with ID recovery and progress reporting

### 2.7 Current Query Execution Pattern (`join_search_count.tpp`)
Three strategies implemented per query:
1. **BaseJoiner**: chain of `BinaryMergeJoin` instances over separate single-table scanners
2. **HashJoiner**: hash-based joins (baseline comparison)
3. **MergedJoiner**: single `PremergedJoin` over one `LeanStoreMergedAdapter` — the target

Each strategy is manually coded in C++ templates. Adding a new query requires writing a new `.tpp` file with explicit template instantiations, join key types, and record type lists.

## 3. Calcite Execution Interface Summary

### 3.1 Three Conventions
| Convention | Interface | Code Gen | Execution |
|------------|-----------|----------|-----------|
| **Enumerable** | `EnumerableRel.implement()` | Java via Janino | JVM bytecode |
| **JDBC** | `JdbcRel.implement()` | SQL string | Remote DB |
| **Bindable** | `BindableRel` | None | Interpreted |

### 3.2 Table SPI
```
Table (base)
├── ScannableTable          → scan(DataContext) → Enumerable<Object[]>
├── FilterableTable         → filter pushdown
├── ProjectableFilterableTable → project + filter pushdown
└── TranslatableTable       → toRel() → custom RelNode
```

### 3.3 Current Merged Index Scan
`EnumerableMergedIndexScan.implement()` returns `EMPTY_ENUMERABLE` — a stub, because real execution happens externally. The plan tree carries all optimization decisions (which merged indexes, source indices, scan groups) but no executable logic.

## 4. Gap Analysis

| Dimension | Calcite | LeanStore | Gap |
|-----------|---------|-----------|-----|
| Language | Java (pure, no JNI) | C++17 | Cross-language bridge needed |
| Type system | `RelDataType` (SQL types) | C++ structs with fold/unfold | Type mapping layer needed |
| Plan representation | `RelNode` tree (Java objects) | C++ template instantiations | Serialization format needed |
| Operator dispatch | `implement()` → Java bytecode | Manual template code per query | Runtime plan interpreter needed |
| Merged index identity | `MergedIndex` descriptor (tables, collations) | `LeanStoreMergedAdapter<Rs...>` template instance | Registry mapping needed |
| Join assembly | `EnumerableMergeJoin` (eliminated by MI rule) | `PremergedJoin<Rs...>` | MI scan replaces both |
| Sort | `EnumerableSort` (eliminated by MI rule) | Pre-sorted in B-tree | Already aligned |
| Aggregation | `EnumerableSortedAggregate` | Not in merged scanner (manual) | Aggregation operator needed in C++ |
| Filter | `EnumerableFilter` | Not in merged scanner (manual) | Filter operator needed in C++ |

### Key Challenge: Compile-Time vs Runtime
LeanStore's merged index is **compile-time parameterized**: `PremergedJoin<Nation, Supplier, Orders, Lineitem>` is a distinct C++ type from `PremergedJoin<Orders, Lineitem>`. Calcite generates plans at **runtime**. The bridge must either:
- (A) Pre-compile the optimizer-chosen template instantiations (not a combinatorial explosion — bounded by the number of pipeline configurations Calcite actually produces)
- (B) Use a runtime-dispatched C++ execution engine that avoids templates
- (C) Generate C++ source from the plan and compile on-the-fly (slow startup)

Option (A) is feasible because the set of instantiations is determined by running Calcite over the query workload first, then compiling exactly those configurations. This is Approach A in the architecture below.

### Template Classification
LeanStore templates fall into exactly **two categories** relevant to the integration:

**Category 1 — Query-plan-dependent (must pre-instantiate):** Templates whose type parameters are record schemas determined by the query plan. Includes `LeanStoreMergedAdapter<Records...>`, `LeanStoreMergedScanner<JK, JR, Records...>`, `PremergedJoin<...>`, `JoinState<...>`, `BinaryMergeJoin<...>`, `Heap<...>`. Pre-enumerating these classes automatically compiles all their member function templates (e.g., `insert<Record>()`, `seek<RecordType>()`, `get_next_all<Is...>()`) as part of the same instantiation — no separate enumeration needed for member templates or index-sequence metaprogramming helpers.

**Category 2 — Plan-independent (stay as templates):** Generic utilities whose logic does not vary by record schema: `overloaded<Ts...>`, `for_each`, `toType<Records...>`, `key_traits<K>`, `MatchKeyEqual<SK>`, `struct_from_bytes<T>`. These are called at runtime with concrete types from Category 1 instances and require no additional enumeration.

## 5. Recommended Integration Architecture

### Approach: Plan Export + C++ Runtime Interpreter

```
┌─────────────────────────────────────────────────────┐
│  Calcite (Java)                                      │
│                                                      │
│  SQL ──→ Parse ──→ Volcano ──→ MI Rules ──→ Plan    │
│                                                      │
│  Plan ──→ JSON/Protobuf serialization ──→ stdout    │
└──────────────────────────┬──────────────────────────┘
                           │ plan descriptor
                           ▼
┌─────────────────────────────────────────────────────┐
│  C++ Plan Interpreter (new component)                │
│                                                      │
│  Parse plan ──→ Build operator tree ──→ Execute     │
│                                                      │
│  Operators:                                          │
│    MergedIndexScan(mi_id, source_idx)               │
│      → LeanStoreMergedScanner                       │
│    PremergedJoin(mi_id, source_list)                │
│      → PremergedJoin<Rs...> (pre-instantiated)      │
│    Filter(predicate)                                 │
│      → runtime predicate evaluation                  │
│    SortedAggregate(group_keys, agg_fns)             │
│      → streaming aggregation on sorted input         │
│    Project(expressions)                              │
│      → field selection + arithmetic                  │
└──────────────────────────┬──────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────┐
│  LeanStore Storage Engine                            │
│    B-tree / LSM merged indexes                       │
│    Pre-sorted, interleaved multi-table data          │
└─────────────────────────────────────────────────────┘
```

### Why This Approach
- **No JNI complexity on the data path** — Calcite and LeanStore are separate processes
- **Plan is the contract** — JSON/protobuf plan descriptor is language-agnostic
- **Pre-instantiated templates** — for TPC-H (8 tables) and JOB (~26 tables), enumerate all merged index configurations at compile time; the interpreter dispatches to the right one at runtime
- **Incremental** — start with a few operators, add more as needed

### Template Pre-instantiation Strategy
Run Calcite over the target query workload first to collect the full set of pipeline configurations (each `MergedIndex` descriptor names its record types and key). Then generate a C++ registration file that instantiates exactly those `LeanStoreMergedAdapter<Rs...>` and `PremergedJoin<Rs...>` types and enters them into a runtime registry keyed by MI ID. All member templates and internal metaprogramming helpers are compiled automatically as part of these class instantiations.

For TPC-H: Q9 produces 5 merged indexes → 5 instantiations. For JOB (113 queries, ~26 tables), the number of distinct pipeline configurations is larger but still a small fraction of all possible subsets, because Calcite's cost-based planner selects a bounded set of join orders. A code generator (Calcite → C++ header) automates this step.

## 6. Major Milestones

### Milestone 1: Plan Serialization (Calcite side)
**Goal**: Export optimized plan (after MI substitution) as JSON.
- Serialize `RelNode` tree to JSON, including:
  - Operator type (MergedIndexScan, MergeJoin, Filter, SortedAggregate, Project)
  - MergedIndex ID, source index, collation
  - Filter predicates as expression trees
  - Aggregate functions and group keys
  - Projection expressions
- Use Calcite's existing `RelJsonWriter` / `RelJson` infrastructure as starting point
- **Output**: `calcite-plan.json` for each query

### Milestone 2: C++ Plan Parser
**Goal**: Read JSON plan in C++, build an operator tree.
- Define C++ operator base class with `next()` / iterator interface
- Parse JSON (e.g., nlohmann/json or rapidjson)
- Build operator tree from bottom up
- **Output**: `PlanNode*` tree ready for execution

### Milestone 3: Leaf Operator — MergedIndexScan
**Goal**: Connect plan's `MergedIndexScan` node to `LeanStoreMergedScanner`.
- Registry: map MI ID → `LeanStoreMergedAdapter<Rs...>` instance
- Scanner creation: `adapter.getScanner<JK, JR>()`
- Type conversion: plan's SQL types ↔ LeanStore's fold/unfold types
- **Output**: leaf scans return rows from merged B-tree

### Milestone 4: Join Assembly — PremergedJoin Integration
**Goal**: Connect plan's absorbed join pipelines to `PremergedJoin`.
- When plan has `MergedIndexScan` replacing an entire pipeline (sort + join + agg), dispatch to `PremergedJoin` with the right template parameters
- Map plan's source list to `PremergedJoin<Records...>` instantiation
- **Output**: multi-table join assembly from single merged index scan

### Milestone 5: Non-Leaf Operators (Filter, Aggregate, Project)
**Goal**: Implement runtime operators for plan nodes not absorbed by MI.
- **Filter**: runtime predicate evaluator (comparison, LIKE, arithmetic)
- **SortedAggregate**: streaming aggregation on pre-sorted input (SUM, COUNT, AVG, etc.)
- **Project**: field selection and expression evaluation
- **Sort**: should be rare (MI eliminates most sorts), but needed as fallback
- **Output**: full operator pipeline execution

### Milestone 6: End-to-End TPC-H Validation
**Goal**: Run TPC-H queries end-to-end: SQL → Calcite → JSON plan → C++ interpreter → LeanStore → results.
- Start with Q12 (simplest: 2-table, 1 MI)
- Then Q3-OL (3-table, 2 nested MIs)
- Then Q9 (6-table, 5 nested MIs)
- Validate result correctness against reference TPC-H answers
- **Output**: working end-to-end pipeline for select TPC-H queries

### Milestone 7: JOB Integration
**Goal**: Extend to Join Order Benchmark queries.
- JOB has ~113 queries over IMDB schema (~26 tables)
- Calcite generates plans with MI substitution for JOB queries
- Pre-instantiate merged adapters for JOB table combinations
- **Output**: JOB benchmark results with merged index optimization

### Milestone 8: Maintenance Plan Execution
**Goal**: Execute physical maintenance plans (delta propagation) in LeanStore.
- Parse maintenance plan (includes `MergedIndexDeltaScan` nodes)
- Connect to LeanStore's `insert()`/`erase()` operations for delta application
- Validate incremental maintenance correctness
- **Output**: insert/delete on base tables propagate through MI maintenance plans

## 7. Feasibility Assessment

### Feasible: High Confidence
- **Plan generation**: Already working in Calcite (Q12, Q3-OL, Q9 fully optimized)
- **Execution engine**: Already working in LeanStore (PremergedJoin, MergedScanner proven)
- **The bridge is the new work**: plan serialization + C++ interpreter

### Risks
| Risk | Severity | Mitigation |
|------|----------|------------|
| Template count for JOB | Low | Not combinatorial — bounded by optimizer-chosen configurations; code generator automates instantiation from Calcite plan output |
| Predicate/expression evaluation in C++ | Medium | Start with simple predicates (=, <, >, LIKE); extend as needed |
| Type mapping edge cases (VARCHAR, DECIMAL) | Low | TPC-H/JOB use standard types; map Calcite SQL types to LeanStore Types.hpp |
| Performance of JSON plan parsing | Low | One-time cost per query; negligible vs execution time |
| Aggregation not in current merged scanner | Medium | Implement streaming agg as separate C++ operator above scanner |

### Effort Estimate
- Milestones 1-3: ~2-3 weeks (plan serialization + basic scan)
- Milestones 4-5: ~2-3 weeks (join assembly + operators)
- Milestone 6: ~1-2 weeks (end-to-end validation)
- Milestones 7-8: ~3-4 weeks (JOB + maintenance)
- **Total: ~8-12 weeks** for full integration

### Alternative Approaches Considered
1. **JNI bridge**: Calcite calls LeanStore directly via JNI. Rejected — adds complexity to data path, debugging JNI is painful, and the plan-export approach is cleaner.
2. **Calcite code generation → C++**: Have Calcite generate C++ source code instead of Java. Rejected — requires modifying Calcite's code gen infrastructure deeply; the interpreter approach is more modular.
3. **Embed Calcite in LeanStore via GraalVM**: Run Calcite's JVM inside LeanStore's process. Possible but heavyweight; adds GraalVM dependency.

## 8. Relation to Current Calcite Work

The integration does NOT require changes to the current merged index optimization in Calcite. The plan tree already contains all necessary information:
- `EnumerableMergedIndexScan`: which MI, which source, scan group
- `EnumerableMergedIndexDeltaScan`: delta scans for maintenance
- `MergedIndex` descriptor: tables, collations, row counts
- `Pipeline` structure: nesting, boundaries, logical roots

The serialization layer (Milestone 1) simply exports what's already computed. All current and future Calcite-side work (additional TPC-H queries, JOB queries, cost model refinements) feeds directly into better plans for LeanStore to execute.
