# Session Progress: Merged Index Feature

## What Was Completed

### Commits
1. `32635df48` — Added `CLAUDE.md` (research context) and `main.tex` (paper draft)
2. `0025c1f86` — Core merged index implementation (5 files, 459 insertions)

### Files Created
| File | Status |
|------|--------|
| `core/.../materialize/MergedIndex.java` | Done |
| `core/.../materialize/MergedIndexRegistry.java` | Done |
| `core/.../enumerable/EnumerableMergedIndexScan.java` | Done |
| `core/.../enumerable/PipelineToMergedIndexScanRule.java` | Done |
| `core/.../enumerable/EnumerableRules.java` | Done (constant added) |

### What Each File Does
- **MergedIndex**: Descriptor holding `List<RelOptTable> tables`, `RelCollation collation`, `double rowCount`. Key method: `satisfies(RelCollation)` for prefix-match.
- **MergedIndexRegistry**: Static singleton; `register(MergedIndex)`, `findFor(tables, collation)`, `clear()`. Matches by qualified table name.
- **EnumerableMergedIndexScan**: Leaf `AbstractRelNode` + `EnumerableRel`. Takes `MergedIndex` + joined `rowType`. Cost: `(rc, rc*0.1, rc)`. `implement()` returns empty enumerable (PoC stub).
- **PipelineToMergedIndexScanRule**: Matches `EnumerableMergeJoin(EnumerableSort(TableScan), EnumerableSort(TableScan))`. Looks up registry by collation from left sort. Creates `EnumerableMergedIndexScan` with the merge join's row type.
- **EnumerableRules**: Added `ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE` constant (NOT in `ENUMERABLE_RULES` list).

## What Remains

### 1. Integration Test (highest priority)
File to create: `core/src/test/java/org/apache/calcite/adapter/enumerable/PipelineToMergedIndexScanRuleTest.java`

Test plan:
- Create a custom schema (`s`) with two `PkClusteredTable`-style tables:
  - `A(k INT, x INT)` clustered on column 0 (`k`)
  - `B(k INT, y INT)` clustered on column 0 (`k`)
- Use a two-phase planner (like `SortRemoveRuleTest.Fixture`):
  - Phase 1 rules: `ENUMERABLE_MERGE_JOIN_RULE`, `ENUMERABLE_SORT_RULE`, `ENUMERABLE_TABLE_SCAN_RULE`
  - Between phases: walk the plan to extract `RelOptTable` refs from the two `EnumerableTableScan` nodes, then call `MergedIndexRegistry.register(new MergedIndex(tables, collation, rowCount))`
  - Phase 2 rules: `ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE`
- SQL: `SELECT "a"."x", "b"."y" FROM "s"."A" "a" JOIN "s"."B" "b" ON "a"."k" = "b"."k"`
- Assert:
  - Plan string contains `"EnumerableMergedIndexScan"`
  - Plan string does NOT contain `"EnumerableSort"`
  - Plan string does NOT contain `"EnumerableMergeJoin"`
- Call `MergedIndexRegistry.clear()` in `@AfterEach`

Key pattern to copy from: `SortRemoveRuleTest` + `HrClusteredSchema`
Key challenge: need to extract `RelOptTable` from the intermediate plan to register the index before phase 2.

### 2. Verify Compilation
Run `./gradlew :core:compileJava` to check that:
- Immutables annotation processor generates `ImmutablePipelineToMergedIndexScanRule`
- No missing imports in `EnumerableMergedIndexScan` (`BuiltInMethod.EMPTY_ENUMERABLE`)
- `List.of(...)` works (Java 9+; project uses Java 11+)

### 3. Known Risks / Likely Issues
- `BuiltInMethod.EMPTY_ENUMERABLE` — verify this method name exists; may need to use `Expressions.call(Linq4j.class, "emptyEnumerable")` instead
- `List.of(...)` in `PipelineToMergedIndexScanRule.onMatch` — confirm Java version compatibility (should be fine for Java 11+)
- The `@Value.Immutable` on `PipelineToMergedIndexScanRule.Config` generates `ImmutablePipelineToMergedIndexScanRule` — confirm annotation processor runs on this new file
- `EnumerableMergedIndexScan.implement()` cast to `EnumerableRel` in context — the empty enumerable return type may need explicit generic type parameter

### 4. Optional Extensions (per paper)
- Extend rule to also match `EnumerableSortedAggregate` on top of the join pattern
- Add `MergedIndex` entries to `Lattice` / `MaterializationService` for schema-level registration
- Implement a real `implement()` body that reads from a mock sorted data source
- Add negative test: no registered index → rule does not fire
