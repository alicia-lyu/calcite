/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableLimitSort;
import org.apache.calcite.adapter.enumerable.EnumerableMergedIndexScan;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.materialize.MergedIndex;
import org.apache.calcite.materialize.Pipeline;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shared test helpers for merged-index pipeline tests.
 *
 * <p>Extracted from {@code MergedIndexTpchPlanTest} so that both
 * {@code core} and {@code plus} test suites can reuse them.
 */
public final class MergedIndexTestUtil {

  private MergedIndexTestUtil() {
  }

  // ── Sort injection ──────────────────────────────────────────────────────

  /**
   * Recursively walks the plan tree and injects {@link LogicalSort} nodes
   * before every sort-based operator ({@link Join}, {@link Aggregate})
   * that requires sorted input.
   *
   * <ul>
   *   <li><b>Sort node</b>: recurses into input; drops the Sort if the input
   *       is already sorted on the same fields and the Sort carries no
   *       FETCH/OFFSET (LIMIT nodes cannot be dropped safely).</li>
   *   <li><b>Aggregate node</b>: injects a {@link LogicalSort} on the group
   *       keys before the aggregate input when not already sorted.</li>
   *   <li><b>Join node</b>: injects sorts on join keys before each input
   *       when not already sorted; skips non-equi / cross joins.</li>
   * </ul>
   *
   * <p>Using {@link RelOptUtil#splitJoinCondition} for join keys means
   * multi-join plans (where each join uses a different key) are handled
   * correctly. {@link #inputAlreadySorted} prevents duplicate sorts by
   * drilling through single-input operators to check existing collation.
   */
  public static RelNode injectSortsBeforeSortBasedOps(RelNode node) {
    if (node instanceof Sort) {
      final Sort sort = (Sort) node;
      final RelNode newInput = injectSortsBeforeSortBasedOps(sort.getInput());
      // Drop a redundant Sort when input is already sorted on those fields
      // and the Sort carries no FETCH/OFFSET (LIMIT nodes carry row-count
      // semantics and must not be dropped).
      if (sort.fetch == null && sort.offset == null
          && !sort.getCollation().getFieldCollations().isEmpty()
          && inputAlreadySorted(newInput, sort.getCollation())) {
        return newInput;
      }
      return sort.copy(sort.getTraitSet(), List.of(newInput));
    }
    if (node instanceof Aggregate) {
      final Aggregate agg = (Aggregate) node;
      final RelNode newInput = injectSortsBeforeSortBasedOps(agg.getInput());
      if (!agg.getGroupSet().isEmpty()) {
        final RelCollation aggCollation = RelCollations.of(
            agg.getGroupSet().asList().stream()
                .map(RelFieldCollation::new).collect(Collectors.toList()));
        if (!inputAlreadySorted(newInput, aggCollation)) {
          return agg.copy(agg.getTraitSet(),
              List.of(LogicalSort.create(newInput, aggCollation, null, null)));
        }
      }
      return agg.copy(agg.getTraitSet(), List.of(newInput));
    }
    if (node instanceof Join) {
      final Join join = (Join) node;
      final List<Integer> leftKeys = new ArrayList<>();
      final List<Integer> rightKeys = new ArrayList<>();
      RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(),
          join.getCondition(), leftKeys, rightKeys, new ArrayList<>());
      final RelNode newLeft = injectSortsBeforeSortBasedOps(join.getLeft());
      final RelNode newRight = injectSortsBeforeSortBasedOps(join.getRight());
      if (leftKeys.isEmpty()) {
        // Non-equi join or cross join — recurse but do not inject sorts.
        return join.copy(join.getTraitSet(), List.of(newLeft, newRight));
      }
      // Build multi-column collations from all equi-join keys.
      final RelCollation leftCollation = RelCollations.of(
          leftKeys.stream()
              .map(RelFieldCollation::new)
              .collect(Collectors.toList()));
      final RelCollation rightCollation = RelCollations.of(
          rightKeys.stream()
              .map(RelFieldCollation::new)
              .collect(Collectors.toList()));
      final RelNode sortedLeft = inputAlreadySorted(newLeft, leftCollation)
          ? newLeft : LogicalSort.create(newLeft, leftCollation, null, null);
      final RelNode sortedRight = inputAlreadySorted(newRight, rightCollation)
          ? newRight : LogicalSort.create(newRight, rightCollation, null, null);
      return join.copy(join.getTraitSet(), List.of(sortedLeft, sortedRight));
    }
    final List<RelNode> newInputs = node.getInputs().stream()
        .map(MergedIndexTestUtil::injectSortsBeforeSortBasedOps)
        .collect(Collectors.toList());
    return node.copy(node.getTraitSet(), newInputs);
  }

  /**
   * Returns true if {@code input} is already sorted on {@code required} as a
   * field-index prefix (checking both index and direction).
   *
   * <p>Drills through single-input operators (Aggregate, Project, Filter, etc.)
   * that do not themselves carry a meaningful collation trait, stopping at a
   * {@link Sort} node whose collation is authoritative.
   */
  public static boolean inputAlreadySorted(RelNode input, RelCollation required) {
    RelNode node = input;
    while (node.getInputs().size() == 1 && !(node instanceof Sort)) {
      node = node.getInputs().get(0);
    }
    final RelCollation existing =
        node.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
    if (existing == null || existing.getFieldCollations().isEmpty()) {
      return false;
    }
    final List<RelFieldCollation> req = required.getFieldCollations();
    final List<RelFieldCollation> have = existing.getFieldCollations();
    if (have.size() < req.size()) {
      return false;
    }
    for (int i = 0; i < req.size(); i++) {
      if (req.get(i).getFieldIndex() != have.get(i).getFieldIndex()) {
        return false;
      }
      if (req.get(i).getDirection() != have.get(i).getDirection()) {
        return false;
      }
    }
    return true;
  }

  // ── Filter hoisting ─────────────────────────────────────────────────────

  // ── Widen-then-narrow helpers ──────────────────────────────────────────

  /**
   * Collects every {@link RexInputRef} index referenced by {@code cond}.
   */
  private static Set<Integer> collectInputRefIndices(RexNode cond) {
    final Set<Integer> refs = new HashSet<>();
    cond.accept(new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef ref) {
        refs.add(ref.getIndex());
        return ref;
      }
    });
    return refs;
  }

  /**
   * Returns {@code cond} with every {@link RexInputRef} whose index is
   * {@code >= oldLeftCount} bumped by {@code +shift}.
   *
   * <p>Used when the left side of a {@link Join} has been widened so that
   * right-side field indices must be shifted accordingly.
   */
  private static RexNode shiftRightSideRefs(
      RexNode cond, int oldLeftCount, int shift) {
    if (shift == 0) {
      return cond;
    }
    return cond.accept(new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef ref) {
        return ref.getIndex() >= oldLeftCount
            ? new RexInputRef(ref.getIndex() + shift, ref.getType())
            : ref;
      }
    });
  }

  /** Carrier for the result of {@link #widenProjectsForFilters}. */
  private static final class WidenResult {
    final RelNode node;
    final Set<Integer> needed;

    WidenResult(RelNode node, Set<Integer> needed) {
      this.node = node;
      this.needed = needed;
    }
  }

  /**
   * Post-order recursive pass that widens every {@link Project} on a filter's
   * ancestor path to append the filter's referenced columns as trailing
   * {@link RexInputRef} projections.
   *
   * <p>After widening, {@link #hoistFiltersImpl} can commute filters upward
   * through Projects that previously blocked commutation because they dropped
   * the filter's referenced columns.
   *
   * <p>A narrowing {@link EnumerableProject} is added at the root by
   * {@link #hoistFiltersAboveBoundaries} if the row type changed.
   *
   * @param node the (sub)plan to widen
   * @return a {@link WidenResult} carrying the rewritten node and the set of
   *         input-ref indices needed by filters above this node
   */
  private static WidenResult widenProjectsForFilters(RelNode node) {

    // ── Filter ────────────────────────────────────────────────────────────
    if (node instanceof EnumerableFilter || (node instanceof org.apache.calcite.rel.core.Filter
        && !(node instanceof EnumerableFilter))) {
      final org.apache.calcite.rel.core.Filter filter =
          (org.apache.calcite.rel.core.Filter) node;
      final WidenResult inner = widenProjectsForFilters(filter.getInput());
      // Widening only appends at the tail, so condition indices remain valid.
      final RelNode newFilter = filter.copy(filter.getTraitSet(),
          List.of(inner.node));
      final Set<Integer> needed = new HashSet<>(inner.needed);
      needed.addAll(collectInputRefIndices(filter.getCondition()));
      return new WidenResult(newFilter, needed);
    }

    // ── Project ───────────────────────────────────────────────────────────
    if (node instanceof Project) {
      final Project project = (Project) node;
      final WidenResult inner = widenProjectsForFilters(project.getInput());
      final RelNode newIn = inner.node;
      final Set<Integer> neededBelow = inner.needed;

      final List<RexNode> newProjs = new ArrayList<>(project.getProjects());
      // Build mapping: input-ref-index → project output index (first bare ref wins)
      final Map<Integer, Integer> mapping = new HashMap<>();
      for (int out = 0; out < newProjs.size(); out++) {
        final RexNode p = newProjs.get(out);
        if (p instanceof RexInputRef) {
          mapping.putIfAbsent(((RexInputRef) p).getIndex(), out);
        }
      }

      final Set<Integer> neededAbove = new HashSet<>();
      for (int inputIdx : neededBelow) {
        if (mapping.containsKey(inputIdx)) {
          neededAbove.add(mapping.get(inputIdx));
        } else {
          // Append a new bare ref for this input field
          final RelDataType type =
              newIn.getRowType().getFieldList().get(inputIdx).getType();
          final int newOutIdx = newProjs.size();
          newProjs.add(new RexInputRef(inputIdx, type));
          mapping.put(inputIdx, newOutIdx);
          neededAbove.add(newOutIdx);
        }
      }

      if (newProjs.size() == project.getProjects().size()) {
        // No widening; rebuild with potentially-updated input only.
        final RelNode newProject = project.copy(project.getTraitSet(),
            List.of(newIn));
        return new WidenResult(newProject, neededAbove);
      }

      // Build widened row type by appending new field descriptors.
      final RelDataTypeFactory.Builder b =
          project.getCluster().getTypeFactory().builder();
      for (RelDataTypeField f : project.getRowType().getFieldList()) {
        b.add(f);
      }
      for (int i = project.getRowType().getFieldCount(); i < newProjs.size(); i++) {
        final RexInputRef appended = (RexInputRef) newProjs.get(i);
        b.add("$f" + i, appended.getType());
      }
      final RelDataType newRowType = b.build();
      final RelNode newProject = EnumerableProject.create(newIn, newProjs, newRowType);
      return new WidenResult(newProject, neededAbove);
    }

    // ── Join ──────────────────────────────────────────────────────────────
    if (node instanceof Join) {
      final Join join = (Join) node;
      final WidenResult leftResult = widenProjectsForFilters(join.getLeft());
      final WidenResult rightResult = widenProjectsForFilters(join.getRight());

      final int oldLeftCount = join.getLeft().getRowType().getFieldCount();
      final int newLeftCount = leftResult.node.getRowType().getFieldCount();
      final int shift = newLeftCount - oldLeftCount;

      final RexNode newCond = shiftRightSideRefs(join.getCondition(),
          oldLeftCount, shift);
      final RelNode newJoin = join.copy(join.getTraitSet(), newCond,
          leftResult.node, rightResult.node,
          join.getJoinType(), join.isSemiJoinDone());

      // Merge needed sets: right-side indices must be offset by new left width.
      final Set<Integer> neededAbove = new HashSet<>(leftResult.needed);
      for (int i : rightResult.needed) {
        neededAbove.add(i + newLeftCount);
      }
      return new WidenResult(newJoin, neededAbove);
    }

    // ── EnumerableLimit (passthrough) ─────────────────────────────────────
    if (node instanceof EnumerableLimit) {
      final WidenResult inner = widenProjectsForFilters(
          node.getInputs().get(0));
      final RelNode rebuilt = node.copy(node.getTraitSet(),
          List.of(inner.node));
      return new WidenResult(rebuilt, inner.needed);
    }

    // ── Sort (covers EnumerableSort) — passthrough ────────────────────────
    if (node instanceof Sort) {
      final Sort sort = (Sort) node;
      final WidenResult inner = widenProjectsForFilters(sort.getInput());
      final RelNode rebuilt = sort.copy(sort.getTraitSet(),
          List.of(inner.node));
      return new WidenResult(rebuilt, inner.needed);
    }

    // ── Aggregate ─────────────────────────────────────────────────────────
    if (node instanceof Aggregate) {
      final Aggregate agg = (Aggregate) node;
      final WidenResult inner = widenProjectsForFilters(agg.getInput());
      final RelNode rebuilt = agg.copy(agg.getTraitSet(),
          List.of(inner.node));
      // Aggregates produce new output columns; no filter refs pass through.
      return new WidenResult(rebuilt, Collections.emptySet());
    }

    // ── SetOp ─────────────────────────────────────────────────────────────
    if (node instanceof SetOp) {
      final List<RelNode> newInputs = new ArrayList<>();
      for (RelNode child : node.getInputs()) {
        newInputs.add(widenProjectsForFilters(child).node);
      }
      final RelNode rebuilt = node.copy(node.getTraitSet(), newInputs);
      return new WidenResult(rebuilt, Collections.emptySet());
    }

    // ── Default: recurse into children, discard needed sets ───────────────
    if (node.getInputs().isEmpty()) {
      return new WidenResult(node, Collections.emptySet());
    }
    final List<RelNode> newInputs = new ArrayList<>();
    for (RelNode child : node.getInputs()) {
      newInputs.add(widenProjectsForFilters(child).node);
    }
    final RelNode rebuilt = node.copy(node.getTraitSet(), newInputs);
    return new WidenResult(rebuilt, Collections.emptySet());
  }

  /**
   * Commutes {@link EnumerableFilter} nodes upward past operators that
   * preserve the filter's semantic validity.
   *
   * <p>This is a general-purpose upward commutation pass, not limited to
   * boundary Sorts. It walks the plan in post-order (children first), then
   * for each node tries to peel {@link EnumerableFilter} chains from the top
   * of each child and commute them past the current node.
   *
   * <h3>Commutation rules</h3>
   * <ul>
   *   <li><b>{@link Sort}</b>: always safe — Sort passes all columns through
   *       unchanged, so the filter's {@link RexInputRef} indices are valid
   *       above the Sort without rewriting.</li>
   *   <li><b>{@link Project}</b>: safe only if every {@link RexInputRef} in
   *       the filter condition maps to a <em>bare</em> {@link RexInputRef}
   *       in the Project's output list (no computed expressions, functions,
   *       or literals). When safe, each input ref is rewritten to the
   *       Project-output index that carries that field.</li>
   *   <li><b>{@link Join}</b>: not commuted for now. Joins change field
   *       counts and require careful left/right offset arithmetic; skipping
   *       avoids correctness risks at the cost of missing some opportunities.
   *       (Future work: commute single-side filters through Join.)</li>
   *   <li><b>{@link Aggregate}, {@link SetOp}</b>: never commuted — these
   *       operators collapse or reorder rows, making filter ref indices
   *       invalid above them.</li>
   *   <li>Everything else: default to NOT commuting (safe fallback).</li>
   * </ul>
   *
   * <p>Merged indexes store unfiltered data so they can be shared across
   * queries with different predicates. Hoisting filters above boundary Sorts
   * lets the boundary Sort (and everything below it) be replaced by a merged
   * index scan without carrying the filter into the index.
   *
   * @param plan the Phase 1 physical plan (Volcano output, no HepRelVertex)
   * @return a new plan tree with filters commuted as high as possible
   */
  public static RelNode hoistFiltersAboveBoundaries(RelNode plan) {
    final RelDataType originalRowType = plan.getRowType();
    final WidenResult widened = widenProjectsForFilters(plan);
    final RelNode hoisted = hoistFiltersImpl(widened.node);
    if (!hoisted.getRowType().equals(originalRowType)) {
      // The widen pass added trailing columns that are not part of the query
      // result; add a narrowing Project to restore the original row type.
      final List<RexNode> narrowProjs = new ArrayList<>();
      for (int i = 0; i < originalRowType.getFieldCount(); i++) {
        narrowProjs.add(
            new RexInputRef(i, originalRowType.getFieldList().get(i).getType()));
      }
      return EnumerableProject.create(hoisted, narrowProjs, originalRowType);
    }
    return hoisted;
  }

  /**
   * Recursive post-order implementation of the upward commutation pass.
   *
   * <p>For each node:
   * <ol>
   *   <li>Recursively process all children (post-order).</li>
   *   <li>For each processed child, try to peel {@link EnumerableFilter}
   *       nodes from the top and rewrite their conditions to be valid above
   *       the current node.</li>
   *   <li>If any liftable conditions are found, rebuild the current node
   *       with the stripped children and stack the filters above it.</li>
   * </ol>
   */
  private static RelNode hoistFiltersImpl(RelNode node) {
    // Step 1: recurse into children first (post-order).
    final List<RelNode> processedInputs = new ArrayList<>();
    for (RelNode child : node.getInputs()) {
      processedInputs.add(hoistFiltersImpl(child));
    }

    // Rebuild node with processed children (may be same objects if unchanged).
    final RelNode current;
    if (processedInputs.equals(node.getInputs())) {
      current = node;
    } else {
      current = node.copy(node.getTraitSet(), processedInputs);
    }

    // Step 2: for each input, try to peel and commute filters upward.
    final List<RexNode> liftedConditions = new ArrayList<>();
    final List<RelNode> strippedInputs = new ArrayList<>();
    boolean anyLifted = false;

    for (int i = 0; i < current.getInputs().size(); i++) {
      final RelNode child = current.getInputs().get(i);
      final List<RexNode> peeled = new ArrayList<>();
      final RelNode stripped = peelFilters(child, peeled);

      if (peeled.isEmpty()) {
        // No filters to lift from this child.
        strippedInputs.add(child);
        continue;
      }

      // Try to rewrite each peeled condition to be valid above `current`.
      final List<RexNode> rewritten = new ArrayList<>();
      boolean allRewritten = true;
      for (RexNode cond : peeled) {
        final Optional<RexNode> rw = rewriteForParent(cond, current, i);
        if (rw.isPresent()) {
          rewritten.add(rw.get());
        } else {
          allRewritten = false;
          break;
        }
      }

      if (allRewritten) {
        strippedInputs.add(stripped);
        liftedConditions.addAll(rewritten);
        anyLifted = true;
      } else {
        // Cannot commute — keep child with its filters.
        strippedInputs.add(child);
      }
    }

    if (!anyLifted) {
      return current;
    }

    // Step 3: rebuild current with stripped children, stack filters above.
    final RelNode rebuilt = current.copy(current.getTraitSet(), strippedInputs);
    RelNode result = rebuilt;
    // Stack outermost-first: last lifted condition becomes the outermost filter.
    for (int i = liftedConditions.size() - 1; i >= 0; i--) {
      result = EnumerableFilter.create(result, liftedConditions.get(i));
    }
    return result;
  }

  /**
   * Peels a chain of {@link EnumerableFilter} nodes from the top of
   * {@code node}. Returns the innermost non-filter node (the stripped
   * subtree) and appends the filter conditions (outermost first) to
   * {@code conditions}.
   */
  private static RelNode peelFilters(RelNode node, List<RexNode> conditions) {
    RelNode current = node;
    while (current instanceof EnumerableFilter) {
      final EnumerableFilter filter = (EnumerableFilter) current;
      conditions.add(filter.getCondition());
      current = filter.getInput();
    }
    return current;
  }

  /**
   * Tries to rewrite {@code condition} (currently valid above {@code parent}'s
   * {@code inputIdx}-th input, after filter-peeling) so that it is valid
   * above {@code parent} itself.
   *
   * <p>Returns {@link Optional#empty()} if commutation is not safe.
   *
   * <h3>Commutation rules</h3>
   * <ul>
   *   <li>{@link Sort}: indices unchanged — Sort passes all columns through.</li>
   *   <li>{@link Project}: rewrite each {@link RexInputRef} via the project
   *       list; abort if any ref points to a non-bare-ref projection.</li>
   *   <li>{@link Join}: not commuted (safe fallback). See class Javadoc.</li>
   *   <li>{@link Aggregate}, {@link SetOp}: never commuted.</li>
   *   <li>Everything else: not commuted.</li>
   * </ul>
   */
  private static Optional<RexNode> rewriteForParent(
      RexNode condition, RelNode parent, int inputIdx) {

    if (parent instanceof Sort) {
      // Sort passes all columns through — no index rewriting needed.
      return Optional.of(condition);
    }

    if (parent instanceof Project) {
      // Rewrite filter refs through the project's output list.
      // Only safe if every referenced input field is a bare RexInputRef
      // in the project's output list.
      final Project project = (Project) parent;
      final List<RexNode> projects = project.getProjects();

      // Build a mapping from project-output-index → input-ref-index
      // (only for bare-RexInputRef outputs).
      // We need the reverse: for each input ref `oldIdx` in the filter,
      // find a project output that is RexInputRef(oldIdx).
      // Build: inputRefIndex → projectOutputIndex
      final int[] inputToOutput = new int[project.getInput().getRowType().getFieldCount()];
      java.util.Arrays.fill(inputToOutput, -1);
      for (int j = 0; j < projects.size(); j++) {
        final RexNode proj = projects.get(j);
        if (proj instanceof RexInputRef) {
          final int inputIdx2 = ((RexInputRef) proj).getIndex();
          if (inputToOutput[inputIdx2] == -1) {
            inputToOutput[inputIdx2] = j;
          }
        }
      }

      // Check that all RexInputRefs in the condition are remappable.
      final boolean[] feasible = {true};
      condition.accept(new RexShuttle() {
        @Override public RexNode visitInputRef(RexInputRef ref) {
          final int idx = ref.getIndex();
          if (idx >= inputToOutput.length || inputToOutput[idx] == -1) {
            feasible[0] = false;
          }
          return ref;
        }
      });
      if (!feasible[0]) {
        return Optional.empty();
      }

      // Rewrite: replace each RexInputRef(oldIdx) → RexInputRef(newIdx).
      final RexNode rewritten = condition.accept(new RexShuttle() {
        @Override public RexNode visitInputRef(RexInputRef ref) {
          final int newIdx = inputToOutput[ref.getIndex()];
          return new RexInputRef(newIdx, ref.getType());
        }
      });
      return Optional.of(rewritten);
    }

    // Join, Aggregate, SetOp, and everything else: do not commute.
    // Join commutation requires careful left/right offset tracking;
    // Aggregate and SetOp change column structure entirely.
    return Optional.empty();
  }

  // ── LimitSort splitting ──────────────────────────────────────────────────

  /**
   * Recursively rewrites every {@code EnumerableLimitSort} in the tree as
   * {@code EnumerableLimit(EnumerableSort(...))}. After this pass, boundary
   * sorts are plain {@code EnumerableSort} nodes; LIMIT/OFFSET lives in a
   * separate operator above the sort, forming its own tiny top pipeline.
   */
  public static RelNode splitLimitSorts(RelNode node) {
    // Recurse into children first (post-order).
    final List<RelNode> newInputs = new ArrayList<>();
    boolean changed = false;
    for (RelNode child : node.getInputs()) {
      RelNode newChild = splitLimitSorts(child);
      if (newChild != child) {
        changed = true;
      }
      newInputs.add(newChild);
    }
    RelNode current = changed ? node.copy(node.getTraitSet(), newInputs) : node;

    if (current instanceof EnumerableLimitSort) {
      EnumerableLimitSort ls = (EnumerableLimitSort) current;
      RelNode input = ls.getInput();
      // Build plain EnumerableSort (no fetch/offset)
      EnumerableSort sort =
          EnumerableSort.create(input, ls.getCollation(), null, null);
      // Wrap in EnumerableLimit carrying the fetch/offset
      return EnumerableLimit.create(sort, ls.offset, ls.fetch);
    }
    return current;
  }

  // ── Tree search helpers ─────────────────────────────────────────────────

  /**
   * Collects all {@link Join} nodes in post-order (innermost first).
   */
  public static List<Join> findAllJoins(RelNode node) {
    final List<Join> result = new ArrayList<>();
    for (RelNode input : node.getInputs()) {
      result.addAll(findAllJoins(input));
    }
    if (node instanceof Join) {
      result.add((Join) node);
    }
    return result;
  }

  /** Counts non-overlapping occurrences of {@code sub} in {@code text}. */
  public static int countOccurrences(String text, String sub) {
    int count = 0;
    int idx = 0;
    while ((idx = text.indexOf(sub, idx)) != -1) {
      count++;
      idx += sub.length();
    }
    return count;
  }

  // ── Index creation plan helpers ───────────────────────────────────────

  /**
   * Finds the index creation plan root for pipeline {@code p} in a
   * post-HEP plan. The creation plan is the entire pipeline subtree:
   * the node directly below a parent boundary Sort whose subtree
   * contains MIScans referencing {@code mi}.
   *
   * <p>This captures all pipeline operators (MergeJoin, SortedAggregate,
   * Project, etc.), not just the join node. The creation plan reads from
   * {@code mi} (via MIScans) and produces rows for the parent MI.
   *
   * @return the creation plan root, or null if not found
   */
  public static @Nullable RelNode findCreationPlanRoot(
      RelNode plan, MergedIndex mi) {
    return findBelowBoundarySort(plan, mi);
  }

  /**
   * Walks the plan tree looking for the <b>innermost</b> (deepest) boundary
   * Sort whose input subtree contains MIScans for {@code mi}. Returns the
   * Sort's input (the pipeline root — the entire pipeline subtree).
   *
   * <p>For nested pipelines (Q9), the outermost boundary Sort's subtree
   * contains all levels. We must recurse through boundary Sorts to find
   * the nearest one to the MIScans.
   */
  private static @Nullable RelNode findBelowBoundarySort(
      RelNode node, MergedIndex mi) {
    for (RelNode input : node.getInputs()) {
      if (Pipeline.isBoundarySort(input)) {
        RelNode below = ((EnumerableSort) input).getInput();
        if (containsMIScan(below, mi)) {
          // Try to find a deeper boundary Sort within this subtree
          RelNode deeper = findBelowBoundarySort(below, mi);
          return deeper != null ? deeper : below;
        }
      } else {
        RelNode found = findBelowBoundarySort(input, mi);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  /** Checks if the subtree contains an MIScan referencing {@code mi}. */
  private static boolean containsMIScan(RelNode node, MergedIndex mi) {
    if (node instanceof EnumerableMergedIndexScan
        && ((EnumerableMergedIndexScan) node).mergedIndex == mi) {
      return true;
    }
    for (RelNode input : node.getInputs()) {
      if (containsMIScan(input, mi)) {
        return true;
      }
    }
    return false;
  }

  // ── DAG → tree conversion ─────────────────────────────────────────────

  /**
   * Converts a DAG-shaped RelNode graph into a tree by copying
   * any node that is visited more than once.
   *
   * <p>Calcite's {@code DeltaJoinTransposeRule} reuses the same RelNode
   * references in both union branches of the IVM formula, producing a DAG.
   *
   * <p><b>Limitation:</b> Some leaf nodes (e.g., {@code LogicalTableScan})
   * return {@code this} from {@link RelNode#copy}, so identity-based sharing
   * may persist at leaves. For guaranteed tree-shaped DOT output, use
   * per-visit ID rendering (tree-mode DOT) instead of this method.
   *
   * @param root the (possibly DAG-shaped) plan
   * @return a copy with non-leaf shared nodes duplicated
   */
  public static RelNode treeifyPlan(RelNode root) {
    return treeifyPlan(root, new IdentityHashMap<>());
  }

  private static RelNode treeifyPlan(RelNode node,
      IdentityHashMap<RelNode, Boolean> visited) {
    // First, recursively treeify all children
    final List<RelNode> oldInputs = node.getInputs();
    final List<RelNode> newInputs = new ArrayList<>(oldInputs.size());
    boolean inputChanged = false;
    for (RelNode input : oldInputs) {
      RelNode newInput;
      if (visited.containsKey(input)) {
        // This subtree was already visited — deep copy it
        newInput = deepCopy(input);
      } else {
        visited.put(input, Boolean.TRUE);
        newInput = treeifyPlan(input, visited);
      }
      newInputs.add(newInput);
      if (newInput != input) {
        inputChanged = true;
      }
    }
    if (inputChanged) {
      return node.copy(node.getTraitSet(), newInputs);
    }
    return node;
  }

  /** Deep-copies a RelNode subtree so that no node is shared with the original. */
  private static RelNode deepCopy(RelNode node) {
    final List<RelNode> oldInputs = node.getInputs();
    if (oldInputs.isEmpty()) {
      // Leaf node — copy it to get a new Java object
      return node.copy(node.getTraitSet(), Collections.emptyList());
    }
    final List<RelNode> newInputs = new ArrayList<>(oldInputs.size());
    for (RelNode input : oldInputs) {
      newInputs.add(deepCopy(input));
    }
    return node.copy(node.getTraitSet(), newInputs);
  }
}
