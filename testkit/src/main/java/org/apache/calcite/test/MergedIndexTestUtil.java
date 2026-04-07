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
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.materialize.MergedIndex;
import org.apache.calcite.materialize.Pipeline;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexNode;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
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

  /**
   * Moves {@link EnumerableFilter} nodes that are the <em>direct input</em>
   * of a boundary Sort to just above that boundary Sort.
   *
   * <p>Merged indexes store unfiltered data so they can be shared across
   * queries with different predicates. A filter directly below a boundary
   * Sort references columns from that same row type (the Sort passes all
   * columns through unchanged), so moving it above the Sort is always valid.
   *
   * <p>Only direct-input filters are hoisted. Filters nested deeper (e.g.,
   * below a Project or Aggregate inside the pipeline) cannot be safely
   * hoisted because intermediate operators narrow the row type, making the
   * filter's {@link org.apache.calcite.rex.RexInputRef} indices invalid at
   * the Sort's output level.
   *
   * <p>Multiple chained filters directly below the Sort are all hoisted
   * (innermost condition becomes outermost after hoisting, preserving
   * short-circuit semantics).
   *
   * @param plan the Phase 1 physical plan (Volcano output, no HepRelVertex)
   * @return a new plan tree with direct-input boundary filters hoisted
   */
  public static RelNode hoistFiltersAboveBoundaries(RelNode plan) {
    return hoistFiltersImpl(plan);
  }

  private static RelNode hoistFiltersImpl(RelNode node) {
    // First recurse into children so inner boundaries are processed first.
    final List<RelNode> newInputs = node.getInputs().stream()
        .map(MergedIndexTestUtil::hoistFiltersImpl)
        .collect(Collectors.toList());
    final RelNode rebuilt = node.copy(node.getTraitSet(), newInputs);

    if (!Pipeline.isBoundarySort(rebuilt)) {
      return rebuilt;
    }

    // This is a boundary Sort: peel off any EnumerableFilter nodes that
    // are the direct (possibly chained) input of this Sort.  These filters
    // reference the Sort's own input row type, so hoisting them above the
    // Sort is safe — the Sort passes all columns through unchanged.
    final Sort sort = (Sort) rebuilt;
    final List<RexNode> conditions = new ArrayList<>();
    RelNode current = sort.getInput(0);
    while (current instanceof EnumerableFilter) {
      final EnumerableFilter filter = (EnumerableFilter) current;
      conditions.add(filter.getCondition());
      current = filter.getInput();
    }

    if (conditions.isEmpty()) {
      return rebuilt;
    }

    // Rebuild Sort directly over the filter-free input.
    final RelNode newSort = sort.copy(sort.getTraitSet(), List.of(current));

    // Stack the hoisted filters above the Sort.
    // conditions[0] was the outermost filter (closest to Sort); restore
    // that order so the first collected condition wraps the Sort first.
    RelNode result = newSort;
    for (int i = conditions.size() - 1; i >= 0; i--) {
      result = EnumerableFilter.create(result, conditions.get(i));
    }
    return result;
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
