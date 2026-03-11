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

import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.materialize.Pipeline;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
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

  // ── Pipeline discovery ──────────────────────────────────────────────────

  /**
   * Builds a pipeline tree from the Phase 1 physical plan by identifying
   * {@link EnumerableSort} operators as boundaries between pipelines.
   *
   * @param planRoot the Phase 1 physical plan root
   * @return the root pipeline of the pipeline tree
   */
  public static Pipeline buildPipelineTree(RelNode planRoot) {
    return buildPipeline(planRoot);
  }

  /**
   * Recursively builds a single pipeline rooted at {@code node}.
   * Collects child pipelines by finding Sort boundaries among descendants.
   */
  private static Pipeline buildPipeline(RelNode node) {
    final List<Pipeline> childPipelines = new ArrayList<>();
    collectChildPipelines(node, childPipelines);
    final RelCollation collation;
    if (!childPipelines.isEmpty()
        && !childPipelines.get(0).boundaryCollation.getFieldCollations()
            .isEmpty()) {
      collation = childPipelines.get(0).boundaryCollation;
    } else {
      collation = inferCollation(node);
    }
    final double rowCount =
        node.estimateRowCount(node.getCluster().getMetadataQuery());
    return new Pipeline(node, childPipelines, collation, collation, rowCount);
  }

  /**
   * Recurses into {@code node}'s inputs looking for Sort boundaries.
   * When hitting an {@link EnumerableSort} boundary, creates a child Pipeline
   * rooted at the Sort's input. Otherwise continues recursing.
   */
  private static void collectChildPipelines(RelNode node,
      List<Pipeline> result) {
    for (RelNode input : node.getInputs()) {
      if (Pipeline.isBoundarySort(input)) {
        final EnumerableSort sort = (EnumerableSort) input;
        final RelNode below = sort.getInput();
        final Pipeline child = buildPipeline(below);
        result.add(new Pipeline(below, child.sources,
            child.sharedCollation, sort.getCollation(), child.rowCount));
      } else {
        collectChildPipelines(input, result);
      }
    }
  }

  /**
   * Infers the shared collation for a pipeline rooted at {@code node}.
   * First checks the node's own trait set, then drills through single-input
   * operators to find an authoritative Sort.
   */
  private static RelCollation inferCollation(RelNode node) {
    final RelCollation c = safeGetCollation(node);
    if (c != null && !c.getFieldCollations().isEmpty()) {
      return c;
    }
    RelNode cur = node;
    while (cur.getInputs().size() == 1 && !(cur instanceof Sort)) {
      cur = cur.getInputs().get(0);
    }
    if (cur instanceof Sort) {
      return ((Sort) cur).getCollation();
    }
    final RelCollation deep = safeGetCollation(cur);
    if (deep != null && !deep.getFieldCollations().isEmpty()) {
      return deep;
    }
    return RelCollations.EMPTY;
  }

  /**
   * Safely extracts the first {@link RelCollation} from a node's trait set.
   * Uses {@code getTraits()} to avoid the {@code RelCompositeTrait} cast bug.
   */
  public static @Nullable RelCollation safeGetCollation(RelNode node) {
    final List<RelCollation> collations =
        node.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
    if (collations == null || collations.isEmpty()) {
      return null;
    }
    return collations.get(0);
  }

  /**
   * Post-order flattening of the pipeline tree: leaves first, root last.
   * Includes all pipelines with at least one source.
   */
  public static List<Pipeline> flattenPipelines(Pipeline root) {
    final List<Pipeline> result = new ArrayList<>();
    flattenPostOrder(root, result);
    return result;
  }

  private static void flattenPostOrder(Pipeline p, List<Pipeline> result) {
    for (Pipeline child : p.sources) {
      flattenPostOrder(child, result);
    }
    if (p.sources.size() >= 1) {
      result.add(p);
    }
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
}
