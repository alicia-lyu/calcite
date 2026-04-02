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
package org.apache.calcite.materialize;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalPipelineOutputScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.stream.LogicalDelta;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSets;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for deriving and converting maintenance plans for
 * {@link MergedIndex} pipelines.
 *
 * <p>Provides three operations:
 * <ol>
 *   <li>{@link #deriveMaintenancePlan(Pipeline)} — applies IVM delta rules to
 *       produce a logical maintenance plan for a single pipeline.
 *   <li>{@link #scopeLogicalRoot(Pipeline)} — replaces child pipeline boundaries
 *       with {@link LogicalPipelineOutputScan} placeholders before delta push-down.
 *   <li>{@link #convertToPhysical(RelNode)} — converts a logical maintenance plan
 *       to a fully physical (enumerable) plan via three passes.
 * </ol>
 */
public class MaintenancePlanConverter {

  /** StreamRules for IVM delta derivation.
   *  Excludes DeltaTableScanRule (requires StreamableTable) and
   *  DeltaTableScanToEmptyRule (would convert delta leaves to empty Values). */
  private static final ImmutableList<RelOptRule> IVM_RULES = ImmutableList.of(
      StreamRules.DeltaProjectTransposeRule.DeltaProjectTransposeRuleConfig.DEFAULT.toRule(),
      StreamRules.DeltaFilterTransposeRule.DeltaFilterTransposeRuleConfig.DEFAULT.toRule(),
      StreamRules.DeltaAggregateTransposeRule.DeltaAggregateTransposeRuleConfig.DEFAULT.toRule(),
      StreamRules.DeltaSortTransposeRule.DeltaSortTransposeRuleConfig.DEFAULT.toRule(),
      StreamRules.DeltaUnionTransposeRule.DeltaUnionTransposeRuleConfig.DEFAULT.toRule(),
      StreamRules.DeltaJoinTransposeRule.DeltaJoinTransposeRuleConfig.DEFAULT.toRule());

  /** Private constructor — utility class, not instantiated. */
  private MaintenancePlanConverter() {}

  /**
   * Derives a logical IVM maintenance plan for a pipeline by scoping its
   * logicalRoot, wrapping it in a {@link LogicalDelta}, and pushing the delta
   * through joins/projects/filters via StreamRules.
   *
   * <p>The resulting plan contains one delta branch per source pipeline
   * (separated by the union introduced by DeltaJoinTransposeRule). Each branch
   * starts from a {@link LogicalPipelineOutputScan} placeholder that represents
   * the input from the corresponding child pipeline.
   *
   * <p>Note: DeltaJoinTransposeRule produces a union of two branches —
   * delta-A joined with new-B, then new-A (= A union delta-A) joined with
   * delta-B. Evaluating &#x394;B against new A (= A &#x222A; &#x394;A) is an
   * execution-tier concern outside Calcite's plan-generation scope; the plan
   * structure is correct regardless.
   *
   * @param pipeline the pipeline whose maintenance plan to derive
   */
  public static RelNode deriveMaintenancePlan(Pipeline pipeline) {
    final RelNode scoped = scopeLogicalRoot(pipeline);
    final LogicalDelta delta = LogicalDelta.create(scoped);
    final HepProgram hepProgram = HepProgram.builder()
        .addRuleCollection(IVM_RULES)
        .build();
    final HepPlanner hep = new HepPlanner(hepProgram);
    hep.setRoot(delta);
    return hep.findBestExp();
  }

  /**
   * Converts a logical maintenance plan to a fully physical (enumerable) plan.
   *
   * <p>Three passes:
   * <ol>
   *   <li>Pass 1 (HEP): convert {@code LogicalPipelineOutputScan} nodes to
   *       {@link org.apache.calcite.adapter.enumerable.EnumerableMergedIndexScan} via
   *       {@link EnumerableRules#PIPELINE_OUTPUT_SCAN_RULE}.
   *   <li>Pass 2 (HEP): fold {@code LogicalDelta(EnumerableMergedIndexScan)} →
   *       {@link org.apache.calcite.adapter.enumerable.EnumerableMergedIndexDeltaScan} via
   *       {@link EnumerableRules#ENUMERABLE_DELTA_TO_MERGED_INDEX_DELTA_SCAN_RULE}.
   *   <li>Pass 3 (Volcano): convert remaining logical operators
   *       (LogicalJoin, LogicalUnion, LogicalProject, LogicalFilter, etc.)
   *       to their enumerable counterparts using standard
   *       {@code ENUMERABLE_*} rules. A fresh {@link VolcanoPlanner} is used
   *       so the already-enumerable leaf scans from passes 1–2 are accepted
   *       as-is; only the non-enumerable nodes above them are converted.
   * </ol>
   *
   * @param logicalPlan the logical maintenance plan to convert
   * @return a fully physical (enumerable) maintenance plan
   */
  public static RelNode convertToPhysical(RelNode logicalPlan) {
    // Pass 1: convert LogicalPipelineOutputScan → EnumerableMergedIndexScan
    final HepProgram pass1 = HepProgram.builder()
        .addRuleInstance(EnumerableRules.PIPELINE_OUTPUT_SCAN_RULE)
        .build();
    final HepPlanner hep1 = new HepPlanner(pass1);
    hep1.setRoot(logicalPlan);
    final RelNode afterPass1 = hep1.findBestExp();

    // Pass 2: fold LogicalDelta(EnumerableMergedIndexScan) → EnumerableMergedIndexDeltaScan
    final HepProgram pass2 = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_DELTA_TO_MERGED_INDEX_DELTA_SCAN_RULE)
        .build();
    final HepPlanner hep2 = new HepPlanner(pass2);
    hep2.setRoot(afterPass1);
    final RelNode afterPass2 = hep2.findBestExp();

    // Pass 3: Volcano — convert remaining logical operators to enumerable.
    // The maintenance plan nodes share their cluster with the Phase 1
    // VolcanoPlanner (HEP reuses the same cluster). We extract that planner
    // so Volcano's registerImpl check passes. RuleSetProgram.run() calls
    // planner.clear() first, which is safe since Phase 1 planning is done.
    final VolcanoPlanner volcano =
        (VolcanoPlanner) afterPass2.getCluster().getPlanner();
    final Program pass3 = Programs.of(RuleSets.ofList(
        EnumerableRules.ENUMERABLE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_UNION_RULE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_FILTER_RULE,
        EnumerableRules.ENUMERABLE_SORT_RULE,
        EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
        EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE,
        EnumerableRules.ENUMERABLE_VALUES_RULE,
        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
        CoreRules.SORT_REMOVE));
    final RelTraitSet targetTraits =
        afterPass2.getTraitSet().replace(EnumerableConvention.INSTANCE);
    return pass3.run(volcano, afterPass2, targetTraits,
        ImmutableList.of(), ImmutableList.of());
  }

  /**
   * Creates a scoped copy of a pipeline's logicalRoot, replacing ALL source
   * pipeline boundaries (both leaf table scans and non-leaf MI views) with
   * {@link LogicalPipelineOutputScan} placeholders before delta push-down.
   *
   * <p>Every child pipeline boundary sort whose {@code .getInput()} identity-
   * matches a child's logicalRoot is replaced with a
   * {@link LogicalPipelineOutputScan} carrying the child pipeline. This ensures
   * that delta propagation stays within the current pipeline's scope: it stops
   * at all child pipeline boundaries, whether those boundaries are base-table
   * scans (leaf pipelines) or inner merged-index views (non-leaf pipelines).
   *
   * <p>Identification: a child's logicalRoot was set from a boundary sort's
   * input in {@link Pipeline#captureLogicalRoots}. This method finds boundary
   * sorts whose {@code .getInput()} identity-matches any child's logicalRoot
   * and replaces them with {@link LogicalPipelineOutputScan} placeholders.
   *
   * @param pipeline the pipeline to scope
   * @return scoped copy (or original if no children have a logicalRoot)
   */
  public static RelNode scopeLogicalRoot(Pipeline pipeline) {
    if (pipeline.logicalRoot == null) {
      throw new IllegalArgumentException("Pipeline has no logicalRoot");
    }
    if (pipeline.sources.isEmpty()) {
      return pipeline.logicalRoot; // leaf pipeline, no scoping needed
    }

    // Collect logicalRoots of all children (both leaf table scans and MI views)
    final Map<RelNode, Pipeline> childLogicalRoots = new IdentityHashMap<>();
    for (Pipeline child : pipeline.sources) {
      if (child.logicalRoot != null) {
        childLogicalRoots.put(child.logicalRoot, child);
      }
    }

    if (childLogicalRoots.isEmpty()) {
      return pipeline.logicalRoot; // no children with logicalRoot
    }

    return replaceChildBoundaries(pipeline.logicalRoot, childLogicalRoots);
  }

  /**
   * Walks a RelNode tree and replaces boundary sorts whose input
   * identity-matches a child logicalRoot with a
   * {@link LogicalPipelineOutputScan} placeholder carrying the child pipeline.
   */
  private static RelNode replaceChildBoundaries(RelNode node,
      Map<RelNode, Pipeline> childLogicalRoots) {
    if (Pipeline.isLogicalBoundarySort(node)) {
      final Sort sort = (Sort) node;
      final Pipeline childPipeline = childLogicalRoots.get(sort.getInput());
      if (childPipeline != null) {
        return LogicalPipelineOutputScan.create(
            sort.getCluster(), childPipeline, sort.getRowType());
      }
    }
    // Recurse into inputs
    final List<RelNode> oldInputs = node.getInputs();
    final List<RelNode> newInputs = new ArrayList<>(oldInputs.size());
    boolean changed = false;
    for (RelNode input : oldInputs) {
      final RelNode newInput = replaceChildBoundaries(input, childLogicalRoots);
      newInputs.add(newInput);
      if (newInput != input) {
        changed = true;
      }
    }
    return changed ? node.copy(node.getTraitSet(), newInputs) : node;
  }
}
