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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.materialize.MergedIndex;
import org.apache.calcite.materialize.MergedIndexRegistry;
import org.apache.calcite.materialize.MergedIndexScanGroup;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.TransformationRule;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.Optional;

/**
 * Planner rule that replaces an {@link EnumerableSort} boundary node with a
 * per-source {@link EnumerableMergedIndexScan} when a matching
 * {@link MergedIndex} is registered in {@link MergedIndexRegistry}.
 *
 * <p>Under the "Transparent Per-Source MI Scans" architecture, each boundary
 * Sort in a pipeline is replaced <b>independently</b> by a per-source MI scan
 * returning source-native rows. Parent operators (MergeJoin, SortedAggregate)
 * stay in the plan unchanged.
 *
 * <p>The rule matches by <b>identity</b>: the Sort's input ({@code sortInput})
 * is literally the same {@code RelNode} object stored in
 * {@code pipeline.sources[i].root}. This works because HEP updates
 * {@link HepRelVertex} pointers, not the RelNode objects themselves.
 *
 * <p>All per-source scans within the same pipeline share a
 * {@link MergedIndexScanGroup} instance, discovered by searching the current
 * plan tree for an existing sibling scan referencing the same MI.
 *
 * <p>Register (but do NOT add to {@code EnumerableRules.ENUMERABLE_RULES}):
 * <pre>{@code
 * planner.addRule(EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE);
 * }</pre>
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
@Value.Enclosing
public class PipelineToMergedIndexScanRule
    extends RelRule<PipelineToMergedIndexScanRule.Config>
    implements TransformationRule {

  /** Creates a PipelineToMergedIndexScanRule. */
  protected PipelineToMergedIndexScanRule(Config config) {
    super(config);
  }

  /** Unwraps a {@link HepRelVertex} to its current rel, or returns the node as-is. */
  private static RelNode unwrap(RelNode node) {
    return node instanceof HepRelVertex
        ? ((HepRelVertex) node).getCurrentRel()
        : node;
  }

  /**
   * Returns {@code true} if the Sort is a pipeline boundary: non-empty
   * collation and no FETCH/OFFSET (LimitSort is not a boundary).
   */
  private static boolean isBoundarySort(EnumerableSort sort) {
    return sort.fetch == null && sort.offset == null
        && !sort.getCollation().getFieldCollations().isEmpty();
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final EnumerableSort sort = call.rel(0);
    final RelCollation collation = sort.getCollation();
    final RelNode sortInput = unwrap(sort.getInput());

    // Identity match: sortInput == some pipeline source's root node.
    final Optional<MergedIndexRegistry.SourceMatch> matchOpt =
        MergedIndexRegistry.findForSource(sortInput, collation);
    if (!matchOpt.isPresent()) {
      return;
    }

    final MergedIndexRegistry.SourceMatch match = matchOpt.get();
    final MergedIndex mi = match.mergedIndex;

    // Find or create scan group: search the current plan for an existing
    // MIScan sibling referencing the same MI. If found, reuse its group.
    // Otherwise create a new one (this is the first Sort for this MI).
    MergedIndexScanGroup scanGroup = findExistingScanGroup(call, mi);
    if (scanGroup == null) {
      scanGroup = new MergedIndexScanGroup(mi);
    }

    call.transformTo(EnumerableMergedIndexScan.create(
        sort.getCluster(), mi, match.sourceIndex, scanGroup));
  }

  /**
   * Searches the current plan tree (via the HEP planner's root) for an
   * existing {@link EnumerableMergedIndexScan} referencing {@code mi}.
   * Returns its scanGroup if found, null otherwise.
   */
  private static @Nullable MergedIndexScanGroup findExistingScanGroup(
      RelOptRuleCall call, MergedIndex mi) {
    RelNode root = ((HepPlanner) call.getPlanner()).getRoot();
    return searchForScanGroup(root, mi);
  }

  /**
   * Recursively searches the plan tree for an {@link EnumerableMergedIndexScan}
   * referencing {@code mi} and returns its scan group.
   */
  private static @Nullable MergedIndexScanGroup searchForScanGroup(
      RelNode node, MergedIndex mi) {
    final RelNode n = unwrap(node);
    if (n instanceof EnumerableMergedIndexScan) {
      EnumerableMergedIndexScan scan = (EnumerableMergedIndexScan) n;
      if (scan.mergedIndex == mi) {
        return scan.scanGroup;
      }
    }
    for (RelNode input : n.getInputs()) {
      MergedIndexScanGroup found = searchForScanGroup(input, mi);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        ImmutablePipelineToMergedIndexScanRule.Config.of()
            .withOperandSupplier(b0 ->
                b0.operand(EnumerableSort.class)
                    .predicate(PipelineToMergedIndexScanRule::isBoundarySort)
                    .anyInputs());

    @Override default PipelineToMergedIndexScanRule toRule() {
      return new PipelineToMergedIndexScanRule(this);
    }
  }
}
