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
import org.apache.calcite.materialize.Pipeline;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalPipelineOutputScan;
import org.apache.calcite.rel.rules.TransformationRule;

import org.immutables.value.Value;

import java.util.List;

/**
 * Rule that converts a {@link LogicalPipelineOutputScan} to an
 * {@link EnumerableMergedIndexScan}.
 *
 * <p>A {@code LogicalPipelineOutputScan} is a placeholder that appears in a
 * parent pipeline's maintenance plan where the child pipeline's output is
 * consumed. At physical conversion time this rule resolves the child pipeline
 * to the source position it occupies in the parent merged index, and creates
 * a per-source {@link EnumerableMergedIndexScan} for that position.
 *
 * <p>Matching uses identity comparison ({@code ==}) on {@link Pipeline}
 * objects: the child pipeline stored in the logical node must be the same
 * instance as one of the parent merged index's sources. This works because
 * pipelines are unique objects created once during plan analysis and
 * referenced by identity throughout.
 *
 * <p>Not included in {@link EnumerableRules#ENUMERABLE_RULES}; callers opt in
 * explicitly.
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
@Value.Enclosing
public class PipelineOutputScanRule
    extends RelRule<PipelineOutputScanRule.Config>
    implements TransformationRule {

  /** Creates a PipelineOutputScanRule. */
  protected PipelineOutputScanRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalPipelineOutputScan scan = call.rel(0);
    final Pipeline childPipeline = scan.childPipeline;

    // Find which registered MI contains this child pipeline as a source.
    for (MergedIndex mi : MergedIndexRegistry.allIndexes()) {
      List<Pipeline> sources = mi.getSources();
      for (int i = 0; i < sources.size(); i++) {
        if (sources.get(i) == childPipeline) {
          // Reuse an existing scan group if one was already created for a
          // sibling source of this MI; otherwise create a new one.
          MergedIndexScanGroup scanGroup =
              findExistingScanGroup(call, mi);
          if (scanGroup == null) {
            scanGroup = new MergedIndexScanGroup(mi);
          }
          call.transformTo(
              EnumerableMergedIndexScan.create(
                  scan.getCluster(), mi, i, scanGroup));
          return;
        }
      }
    }
  }

  /**
   * Searches the current plan tree for an existing
   * {@link EnumerableMergedIndexScan} referencing {@code mi} so that sibling
   * scans can share the same {@link MergedIndexScanGroup}.
   */
  private static MergedIndexScanGroup findExistingScanGroup(
      RelOptRuleCall call, MergedIndex mi) {
    return searchForScanGroup(call.getPlanner().getRoot(), mi);
  }

  private static MergedIndexScanGroup searchForScanGroup(
      RelNode node, MergedIndex mi) {
    if (node instanceof EnumerableMergedIndexScan) {
      EnumerableMergedIndexScan existing = (EnumerableMergedIndexScan) node;
      if (existing.mergedIndex == mi) {
        return existing.scanGroup;
      }
    }
    for (RelNode input : node.getInputs()) {
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
        ImmutablePipelineOutputScanRule.Config.of()
            .withOperandSupplier(b ->
                b.operand(LogicalPipelineOutputScan.class).noInputs());

    @Override default PipelineOutputScanRule toRule() {
      return new PipelineOutputScanRule(this);
    }
  }
}
