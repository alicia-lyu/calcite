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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.TransformationRule;

import org.immutables.value.Value;

import java.util.List;

/**
 * Rule that converts a {@link LogicalTableScan} to an
 * {@link EnumerableMergedIndexScan} when the table is a source in a
 * registered {@link MergedIndex}.
 *
 * <p>Used during physical conversion of maintenance plans. Each logical table
 * scan in a maintenance plan represents a full scan of that table's rows as
 * they are stored in the merged index. This rule substitutes the logical node
 * with the appropriate physical per-source scan.
 *
 * <p>Matching is by qualified name: the scan's table is compared against the
 * leaf {@link RelOptTable} of each pipeline source in every registered
 * merged index.
 *
 * <p>Not included in {@link EnumerableRules#ENUMERABLE_RULES}; callers opt in
 * explicitly.
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
@Value.Enclosing
public class LogicalTableScanToMergedIndexRule
    extends RelRule<LogicalTableScanToMergedIndexRule.Config>
    implements TransformationRule {

  /** Creates a LogicalTableScanToMergedIndexRule. */
  protected LogicalTableScanToMergedIndexRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalTableScan scan = call.rel(0);
    final List<String> qualifiedName = scan.getTable().getQualifiedName();

    for (MergedIndex mi : MergedIndexRegistry.allIndexes()) {
      List<Pipeline> sources = mi.getSources();
      for (int i = 0; i < sources.size(); i++) {
        Pipeline source = sources.get(i);
        // Only match base-table sources (no inner merged index).
        if (source.mergedIndex != null) {
          continue;
        }
        final RelOptTable leafTable = MergedIndex.findLeafScan(source.root);
        if (leafTable != null
            && leafTable.getQualifiedName().equals(qualifiedName)) {
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
      org.apache.calcite.rel.RelNode node, MergedIndex mi) {
    if (node instanceof EnumerableMergedIndexScan) {
      EnumerableMergedIndexScan existing = (EnumerableMergedIndexScan) node;
      if (existing.mergedIndex == mi) {
        return existing.scanGroup;
      }
    }
    for (org.apache.calcite.rel.RelNode input : node.getInputs()) {
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
        ImmutableLogicalTableScanToMergedIndexRule.Config.of()
            .withOperandSupplier(b ->
                b.operand(LogicalTableScan.class).noInputs());

    @Override default LogicalTableScanToMergedIndexRule toRule() {
      return new LogicalTableScanToMergedIndexRule(this);
    }
  }
}
