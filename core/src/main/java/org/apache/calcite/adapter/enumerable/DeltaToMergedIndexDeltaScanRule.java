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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.stream.LogicalDelta;

import org.immutables.value.Value;

/**
 * Planner rule that converts a {@link LogicalDelta} whose input is an
 * {@link EnumerableMergedIndexScan} into an
 * {@link EnumerableMergedIndexDeltaScan}.
 *
 * <p>This rule fires during incremental maintenance plan resolution. When the
 * IVM planner sees {@code LogicalDelta(EnumerableMergedIndexScan)}, it replaces
 * the pair with a single physical delta-scan operator that streams newly
 * inserted records from the merged index.
 *
 * <p>Not included in {@link EnumerableRules#ENUMERABLE_RULES}; callers opt in
 * explicitly (e.g., maintenance plan tests).
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
public class DeltaToMergedIndexDeltaScanRule
    extends RelRule<DeltaToMergedIndexDeltaScanRule.Config>
    implements TransformationRule {

  /** Creates a DeltaToMergedIndexDeltaScanRule. */
  protected DeltaToMergedIndexDeltaScanRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalDelta delta = call.rel(0);
    final EnumerableMergedIndexScan scan = call.rel(1);
    call.transformTo(
        EnumerableMergedIndexDeltaScan.create(
            delta.getCluster(), scan.mergedIndex, scan.getRowType()));
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableDeltaToMergedIndexDeltaScanRuleConfig.builder()
        .operandSupplier(b0 -> b0.operand(LogicalDelta.class).oneInput(
            b1 -> b1.operand(EnumerableMergedIndexScan.class).noInputs()))
        .build();

    @Override default DeltaToMergedIndexDeltaScanRule toRule() {
      return new DeltaToMergedIndexDeltaScanRule(this);
    }
  }
}
