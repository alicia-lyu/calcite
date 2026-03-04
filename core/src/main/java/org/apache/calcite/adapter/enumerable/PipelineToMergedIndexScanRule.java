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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.TransformationRule;

import org.immutables.value.Value;

import java.util.List;
import java.util.Optional;

/**
 * Planner rule that replaces an order-based pipeline of:
 *
 * <pre>
 *   EnumerableSort(A) ──→ EnumerableMergeJoin
 *   EnumerableSort(B) ──→/
 * </pre>
 *
 * with a single {@link EnumerableMergedIndexScan} when a matching
 * {@link MergedIndex} is registered in {@link MergedIndexRegistry}.
 *
 * <p>The merged index physically pre-sorts all participating table records
 * by the shared key, so no run-time sorting, merging, or aggregation step is
 * needed. The {@link EnumerableMergedIndexScan} performs join assembly in one
 * sequential pass over the index.
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

  @Override public void onMatch(RelOptRuleCall call) {
    // Pattern (depth-first, left-to-right):
    //   rel(0) = EnumerableMergeJoin
    //   rel(1) = left  EnumerableSort
    //   rel(2) = left  EnumerableTableScan  (table A)
    //   rel(3) = right EnumerableSort
    //   rel(4) = right EnumerableTableScan  (table B)
    final EnumerableMergeJoin join = call.rel(0);
    final EnumerableSort leftSort = call.rel(1);
    final TableScan leftScan = call.rel(2);
    final TableScan rightScan = call.rel(4);

    // The shared collation is the left sort's collation —
    // both inputs must be sorted on the join keys.
    final RelCollation collation = leftSort.getCollation();

    final List<RelOptTable> tables =
        List.of(leftScan.getTable(), rightScan.getTable());

    final Optional<MergedIndex> index =
        MergedIndexRegistry.findFor(tables, collation);

    if (!index.isPresent()) {
      return;
    }

    // The row type of the scan matches the merge-join output
    // (all fields from both tables concatenated).
    final EnumerableMergedIndexScan scan =
        EnumerableMergedIndexScan.create(
            join.getCluster(),
            index.get(),
            join.getRowType());

    call.transformTo(scan);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        ImmutablePipelineToMergedIndexScanRule.Config.of()
            .withOperandSupplier(b0 ->
                b0.operand(EnumerableMergeJoin.class)
                    .inputs(
                        b1 -> b1.operand(EnumerableSort.class)
                            .oneInput(b2 ->
                                b2.operand(EnumerableTableScan.class)
                                    .noInputs()),
                        b1 -> b1.operand(EnumerableSort.class)
                            .oneInput(b2 ->
                                b2.operand(EnumerableTableScan.class)
                                    .noInputs())));

    @Override default PipelineToMergedIndexScanRule toRule() {
      return new PipelineToMergedIndexScanRule(this);
    }
  }
}
