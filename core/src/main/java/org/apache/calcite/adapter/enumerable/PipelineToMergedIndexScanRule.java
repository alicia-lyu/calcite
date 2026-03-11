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
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.TransformationRule;

import org.checkerframework.checker.nullness.qual.Nullable;
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
 * with a single {@link EnumerableMergedIndexScan} (inner pipeline) or
 * {@link EnumerableMergedIndexJoin} wrapping a scan (outer pipeline),
 * when a matching {@link MergedIndex} is registered in
 * {@link MergedIndexRegistry}.
 *
 * <h3>Two cases</h3>
 * <ul>
 *   <li><b>Inner pipeline</b> (both sources are base tables): rule produces a
 *       leaf {@link EnumerableMergedIndexScan}. Join assembly and aggregation
 *       happen at maintenance time (pre-stored in the outer merged index).
 *   <li><b>Outer pipeline</b> (left source is a {@link MergedIndex} view):
 *       rule produces
 *       {@code EnumerableMergedIndexJoin → EnumerableMergedIndexScan(leaf)}.
 *       The outer scan reads co-located (inner-join-result + right-table)
 *       records by the shared key; the join assembles the Cartesian product.
 * </ul>
 *
 * <h3>Accepted patterns for one join side</h3>
 * <ul>
 *   <li>{@code Sort → TableScan} — base table source
 *   <li>{@code Sort → SortedAggregate → Sort → TableScan} — aggregate is
 *       maintenance-time; treated as base table source
 *   <li>{@code Sort → EnumerableMergedIndexScan} — inner pipeline view source
 * </ul>
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

  @Override public void onMatch(RelOptRuleCall call) {
    final EnumerableMergeJoin join = call.rel(0);
    final RelNode leftNode = unwrap(join.getLeft());
    final RelNode rightNode = unwrap(join.getRight());
    final Object leftSource = extractSource(leftNode);
    final Object rightSource = extractSource(rightNode);
    if (leftSource == null || rightSource == null) {
      return;
    }

    // Shared collation: prefer an explicit EnumerableSort; fall back to
    // the node's trait-set collation (e.g. SortedAggregate output).
    final RelCollation collation = extractCollation(leftNode, rightNode);
    if (collation == null || collation.getFieldCollations().isEmpty()) {
      return;
    }

    final Optional<MergedIndex> idxOpt =
        MergedIndexRegistry.findFor(List.of(leftSource, rightSource), collation);
    if (!idxOpt.isPresent()) {
      return;
    }
    final MergedIndex idx = idxOpt.get();

    final EnumerableMergedIndexScan scan =
        EnumerableMergedIndexScan.create(join.getCluster(), idx, join.getRowType());

    if (leftSource instanceof MergedIndex) {
      // Outer pipeline: outer scan + join assembly at query time.
      call.transformTo(EnumerableMergedIndexJoin.create(
          join.getCluster(), idx, JoinRelType.INNER, scan));
    } else {
      // Inner pipeline: leaf scan only; join assembly + agg are maintenance-time.
      call.transformTo(scan);
    }
  }

  /**
   * Extracts the shared collation from the merge-join inputs.
   *
   * <p>Prefers an explicit {@link EnumerableSort}'s collation; falls back to
   * the trait-set collation of the first input (e.g. when a
   * {@link EnumerableSortedAggregate} directly feeds the join).
   */
  private static @Nullable RelCollation extractCollation(RelNode left, RelNode right) { // lwh future work: choose the most specific collation if multiple are present (both sides should be compatible but not necessarily identical)
    if (left instanceof EnumerableSort) {
      return ((EnumerableSort) left).getCollation();
    }
    if (right instanceof EnumerableSort) {
      return ((EnumerableSort) right).getCollation();
    }
    // Neither input is an explicit Sort — try the trait set.
    final List<RelCollation> collations =
        left.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
    if (collations != null && !collations.isEmpty()) {
      return collations.get(0);
    }
    return null;
  }

  /**
   * Extracts the source identity for one side of a {@link EnumerableMergeJoin}.
   *
   * <p>Accepted patterns:
   * <ul>
   *   <li>{@code Sort → TableScan} → returns {@link org.apache.calcite.plan.RelOptTable}
   *   <li>{@code Sort → (Aggregate | Project | ...) → ... → TableScan} — drills through
   *       any chain of single-input operators to the base table (operators such as aggregates
   *       and projects are maintenance-time and do not affect source identity)
   *   <li>{@code Sort → EnumerableMergedIndexScan} → returns
   *       {@link MergedIndex} (inner pipeline view)
   *   <li>{@code Sort → EnumerableMergedIndexJoin → EnumerableMergedIndexScan} → returns
   *       {@link MergedIndex} from the underlying scan (outer pipeline view, produced after
   *       a prior HEP pass replaced an outer pipeline)
   *   <li>{@code SortedAggregate → ... → TableScan} — a sorted operator that is not
   *       an explicit Sort; drills through to the base table
   *   <li>{@code EnumerableMergedIndexScan} — direct scan (no Sort wrapper)
   * </ul>
   *
   * @param node the direct input to the merge join (may be Sort, SortedAggregate, etc.)
   * @return the source object, or {@code null} if the pattern is unrecognized
   */
  private static @Nullable Object extractSource(RelNode node) {
    if (node instanceof EnumerableSort) {
      final RelNode below = unwrap(((EnumerableSort) node).getInput());
      return extractSourceBelow(below);
    }
    // Direct MergedIndexScan (no Sort wrapper).
    if (node instanceof EnumerableMergedIndexScan) {
      return ((EnumerableMergedIndexScan) node).mergedIndex;
    }
    // Single-input sorted operator (e.g. SortedAggregate): drill through.
    if (node.getInputs().size() == 1) {
      return extractSourceBelow(node);
    }
    return null;
  }

  /**
   * Given a node below a Sort (or a single-input sorted operator), identifies
   * the source as a {@link MergedIndex} or {@link RelOptTable}.
   */
  private static @Nullable Object extractSourceBelow(RelNode below) {
    if (below instanceof EnumerableMergedIndexScan) {
      return ((EnumerableMergedIndexScan) below).mergedIndex;
    }
    // EnumerableMergedIndexJoin → EnumerableMergedIndexScan:
    // after an outer pipeline is replaced, its result appears as this pattern.
    if (below instanceof EnumerableMergedIndexJoin) {
      final RelNode scanNode = unwrap(((EnumerableMergedIndexJoin) below).getInput(0));
      if (scanNode instanceof EnumerableMergedIndexScan) {
        return ((EnumerableMergedIndexScan) scanNode).mergedIndex;
      }
    }
    // Drill through any chain of single-input operators (aggregate, project, filter, etc.)
    // to reach a base-table scan. These intermediate operators are maintenance-time only.
    return findLeafTableScan(below);
  }

  /**
   * Drills through a chain of single-input operators (unwrapping any
   * {@link HepRelVertex}) to find the leaf {@link EnumerableTableScan}.
   * Returns {@code null} if the chain splits (multi-input) or has no scan.
   */
  private static @Nullable RelOptTable findLeafTableScan(RelNode node) {
    final RelNode n = unwrap(node);
    if (n instanceof EnumerableTableScan) {
      return ((EnumerableTableScan) n).getTable();
    }
    if (n.getInputs().size() == 1) {
      return findLeafTableScan(n.getInputs().get(0)); // unwrapped at next call's entry
    }
    return null;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        ImmutablePipelineToMergedIndexScanRule.Config.of()
            .withOperandSupplier(b0 ->
                b0.operand(EnumerableMergeJoin.class).anyInputs());

    @Override default PipelineToMergedIndexScanRule toRule() {
      return new PipelineToMergedIndexScanRule(this);
    }
  }
}
