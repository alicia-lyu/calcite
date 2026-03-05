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
package org.apache.calcite.adapter.tpch;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableMergedIndexScan;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.materialize.MergedIndex;
import org.apache.calcite.materialize.MergedIndexRegistry;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSets;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * TPC-H plan observation test for {@link EnumerableMergedIndexScan}.
 *
 * <p>Demonstrates BEFORE/AFTER query plans for a simplified TPC-H Q3 query
 * (ORDERS ⋈ LINEITEM on orderkey) to show how the merged index substitution
 * collapses an order-based pipeline into a single scan.
 *
 * <p>This is a plan-only test — {@code EnumerableMergedIndexScan.implement()}
 * returns an empty enumerable stub; no actual TPC-H data is read.
 *
 * <h3>Paste DOT output for visualization</h3>
 * <pre>
 *   echo "&lt;dot&gt;" | dot -Tsvg -o plan.svg
 *   or: https://dreampuf.github.io/GraphvizOnline/
 * </pre>
 */
class MergedIndexTpchPlanTest {

  @AfterEach
  void clearRegistry() {
    MergedIndexRegistry.clear();
  }

  /**
   * Observes the BEFORE (order-based pipeline) and AFTER (merged index) plans
   * for a simplified TPC-H Q3 query joining ORDERS and LINEITEM on orderkey.
   *
   * <p>Expected BEFORE structure:
   * <pre>
   *   EnumerableSort / EnumerableLimit
   *     EnumerableAggregate / EnumerableSortedAggregate
   *       EnumerableMergeJoin(condition=[=($0, $9)], joinType=[inner])
   *         EnumerableSort(sort0=[$0], dir0=[ASC])
   *           EnumerableTableScan(table=[[tpch, orders]])
   *         EnumerableSort(sort0=[$0], dir0=[ASC])
   *           EnumerableTableScan(table=[[tpch, lineitem]])
   * </pre>
   *
   * <p>Expected AFTER structure:
   * <pre>
   *   EnumerableSort / EnumerableLimit
   *     EnumerableAggregate / EnumerableSortedAggregate
   *       EnumerableMergedIndexScan(tables=[[tpch,orders],[tpch,lineitem]], ...)
   * </pre>
   */
  @Test void tpchOrdersLineitemMergedIndex() throws Exception {
    // Simplified TPC-H Q3: orders x lineitem join on orderkey with aggregation.
    // o_orderkey is column 0 of orders; l_orderkey is column 0 of lineitem.
    final String sql = "SELECT o.o_orderkey, o.o_orderdate,"
        + " SUM(l.l_extendedprice) AS revenue"
        + " FROM tpch.orders o"
        + " JOIN tpch.lineitem l ON o.o_orderkey = l.l_orderkey"
        + " GROUP BY o.o_orderkey, o.o_orderdate"
        + " ORDER BY o.o_orderkey"
        + " LIMIT 10";

    // Scale 0.01 keeps the schema lightweight (plan-only; no data is scanned).
    // Register as "TPCH" (uppercase) because the default SqlParser folds
    // unquoted identifiers to uppercase, so "tpch.orders" resolves to "TPCH.ORDERS".
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("TPCH", new TpchSchema(0.01, 0, 1, false));

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(rootSchema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(
            // Phase 1: convert logical plan to the order-based enumerable pipeline.
            Programs.of(RuleSets.ofList(
                EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_RULE,
                EnumerableRules.ENUMERABLE_SORT_RULE,
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE,
                EnumerableRules.ENUMERABLE_LIMIT_RULE,
                EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE)))
        .build();

    final Planner planner = Frameworks.getPlanner(config);
    final SqlNode parsed = planner.parse(sql);
    final SqlNode validated = planner.validate(parsed);
    final RelRoot root = planner.rel(validated);

    // Inject LogicalSort(orderkey ASC) at each join input so that
    // ENUMERABLE_SORT_RULE can convert them to EnumerableSort nodes in phase 1.
    // o_orderkey = column 0 of orders; l_orderkey = column 0 of lineitem.
    final RelCollation joinKeyCollation = RelCollations.of(0);
    final RelNode logicalWithSorts =
        injectSortsBeforeJoin(root.rel, joinKeyCollation);

    final RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        planner.transform(0, desiredTraits, logicalWithSorts);

    System.out.println("=== BEFORE (order-based pipeline) ===");
    System.out.println(dumpText(phase1Plan));
    System.out.println("=== DOT (paste into https://dreampuf.github.io/GraphvizOnline/) ===");
    System.out.println(dumpDot(phase1Plan));

    // Navigate through any wrapping nodes (Sort, Limit, Aggregate, Project)
    // to find the EnumerableMergeJoin.
    final EnumerableMergeJoin join = Objects.requireNonNull(
        findMergeJoin(phase1Plan),
        "Phase 1 plan does not contain an EnumerableMergeJoin:\n"
            + dumpText(phase1Plan));

    // ── Register merged index ──────────────────────────────────────────────
    // Extract the two table references from the sort → scan leaves.
    final EnumerableSort leftSort = (EnumerableSort) join.getLeft();
    final EnumerableSort rightSort = (EnumerableSort) join.getRight();
    final RelOptTable tableOrders =
        ((EnumerableTableScan) leftSort.getInput()).getTable();
    final RelOptTable tableLineitem =
        ((EnumerableTableScan) rightSort.getInput()).getTable();
    final RelCollation collation = leftSort.getCollation();
    final double rowCount =
        join.estimateRowCount(join.getCluster().getMetadataQuery());

    MergedIndexRegistry.register(
        new MergedIndex(List.of(tableOrders, tableLineitem), collation, rowCount));

    // ── Phase 2: HEP planner fires PipelineToMergedIndexScanRule ──────────
    final HepProgram hepProgram = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
        .build();
    final HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(phase1Plan);
    final RelNode phase2Plan = hepPlanner.findBestExp();

    System.out.println("=== AFTER (merged index plan) ===");
    System.out.println(dumpText(phase2Plan));

    // ── Assert ────────────────────────────────────────────────────────────
    final String planStr = dumpText(phase2Plan);
    // The MergeJoin and its two Sort children must be gone.
    assertThat(planStr, containsString("EnumerableMergedIndexScan"));
    assertThat(planStr, not(containsString("EnumerableMergeJoin")));
    // NOTE: do NOT assert not(containsString("EnumerableSort")) here —
    // the outer EnumerableSort for ORDER BY above the aggregate legitimately
    // remains in the plan after the rule fires.
  }

  // ── Helpers (same logic as PipelineToMergedIndexScanRuleTest) ────────────

  /**
   * Recursively walks the plan tree and wraps each direct input of every
   * {@link Join} in a {@link LogicalSort} with the given collation.
   *
   * <p>This ensures {@code ENUMERABLE_SORT_RULE} has concrete sort nodes to
   * convert, giving the Volcano planner the physical {@link EnumerableSort}
   * operators that {@link EnumerableMergeJoin} requires from its inputs.
   */
  private static RelNode injectSortsBeforeJoin(RelNode node,
      RelCollation collation) {
    if (node instanceof Join) {
      final Join join = (Join) node;
      final RelNode sortedLeft =
          LogicalSort.create(join.getLeft(), collation, null, null);
      final RelNode sortedRight =
          LogicalSort.create(join.getRight(), collation, null, null);
      return join.copy(join.getTraitSet(), List.of(sortedLeft, sortedRight));
    }
    final List<RelNode> newInputs = node.getInputs().stream()
        .map(input -> injectSortsBeforeJoin(input, collation))
        .collect(Collectors.toList());
    return node.copy(node.getTraitSet(), newInputs);
  }

  /** Recursively finds the first {@link EnumerableMergeJoin} in the plan tree. */
  private static @Nullable EnumerableMergeJoin findMergeJoin(RelNode node) {
    if (node instanceof EnumerableMergeJoin) {
      return (EnumerableMergeJoin) node;
    }
    for (RelNode input : node.getInputs()) {
      final EnumerableMergeJoin found = findMergeJoin(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private static String dumpText(RelNode rel) {
    return RelOptUtil.dumpPlan("", rel, SqlExplainFormat.TEXT,
        SqlExplainLevel.EXPPLAN_ATTRIBUTES);
  }

  private static String dumpDot(RelNode rel) {
    return RelOptUtil.dumpPlan("", rel, SqlExplainFormat.DOT,
        SqlExplainLevel.EXPPLAN_ATTRIBUTES);
  }
}
