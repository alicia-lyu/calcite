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
import org.apache.calcite.rel.RelFieldCollation;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

/**
 * TPC-H plan observation test for {@link EnumerableMergedIndexScan}.
 *
 * <p>Demonstrates BEFORE/AFTER query plans for TPC-H Q3 (3-table: CUSTOMER,
 * ORDERS, LINEITEM — partial substitution) and Q12 (2-table: ORDERS, LINEITEM
 * — full substitution) to show merged index plan transformation.
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
   * TPC-H Q3 (no date filter): CUSTOMER ⋈ ORDERS ⋈ LINEITEM.
   *
   * <p>Demonstrates <em>partial</em> pipeline substitution: the inner pipeline
   * (CUSTOMER ⋈ ORDERS on custkey) is replaced by a merged index scan, while
   * the outer pipeline (result ⋈ LINEITEM on orderkey) remains unchanged.
   *
   * <p>Expected AFTER structure:
   * <pre>
   *   (Sort / Limit / Aggregate ...)
   *     EnumerableMergeJoin(o_orderkey = l_orderkey)  ← outer join remains
   *       EnumerableSort(o_orderkey)
   *         EnumerableMergedIndexScan([TPCH, CUSTOMER]:C_CUSTKEY,
   *                                   [TPCH, ORDERS]:O_CUSTKEY)
   *       EnumerableSort(l_orderkey)
   *         EnumerableTableScan(LINEITEM)
   * </pre>
   */
  @Test void tpchQ3() throws Exception {
    // TPC-H Q3 (no date filter): CUSTOMER ⋈ ORDERS on custkey, then ⋈ LINEITEM
    // on orderkey. The merged index covers the inner pipeline only.
    final String sql = "SELECT l.l_orderkey,"
        + " SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,"
        + " o.o_orderdate, o.o_shippriority"
        + " FROM tpch.customer c"
        + " JOIN tpch.orders o ON c.c_custkey = o.o_custkey"
        + " JOIN tpch.lineitem l ON l.l_orderkey = o.o_orderkey"
        + " GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority"
        + " ORDER BY revenue DESC, o.o_orderdate"
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

    // Inject LogicalSort at each join input using the actual join-condition keys.
    // injectSortsBeforeJoin recurses and handles each nested join independently.
    final RelNode logicalWithSorts = injectSortsBeforeJoin(root.rel);

    final RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        planner.transform(0, desiredTraits, logicalWithSorts);

    System.out.println("=== Q3 BEFORE (order-based pipeline) ===");
    System.out.println(dumpText(phase1Plan));
    System.out.println("=== Q3 DOT (paste into https://dreampuf.github.io/GraphvizOnline/) ===");
    System.out.println(dumpDot(phase1Plan));

    // Find the leaf merge join (CUSTOMER ⋈ ORDERS): its two inputs are
    // EnumerableSort → EnumerableTableScan (no nested join).
    final EnumerableMergeJoin innerJoin = Objects.requireNonNull(
        findLeafMergeJoin(phase1Plan),
        "Phase 1 plan has no leaf EnumerableMergeJoin:\n" + dumpText(phase1Plan));

    // ── Register merged index for (CUSTOMER, ORDERS) ──────────────────────
    final EnumerableSort leftSort = (EnumerableSort) innerJoin.getLeft();
    final EnumerableSort rightSort = (EnumerableSort) innerJoin.getRight();
    final RelOptTable tableLeft =
        ((EnumerableTableScan) leftSort.getInput()).getTable();
    final RelOptTable tableRight =
        ((EnumerableTableScan) rightSort.getInput()).getTable();
    final RelCollation collationLeft = leftSort.getCollation();
    final RelCollation collationRight = rightSort.getCollation();
    final double rowCount =
        innerJoin.estimateRowCount(innerJoin.getCluster().getMetadataQuery());

    MergedIndexRegistry.register(new MergedIndex(
        List.of(tableLeft, tableRight),
        List.of(collationLeft, collationRight),
        collationLeft,
        rowCount));

    // ── Phase 2: HEP planner fires PipelineToMergedIndexScanRule ──────────
    final HepProgram hepProgram = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
        .build();
    final HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(phase1Plan);
    final RelNode phase2Plan = hepPlanner.findBestExp();

    System.out.println("=== Q3 AFTER (merged index plan) ===");
    System.out.println(dumpText(phase2Plan));
    System.out.println("=== Q3 AFTER DOT (paste into https://dreampuf.github.io/GraphvizOnline/) ===");
    System.out.println(dumpDot(phase2Plan));

    // ── Assert ────────────────────────────────────────────────────────────
    final String planStr = dumpText(phase2Plan);
    // The inner MergeJoin (CUSTOMER ⋈ ORDERS) is replaced by a merged index scan.
    assertThat(planStr, containsString("EnumerableMergedIndexScan"));
    // The outer MergeJoin (orderkey join) is NOT replaced — partial substitution.
    assertThat(planStr, containsString("EnumerableMergeJoin"));
  }

  /**
   * TPC-H Q12 (no date filter): ORDERS ⋈ LINEITEM on orderkey.
   *
   * <p>Demonstrates <em>full</em> 2-table pipeline substitution: the entire
   * Sort(ORDERS) ⋈ Sort(LINEITEM) pipeline collapses into one scan.
   *
   * <p>Expected AFTER structure:
   * <pre>
   *   (Sort / Aggregate ...)
   *     EnumerableMergedIndexScan([TPCH, ORDERS]:O_ORDERKEY,
   *                               [TPCH, LINEITEM]:L_ORDERKEY)
   * </pre>
   */
  @Test void tpchQ12() throws Exception {
    // TPC-H Q12 (no date filter): count high/low priority lines per ship mode.
    final String sql = "SELECT l.l_shipmode, o.o_orderpriority,"
        + " SUM(CASE WHEN o.o_orderpriority = '1-URGENT'"
        + "     THEN 1 ELSE 0 END) AS high_line_count,"
        + " SUM(CASE WHEN o.o_orderpriority <> '1-URGENT'"
        + "     THEN 1 ELSE 0 END) AS low_line_count"
        + " FROM tpch.orders o"
        + " JOIN tpch.lineitem l ON o.o_orderkey = l.l_orderkey"
        + " GROUP BY l.l_shipmode, o.o_orderpriority"
        + " ORDER BY l.l_shipmode, o.o_orderpriority";

    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("TPCH", new TpchSchema(0.01, 0, 1, false));

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(rootSchema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(
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

    final RelNode logicalWithSorts = injectSortsBeforeJoin(root.rel);

    final RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        planner.transform(0, desiredTraits, logicalWithSorts);

    System.out.println("=== Q12 BEFORE (order-based pipeline) ===");
    System.out.println(dumpText(phase1Plan));
    System.out.println("=== Q12 DOT (paste into https://dreampuf.github.io/GraphvizOnline/) ===");
    System.out.println(dumpDot(phase1Plan));

    final EnumerableMergeJoin join = Objects.requireNonNull(
        findMergeJoin(phase1Plan),
        "Phase 1 plan does not contain an EnumerableMergeJoin:\n"
            + dumpText(phase1Plan));

    // ── Register merged index for (ORDERS, LINEITEM) ──────────────────────
    final EnumerableSort leftSort = (EnumerableSort) join.getLeft();
    final EnumerableSort rightSort = (EnumerableSort) join.getRight();
    final RelOptTable tableOrders =
        ((EnumerableTableScan) leftSort.getInput()).getTable();
    final RelOptTable tableLineitem =
        ((EnumerableTableScan) rightSort.getInput()).getTable();
    final RelCollation collationOrders = leftSort.getCollation();
    final RelCollation collationLineitem = rightSort.getCollation();
    final double rowCount =
        join.estimateRowCount(join.getCluster().getMetadataQuery());

    MergedIndexRegistry.register(new MergedIndex(
        List.of(tableOrders, tableLineitem),
        List.of(collationOrders, collationLineitem),
        collationOrders,
        rowCount));

    // ── Phase 2: HEP planner fires PipelineToMergedIndexScanRule ──────────
    final HepProgram hepProgram = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
        .build();
    final HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(phase1Plan);
    final RelNode phase2Plan = hepPlanner.findBestExp();

    System.out.println("=== Q12 AFTER (merged index plan) ===");
    System.out.println(dumpText(phase2Plan));
    System.out.println("=== Q12 AFTER DOT (paste into https://dreampuf.github.io/GraphvizOnline/) ===");
    System.out.println(dumpDot(phase2Plan));

    // ── Assert ────────────────────────────────────────────────────────────
    final String planStr = dumpText(phase2Plan);
    assertThat(planStr, containsString("EnumerableMergedIndexScan"));
    assertThat(planStr, not(containsString("EnumerableMergeJoin")));
  }

  /**
   * TPC-H Q3 variant: ORDERS⋈LINEITEM as the leaf join (on orderkey);
   * CUSTOMER as the outer join (on custkey). Order-based algorithms only
   * ({@code ENUMERABLE_AGGREGATE_RULE} intentionally omitted).
   *
   * <p>Demonstrates that the choice of which pipeline to substitute with a
   * merged index is determined by which join is the leaf join in the plan.
   * Here ORDERS+LINEITEM are interleaved by orderkey in the merged index, while
   * the outer join with CUSTOMER on custkey remains an explicit merge join.
   *
   * <p>Expected AFTER structure:
   * <pre>
   *   (LimitSort / Project / SortedAggregate ...)
   *     EnumerableMergeJoin(o_custkey = c_custkey)   ← outer join REMAINS
   *       EnumerableSort(custkey)
   *         EnumerableMergedIndexScan([TPCH, ORDERS]:O_ORDERKEY,
   *                                   [TPCH, LINEITEM]:L_ORDERKEY)
   *       EnumerableSort(custkey)
   *         EnumerableTableScan(CUSTOMER)
   * </pre>
   */
  @Test void tpchQ3OrdersLineitem() throws Exception {
    final String sql = "SELECT l.l_orderkey,"
        + " SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,"
        + " o.o_orderdate, o.o_shippriority"
        + " FROM tpch.orders o"
        + " JOIN tpch.lineitem l ON l.l_orderkey = o.o_orderkey"
        + " JOIN tpch.customer c ON o.o_custkey = c.c_custkey"
        + " GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority"
        + " ORDER BY revenue DESC, o.o_orderdate"
        + " LIMIT 10";

    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("TPCH", new TpchSchema(0.01, 0, 1, false));

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(rootSchema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(
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

    final RelNode logicalWithSorts = injectSortsBeforeJoin(root.rel);
    final RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        planner.transform(0, desiredTraits, logicalWithSorts);

    final String beforeStr = dumpText(phase1Plan);
    System.out.println("=== Q3 OL BEFORE (order-based pipeline) ===");
    System.out.println(beforeStr);
    System.out.println("=== Q3 OL DOT ===");
    System.out.println(dumpDot(phase1Plan));

    // Find the leaf merge join (ORDERS ⋈ LINEITEM): both inputs are
    // EnumerableSort → EnumerableTableScan.
    final EnumerableMergeJoin innerJoin = Objects.requireNonNull(
        findLeafMergeJoin(phase1Plan),
        "Phase 1 plan has no leaf EnumerableMergeJoin:\n" + beforeStr);

    // ── Register merged index for (ORDERS, LINEITEM) ───────────────────────
    final EnumerableSort leftSort = (EnumerableSort) innerJoin.getLeft();
    final EnumerableSort rightSort = (EnumerableSort) innerJoin.getRight();
    final RelOptTable tableLeft =
        ((EnumerableTableScan) leftSort.getInput()).getTable();
    final RelOptTable tableRight =
        ((EnumerableTableScan) rightSort.getInput()).getTable();
    final RelCollation collationLeft = leftSort.getCollation();
    final RelCollation collationRight = rightSort.getCollation();
    final double rowCount =
        innerJoin.estimateRowCount(innerJoin.getCluster().getMetadataQuery());

    MergedIndexRegistry.register(new MergedIndex(
        List.of(tableLeft, tableRight),
        List.of(collationLeft, collationRight),
        collationLeft,
        rowCount));

    // ── Phase 2: HEP planner fires PipelineToMergedIndexScanRule ──────────
    final HepProgram hepProgram = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
        .build();
    final HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(phase1Plan);
    final RelNode phase2Plan = hepPlanner.findBestExp();

    final String afterStr = dumpText(phase2Plan);
    System.out.println("=== Q3 OL AFTER (merged index plan) ===");
    System.out.println(afterStr);
    System.out.println("=== Q3 OL AFTER DOT ===");
    System.out.println(dumpDot(phase2Plan));

    // ── Assert ────────────────────────────────────────────────────────────
    assertThat(afterStr, containsString("EnumerableMergedIndexScan"));
    assertThat(afterStr, containsString("O_ORDERKEY"));
    assertThat(afterStr, containsString("L_ORDERKEY"));
    // Outer join (CUSTOMER on custkey) remains
    assertThat(afterStr, containsString("EnumerableMergeJoin"));
    // ORDERS + LINEITEM sorts are replaced; fewer sorts after substitution
    final int sortsBefore = countOccurrences(beforeStr, "EnumerableSort");
    final int sortsAfter  = countOccurrences(afterStr,  "EnumerableSort");
    assertThat(sortsAfter, lessThan(sortsBefore));
  }

  /**
   * TPC-H Q9 (color = 'green'): 6-table join with merged-index substitution
   * at each leaf join pair detected dynamically in the Phase 1 plan.
   *
   * <p>After Phase 2, each {@code Sort+Sort+MergeJoin} leaf triple is replaced
   * by a single {@code EnumerableMergedIndexScan}, reducing the total number of
   * {@code EnumerableSort} nodes in the plan.
   *
   * <h3>Ideal BEFORE structure (order-based pipelines)</h3>
   * <pre>
   *   Pipeline 1 — (suppkey / nationkey):
   *     MergeJoin(nationkey)
   *       Sort(nationkey) → MergeJoin(suppkey)
   *                           Sort(suppkey) → TableScan(SUPPLIER)
   *                           Sort(suppkey) → TableScan(PARTSUPP)
   *       TableScan(NATION)
   *
   *   Pipeline 2 — (orderkey / partkey / suppkey+partkey):
   *     MergeJoin(suppkey,partkey)
   *       [Pipeline 1 result]
   *       Sort(suppkey,partkey) → SemiJoin(partkey)
   *                                 Sort(partkey) → MergeJoin(orderkey)
   *                                                   Sort(orderkey) → TableScan(ORDERS)
   *                                                   Sort(orderkey) → TableScan(LINEITEM)
   *                                 Filter(LIKE) → TableScan(PART)
   *   Sort(nation,o_year) → Aggregate(nation,o_year)
   * </pre>
   *
   * <h3>Ideal AFTER structure (merged indexes substituted)</h3>
   * <pre>
   *   Each Sort→Sort→MergeJoin leaf replaced by MergedIndexScan.
   *   E.g.:  Sort(suppkey)→SUPPLIER + Sort(suppkey)→PARTSUPP + MergeJoin(suppkey)
   *          → MergedIndexScan(SUPPLIER+PARTSUPP, collation=[suppkey])
   *
   *   Remaining structure (inter-pipeline joins, filter, aggregate) unchanged.
   * </pre>
   *
   * <p>Full DOT diagrams for the ideal plans are in {@code SESSION_PROGRESS.md}
   * under "Q9 Reference Plans".
   */
  @Test void tpchQ9() throws Exception {
    // Use explicit JOIN ... ON ... syntax so all join conditions are equi-joins
    // and injectSortsBeforeJoin can extract keys via splitJoinCondition.
    // The LIKE filter stays in WHERE and becomes a LogicalFilter on PART.
    final String sql = "SELECT n.n_name AS nation,"
        + " EXTRACT(YEAR FROM o.o_orderdate) AS o_year,"
        + " SUM(l.l_extendedprice * (1 - l.l_discount)"
        + "     - ps.ps_supplycost * l.l_quantity) AS sum_profit"
        + " FROM tpch.lineitem l"
        + " JOIN tpch.orders o ON o.o_orderkey = l.l_orderkey"
        + " JOIN tpch.partsupp ps ON ps.ps_suppkey = l.l_suppkey"
        + "   AND ps.ps_partkey = l.l_partkey"
        + " JOIN tpch.supplier s ON s.s_suppkey = l.l_suppkey"
        + " JOIN tpch.nation n ON s.s_nationkey = n.n_nationkey"
        + " JOIN tpch.part p ON p.p_partkey = l.l_partkey"
        + " WHERE p.p_name LIKE '%green%'"
        + " GROUP BY n.n_name, EXTRACT(YEAR FROM o.o_orderdate)"
        + " ORDER BY n.n_name, o_year DESC";

    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("TPCH", new TpchSchema(0.01, 0, 1, false));

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(rootSchema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(
            Programs.of(RuleSets.ofList(
                EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_RULE,
                EnumerableRules.ENUMERABLE_FILTER_RULE,
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

    final RelNode logicalWithSorts = injectSortsBeforeJoin(root.rel);
    final RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        planner.transform(0, desiredTraits, logicalWithSorts);

    final String beforeStr = dumpText(phase1Plan);
    System.out.println("=== Q9 BEFORE (order-based pipeline) ===");
    System.out.println(beforeStr);
    System.out.println("=== Q9 DOT ===");
    System.out.println(dumpDot(phase1Plan));

    // Dynamically find and register all leaf merge joins (Sort→Scan on both sides).
    final List<EnumerableMergeJoin> leafJoins = findAllLeafMergeJoins(phase1Plan);
    for (EnumerableMergeJoin lj : leafJoins) {
      final EnumerableSort ls = (EnumerableSort) lj.getLeft();
      final EnumerableSort rs = (EnumerableSort) lj.getRight();
      final RelOptTable tl = ((EnumerableTableScan) ls.getInput()).getTable();
      final RelOptTable tr = ((EnumerableTableScan) rs.getInput()).getTable();
      final double rc = lj.estimateRowCount(lj.getCluster().getMetadataQuery());
      MergedIndexRegistry.register(new MergedIndex(
          List.of(tl, tr),
          List.of(ls.getCollation(), rs.getCollation()),
          ls.getCollation(), rc));
    }

    // ── Phase 2: HEP planner fires PipelineToMergedIndexScanRule ──────────
    final HepProgram hepProgram = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
        .build();
    final HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(phase1Plan);
    final RelNode phase2Plan = hepPlanner.findBestExp();

    final String afterStr = dumpText(phase2Plan);
    System.out.println("=== Q9 AFTER (merged index plan) ===");
    System.out.println(afterStr);
    System.out.println("=== Q9 AFTER DOT ===");
    System.out.println(dumpDot(phase2Plan));

    // ── Assert ────────────────────────────────────────────────────────────
    assertThat(afterStr, containsString("EnumerableMergedIndexScan"));
    // Each substituted leaf join pair removes 2 sorts; at least one substitution
    final int sortsBefore = countOccurrences(beforeStr, "EnumerableSort");
    final int sortsAfter  = countOccurrences(afterStr,  "EnumerableSort");
    assertThat(sortsAfter, lessThan(sortsBefore));
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  /**
   * Recursively walks the plan tree and, at each {@link Join}, injects a
   * {@link LogicalSort} before each input using the keys extracted from the
   * join condition via {@link RelOptUtil#splitJoinCondition}.
   *
   * <p>This ensures {@code ENUMERABLE_SORT_RULE} has concrete sort nodes to
   * convert, giving the Volcano planner the physical {@link EnumerableSort}
   * operators that {@link EnumerableMergeJoin} requires from its inputs.
   * Using the actual join-condition keys means multi-join plans (where each
   * join uses a different key) are handled correctly.
   */
  private static RelNode injectSortsBeforeJoin(RelNode node) {
    if (node instanceof Join) {
      final Join join = (Join) node;
      final List<Integer> leftKeys = new ArrayList<>();
      final List<Integer> rightKeys = new ArrayList<>();
      RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(),
          join.getCondition(), leftKeys, rightKeys, new ArrayList<>());
      final RelNode newLeft = injectSortsBeforeJoin(join.getLeft());
      final RelNode newRight = injectSortsBeforeJoin(join.getRight());
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
      return join.copy(join.getTraitSet(), List.of(
          LogicalSort.create(newLeft, leftCollation, null, null),
          LogicalSort.create(newRight, rightCollation, null, null)));
    }
    final List<RelNode> newInputs = node.getInputs().stream()
        .map(MergedIndexTpchPlanTest::injectSortsBeforeJoin)
        .collect(Collectors.toList());
    return node.copy(node.getTraitSet(), newInputs);
  }

  /**
   * Finds the innermost {@link EnumerableMergeJoin} whose two inputs are both
   * {@code EnumerableSort → EnumerableTableScan} (leaf join, no nested joins).
   *
   * <p>Used for multi-join plans to locate the inner pipeline to substitute.
   */
  private static @Nullable EnumerableMergeJoin findLeafMergeJoin(RelNode node) {
    if (node instanceof EnumerableMergeJoin) {
      final EnumerableMergeJoin join = (EnumerableMergeJoin) node;
      if (join.getLeft() instanceof EnumerableSort
          && join.getRight() instanceof EnumerableSort
          && ((EnumerableSort) join.getLeft()).getInput()
              instanceof EnumerableTableScan
          && ((EnumerableSort) join.getRight()).getInput()
              instanceof EnumerableTableScan) {
        return join;
      }
    }
    for (RelNode input : node.getInputs()) {
      final EnumerableMergeJoin found = findLeafMergeJoin(input);
      if (found != null) {
        return found;
      }
    }
    return null;
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

  /**
   * Finds ALL leaf {@link EnumerableMergeJoin} nodes in the plan tree —
   * joins whose two inputs are both {@code EnumerableSort → EnumerableTableScan}.
   *
   * <p>Used for multi-join plans (e.g., TPC-H Q9) to register a merged index
   * for every leaf join pair discovered dynamically.
   */
  private static List<EnumerableMergeJoin> findAllLeafMergeJoins(RelNode node) {
    final List<EnumerableMergeJoin> result = new ArrayList<>();
    if (node instanceof EnumerableMergeJoin) {
      final EnumerableMergeJoin j = (EnumerableMergeJoin) node;
      if (j.getLeft() instanceof EnumerableSort
          && j.getRight() instanceof EnumerableSort
          && ((EnumerableSort) j.getLeft()).getInput() instanceof EnumerableTableScan
          && ((EnumerableSort) j.getRight()).getInput() instanceof EnumerableTableScan) {
        result.add(j);
        return result; // don't recurse into a leaf join's inputs
      }
    }
    for (RelNode input : node.getInputs()) {
      result.addAll(findAllLeafMergeJoins(input));
    }
    return result;
  }

  /** Counts non-overlapping occurrences of {@code sub} in {@code text}. */
  private static int countOccurrences(String text, String sub) {
    int count = 0;
    int idx = 0;
    while ((idx = text.indexOf(sub, idx)) != -1) {
      count++;
      idx += sub.length();
    }
    return count;
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
