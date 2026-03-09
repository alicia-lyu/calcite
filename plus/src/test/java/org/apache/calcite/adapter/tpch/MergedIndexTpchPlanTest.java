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
import org.apache.calcite.adapter.enumerable.EnumerableSortedAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.materialize.MergedIndex;
import org.apache.calcite.materialize.MergedIndexRegistry;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

/**
 * TPC-H plan observation test for {@link EnumerableMergedIndexScan}.
 *
 * <p>Tests run sequentially ({@link ExecutionMode#SAME_THREAD}) because
 * {@link MergedIndexRegistry} is a static singleton: parallel execution
 * causes cross-test registry pollution where one test's registered indexes
 * (identified by qualified table name) are found by another test's HEP pass,
 * producing the wrong {@link MergedIndex} object for identity-based lookups.
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
@Execution(ExecutionMode.SAME_THREAD)
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
    writeDotFile("q3_before", phase1Plan);

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
    writeDotFile("q3_after", phase2Plan);

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
    writeDotFile("q12_before", phase1Plan);

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
    writeDotFile("q12_after", phase2Plan);

    // ── Assert ────────────────────────────────────────────────────────────
    final String planStr = dumpText(phase2Plan);
    assertThat(planStr, containsString("EnumerableMergedIndexScan"));
    assertThat(planStr, not(containsString("EnumerableMergeJoin")));
  }

  /**
   * TPC-H Q3 variant with full pipeline substitution: both the inner pipeline
   * (LINEITEM aggregate ⋈ ORDERS on orderkey) and the outer pipeline
   * (inner_view ⋈ CUSTOMER on custkey) are replaced by merged index scans.
   *
   * <p>Uses a subquery form to force the desired join order:
   * the aggregate over LINEITEM is pushed into a derived table {@code v},
   * which joins ORDERS on orderkey, which joins CUSTOMER on custkey.
   *
   * <p>Two pipelines are registered bottom-up:
   * <ol>
   *   <li>Inner (orderkey): sources = [LINEITEM, ORDERS]; operators = SortedAgg + MergeJoin.
   *       At query time this pipeline is the <em>maintenance plan</em> only — it is replaced
   *       by a leaf {@link EnumerableMergedIndexScan} because join assembly and aggregation
   *       are pre-computed at update time.
   *   <li>Outer (custkey): sources = [inner_view, CUSTOMER]; operator = MergeJoin.
   *       At query time this produces {@code EnumerableMergedIndexJoin → scan}, where the
   *       scan reads the outer merged index and the join assembles co-located record groups.
   * </ol>
   *
   * <h3>Expected AFTER (query-time plan)</h3>
   * <pre>
   *   EnumerableLimitSort(ORDER BY l_revenue DESC, o_orderdate)
   *     EnumerableMergedIndexJoin(custkey, INNER)
   *       EnumerableMergedIndexScan(outer_index)   ← stores inner_view + CUSTOMER by custkey
   * </pre>
   *
   * <p>Full DOT diagrams for BEFORE/AFTER are in {@code test-dot-output/q3ol_*.dot}.
   */
  @Test void tpchQ3OrdersLineitem() throws Exception {
    // Subquery form: aggregate lineitem by orderkey, then join ORDERS on orderkey,
    // then join CUSTOMER on custkey. The subquery forces the join order so that
    // lineitem's aggregate appears as the left input to the inner join.
    final String sql =
        "SELECT v.l_orderkey, v.l_revenue, o.o_orderdate, o.o_shippriority"
            + " FROM (SELECT l.l_orderkey,"
            + "   SUM(l.l_extendedprice * (1 - l.l_discount)) AS l_revenue"
            + "   FROM tpch.lineitem l GROUP BY l.l_orderkey) AS v"
            + " JOIN tpch.orders o ON v.l_orderkey = o.o_orderkey"
            + " JOIN tpch.customer c ON o.o_custkey = c.c_custkey"
            + " ORDER BY v.l_revenue DESC, o.o_orderdate LIMIT 10";

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
    writeDotFile("q3ol_before", phase1Plan);

    // ── Discover all interesting-ordering pipelines (bottom-up) ──────────
    // findAllPipelines walks the plan post-order, identifying each
    // EnumerableMergeJoin whose inputs can be resolved to sources
    // (base tables or inner pipeline views). Returns pipelines in
    // bottom-up order so inner pipeline is registered before outer.
    final List<Pipeline> pipelines = findAllPipelines(phase1Plan);
    assertThat("Expected 2 pipelines (inner orderkey + outer custkey)",
        pipelines.size(), is(2));

    // ── Register merged indexes bottom-up ─────────────────────────────────
    for (Pipeline p : pipelines) {
      // Resolve Pipeline sources to MergedIndex (already registered inner ones)
      final List<Object> resolved = p.sources.stream()
          .map(s -> s instanceof Pipeline ? ((Pipeline) s).mergedIndex : s)
          .collect(Collectors.toList());
      p.mergedIndex = MergedIndex.of(resolved, p.sourceCollations,
          p.sharedCollation, p.rowCount);
      MergedIndexRegistry.register(p.mergedIndex);
    }

    // ── Phase 2: two HEP passes for two-level pipeline dependency ─────────
    // Pass 1 replaces inner joins (Sort→Agg/Scan + Sort→Scan → MergeJoin)
    // with a leaf EnumerableMergedIndexScan.
    // Pass 2 then sees Sort→MergedIndexScan on the outer join's left input
    // and replaces the outer join with EnumerableMergedIndexJoin → scan.
    // A single-pass approach is unreliable because HEP may process the outer
    // join before the inner one fires, leaving the outer join unmatched.
    final HepProgram hepPass = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
        .build();
    final HepPlanner hepPlanner1 = new HepPlanner(hepPass);
    hepPlanner1.setRoot(phase1Plan);
    final RelNode afterPass1 = hepPlanner1.findBestExp();

    final HepPlanner hepPlanner2 = new HepPlanner(hepPass);
    hepPlanner2.setRoot(afterPass1);
    final RelNode phase2Plan = hepPlanner2.findBestExp();

    final String afterStr = dumpText(phase2Plan);
    System.out.println("=== Q3 OL AFTER (merged index plan) ===");
    System.out.println(afterStr);
    writeDotFile("q3ol_after", phase2Plan);

    // ── Assert ────────────────────────────────────────────────────────────
    // No raw Calcite join, sorted aggregate, or intermediate sorts in query plan
    assertThat(afterStr, not(containsString("EnumerableMergeJoin")));
    assertThat(afterStr, not(containsString("EnumerableSortedAggregate")));
    assertThat(afterStr, not(containsString("EnumerableSort(")));
    // Query-time operators present
    assertThat(afterStr, containsString("EnumerableMergedIndexJoin")); // outer assembly
    assertThat(afterStr, containsString("EnumerableMergedIndexScan"));  // outer leaf scan
    // Maintenance-time aggregation NOT in query plan
    assertThat(afterStr, not(containsString("EnumerableMergedIndexAggregate")));
    // Outer merged index involves CUSTOMER key
    assertThat(afterStr, containsString("C_CUSTKEY"));
  }

  // ── Pipeline discovery helpers for tpchQ3OrdersLineitem ──────────────────

  /**
   * Holds one interesting-ordering pipeline discovered in the Phase 1 plan.
   * Sources are either {@link RelOptTable} (base table) or another
   * {@link Pipeline} (inner view). After registration, {@link #mergedIndex}
   * is set to the registered {@link MergedIndex}.
   */
  private static class Pipeline {
    final List<Object> sources;              // RelOptTable | Pipeline
    final List<RelCollation> sourceCollations;
    final RelCollation sharedCollation;
    final double rowCount;
    MergedIndex mergedIndex;                 // set after registration

    Pipeline(List<Object> sources, List<RelCollation> sourceCollations,
        RelCollation sharedCollation, double rowCount) {
      this.sources = sources;
      this.sourceCollations = sourceCollations;
      this.sharedCollation = sharedCollation;
      this.rowCount = rowCount;
    }
  }

  /**
   * Discovers all interesting-ordering pipelines in the plan tree, returned
   * in bottom-up (post-order) order so inner pipelines precede outer ones.
   *
   * <p>Each pipeline corresponds to one {@link EnumerableMergeJoin} whose
   * inputs can be resolved — either base tables or an inner pipeline that was
   * already collected. Sources are recorded as {@link RelOptTable} or
   * {@link Pipeline} objects.
   */
  private static List<Pipeline> findAllPipelines(RelNode root) {
    final IdentityHashMap<RelNode, Pipeline> byJoin = new IdentityHashMap<>();
    final List<Pipeline> ordered = new ArrayList<>();
    collectPipelines(root, byJoin, ordered);
    // ordered preserves post-order insertion, guaranteeing inner pipelines
    // appear before outer ones regardless of IdentityHashMap iteration order.
    return ordered;
  }

  private static void collectPipelines(RelNode node,
      IdentityHashMap<RelNode, Pipeline> byJoin, List<Pipeline> ordered) {
    // Post-order: recurse into inputs first so inner joins are processed before outer
    for (RelNode input : node.getInputs()) {
      collectPipelines(input, byJoin, ordered);
    }
    if (!(node instanceof EnumerableMergeJoin)) {
      return;
    }
    final EnumerableMergeJoin join = (EnumerableMergeJoin) node;
    final Object leftSrc = extractTestSource(join.getLeft(), byJoin);
    final Object rightSrc = extractTestSource(join.getRight(), byJoin);
    if (leftSrc == null || rightSrc == null) {
      return;
    }
    final RelCollation lc = ((EnumerableSort) join.getLeft()).getCollation();
    final RelCollation rc = ((EnumerableSort) join.getRight()).getCollation();
    final double rowCount =
        join.estimateRowCount(join.getCluster().getMetadataQuery());
    final Pipeline p =
        new Pipeline(List.of(leftSrc, rightSrc), List.of(lc, rc), lc, rowCount);
    byJoin.put(join, p);
    ordered.add(p);
  }

  /**
   * Identifies the source for one side of a {@link EnumerableMergeJoin}
   * in the pre-HEP Phase 1 plan.
   *
   * <p>Accepted patterns (all wrapped in an outer {@link EnumerableSort}):
   * <ul>
   *   <li>{@code Sort → TableScan} — base table
   *   <li>{@code Sort → (Aggregate | Project | ...) → ... → TableScan} — drills through
   *       single-input operators; intermediate aggregates/projects are maintenance-time
   *   <li>{@code Sort → EnumerableMergeJoin} (already in {@code byJoin}) — inner pipeline view
   * </ul>
   */
  private static @Nullable Object extractTestSource(RelNode sortNode,
      IdentityHashMap<RelNode, Pipeline> byJoin) {
    if (!(sortNode instanceof EnumerableSort)) {
      return null;
    }
    final RelNode below = ((EnumerableSort) sortNode).getInput();
    // Post-order traversal ensures the inner MergeJoin is already in byJoin
    if (below instanceof EnumerableMergeJoin && byJoin.containsKey(below)) {
      return byJoin.get(below);
    }
    // Drill through any chain of single-input operators to reach the base table scan
    return findLeafScan(below);
  }

  /**
   * Drills through a chain of single-input operators to find the leaf
   * {@link EnumerableTableScan}, returning its table. Returns {@code null}
   * if the chain splits (multi-input) or has no scan at the leaf.
   */
  private static @Nullable RelOptTable findLeafScan(RelNode node) {
    if (node instanceof EnumerableTableScan) {
      return ((EnumerableTableScan) node).getTable();
    }
    if (node.getInputs().size() == 1) {
      return findLeafScan(node.getInputs().get(0));
    }
    return null;
  }

  /**
   * <h3>Ideal AFTER structure (merged indexes substituted)</h3>
   * <pre>
   *       Only Sort(nation,o_year) → Aggregate(nation,o_year) appear in the query plan, with the sort replaced by the indexed join view produced by the last merge join.
   *
   * </pre>
   *
   */
  @Test void tpchQ9() throws Exception {
    // Use explicit JOIN ... ON ... syntax so all join conditions are equi-joins
    // and injectSortsBeforeJoin can extract keys via splitJoinCondition.
    // The LIKE filter stays in WHERE and becomes a LogicalFilter on PART.
    // final String sql = "SELECT n.n_name AS nation,"
    //     + " EXTRACT(YEAR FROM o.o_orderdate) AS o_year,"
    //     + " SUM(l.l_extendedprice * (1 - l.l_discount)"
    //     + "     - ps.ps_supplycost * l.l_quantity) AS sum_profit"
    //     + " FROM tpch.lineitem l"
    //     + " JOIN tpch.orders o ON o.o_orderkey = l.l_orderkey"
    //     + " JOIN tpch.partsupp ps ON ps.ps_suppkey = l.l_suppkey"
    //     + "   AND ps.ps_partkey = l.l_partkey"
    //     + " JOIN tpch.supplier s ON s.s_suppkey = l.l_suppkey"
    //     + " JOIN tpch.nation n ON s.s_nationkey = n.n_nationkey"
    //     + " JOIN tpch.part p ON p.p_partkey = l.l_partkey"
    //     + " WHERE p.p_name LIKE '%green%'"
    //     + " GROUP BY n.n_name, EXTRACT(YEAR FROM o.o_orderdate)"
    //     + " ORDER BY n.n_name, o_year DESC";
    // Rewrite SQL query to order joins
        final String sql = "
        WITH filtered_ol AS (
        SELECT l.l_orderkey, l.l_suppkey, l.l_partkey,
               EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
               SUM(l.l_extendedprice * (1 - l.l_discount)
               - ps.ps_supplycost * l.l_quantity) AS sum_profit
        FROM tpch.orders o JOIN tpch.lineitem l ON o.o_orderkey = l.l_orderkey" + // first join OL because the subsequent steps will destroy the order on orderkey
        "   JOIN tpch.part ON p.p_partkey = l.l_partkey" // then reduces the number of rows early on
        +"  JOIN tpch.partsupp ps ON ps.ps_suppkey = l.l_suppkey AND ps.ps_partkey = l.l_partkey
        WHERE p.p_name LIKE '%green%'
        ) SELECT n.n_name AS nation, o_year, SUM(sum_profit) AS sum_profit
        FROM filtered_ol JOIN tpch.supplier s ON filtered_ol.l_suppkey = s.s_suppkey" // first join with supplier because filtered_ol is sorted on partkey, suppkey. Resort is cheaper.
        + "
        JOIN tpch.nation n ON s.s_nationkey = n.n_nationkey" // The final join result is sorted on (nationkey, suppkey, partkey, orderkey)
        + "
        GROUP BY n.n_name, o_year
        ORDER BY n.n_name, o_year DESC
        ";

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
    writeDotFile("q9_before", phase1Plan);

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
    writeDotFile("q9_after", phase2Plan);

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

//   private static @Nullable EnumerableInterestingOrderingPipeline findPipeline(RelNode node) {
    // 1. implement EnumerableInterestingOrderingPipeline which include one or multiple operators which require EnumerableSorts all on the same key. The EnumerableSorts are not included in EnumerableInterestingOrderingPipeline but reside just before this pipeline
    // 2. Identify a pipeline as large as possible, which the search only cut off when it reaches a different EnumerableSort. 
// }
// private static @Nullable list<EnumerableInterestingOrderingPipeline> findAllPipelines(RelNode node) {
    // Do findPipeline for the entire plan, and it should be segregated into multiple pipelines with EnumerableSorts as the dividers.
    // The inputs to each pipeline is all EnumerableSorts on the same key, and a merged index should be created on all those inputs. An input may be a table or a view (i.e., query result) of another pipeline.                                              
// }

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

  /**
   * Writes two Graphviz DOT plan files to {@code test-dot-output/}:
   * <ul>
   *   <li>{@code <name>.dot} — plain format: full first-line explain label, no colors.
   *   <li>{@code <name>_color.dot} — presentation format: color-coded nodes with
   *       shortened labels using actual column names instead of {@code $N} indices.
   * </ul>
   */
  private static void writeDotFile(String name, RelNode rel) {
    writeDotToFile(name + ".dot", dumpDot(rel));
    writeDotToFile(name + "_color.dot", dumpDotColor(rel));
  }

  private static void writeDotToFile(String filename, String content) {
    final java.nio.file.Path dir = java.nio.file.Paths.get("test-dot-output");
    try {
      java.nio.file.Files.createDirectories(dir);
      final java.nio.file.Path file = dir.resolve(filename);
      java.nio.file.Files.writeString(file, content);
      System.out.println("DOT written → " + file.toAbsolutePath());
    } catch (java.io.IOException e) {
      System.err.println("Failed to write DOT file " + filename + ": " + e.getMessage());
    }
  }

  private static String dumpText(RelNode rel) {
    return RelOptUtil.dumpPlan("", rel, SqlExplainFormat.TEXT,
        SqlExplainLevel.EXPPLAN_ATTRIBUTES);
  }

  /**
   * Plain DOT: each node gets its full first-line explain label and no color.
   * Every node has a unique integer ID so visually identical siblings
   * (e.g., two {@code EnumerableSort(sort0=[$0])} nodes) are never merged.
   */
  private static String dumpDot(RelNode root) {
    final StringBuilder sb = new StringBuilder("digraph {\n  rankdir=BT;\n");
    final java.util.IdentityHashMap<RelNode, Integer> ids = new java.util.IdentityHashMap<>();
    final int[] counter = {0};
    dumpDotNode(root, sb, ids, counter);
    sb.append("}\n");
    return sb.toString();
  }

  private static int dumpDotNode(RelNode node, StringBuilder sb,
      java.util.IdentityHashMap<RelNode, Integer> ids, int[] counter) {
    if (ids.containsKey(node)) {
      return ids.get(node);
    }
    final int id = counter[0]++;
    ids.put(node, id);
    final String explain = RelOptUtil.dumpPlan("", node,
        SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES).trim();
    final String label = explain.lines().findFirst()
        .orElse(node.getClass().getSimpleName()).trim().replace("\"", "'");
    sb.append("  n").append(id).append(" [label=\"").append(label).append("\"];\n");
    final List<RelNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      final int childId = dumpDotNode(inputs.get(i), sb, ids, counter);
      sb.append("  n").append(childId).append(" -> n").append(id)
          .append(" [label=\"").append(i).append("\"];\n");
    }
    return id;
  }

  /**
   * Colorful DOT: nodes are color-coded by operator type and labeled with
   * short, human-readable names — actual column names instead of {@code $N}
   * field-index references, and without the {@code Enumerable} prefix.
   *
   * <p>Color legend:
   * <ul>
   *   <li>MergedIndexScan — light green (#90EE90)
   *   <li>MergedIndexJoin — lime green (#32CD32)
   *   <li>MergeJoin — gold (#FFD700)
   *   <li>Sort / LimitSort — light salmon (#FFA07A)
   *   <li>TableScan — light blue (#ADD8E6)
   *   <li>Aggregate — plum (#DDA0DD)
   *   <li>Project — light gray (#D3D3D3)
   *   <li>Filter — peach (#FFDAB9)
   * </ul>
   */
  private static String dumpDotColor(RelNode root) {
    final StringBuilder sb = new StringBuilder(
        "digraph {\n  rankdir=BT;\n  node [fontname=\"Helvetica\"];\n");
    final java.util.IdentityHashMap<RelNode, Integer> ids = new java.util.IdentityHashMap<>();
    final int[] counter = {0};
    dumpDotColorNode(root, sb, ids, counter);
    sb.append("}\n");
    return sb.toString();
  }

  private static int dumpDotColorNode(RelNode node, StringBuilder sb,
      java.util.IdentityHashMap<RelNode, Integer> ids, int[] counter) {
    if (ids.containsKey(node)) {
      return ids.get(node);
    }
    final int id = counter[0]++;
    ids.put(node, id);
    final String explain = RelOptUtil.dumpPlan("", node,
        SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES).trim();
    final String firstLine = explain.lines().findFirst()
        .orElse(node.getClass().getSimpleName()).trim();
    final String label = nodeLabel(node, firstLine);
    final String color = nodeColor(node);
    sb.append("  n").append(id)
        .append(" [label=\"").append(label).append("\"")
        .append(", style=filled, fillcolor=\"").append(color).append("\"")
        .append("];\n");
    final List<RelNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      final int childId = dumpDotColorNode(inputs.get(i), sb, ids, counter);
      sb.append("  n").append(childId).append(" -> n").append(id)
          .append(" [label=\"").append(i).append("\"];\n");
    }
    return id;
  }

  /**
   * Returns a short, presentation-friendly label for a colorful DOT node.
   *
   * <p>Strips the {@code Enumerable} prefix. Resolves {@code $N} field-index
   * references to actual column names using the node's input row type.
   * Drops internal attributes like {@code sort0=}, {@code dir0=}, {@code joinType=}.
   */
  private static String nodeLabel(RelNode node, String firstLine) {
    final int parenIdx = firstLine.indexOf('(');
    final String fullCls = parenIdx >= 0 ? firstLine.substring(0, parenIdx) : firstLine;
    final String cls = fullCls.startsWith("Enumerable")
        ? fullCls.substring("Enumerable".length()) : fullCls;
    if (parenIdx < 0 || !firstLine.endsWith(")")) {
      return cls.replace("\"", "'");
    }
    final String inner = firstLine.substring(parenIdx + 1, firstLine.length() - 1);

    // TableScan: show only the table name (last element of the qualified name).
    if (fullCls.contains("TableScan")) {
      final int lb = inner.lastIndexOf(", ");
      final String name = (lb >= 0 ? inner.substring(lb + 2) : inner)
          .replace("[", "").replace("]", "");
      return (cls + "\\n" + name).replace("\"", "'");
    }

    // MergedIndexScan / MergedIndexJoin: show table list, drop collation attribute.
    if (fullCls.contains("MergedIndex")) {
      final int collationIdx = inner.indexOf(", collation");
      final String tables = collationIdx >= 0 ? inner.substring(0, collationIdx) : inner;
      final String attrs = tables.replace("], ", "]\\n").replace(", [", "\\n[");
      return (cls + "\\n" + attrs).replace("\"", "'");
    }

    // MergeJoin: resolve join keys to "LEFT_COL = RIGHT_COL" using splitJoinCondition.
    if (node instanceof EnumerableMergeJoin) {
      final EnumerableMergeJoin mj = (EnumerableMergeJoin) node;
      final List<Integer> lk = new ArrayList<>();
      final List<Integer> rk = new ArrayList<>();
      RelOptUtil.splitJoinCondition(mj.getLeft(), mj.getRight(),
          mj.getCondition(), lk, rk, new ArrayList<>());
      final List<org.apache.calcite.rel.type.RelDataTypeField> lf =
          mj.getLeft().getRowType().getFieldList();
      final List<org.apache.calcite.rel.type.RelDataTypeField> rf =
          mj.getRight().getRowType().getFieldList();
      final StringBuilder cond = new StringBuilder();
      for (int i = 0; i < lk.size(); i++) {
        if (i > 0) {
          cond.append("\\n");
        }
        cond.append(lk.get(i) < lf.size() ? lf.get(lk.get(i)).getName() : "$" + lk.get(i));
        cond.append(" = ");
        cond.append(rk.get(i) < rf.size() ? rf.get(rk.get(i)).getName() : "$" + rk.get(i));
      }
      return (cls + "\\n" + cond).replace("\"", "'");
    }

    // Sort / LimitSort: resolve sort fields to "COL_NAME ASC/DESC", one per line.
    if (node instanceof org.apache.calcite.rel.core.Sort
        && !node.getInputs().isEmpty()) {
      final org.apache.calcite.rel.core.Sort sort =
          (org.apache.calcite.rel.core.Sort) node;
      final List<org.apache.calcite.rel.type.RelDataTypeField> fields =
          node.getInputs().get(0).getRowType().getFieldList();
      final String keys = sort.getCollation().getFieldCollations().stream()
          .map(fc -> {
            final String fname = fc.getFieldIndex() < fields.size()
                ? fields.get(fc.getFieldIndex()).getName() : "$" + fc.getFieldIndex();
            final String dir =
                fc.getDirection() == RelFieldCollation.Direction.DESCENDING ? " DESC" : " ASC";
            return fname + dir;
          })
          .collect(Collectors.joining("\\n"));
      final String fetchStr = sort.fetch != null ? "\\nLIMIT " + sort.fetch : "";
      return (cls + "\\n" + keys + fetchStr).replace("\"", "'");
    }

    // Aggregate / SortedAggregate: resolve group-by indices to column names.
    if (fullCls.contains("Aggregate") && !node.getInputs().isEmpty()) {
      final List<org.apache.calcite.rel.type.RelDataTypeField> fields =
          node.getInputs().get(0).getRowType().getFieldList();
      final int grpStart = inner.indexOf("group=[{");
      final int grpEnd = grpStart >= 0 ? inner.indexOf("}]", grpStart) : -1;
      if (grpStart >= 0 && grpEnd >= 0) {
        final String body = inner.substring(grpStart + "group=[{".length(), grpEnd).trim();
        final String resolved = body.isEmpty() ? "(none)"
            : java.util.Arrays.stream(body.split(",\\s*"))
                .map(s -> {
                  try {
                    final int idx = Integer.parseInt(s.trim());
                    return idx < fields.size() ? fields.get(idx).getName() : "$" + idx;
                  } catch (NumberFormatException e) {
                    return s.trim();
                  }
                })
                .collect(Collectors.joining(", "));
        return (cls + "\\ngroup: " + resolved).replace("\"", "'");
      }
    }

    // Default: just the short class name (Project, Filter, etc.).
    return cls.replace("\"", "'");
  }

  /** Returns a fill color for the colorful DOT node based on operator type. */
  private static String nodeColor(RelNode node) {
    final String cls = node.getClass().getSimpleName();
    if (cls.contains("MergedIndexScan")) return "#90EE90"; // light green
    if (cls.contains("MergedIndexJoin")) return "#32CD32"; // lime green
    if (cls.contains("MergeJoin"))       return "#FFD700"; // gold
    if (cls.contains("LimitSort"))       return "#FFA07A"; // light salmon
    if (cls.contains("Sort"))            return "#FFA07A"; // light salmon
    if (cls.contains("TableScan"))       return "#ADD8E6"; // light blue
    if (cls.contains("Aggregate"))       return "#DDA0DD"; // plum
    if (cls.contains("Project"))         return "#D3D3D3"; // light gray
    if (cls.contains("Filter"))          return "#FFDAB9"; // peach
    return "white";
  }
}
