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
import org.apache.calcite.materialize.MergedIndex;
import org.apache.calcite.materialize.MergedIndexRegistry;
import org.apache.calcite.materialize.Pipeline;
import org.apache.calcite.materialize.TaggedRowSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.stream.LogicalDelta;
import org.apache.calcite.rel.stream.StreamRules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.MergedIndexTestUtil;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSets;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * TPC-H plan observation test for {@link EnumerableMergedIndexScan}.
 *
 * <p>Tests run sequentially ({@link ExecutionMode#SAME_THREAD}) because
 * {@link MergedIndexRegistry} is a static singleton: parallel execution
 * causes cross-test registry pollution where one test's registered indexes
 * (identified by qualified table name) are found by another test's HEP pass,
 * producing the wrong {@link MergedIndex} object for identity-based lookups.
 *
 * <p>Demonstrates BEFORE/AFTER query plans for TPC-H Q12 (2-table: ORDERS,
 * LINEITEM — full substitution), Q3-OL (3-table: ORDERS, LINEITEM, CUSTOMER
 * — two-level pipeline), and Q9 (6-table — five-level pipeline).
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

  @AfterEach
  void clearRegistry() {
    MergedIndexRegistry.clear();
  }

  /**
   * TPC-H Q12 (no date filter): ORDERS ⋈ LINEITEM on orderkey.
   *
   * <p>Two pipelines are discovered:
   * <ol>
   *   <li>Join pipeline (orderkey): ORDERS ⋈ LINEITEM — merged index
   *   <li>Indexed view (l_shipmode): single-source pipeline above the join,
   *       sorted by the GROUP BY key. The boundary Sort(l_shipmode) is replaced
   *       by a MIScan that absorbs the MergeJoin.
   * </ol>
   *
   * <p>Expected AFTER structure:
   * <pre>
   *   EnumerableSort(l_shipmode)                  — ORDER BY (no-op)
   *     EnumerableSortedAggregate(l_shipmode)
   *       EnumerableMergedIndexScan(ivMI)          — indexed view scan
   * </pre>
   */
  @Test void tpchQ12() throws Exception {
    // TPC-H Q12 (no date filter): count high/low priority lines per ship mode.
    final String sql = "SELECT l.l_shipmode,"
        + " SUM(CASE WHEN o.o_orderpriority = '1-URGENT'"
        + "     THEN 1 ELSE 0 END) AS high_line_count,"
        + " SUM(CASE WHEN o.o_orderpriority <> '1-URGENT'"
        + "     THEN 1 ELSE 0 END) AS low_line_count"
        + " FROM tpch.orders o"
        + " JOIN tpch.lineitem l ON o.o_orderkey = l.l_orderkey"
        + " WHERE l.l_shipmode IN ('MAIL', 'SHIP')"
        + " AND l.l_commitdate < l.l_receiptdate"
        + " AND l.l_shipdate < l.l_commitdate"
        + " AND l.l_receiptdate >= DATE '1994-01-01'"
        + " AND l.l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR"
        + " GROUP BY l.l_shipmode"
        + " ORDER BY l.l_shipmode"; // Note o_orderpriority was deleted per TPC-H 2017 spec

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
                EnumerableRules.ENUMERABLE_FILTER_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE,
                EnumerableRules.ENUMERABLE_LIMIT_RULE,
                EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE)))
        .build();

    final Planner planner = Frameworks.getPlanner(config);
    final SqlNode parsed = planner.parse(sql);
    final SqlNode validated = planner.validate(parsed);
    final RelRoot root = planner.rel(validated);

    final RelNode logicalWithSorts =
        MergedIndexTestUtil.injectSortsBeforeSortBasedOps(root.rel);

    final RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        planner.transform(0, desiredTraits, logicalWithSorts);

    System.out.println("=== Q12 BEFORE (order-based pipeline) ===");
    System.out.println(dumpText(phase1Plan));
    writeDotFile("q12/before-pipeline", phase1Plan);

    // ── Discover pipelines and register merged index ──────────────────────
    final Pipeline rootPipeline = Pipeline.buildTree(phase1Plan);
    final List<Pipeline> pipelines = rootPipeline.flatten();
    assertThat("Expected 2 pipelines for Q12 (1 join + 1 indexed view)",
        pipelines.size(), is(2));

    // Capture logical subtrees for IVM — must use logical nodes (SQL types),
    // not physical EnumerableMergeJoin (JavaType) from Phase 1.
    Pipeline.captureLogicalRoots(rootPipeline, logicalWithSorts);

    // ── Verify assembly subtree ───────────────────────────────────────────
    final Pipeline.AssemblySubtree asmQ12 = pipelines.get(0).findAssemblySubtree();
    assertThat("Q12 assembly subtree should exist", asmQ12 != null, is(true));
    assertThat("Q12 LCA should be MergeJoin",
        asmQ12.lca, instanceOf(EnumerableMergeJoin.class));
    assertThat("Q12 assembly should contain only the MergeJoin",
        asmQ12.nodes, hasSize(1));
    assertThat("Q12 should have 2 boundary sorts",
        asmQ12.boundarySorts, hasSize(2));

    // ── Create MergedIndexes (bottom-up) and set maintenance plans ────────
    // Pipeline 0: join pipeline (ORDERS ⋈ LINEITEM on orderkey)
    final Pipeline joinPipeline = pipelines.get(0);
    new MergedIndex(joinPipeline);

    // Pipeline 1: indexed view (single-source, sorted by l_shipmode)
    final Pipeline ivPipeline = pipelines.get(1);
    new MergedIndex(ivPipeline);

    // Set maintenance plans for all pipelines with captured logical roots
    for (Pipeline p : pipelines) {
      if (p.logicalRoot != null) {
        p.mergedIndex.setMaintenancePlan(deriveMaintenancePlan(p));
      }
    }

    System.out.println("=== Q12 MAINTENANCE PLAN (incremental) ===");
    System.out.println(dumpText(joinPipeline.mergedIndex.getMaintenancePlan()));
    writeDotFile("q12/maintenance", joinPipeline.mergedIndex.getMaintenancePlan());
    writeDotFileTree("q12/maintenance-tree",
        joinPipeline.mergedIndex.getMaintenancePlan());

    // ── Phase 2: incremental MI registration with multi-stage HEP ────────
    // Pass 1: replace join pipeline boundary Sorts (orderkey) with MIScans.
    // Pass 2: replace indexed view boundary Sort (l_shipmode) with MIScan,
    //         absorbing the MergeJoin and join MIScans into a single scan.
    final HepProgram hepProgram = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
        .build();
    RelNode currentPlan = phase1Plan;
    for (int i = 0; i < pipelines.size(); i++) {
      Pipeline p = pipelines.get(i);
      MergedIndexRegistry.register(p.mergedIndex);
      final HepPlanner hp = new HepPlanner(hepProgram);
      hp.setRoot(currentPlan);
      currentPlan = hp.findBestExp();

      // Capture index creation plan for non-root pipelines.
      // The creation plan = entire pipeline execution, producing output
      // rows for the parent MI. Found below the parent's boundary Sort.
      boolean isRoot = (i == pipelines.size() - 1);
      if (!isRoot) {
        RelNode creationRoot = MergedIndexTestUtil.findCreationPlanRoot(
            currentPlan, p.mergedIndex);
        if (creationRoot != null) {
          p.mergedIndex.setIndexCreationPlan(creationRoot);
        }
      }
    }
    final RelNode phase2Plan = currentPlan;

    System.out.println("=== Q12 AFTER (merged index plan) ===");
    System.out.println(dumpText(phase2Plan));
    writeDotFile("q12/root-pipeline-query-plan", phase2Plan);

    // ── Index creation plans ─────────────────────────────────────────────
    System.out.println("=== Q12 INDEX CREATION PLANS ===");
    for (int i = 0; i < pipelines.size() - 1; i++) {
      Pipeline p = pipelines.get(i);
      RelNode cp = p.mergedIndex.getIndexCreationPlan();
      System.out.println("-- pipeline " + i + ": " + p.mergedIndex);
      System.out.println(cp != null ? dumpText(cp) : "(none)");
      if (cp != null) {
        writeDotFile("q12/pipeline-" + i + "-index-creation-plan", cp);
      }
    }

    // ── Assert ────────────────────────────────────────────────────────────
    // After indexed view replacement: the Sort(l_shipmode) above MergeJoin
    // is replaced by a single MIScan. The MergeJoin and its join MIScans
    // are absorbed — only the indexed view MIScan remains.
    final String planStr = dumpText(phase2Plan);
    assertThat(planStr, containsString("EnumerableMergedIndexScan"));
    // MergeJoin absorbed into indexed view scan
    assertThat(planStr, not(containsString("EnumerableMergeJoin")));
    // Single indexed view MIScan replaces the entire join + sort pipeline
    assertThat(MergedIndexTestUtil.countOccurrences(planStr,
        "EnumerableMergedIndexScan"), is(1));
    // No TableScan remains (absorbed into MI scans)
    assertThat(planStr, not(containsString("EnumerableTableScan")));
    // Join pipeline has maintenance plan with Project above Union of 2 delta branches.
    // LogicalProject is the remaining operator above the join assembly LCA.
    assertThat("Q12 join MI missing maintenance plan",
        joinPipeline.mergedIndex.getMaintenancePlan() != null, is(true));
    final String maintStr12 = dumpText(joinPipeline.mergedIndex.getMaintenancePlan());
    assertThat(maintStr12, containsString("LogicalProject"));  // remaining op above assembly
    assertThat(maintStr12, containsString("LogicalUnion"));
    assertThat(MergedIndexTestUtil.countOccurrences(maintStr12, "LogicalJoin"), is(2));
    assertThat(MergedIndexTestUtil.countOccurrences(maintStr12, "LogicalDelta"), is(2));
    // Non-root pipelines have index creation plans with MIScans, no boundary Sorts
    for (int i = 0; i < pipelines.size() - 1; i++) {
      RelNode cp = pipelines.get(i).mergedIndex.getIndexCreationPlan();
      assertThat("creation plan should exist for level " + i, cp != null, is(true));
      String cpStr = dumpText(cp);
      assertThat(cpStr, containsString("EnumerableMergedIndexScan"));
      assertThat(MergedIndexTestUtil.countOccurrences(cpStr, "EnumerableSort("), is(0));
    }

    // ── TaggedRowSchema for Q12 (ORDERS ⋈ LINEITEM on orderkey) ──────────
    // Verify byte-width metadata for cost estimation and slot counts.
    final TaggedRowSchema schema = joinPipeline.mergedIndex.getTaggedRowSchema();
    assertThat("Q12 keyFieldCount", schema.keyFieldCount, is(1));
    assertThat("Q12 sourceCount", schema.sourceCount, is(2));
    assertThat("Q12 domainCount", schema.domainCount, is(2));

    // orderkey is IDENTIFIER → Long.class → BIGINT = 8 bytes
    assertThat("Q12 keyFieldByteWidths[0]",
        schema.keyFieldByteWidths.get(0), is(8.0));
    // keyPrefixByteWidth = 1 (tag) + 8 (BIGINT) = 9
    assertThat("Q12 keyPrefixByteWidth", schema.keyPrefixByteWidth, is(9.0));

    // ORDERS has 9 cols, 1 key → 8 payload; LINEITEM has 16 cols, 1 key → 15
    assertThat("Q12 payloadFieldCounts[0]",
        schema.payloadFieldCounts.get(0), is(8));
    assertThat("Q12 payloadFieldCounts[1]",
        schema.payloadFieldCounts.get(1), is(15));

    // Total record byte widths: keyPrefix(9) + indexId(2) + payload
    assertThat("Q12 totalRecordByteWidth(0)",
        schema.totalRecordByteWidth(0),
        is(9.0 + 2 + schema.payloadByteWidths.get(0)));
    assertThat("Q12 totalRecordByteWidth(1)",
        schema.totalRecordByteWidth(1),
        is(9.0 + 2 + schema.payloadByteWidths.get(1)));

    // Slot counts: 2*1 + 2 + payloadFieldCount
    assertThat("Q12 taggedRowSlotCount(0)",
        schema.taggedRowSlotCount(0), is(2 + 2 + 8));
    assertThat("Q12 taggedRowSlotCount(1)",
        schema.taggedRowSlotCount(1), is(2 + 2 + 15));

    System.out.println("=== Q12 TaggedRowSchema ===");
    System.out.println("  keyFieldCount=" + schema.keyFieldCount);
    System.out.println("  keyPrefixByteWidth=" + schema.keyPrefixByteWidth);
    System.out.println("  payloadByteWidths=" + schema.payloadByteWidths);
    System.out.println("  totalRecordByteWidths=" + schema.totalRecordByteWidths);
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
   *       At query time, the MergeJoin stays and each boundary Sort is replaced by
   *       a per-source {@link EnumerableMergedIndexScan}.
   * </ol>
   *
   * <h3>Expected AFTER (per-source MI scan plan)</h3>
   * <pre>
   *   EnumerableLimitSort(ORDER BY l_revenue DESC, o_orderdate)
   *     EnumerableProject(...)
   *       EnumerableMergeJoin(custkey)          ← stays in plan (per-source architecture)
   *         EnumerableMergedIndexScan(MI_outer, source=inner_view)
   *         EnumerableMergedIndexScan(MI_outer, source=CUSTOMER)
   * </pre>
   *
   * <p>Full DOT diagrams for BEFORE/AFTER are in {@code test-dot-output/q3ol_*.dot}.
   */
  @SuppressWarnings("deprecation")
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

    final RelNode logicalWithSorts =
        MergedIndexTestUtil.injectSortsBeforeSortBasedOps(root.rel);

    final RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        planner.transform(0, desiredTraits, logicalWithSorts);

    final String beforeStr = dumpText(phase1Plan);
    System.out.println("=== Q3 OL BEFORE (order-based pipeline) ===");
    System.out.println(beforeStr);
    writeDotFile("q3ol/before-pipeline", phase1Plan);

    // ── Discover all interesting-ordering pipelines (bottom-up) ──────────
    // buildPipelineTree walks top-down, cutting at Sort boundaries.
    // flattenPipelines returns non-trivial pipelines in post-order
    // (inner first) so inner pipeline is registered before outer.
    final Pipeline rootPipeline = Pipeline.buildTree(phase1Plan);
    final List<Pipeline> pipelines = rootPipeline.flatten();

    // Capture logical subtrees for IVM — logical nodes have SQL row types
    // compatible with StreamRules; physical EnumerableMergeJoin uses JavaType.
    Pipeline.captureLogicalRoots(rootPipeline, logicalWithSorts);
    assertThat("Expected 2 pipelines (inner orderkey + outer custkey)",
        pipelines.size(), is(2));

    // ── Verify assembly subtrees ──────────────────────────────────────────
    // Inner pipeline (orderkey): MergeJoin with SortedAggregate on one side
    // (no intermediate Sort between SortedAgg and the boundary Sort below it).
    // Assembly = {MergeJoin, SortedAggregate}.
    final Pipeline.AssemblySubtree asmInner =
        pipelines.get(0).findAssemblySubtree();
    assertThat("Q3-OL inner assembly subtree should exist",
        asmInner != null, is(true));
    assertThat("Q3-OL inner LCA should be MergeJoin",
        asmInner.lca, instanceOf(EnumerableMergeJoin.class));
    assertThat("Q3-OL inner assembly should contain MergeJoin + SortedAggregate",
        asmInner.nodes, hasSize(2));
    assertThat("Q3-OL inner should have 2 boundary sorts",
        asmInner.boundarySorts, hasSize(2));

    // Outer pipeline (custkey): just a MergeJoin with 2 boundary sorts.
    // Assembly = {MergeJoin}.
    final Pipeline.AssemblySubtree asmOuter =
        pipelines.get(1).findAssemblySubtree();
    assertThat("Q3-OL outer assembly subtree should exist",
        asmOuter != null, is(true));
    assertThat("Q3-OL outer LCA should be MergeJoin",
        asmOuter.lca, instanceOf(EnumerableMergeJoin.class));
    assertThat("Q3-OL outer assembly should contain only MergeJoin",
        asmOuter.nodes, hasSize(1));
    assertThat("Q3-OL outer should have 2 boundary sorts",
        asmOuter.boundarySorts, hasSize(2));

    // ── Create MergedIndexes (bottom-up) and set maintenance plans ────────
    for (Pipeline p : pipelines) {
      new MergedIndex(p);
    }
    // Set maintenance plans for all pipelines with captured logical roots
    for (Pipeline p : pipelines) {
      if (p.logicalRoot != null) {
        p.mergedIndex.setMaintenancePlan(deriveMaintenancePlan(p));
      }
    }

    printMaintenancePlans("Q3-OL", pipelines);
    for (int i = 0; i < pipelines.size(); i++) {
      final RelNode mp = pipelines.get(i).mergedIndex.getMaintenancePlan();
      if (mp != null) {
        writeDotFile("q3ol/maintenance-" + i, mp);
        writeDotFileTree("q3ol/maintenance-" + i + "-tree", mp);
      }
    }

    // ── Phase 2: incremental MI registration with multi-stage HEP ────────
    // Each pass registers ONE level's MI and replaces that level's boundary
    // Sorts with per-source MIScans. Leaf pipeline first, then root.
    // After each pass, deeper Sorts are already replaced, so the rule only
    // matches the current level's Sorts.
    final HepProgram hepPass = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
        .build();

    RelNode currentPlan = phase1Plan;
    for (int i = 0; i < pipelines.size(); i++) {
      Pipeline p = pipelines.get(i);
      MergedIndexRegistry.register(p.mergedIndex);
      final HepPlanner hp = new HepPlanner(hepPass);
      hp.setRoot(currentPlan);
      currentPlan = hp.findBestExp();

      // Capture index creation plan for non-root pipelines.
      // The creation plan = entire pipeline execution, producing output
      // rows for the parent MI. Found below the parent's boundary Sort.
      boolean isRoot = (i == pipelines.size() - 1);
      if (!isRoot) {
        RelNode creationRoot = MergedIndexTestUtil.findCreationPlanRoot(
            currentPlan, p.mergedIndex);
        if (creationRoot != null) {
          p.mergedIndex.setIndexCreationPlan(creationRoot);
        }
      }
    }
    final RelNode phase2Plan = currentPlan;

    final String afterStr = dumpText(phase2Plan);
    System.out.println("=== Q3 OL AFTER (merged index plan) ===");
    System.out.println(afterStr);
    writeDotFile("q3ol/root-pipeline-query-plan", phase2Plan);

    // ── Index creation plans ─────────────────────────────────────────────
    System.out.println("=== Q3-OL INDEX CREATION PLANS ===");
    // Write index creation plans for non-root pipelines only.
    // pipelines.get(pipelines.size()-1) is the root query pipeline — its output
    // IS the query result, not an intermediate MI entry. No parent MI to populate.
    for (int i = 0; i < pipelines.size() - 1; i++) {
      String level = (i == 0) ? "leaf" : "branch";
      Pipeline p = pipelines.get(i);
      RelNode cp = p.mergedIndex.getIndexCreationPlan();
      System.out.println("-- " + level + " level " + i
          + ": reads from " + p.mergedIndex + " → populates parent MI");
      System.out.println(cp != null ? dumpText(cp) : "(none)");
      if (cp != null) {
        // Number from root: smaller = closer to root
        int fromRoot = pipelines.size() - 1 - i;
        writeDotFile("q3ol/" + level + "-" + fromRoot + "-index-creation-plan", cp);
      }
    }

    // ── Assert ────────────────────────────────────────────────────────────
    // Per-source MI scan architecture: MergeJoins stay, boundary Sorts replaced.
    // Leaf pipeline: 2 MIScans (LINEITEM, ORDERS on orderkey).
    // Root pipeline: 2 MIScans (leaf_view, CUSTOMER on custkey).
    assertThat(afterStr, containsString("EnumerableMergedIndexScan"));
    // No EnumerableMergedIndexJoin (obsolete under per-source architecture)
    assertThat(afterStr, not(containsString("EnumerableMergedIndexJoin")));
    // MergeJoins stay in the plan (outer custkey join + inner orderkey join)
    assertThat(afterStr, containsString("EnumerableMergeJoin"));
    // Exactly 2 MIScans in the final plan: one per source in the outer pipeline
    // (inner_view + CUSTOMER). The inner pipeline's MIScans are absorbed into the
    // outer MIScan's view([0]) reference — not visible as separate nodes in the root plan.
    assertThat("Q3-OL should have exactly 2 MIScans in the final plan",
        MergedIndexTestUtil.countOccurrences(afterStr, "EnumerableMergedIndexScan"), is(2));
    // No base TableScans remain (all absorbed into MI scans)
    assertThat(afterStr, not(containsString("EnumerableTableScan")));
    // All pipelines have maintenance plans.
    // Inner pipeline (leaf): exactly 2 LogicalDelta branches (LINEITEM + ORDERS).
    // Outer pipeline (root): scoped to its own operators only — inner view
    //   replaced by LogicalValues placeholder. Exactly 2 LogicalDelta leaves
    //   (inner_view_placeholder + CUSTOMER). No inner pipeline operators
    //   (no LogicalAggregate). Exactly 2 LogicalJoin nodes.
    for (Pipeline p : pipelines) {
      assertThat("Q3-OL pipeline missing maintenance plan",
          p.mergedIndex.getMaintenancePlan() != null, is(true));
      final String m = dumpText(p.mergedIndex.getMaintenancePlan());
      assertThat(MergedIndexTestUtil.countOccurrences(m, "LogicalDelta"),
          greaterThanOrEqualTo(2));
    }
    // Outer maintenance plan scoping assertions
    final String outerMaint = dumpText(pipelines.get(1).mergedIndex.getMaintenancePlan());
    // Inner view becomes a LogicalValues placeholder (ChildViewOutput)
    assertThat(outerMaint, containsString("LogicalValues"));
    // Exactly 2 LogicalDelta leaves: one per delta branch (inner_view + CUSTOMER)
    assertThat(MergedIndexTestUtil.countOccurrences(outerMaint, "LogicalDelta"), is(2));
    // Exactly 2 LogicalJoin nodes: one per union branch (outer join only)
    assertThat(MergedIndexTestUtil.countOccurrences(outerMaint, "LogicalJoin"), is(2));
    // No LogicalAggregate: inner pipeline's operator, scoped out
    assertThat(outerMaint, not(containsString("LogicalAggregate")));
    // Non-root pipelines have index creation plans
    for (int i = 0; i < pipelines.size() - 1; i++) {
      RelNode cp = pipelines.get(i).mergedIndex.getIndexCreationPlan();
      assertThat("Q3-OL creation plan should exist for level " + i,
          cp != null, is(true));
      String cpStr = dumpText(cp);
      assertThat(cpStr, containsString("EnumerableMergedIndexScan"));
      // No boundary sorts within the creation plan (they were replaced)
      assertThat(MergedIndexTestUtil.countOccurrences(cpStr,
          "EnumerableSort("), is(0));
    }

    // ── Verify DeltaToMergedIndexDeltaScanRule ────────────────────────────
    // Construct a synthetic LogicalDelta(EnumerableMergedIndexScan) and verify
    // the rule converts it to EnumerableMergedIndexDeltaScan.
    final MergedIndex innerMi = pipelines.get(0).mergedIndex;
    final EnumerableMergedIndexScan innerScan =
        EnumerableMergedIndexScan.create(
            phase2Plan.getCluster(), innerMi, 0,
            new org.apache.calcite.materialize.MergedIndexScanGroup(innerMi));
    final RelNode deltaOfScan = LogicalDelta.create(innerScan);
    final HepProgram deltaProgram = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_DELTA_TO_MERGED_INDEX_DELTA_SCAN_RULE)
        .build();
    final HepPlanner deltaPlanner = new HepPlanner(deltaProgram);
    deltaPlanner.setRoot(deltaOfScan);
    final RelNode resolvedDeltaPlan = deltaPlanner.findBestExp();
    assertThat(dumpText(resolvedDeltaPlan), containsString("EnumerableMergedIndexDeltaScan"));
  }

  /**
   * TPC-H Q9 (no color filter on part name): 6-table join
   * ORDERS ⋈ LINEITEM ⋈ PART ⋈ PARTSUPP ⋈ SUPPLIER ⋈ NATION.
   *
   * <p>Demonstrates <em>full</em> 6-table pipeline substitution across five
   * nested merged indexes registered bottom-up:
   * <ol>
   *   <li>OL: ORDERS ⋈ LINEITEM on {@code o_orderkey = l_orderkey}
   *   <li>OLP: view(OL) ⋈ PART on {@code l_partkey = p_partkey}
   *   <li>OLPS: view(OLP) ⋈ PARTSUPP on {@code (l_partkey, l_suppkey) = (ps_partkey, ps_suppkey)}
   *   <li>OLPPS: view(OLPS) ⋈ SUPPLIER on {@code l_suppkey = s_suppkey}
   *   <li>OLPPSS+NATION: view(OLPPS) ⋈ NATION on {@code s_nationkey = n_nationkey}
   * </ol>
   *
   * <p>Five HEP passes are applied: each pass fires {@code PipelineToMergedIndexScanRule}
   * once, bottom-up, until all intermediate merge joins are eliminated.
   * {@code EnumerableFilter(p_name LIKE '%green%')} remains in the query-time plan
   * because the PART filter cannot be pushed below the assembled join result.
   *
   * <h3>Expected BEFORE structure (after sort-direction fix)</h3>
   * <pre>
   *   EnumerableAggregate(n_name, o_year)
   *     EnumerableSort(n_name ASC, o_year DESC)   ← GROUP BY (boundary, direction fixed)
   *       EnumerableProject
   *         EnumerableFilter(p_name LIKE '%green%')
   *           EnumerableMergeJoin(s_nationkey = n_nationkey)
   *             EnumerableSort(s_nationkey) → ... 4 nested MergeJoins ...
   *             EnumerableSort(s_nationkey) → Scan(NATION)
   * </pre>
   *
   * <p>The ORDER BY Sort is dropped by {@code propagateOrderByDirection} because
   * after propagating {@code o_year DESC} to the GROUP BY sort, the ORDER BY
   * becomes redundant — the Aggregate output is already in (n_name ASC, o_year DESC)
   * order. Removing it reduces pipeline count by one.
   *
   * <h3>Expected AFTER structure (indexed view)</h3>
   * <pre>
   *   EnumerableAggregate(n_name, o_year)          ← stays in query plan
   *     EnumerableMergedIndexScan(ivMI)             ← single scan, joins collapsed
   * </pre>
   *
   * <p>Six pipelines are discovered: 5 join pipelines (nested bottom-up) plus
   * 1 indexed view on (n_name ASC, o_year DESC) from the GROUP BY Sort boundary.
   * The Aggregate remains in the query-time plan above the MIScan.
   *
   * <p>Full DOT diagrams for BEFORE/AFTER are in {@code test-dot-output/q9/}.
   */
  @Test void tpchQ9() throws Exception {
    // Use explicit JOIN ... ON ... syntax so all join conditions are equi-joins
    // and injectSortsBeforeSortBasedOps can extract keys via splitJoinCondition.
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
    // Rewrite SQL with explicit join order to maximize interesting-ordering pipeline reuse:
    // ORDERS ⋈ LINEITEM (orderkey) → PART (partkey, filter early) → PARTSUPP (partkey,suppkey)
    // → SUPPLIER (suppkey prefix reused) → NATION (nationkey).
    // This left-deep tree lets injectSortsBeforeSortBasedOps inject the minimum number of sorts.
    final String sql = "SELECT n.n_name AS nation,"
        + " EXTRACT(YEAR FROM o.o_orderdate) AS o_year,"
        + " SUM(l.l_extendedprice * (1 - l.l_discount)"
        + "   - ps.ps_supplycost * l.l_quantity) AS sum_profit"
        + " FROM tpch.orders o"
        + " JOIN tpch.lineitem l ON o.o_orderkey = l.l_orderkey"
        + " JOIN tpch.part p ON p.p_partkey = l.l_partkey"
        + " JOIN tpch.partsupp ps ON ps.ps_partkey = l.l_partkey"
        + "   AND ps.ps_suppkey = l.l_suppkey"
        + " JOIN tpch.supplier s ON s.s_suppkey = l.l_suppkey"
        + " JOIN tpch.nation n ON s.s_nationkey = n.n_nationkey"
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

    final RelNode injected =
        MergedIndexTestUtil.injectSortsBeforeSortBasedOps(root.rel);
    // Propagate ORDER BY (n_name ASC, o_year DESC) direction to the
    // pre-aggregate GROUP BY sort (n_name ASC, o_year ASC → DESC),
    // then drop the now-redundant ORDER BY sort.
    final RelNode logicalWithSorts = propagateOrderByDirection(injected);

    // Strip the ORDER BY collation from desired traits: propagateOrderByDirection
    // already removed the ORDER BY Sort node, so Volcano should not require that
    // collation on the root output. Only EnumerableConvention is needed.
    final RelTraitSet desiredTraits =
        root.rel.getTraitSet()
            .replace(EnumerableConvention.INSTANCE)
            .replace(RelCollations.EMPTY);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        planner.transform(0, desiredTraits, logicalWithSorts);

    final String beforeStr = dumpText(phase1Plan);
    System.out.println("=== Q9 BEFORE (order-based pipeline) ===");
    System.out.println(beforeStr);
    writeDotFile("q9/before-pipeline", phase1Plan);

    // Discover all 5 join pipelines bottom-up (inner first) and register nested MergedIndexes.
    final Pipeline rootPipeline = Pipeline.buildTree(phase1Plan);
    final List<Pipeline> pipelines = rootPipeline.flatten();

    // Capture logical subtrees for IVM — logical nodes have SQL row types
    // compatible with StreamRules; physical EnumerableMergeJoin uses JavaType.
    Pipeline.captureLogicalRoots(rootPipeline, logicalWithSorts);
    assertThat("Expected 6 pipelines for Q9 (5 join + 1 indexed view)",
        pipelines.size(), is(6));

    // Separate join pipelines (>= 2 sources) from indexed views (1 source)
    final List<Pipeline> joinPipelinesQ9 = pipelines.stream()
        .filter(p -> p.sources.size() >= 2)
        .collect(Collectors.toList());
    final List<Pipeline> indexedViewsQ9 = pipelines.stream()
        .filter(p -> p.sources.size() == 1)
        .collect(Collectors.toList());
    assertThat("Expected 5 join pipelines", joinPipelinesQ9.size(), is(5));
    assertThat("Expected 1 indexed view", indexedViewsQ9.size(), is(1));

    // ── Verify assembly subtrees (join pipelines only) ────────────────────
    for (int i = 0; i < joinPipelinesQ9.size(); i++) {
      final Pipeline.AssemblySubtree asm = joinPipelinesQ9.get(i).findAssemblySubtree();
      assertThat("Q9 pipeline " + i + " assembly subtree should exist",
          asm != null, is(true));
      assertThat("Q9 pipeline " + i + " LCA should be MergeJoin",
          asm.lca, instanceOf(EnumerableMergeJoin.class));
      assertThat("Q9 pipeline " + i + " assembly should contain only MergeJoin",
          asm.nodes, hasSize(1));
      assertThat("Q9 pipeline " + i + " should have 2 boundary sorts",
          asm.boundarySorts, hasSize(2));
    }

    // ── Create MergedIndexes (bottom-up) and set maintenance plans ────────
    // Join pipelines: 2 delta branches each (one per source)
    for (Pipeline p : joinPipelinesQ9) {
      new MergedIndex(p);
    }
    // Indexed views: single-source
    for (Pipeline iv : indexedViewsQ9) {
      new MergedIndex(iv);
    }
    // Set maintenance plans for all pipelines with captured logical roots
    for (Pipeline p : pipelines) {
      if (p.logicalRoot != null) {
        p.mergedIndex.setMaintenancePlan(deriveMaintenancePlan(p));
      }
    }

    printMaintenancePlans("Q9", joinPipelinesQ9);
    for (int i = 0; i < joinPipelinesQ9.size(); i++) {
      final RelNode mp = joinPipelinesQ9.get(i).mergedIndex.getMaintenancePlan();
      if (mp != null) {
        writeDotFile("q9/maintenance-" + i, mp);
        writeDotFileTree("q9/maintenance-" + i + "-tree", mp);
      }
    }

    // ── Phase 2: incremental MI registration with multi-stage HEP ────────
    // Each pass registers ONE level's MI and replaces that level's boundary
    // Sorts with per-source MIScans. Registering incrementally ensures each
    // pass only matches the current level's Sorts (deeper Sorts were already
    // replaced in prior passes). Leaf pipeline first, root last.
    final HepProgram hepPass = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
        .build();
    RelNode current = phase1Plan;
    for (int i = 0; i < pipelines.size(); i++) {
      Pipeline p = pipelines.get(i);
      MergedIndexRegistry.register(p.mergedIndex);
      final HepPlanner hp = new HepPlanner(hepPass);
      hp.setRoot(current);
      current = hp.findBestExp();

      // Capture index creation plan for non-root pipelines.
      // The creation plan = entire pipeline execution, producing output
      // rows for the parent MI. Found below the parent's boundary Sort.
      boolean isRoot = (i == pipelines.size() - 1);
      if (!isRoot) {
        RelNode creationRoot = MergedIndexTestUtil.findCreationPlanRoot(
            current, p.mergedIndex);
        if (creationRoot != null) {
          p.mergedIndex.setIndexCreationPlan(creationRoot);
        }
      }
    }
    final RelNode phase2Plan = current;

    final String afterStr = dumpText(phase2Plan);
    System.out.println("=== Q9 AFTER (merged index plan) ===");
    System.out.println(afterStr);
    writeDotFile("q9/root-pipeline-query-plan", phase2Plan);

    // ── Index creation plans ─────────────────────────────────────────────
    System.out.println("=== Q9 INDEX CREATION PLANS ===");
    for (int i = 0; i < pipelines.size() - 1; i++) {
      String level = (i == 0) ? "leaf" : "branch";
      Pipeline p = pipelines.get(i);
      RelNode cp = p.mergedIndex.getIndexCreationPlan();
      System.out.println("-- " + level + " level " + i
          + ": reads from " + p.mergedIndex + " → populates parent MI");
      System.out.println(cp != null ? dumpText(cp) : "(none)");
      if (cp != null) {
        // Number from root: smaller = closer to root
        int fromRoot = pipelines.size() - 1 - i;
        writeDotFile("q9/" + level + "-" + fromRoot + "-index-creation-plan", cp);
      }
    }

    // ── Assert ────────────────────────────────────────────────────────────
    // With the ORDER BY sort removed (redundant after direction propagation),
    // there is exactly 1 indexed view pipeline: the GROUP BY Sort(n_name ASC,
    // o_year DESC) boundary is replaced by a MIScan absorbing the join+filter
    // subtree. The Aggregate sits above the MIScan in the query-time plan.
    // AFTER structure:
    //   EnumerableAggregate(n_name, o_year)
    //     EnumerableMergedIndexScan(ivMI)   ← single scan, joins collapsed
    assertThat(afterStr, containsString("EnumerableMergedIndexScan"));
    assertThat(afterStr, not(containsString("EnumerableMergeJoin")));
    assertThat(afterStr, not(containsString("EnumerableMergedIndexJoin")));
    assertThat(afterStr, not(containsString("EnumerableSort")));
    // Aggregate remains in the query-time plan (above the MIScan boundary)
    assertThat(afterStr, containsString("EnumerableAggregate"));
    assertThat(MergedIndexTestUtil.countOccurrences(afterStr,
        "EnumerableMergedIndexScan"), is(1));
    // Join pipelines have maintenance plans.
    // Leaf pipeline: exactly 2 LogicalDelta branches (one per direct join input).
    // Non-leaf pipelines: >= 2 LogicalDelta branches — the full subtree includes
    // nested join inputs, so Delta propagates to all leaf table scans.
    for (Pipeline p : joinPipelinesQ9) {
      assertThat("Q9 pipeline missing maintenance plan",
          p.mergedIndex.getMaintenancePlan() != null, is(true));
      final String m = dumpText(p.mergedIndex.getMaintenancePlan());
      assertThat(MergedIndexTestUtil.countOccurrences(m, "LogicalDelta"),
          greaterThanOrEqualTo(2));
    }
    // Non-root join pipelines have index creation plans
    for (int i = 0; i < joinPipelinesQ9.size() - 1; i++) {
      RelNode cp = joinPipelinesQ9.get(i).mergedIndex.getIndexCreationPlan();
      assertThat("Q9 creation plan should exist for level " + i,
          cp != null, is(true));
      String cpStr = dumpText(cp);
      assertThat(cpStr, containsString("EnumerableMergedIndexScan"));
      // No boundary sorts within the creation plan (they were replaced)
      assertThat(MergedIndexTestUtil.countOccurrences(cpStr,
          "EnumerableSort("), is(0));
    }
  }

  // ── IVM helpers ──────────────────────────────────────────────────────────

  /**
   * If the root is a Sort (ORDER BY) whose input chain contains an Aggregate
   * with a Sort (GROUP BY) on the same key fields but different directions,
   * propagates the ORDER BY directions to the GROUP BY sort.
   *
   * <p>The ORDER BY Sort is <em>removed</em> from the plan after direction
   * propagation — the GROUP BY sort already produces output in the correct
   * order, so the ORDER BY is redundant. Removing it reduces the pipeline
   * count by one (one fewer boundary sort → one fewer indexed view pipeline).
   * The caller must strip the collation from {@code desiredTraits} before
   * passing to Volcano, since the root output no longer has a Sort node.
   *
   * <p>This fixes Q9: the GROUP BY sort {@code (n_name ASC, o_year ASC)}
   * becomes {@code (n_name ASC, o_year DESC)} matching the ORDER BY, and
   * the redundant ORDER BY sort is eliminated.
   *
   * @param node the plan root (potentially a Sort)
   * @return the plan with propagated directions, or unchanged if no match
   */
  private static RelNode propagateOrderByDirection(RelNode node) {
    if (!(node instanceof Sort)) {
      return node;
    }
    final Sort orderBy = (Sort) node;
    if (orderBy.fetch != null || orderBy.offset != null) {
      return node;
    }

    // Drill through single-input operators to find the Aggregate
    RelNode cur = orderBy.getInput();
    final List<RelNode> chain = new ArrayList<>();
    while (cur != null && !(cur instanceof Aggregate)
        && cur.getInputs().size() == 1) {
      chain.add(cur);
      cur = cur.getInputs().get(0);
    }
    if (!(cur instanceof Aggregate)) {
      return node;
    }

    final Aggregate agg = (Aggregate) cur;
    final RelNode aggInput = agg.getInput();
    if (!(aggInput instanceof Sort)) {
      return node;
    }

    final Sort groupBy = (Sort) aggInput;
    final List<RelFieldCollation> obFields =
        orderBy.getCollation().getFieldCollations();
    final List<RelFieldCollation> gbFields =
        groupBy.getCollation().getFieldCollations();
    if (obFields.size() != gbFields.size()) {
      return node;
    }

    // Check same key fields and whether directions differ
    boolean sameKeys = true;
    boolean sameDirections = true;
    for (int i = 0; i < obFields.size(); i++) {
      if (obFields.get(i).getFieldIndex() != gbFields.get(i).getFieldIndex()) {
        sameKeys = false;
        break;
      }
      if (obFields.get(i).getDirection() != gbFields.get(i).getDirection()) {
        sameDirections = false;
      }
    }
    if (!sameKeys || sameDirections) {
      return node; // fields don't match, or directions already match
    }

    // Build new GROUP BY collation with ORDER BY directions
    final List<RelFieldCollation> newGbFields = new ArrayList<>();
    for (int i = 0; i < gbFields.size(); i++) {
      newGbFields.add(new RelFieldCollation(
          gbFields.get(i).getFieldIndex(),
          obFields.get(i).getDirection()));
    }
    final RelNode newGroupBy = LogicalSort.create(
        groupBy.getInput(), RelCollations.of(newGbFields), null, null);
    RelNode result = agg.copy(agg.getTraitSet(), List.of(newGroupBy));

    // Rebuild intermediate nodes (between ORDER BY and Aggregate)
    for (int i = chain.size() - 1; i >= 0; i--) {
      final RelNode n = chain.get(i);
      result = n.copy(n.getTraitSet(), List.of(result));
    }
    // Drop the ORDER BY Sort: after direction propagation the GROUP BY sort
    // already produces output in (n_name ASC, o_year DESC) order, making the
    // ORDER BY redundant. Removing it reduces the pipeline count by one
    // (one fewer boundary sort → one fewer indexed view pipeline).
    return result;
  }

  /**
   * Derives the IVM maintenance plan for a pipeline.
   *
   * <p>Creates a scoped copy of the pipeline's logical subtree where non-leaf
   * child pipeline subtrees are replaced with {@link LogicalValues#createEmpty}
   * placeholders. This ensures Delta only propagates through THIS pipeline's
   * operators, not child pipeline operators. The original {@code p.logicalRoot}
   * stays immutable — just as the physical pipeline uses {@code s.root} for
   * each {@code p.sources} to define scope without modifying the physical tree.
   *
   * <p>Wraps the scoped subtree in {@link LogicalDelta} and applies
   * {@link StreamRules} via HEP to push Delta down through all operators:
   * <pre>
   *   Delta(A &#x22CA; B)          &#x2192; (A &#x22CA; Delta(B)) &#x222A; (Delta(A) &#x22CA; B)
   *   Delta(Project(X))      &#x2192; Project(Delta(X))
   *   Delta(Aggregate(X))    &#x2192; Aggregate(Delta(X))
   *   Delta(Filter(X))       &#x2192; Filter(Delta(X))
   *   Delta(Sort(X))         &#x2192; Sort(Delta(X))
   * </pre>
   *
   * <p>{@code LogicalDelta(TableScan)} at leaves represents the change stream.
   * DeltaTableScanRule and DeltaTableScanToEmptyRule are excluded so that
   * Delta markers remain on leaf scans for consumer use.
   *
   * <h3>DAG shape</h3>
   *
   * <p>The returned plan may be a DAG (not a tree): {@code DeltaJoinTransposeRule}
   * reuses the same {@link RelNode} references in both union branches, so base
   * table scans can have multiple parents. This is standard Calcite behavior and
   * is correct — use {@code writeDotFileTree} for guaranteed tree-shaped DOT
   * output (per-visit IDs; no identity dedup).
   *
   * <h3>Batch-delta semantics</h3>
   *
   * <p>The IVM join formula {@code &#x394;(A &#x22CA; B) = (&#x394;A &#x22CA; B) &#x222A; (A &#x22CA; &#x394;B)} is exact
   * for single-tuple deltas (one insert at a time) but double-counts
   * {@code &#x394;A &#x22CA; &#x394;B} for simultaneous batch deltas to both sides. Production
   * systems resolve this via ordered application: process &#x394;A against old B first,
   * then &#x394;B against new A (= A &#x222A; &#x394;A). This ordering is an execution-tier concern
   * outside Calcite's plan-generation scope; the plan structure is correct
   * regardless.
   *
   * @param pipeline the pipeline whose maintenance plan to derive
   */
  private static RelNode deriveMaintenancePlan(Pipeline pipeline) {
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
   * Creates a scoped copy of a pipeline's logicalRoot, replacing non-leaf
   * child pipeline subtrees with {@link LogicalValues#createEmpty} placeholders.
   *
   * <p>A "non-leaf child" is a child pipeline with its own sources
   * ({@code child.sources.isEmpty() == false}) — it represents a merged index
   * view whose maintenance is handled by its own maintenance plan. Leaf children
   * (base table scans) are left intact so Delta propagates to their scans.
   *
   * <p>Identification: a child's logicalRoot was set from a boundary sort's
   * input in {@link Pipeline#captureLogicalRoots}. This method finds boundary
   * sorts whose {@code .getInput()} identity-matches a non-leaf child's
   * logicalRoot and replaces them with empty Values placeholders.
   *
   * @param pipeline the pipeline to scope
   * @return scoped copy (or original if no non-leaf children)
   */
  private static RelNode scopeLogicalRoot(Pipeline pipeline) {
    if (pipeline.logicalRoot == null) {
      throw new IllegalArgumentException("Pipeline has no logicalRoot");
    }
    if (pipeline.sources.isEmpty()) {
      return pipeline.logicalRoot; // leaf pipeline, no scoping needed
    }

    // Collect logicalRoots of non-leaf children (merged index views)
    final Set<RelNode> childLogicalRoots =
        Collections.newSetFromMap(new IdentityHashMap<>());
    for (Pipeline child : pipeline.sources) {
      if (child.logicalRoot != null && !child.sources.isEmpty()) {
        childLogicalRoots.add(child.logicalRoot);
      }
    }

    if (childLogicalRoots.isEmpty()) {
      return pipeline.logicalRoot; // all children are leaves
    }

    return replaceChildBoundaries(pipeline.logicalRoot, childLogicalRoots);
  }

  /**
   * Walks a RelNode tree and replaces boundary sorts whose input
   * identity-matches a child logicalRoot with a {@link LogicalValues} placeholder.
   */
  private static RelNode replaceChildBoundaries(RelNode node,
      Set<RelNode> childLogicalRoots) {
    if (Pipeline.isLogicalBoundarySort(node)) {
      final Sort sort = (Sort) node;
      if (childLogicalRoots.contains(sort.getInput())) {
        return LogicalValues.createEmpty(
            sort.getCluster(), sort.getRowType());
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

  /** Prints maintenance plans for all pipelines to stdout. */
  private static void printMaintenancePlans(String label, List<Pipeline> pipelines) {
    System.out.println("=== " + label + " MAINTENANCE PLANS ===");
    for (int i = 0; i < pipelines.size(); i++) {
      System.out.println("-- Level " + i + ": " + pipelines.get(i).mergedIndex);
      final RelNode plan = pipelines.get(i).mergedIndex.getMaintenancePlan();
      System.out.println(plan != null ? dumpText(plan) : "  (none)");
    }
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

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
    final java.nio.file.Path file = java.nio.file.Paths.get("test-dot-output", filename);
    try {
      java.nio.file.Files.createDirectories(file.getParent());
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
      final String edgeLabel = inputs.size() > 1 ? (i == 0 ? "left" : "right") : "";
      sb.append("  n").append(childId).append(" -> n").append(id);
      if (!edgeLabel.isEmpty()) {
        sb.append(" [label=\"").append(edgeLabel).append("\"]");
      }
      sb.append(";\n");
    }
    return id;
  }

  /**
   * Renders a RelNode graph as a DOT string in tree mode (no dedup).
   * Each visit to a shared node produces a separate DOT node,
   * ensuring the output is always a tree even if the input is a DAG.
   *
   * <p>Motivation: {@code LogicalTableScan.copy()} returns {@code this},
   * so identity-based dedup in {@link #dumpDotColor} cannot be fixed by
   * copying leaf nodes. Per-visit IDs bypass the problem entirely.
   */
  private static String dumpDotTree(RelNode rel) {
    final StringBuilder sb = new StringBuilder("digraph {\n  rankdir=BT;\n");
    final int[] counter = {0};
    dumpDotTreeNode(rel, sb, counter);
    sb.append("}\n");
    return sb.toString();
  }

  /** Recursively renders nodes in tree mode — no identity dedup.
   * Returns the node ID assigned to this visit. */
  private static int dumpDotTreeNode(RelNode node, StringBuilder sb,
      int[] counter) {
    final int id = counter[0]++;
    final String explain = RelOptUtil.dumpPlan("", node,
        SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES).trim();
    final String label = explain.lines().findFirst()
        .orElse(node.getClass().getSimpleName()).trim().replace("\"", "'");
    sb.append("  n").append(id).append(" [label=\"").append(label)
        .append("\\n#").append(id).append("\"];\n");
    final List<RelNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      final int childId = dumpDotTreeNode(inputs.get(i), sb, counter);
      sb.append("  n").append(childId).append(" -> n").append(id).append(";\n");
    }
    return id;
  }

  /**
   * Renders a RelNode graph as a colorful DOT string in tree mode (no dedup).
   * Each visit to a shared node produces a separate DOT node,
   * ensuring the output is always a tree even if the input is a DAG.
   */
  private static String dumpDotColorTree(RelNode rel) {
    final StringBuilder sb = new StringBuilder(
        "digraph {\n  rankdir=BT;\n  node [fontname=\"Helvetica\"];\n");
    final int[] counter = {0};
    dumpDotColorTreeNode(rel, sb, counter);
    sb.append("}\n");
    return sb.toString();
  }

  /** Recursively renders nodes in tree mode with colors — no identity dedup.
   * Returns the node ID assigned to this visit. */
  private static int dumpDotColorTreeNode(RelNode node, StringBuilder sb,
      int[] counter) {
    final int id = counter[0]++;
    final String explain = RelOptUtil.dumpPlan("", node,
        SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES).trim();
    final String firstLine = explain.lines().findFirst()
        .orElse(node.getClass().getSimpleName()).trim();
    final String label = nodeLabel(node, firstLine);
    final String color = nodeColor(node);
    sb.append("  n").append(id)
        .append(" [label=\"").append(label).append("\\n#").append(id).append("\"")
        .append(", style=filled, fillcolor=\"").append(color).append("\"")
        .append("];\n");
    final List<RelNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      final int childId = dumpDotColorTreeNode(inputs.get(i), sb, counter);
      final String edgeLabel = inputs.size() > 1 ? (i == 0 ? "left" : "right") : "";
      sb.append("  n").append(childId).append(" -> n").append(id);
      if (!edgeLabel.isEmpty()) {
        sb.append(" [label=\"").append(edgeLabel).append("\"]");
      }
      sb.append(";\n");
    }
    return id;
  }

  /**
   * Writes tree-mode DOT files (no identity dedup) for {@code rel}.
   * Unlike {@link #writeDotFile}, this never merges shared nodes —
   * each traversal visit gets a fresh DOT node ID.
   */
  private static void writeDotFileTree(String name, RelNode rel) {
    writeDotToFile(name + ".dot", dumpDotTree(rel));
    writeDotToFile(name + "_color.dot", dumpDotColorTree(rel));
  }

  /**
   * Returns a short, presentation-friendly label for a colorful DOT node.
   *
   * <p>Strips the {@code Enumerable} prefix. Resolves {@code $N} field-index
   * references to actual column names using the node's input row type.
   * Drops internal attributes like {@code sort0=}, {@code dir0=}, {@code joinType=}.
   */
  private static String nodeLabel(RelNode node, String firstLine) {
    // Empty LogicalValues placeholders inserted by replaceChildBoundaries
    // represent the output of a child pipeline's merged index view.
    if (node instanceof LogicalValues && ((LogicalValues) node).getTuples().isEmpty()) {
      return "ChildViewOutput";
    }
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
    // Physical operators (query/creation plans)
    if (cls.contains("MergedIndexScan")) return "#90EE90"; // light green
    if (cls.contains("MergedIndexJoin")) return "#32CD32"; // lime green
    if (cls.contains("MergeJoin"))       return "#FFD700"; // gold
    // Logical operators (maintenance plans)
    if (cls.contains("Delta"))           return "#FF6347"; // tomato — change stream
    if (cls.contains("Union"))           return "#FFFFE0"; // light yellow — set union
    if (cls.contains("Join"))            return "#FFD700"; // gold — same concept as MergeJoin
    if (cls.contains("Values"))          return "#98FB98"; // pale green — child view output
    // Shared operators
    if (cls.contains("LimitSort"))       return "#FFA07A"; // light salmon
    if (cls.contains("Sort"))            return "#FFA07A"; // light salmon
    if (cls.contains("TableScan"))       return "#ADD8E6"; // light blue
    if (cls.contains("Aggregate"))       return "#DDA0DD"; // plum
    if (cls.contains("Project"))         return "#D3D3D3"; // light gray
    if (cls.contains("Filter"))          return "#FFDAB9"; // peach
    return "white";
  }
}
