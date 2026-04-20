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
import org.apache.calcite.adapter.enumerable.EnumerableMergedIndexDeltaScan;
import org.apache.calcite.adapter.enumerable.EnumerableMergedIndexScan;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.materialize.MaintenancePlanConverter;
import org.apache.calcite.materialize.MergedIndex;
import org.apache.calcite.materialize.MergedIndexRegistry;
import org.apache.calcite.materialize.Pipeline;
import org.apache.calcite.materialize.TaggedRowSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.stream.LogicalDelta;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.MergedIndexTestUtil;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSets;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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

    final RelNode injected =
        MergedIndexTestUtil.injectSortsBeforeSortBasedOps(root.rel);
    // Hoist filters on the logical plan so maintenance plans are filter-free.
    final RelNode logicalWithSorts =
        MergedIndexTestUtil.hoistFiltersAboveBoundaries(injected);

    final RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        MergedIndexTestUtil.splitLimitSorts(
            planner.transform(0, desiredTraits, logicalWithSorts));

    // ── Discover pipelines (before writing DOT so clusters can be annotated) ─
    final Pipeline rootPipeline = Pipeline.buildTree(phase1Plan);
    final List<Pipeline> pipelines = rootPipeline.flatten();

    System.out.println("=== Q12 BEFORE (order-based pipeline) ===");
    System.out.println(dumpText(phase1Plan));
    writeDotFile("q12/before-pipeline", phase1Plan, rootPipeline);
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
      if (p != rootPipeline && p.logicalRoot != null) {
        p.mergedIndex.setMaintenancePlan(MaintenancePlanConverter.deriveMaintenancePlan(p));
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

    // ── Convert logical maintenance plan to physical ────────
    System.out.println("=== Q12 PHYSICAL MAINTENANCE PLAN ===");
    final RelNode physicalMaintQ12 = MaintenancePlanConverter.convertToPhysical(
        joinPipeline.mergedIndex.getMaintenancePlan());
    System.out.println(dumpText(physicalMaintQ12));
    writeDotFile("q12/physical-maintenance", physicalMaintQ12);

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

    final RelNode injected =
        MergedIndexTestUtil.injectSortsBeforeSortBasedOps(root.rel);
    // Hoist filters on the logical plan so maintenance plans are filter-free.
    final RelNode logicalWithSorts =
        MergedIndexTestUtil.hoistFiltersAboveBoundaries(injected);

    final RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        MergedIndexTestUtil.splitLimitSorts(
            planner.transform(0, desiredTraits, logicalWithSorts));

    // ── Discover all interesting-ordering pipelines (bottom-up) ──────────
    // buildPipelineTree walks top-down, cutting at Sort boundaries.
    // flattenPipelines returns non-trivial pipelines in post-order
    // (inner first) so inner pipeline is registered before outer.
    final Pipeline rootPipeline = Pipeline.buildTree(phase1Plan);
    final List<Pipeline> pipelines = rootPipeline.flatten();

    final String beforeStr = dumpText(phase1Plan);
    System.out.println("=== Q3 OL BEFORE (order-based pipeline) ===");
    System.out.println(beforeStr);
    writeDotFile("q3ol/before-pipeline", phase1Plan, rootPipeline);

    // Capture logical subtrees for IVM — logical nodes have SQL row types
    // compatible with StreamRules; physical EnumerableMergeJoin uses JavaType.
    Pipeline.captureLogicalRoots(rootPipeline, logicalWithSorts);
    // After splitLimitSorts, the top-level EnumerableLimitSort becomes
    // EnumerableLimit(EnumerableSort(revenue DESC, orderdate ASC)), so the
    // plain Sort is a third pipeline boundary on top of the outer custkey pipeline.
    assertThat("Expected 3 pipelines (inner orderkey + outer custkey + top ORDER BY sort)",
        pipelines.size(), is(3));

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
      if (p != rootPipeline && p.logicalRoot != null) {
        p.mergedIndex.setMaintenancePlan(MaintenancePlanConverter.deriveMaintenancePlan(p));
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

    // ── Convert logical maintenance plans to physical ────────
    System.out.println("=== Q3-OL PHYSICAL MAINTENANCE PLANS ===");
    for (int i = 0; i < pipelines.size(); i++) {
      final RelNode mp = pipelines.get(i).mergedIndex != null
          ? pipelines.get(i).mergedIndex.getMaintenancePlan() : null;
      if (mp != null) {
        System.out.println("--- Pipeline " + i + " physical maintenance ---");
        final RelNode physicalMaint = MaintenancePlanConverter.convertToPhysical(mp);
        System.out.println(dumpText(physicalMaint));
        writeDotFile("q3ol/physical-maintenance-" + i, physicalMaint);
      }
    }

    // ── Assert ────────────────────────────────────────────────────────────
    // Per-source MI scan architecture: MergeJoins stay, boundary Sorts replaced.
    // Leaf pipeline: 2 MIScans (LINEITEM, ORDERS on orderkey).
    // Root pipeline: 2 MIScans (leaf_view, CUSTOMER on custkey).
    assertThat(afterStr, containsString("EnumerableMergedIndexScan"));
    // No EnumerableMergedIndexJoin (obsolete under per-source architecture)
    assertThat(afterStr, not(containsString("EnumerableMergedIndexJoin")));
    // After splitLimitSorts, the top-level EnumerableLimitSort becomes
    // EnumerableLimit(EnumerableSort(...)), and that Sort is now a third pipeline
    // boundary. All three pipelines collapse to MIScans, so the final plan has:
    //   EnumerableLimit → EnumerableMergedIndexScan(top ORDER BY pipeline, 1 source)
    // The MergeJoins are all absorbed into nested MI views, leaving no join in the plan.
    assertThat(afterStr, not(containsString("EnumerableMergeJoin")));
    // EnumerableLimit stays at the top (FETCH=10 is preserved)
    assertThat(afterStr, containsString("EnumerableLimit"));
    // Exactly 1 MIScan in the final plan: the top ORDER BY pipeline has one source
    // (the outer custkey view), which itself references the inner orderkey view.
    assertThat("Q3-OL should have exactly 1 MIScan in the final plan after full 3-level collapse",
        MergedIndexTestUtil.countOccurrences(afterStr, "EnumerableMergedIndexScan"), is(1));
    // No base TableScans remain (all absorbed into MI scans)
    assertThat(afterStr, not(containsString("EnumerableTableScan")));
    // Only non-root pipelines have maintenance plans.
    // Root pipeline (outer custkey join) is the final query result — not stored
    // in any parent merged index — so it does not need a maintenance plan.
    // Inner pipeline (leaf, orderkey): exactly 2 LogicalDelta branches (LINEITEM + ORDERS).
    for (Pipeline p : pipelines) {
      if (p == rootPipeline) {
        assertThat("Q3-OL root pipeline should NOT have a maintenance plan",
            p.mergedIndex.getMaintenancePlan(), nullValue());
      } else {
        assertThat("Q3-OL non-root pipeline missing maintenance plan",
            p.mergedIndex.getMaintenancePlan() != null, is(true));
        final String m = dumpText(p.mergedIndex.getMaintenancePlan());
        assertThat(MergedIndexTestUtil.countOccurrences(m, "LogicalDelta"),
            greaterThanOrEqualTo(2));
      }
    }
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
   * <h3>Expected BEFORE structure (after sort-direction fix and widen-then-narrow)</h3>
   * <pre>
   *   EnumerableAggregate(n_name, o_year)
   *     EnumerableFilter(condition=[LIKE($3, '%green%')])  ← hoisted above Sort
   *       EnumerableSort(n_name ASC, o_year DESC)   ← GROUP BY boundary sort
   *         EnumerableProject(NATION=[$47], O_YEAR=[EXTRACT(...)], $f2=[...], $f3=[$26])
   *           EnumerableMergeJoin(s_nationkey = n_nationkey)
   *             EnumerableSort(s_nationkey) → ... 4 nested MergeJoins ...
   *             EnumerableSort(s_nationkey) → Scan(NATION)
   * </pre>
   *
   * <p>The widen pre-pass in {@code hoistFiltersAboveBoundaries} appends
   * {@code $f3=[$26]} (p_name, field 26 of the wide join output) as a trailing
   * column to the {@code EnumerableProject}. This makes the Project commutable:
   * the filter condition {@code LIKE($26, '%green%')} is rewritten to
   * {@code LIKE($3, '%green%')} (output index of the appended column) and
   * hoisted above the Sort. A narrowing {@code EnumerableProject} at the root
   * restores the original 3-column row type.
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
    // Hoist filters on the logical plan so maintenance plans are filter-free.
    final RelNode logicalWithSorts =
        MergedIndexTestUtil.hoistFiltersAboveBoundaries(
            propagateOrderByDirection(injected));

    // Strip the ORDER BY collation from desired traits: propagateOrderByDirection
    // already removed the ORDER BY Sort node, so Volcano should not require that
    // collation on the root output. Only EnumerableConvention is needed.
    final RelTraitSet desiredTraits =
        root.rel.getTraitSet()
            .replace(EnumerableConvention.INSTANCE)
            .replace(RelCollations.EMPTY);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        MergedIndexTestUtil.splitLimitSorts(
            planner.transform(0, desiredTraits, logicalWithSorts));

    // Discover all 5 join pipelines bottom-up (inner first) and register nested MergedIndexes.
    final Pipeline rootPipeline = Pipeline.buildTree(phase1Plan);
    final List<Pipeline> pipelines = rootPipeline.flatten();

    final String beforeStr = dumpText(phase1Plan);
    System.out.println("=== Q9 BEFORE (order-based pipeline) ===");
    System.out.println(beforeStr);
    writeDotFile("q9/before-pipeline", phase1Plan, rootPipeline);

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
      if (p != rootPipeline && p.logicalRoot != null) {
        p.mergedIndex.setMaintenancePlan(MaintenancePlanConverter.deriveMaintenancePlan(p));
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

    // ── Convert logical maintenance plans to physical ────────
    System.out.println("=== Q9 PHYSICAL MAINTENANCE PLANS ===");
    for (int i = 0; i < joinPipelinesQ9.size(); i++) {
      final RelNode mp = joinPipelinesQ9.get(i).mergedIndex.getMaintenancePlan();
      if (mp != null) {
        System.out.println("--- Pipeline " + i + " physical maintenance ---");
        final RelNode physicalMaint = MaintenancePlanConverter.convertToPhysical(mp);
        System.out.println(dumpText(physicalMaint));
        writeDotFile("q9/physical-maintenance-" + i, physicalMaint);
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
    // Non-root join pipelines have maintenance plans.
    // Root pipeline is the final query result — not stored in any parent MI —
    // so it does not get a maintenance plan.
    // Leaf pipeline: exactly 2 LogicalDelta branches (one per direct join input).
    // Non-leaf pipelines: >= 2 LogicalDelta branches — the full subtree includes
    // nested join inputs, so Delta propagates to all leaf table scans.
    for (Pipeline p : joinPipelinesQ9) {
      if (p == rootPipeline) {
        assertThat("Q9 root pipeline should NOT have a maintenance plan",
            p.mergedIndex.getMaintenancePlan(), nullValue());
      } else {
        assertThat("Q9 pipeline missing maintenance plan",
            p.mergedIndex.getMaintenancePlan() != null, is(true));
        final String m = dumpText(p.mergedIndex.getMaintenancePlan());
        assertThat(MergedIndexTestUtil.countOccurrences(m, "LogicalDelta"),
            greaterThanOrEqualTo(2));
      }
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

  // ── Delegates to TpchPlanTestUtil ────────────────────────────────────────
  // All visualization and IVM helpers live in TpchPlanTestUtil so they can
  // be shared with MergedIndexSinglePipelineTpchPlanTest.

  private static RelNode propagateOrderByDirection(RelNode node) {
    return TpchPlanTestUtil.propagateOrderByDirection(node);
  }

  private static void writeDotFile(String name, RelNode rel) {
    TpchPlanTestUtil.writeDotFile(name, rel);
  }

  private static void writeDotFile(String name, RelNode rel,
      @org.checkerframework.checker.nullness.qual.Nullable Pipeline rootPipeline) {
    TpchPlanTestUtil.writeDotFile(name, rel, rootPipeline);
  }

  private static void writeDotFileTree(String name, RelNode rel) {
    TpchPlanTestUtil.writeDotFileTree(name, rel);
  }

  private static String dumpText(RelNode rel) {
    return TpchPlanTestUtil.dumpText(rel);
  }

  private static void printMaintenancePlans(String label, List<Pipeline> pipelines) {
    TpchPlanTestUtil.printMaintenancePlans(label, pipelines);
  }
}
