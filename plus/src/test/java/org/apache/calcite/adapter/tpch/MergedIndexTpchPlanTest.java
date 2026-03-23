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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.stream.LogicalDelta;
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
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
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

  @AfterEach
  void clearRegistry() {
    MergedIndexRegistry.clear();
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
    final String sql = "SELECT l.l_shipmode,"
        + " SUM(CASE WHEN o.o_orderpriority = '1-URGENT'"
        + "     THEN 1 ELSE 0 END) AS high_line_count,"
        + " SUM(CASE WHEN o.o_orderpriority <> '1-URGENT'"
        + "     THEN 1 ELSE 0 END) AS low_line_count"
        + " FROM tpch.orders o"
        + " JOIN tpch.lineitem l ON o.o_orderkey = l.l_orderkey"
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

    // Capture logical join for IVM — must use logical node (SQL types),
    // not the physical EnumerableMergeJoin (JavaType) from Phase 1.
    final Join logicalJoinQ12 =
        MergedIndexTestUtil.findAllJoins(logicalWithSorts).get(0);

    final RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        planner.transform(0, desiredTraits, logicalWithSorts);

    System.out.println("=== Q12 BEFORE (order-based pipeline) ===");
    System.out.println(dumpText(phase1Plan));
    writeDotFile("q12_before", phase1Plan);

    // ── Discover pipelines and register merged index ──────────────────────
    final Pipeline pipelineTree = MergedIndexTestUtil.buildPipelineTree(phase1Plan);
    final List<Pipeline> pipelines = MergedIndexTestUtil.flattenPipelines(pipelineTree).stream()
        .filter(p -> p.sources.size() >= 2)
        .collect(Collectors.toList());
    assertThat("Expected 1 join pipeline for Q12", pipelines.size(), is(1));

    // ── Verify assembly subtree ───────────────────────────────────────────
    final Pipeline.AssemblySubtree asmQ12 = pipelines.get(0).findAssemblySubtree();
    assertThat("Q12 assembly subtree should exist", asmQ12 != null, is(true));
    assertThat("Q12 LCA should be MergeJoin",
        asmQ12.lca, instanceOf(EnumerableMergeJoin.class));
    assertThat("Q12 assembly should contain only the MergeJoin",
        asmQ12.nodes, hasSize(1));
    assertThat("Q12 should have 2 boundary sorts",
        asmQ12.boundarySorts, hasSize(2));

    final Pipeline p = pipelines.get(0);
    new MergedIndex(p);
    p.mergedIndex.setMaintenancePlan(deriveIncrementalPlan(
        List.of(logicalJoinQ12.getLeft(), logicalJoinQ12.getRight())));
    MergedIndexRegistry.register(p.mergedIndex);

    System.out.println("=== Q12 MAINTENANCE PLAN (incremental) ===");
    System.out.println(dumpText(p.mergedIndex.getMaintenancePlan()));
    writeDotFile("q12_maintenance", p.mergedIndex.getMaintenancePlan());

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
    // Per-source MI scan: MergeJoin stays, boundary Sorts replaced by 2 MIScans
    final String planStr = dumpText(phase2Plan);
    assertThat(planStr, containsString("EnumerableMergedIndexScan"));
    assertThat(planStr, containsString("EnumerableMergeJoin"));
    // Boundary Sorts (inside MergeJoin) replaced; non-boundary Sorts
    // (GROUP BY on l_shipmode) may remain above the pipeline.
    assertThat(MergedIndexTestUtil.countOccurrences(planStr,
        "EnumerableMergedIndexScan"), is(2));
    // No TableScan remains (absorbed into MI scans)
    assertThat(planStr, not(containsString("EnumerableTableScan")));
    assertThat("Q12 MI missing maintenance plan",
        p.mergedIndex.getMaintenancePlan() != null, is(true));
    final String maintStr12 = dumpText(p.mergedIndex.getMaintenancePlan());
    assertThat(maintStr12, containsString("LogicalUnion"));
    // Each source inserts independently — no join needed in the maintenance plan.
    assertThat(MergedIndexTestUtil.countOccurrences(maintStr12, "LogicalJoin"), is(0));
    assertThat(MergedIndexTestUtil.countOccurrences(maintStr12, "LogicalDelta"), is(2));

    // ── TaggedRowSchema for Q12 (ORDERS ⋈ LINEITEM on orderkey) ──────────
    // Verify byte-width metadata for cost estimation and slot counts.
    final TaggedRowSchema schema = p.mergedIndex.getTaggedRowSchema();
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

    // Capture logical joins (post-order) for IVM — logical nodes have SQL row types
    // compatible with StreamRules; physical EnumerableMergeJoin uses JavaType.
    final List<Join> logicalJoinsOL =
        MergedIndexTestUtil.findAllJoins(logicalWithSorts);

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
    // buildPipelineTree walks top-down, cutting at Sort boundaries.
    // flattenPipelines returns non-trivial pipelines in post-order
    // (inner first) so inner pipeline is registered before outer.
    final Pipeline pipelineTree = MergedIndexTestUtil.buildPipelineTree(phase1Plan);
    final List<Pipeline> pipelines = MergedIndexTestUtil.flattenPipelines(pipelineTree).stream()
        .filter(p -> p.sources.size() >= 2)
        .collect(Collectors.toList());
    assertThat("Expected 2 join pipelines (inner orderkey + outer custkey)",
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
    for (int i = 0; i < pipelines.size(); i++) {
      final Pipeline p = pipelines.get(i);
      new MergedIndex(p);
      // Use logical join (SQL types) for IVM derivation — matches StreamRules expectations
      final Join ljOL = logicalJoinsOL.get(i);
      p.mergedIndex.setMaintenancePlan(deriveIncrementalPlan(
          List.of(ljOL.getLeft(), ljOL.getRight())));
    }

    printMaintenancePlans("Q3-OL", pipelines);
    for (int i = 0; i < pipelines.size(); i++) {
      writeDotFile("q3ol_maintenance_" + i, pipelines.get(i).mergedIndex.getMaintenancePlan());
    }

    // ── Phase 2: incremental MI registration with multi-stage HEP ────────
    // Each pass registers ONE level's MI and replaces that level's boundary
    // Sorts with per-source MIScans. Inner pipeline first, then outer.
    // After each pass, deeper Sorts are already replaced, so the rule only
    // matches the current level's Sorts.
    final HepProgram hepPass = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
        .build();

    RelNode currentPlan = phase1Plan;
    for (Pipeline p : pipelines) {
      MergedIndexRegistry.register(p.mergedIndex);
      final HepPlanner hp = new HepPlanner(hepPass);
      hp.setRoot(currentPlan);
      currentPlan = hp.findBestExp();
    }
    final RelNode phase2Plan = currentPlan;

    final String afterStr = dumpText(phase2Plan);
    System.out.println("=== Q3 OL AFTER (merged index plan) ===");
    System.out.println(afterStr);
    writeDotFile("q3ol_after", phase2Plan);

    // ── Assert ────────────────────────────────────────────────────────────
    // Per-source MI scan architecture: MergeJoins stay, boundary Sorts replaced.
    // Inner pipeline: 2 MIScans (LINEITEM, ORDERS on orderkey).
    // Outer pipeline: 2 MIScans (inner_view, CUSTOMER on custkey).
    assertThat(afterStr, containsString("EnumerableMergedIndexScan"));
    // No EnumerableMergedIndexJoin (obsolete under per-source architecture)
    assertThat(afterStr, not(containsString("EnumerableMergedIndexJoin")));
    // MergeJoins stay in the plan
    assertThat(afterStr, containsString("EnumerableMergeJoin"));
    // No base TableScans remain (all absorbed into MI scans)
    assertThat(afterStr, not(containsString("EnumerableTableScan")));
    // All pipelines have maintenance plans; each has exactly 2 LogicalDelta branches.
    for (Pipeline p : pipelines) {
      assertThat("Q3-OL pipeline missing maintenance plan",
          p.mergedIndex.getMaintenancePlan() != null, is(true));
      final String m = dumpText(p.mergedIndex.getMaintenancePlan());
      assertThat(MergedIndexTestUtil.countOccurrences(m, "LogicalDelta"), is(2));
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
   * <h3>Expected BEFORE structure (order-based pipeline)</h3>
   * <pre>
   *   EnumerableSort(n_name ASC, o_year DESC)
   *     EnumerableAggregate(n_name, o_year)
   *       EnumerableFilter(p_name LIKE '%green%')
   *         EnumerableMergeJoin(s_nationkey = n_nationkey)
   *           EnumerableSort(s_nationkey)
   *             EnumerableMergeJoin(l_suppkey = s_suppkey)
   *               EnumerableSort(l_suppkey)
   *                 EnumerableMergeJoin(l_partkey, l_suppkey = ps_partkey, ps_suppkey)
   *                   EnumerableSort(l_partkey, l_suppkey)
   *                     EnumerableMergeJoin(l_partkey = p_partkey)
   *                       EnumerableSort(l_partkey)
   *                         EnumerableMergeJoin(o_orderkey = l_orderkey)
   *                           EnumerableSort → Scan(ORDERS)
   *                           EnumerableSort → Scan(LINEITEM)
   *                       EnumerableSort → Scan(PART)
   *                   EnumerableSort → Scan(PARTSUPP)
   *               EnumerableSort → Scan(SUPPLIER)
   *           EnumerableSort → Scan(NATION)
   * </pre>
   *
   * <h3>Expected AFTER structure (per-source MI scan plan)</h3>
   * <pre>
   *   EnumerableSort(n_name ASC, o_year DESC)   ← ORDER BY only
   *     EnumerableAggregate(n_name, o_year)
   *       EnumerableProject
   *         EnumerableFilter(p_name LIKE '%green%')
   *           EnumerableMergeJoin(nationkey)     ← stays in plan (per-source architecture)
   *             EnumerableMergedIndexScan(MI_root, source=view(OLPPS))
   *             EnumerableMergedIndexScan(MI_root, source=NATION)
   * </pre>
   *
   * <p>Full DOT diagrams for BEFORE/AFTER are in {@code test-dot-output/q9_before.dot}
   * and {@code test-dot-output/q9_after.dot}.
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

    final RelNode logicalWithSorts =
        MergedIndexTestUtil.injectSortsBeforeSortBasedOps(root.rel);

    // Capture logical joins (post-order) for IVM — same order as findAllPipelines.
    final List<Join> logicalJoinsQ9 =
        MergedIndexTestUtil.findAllJoins(logicalWithSorts);

    final RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1: logical → physical pipeline ──────────────────────────────
    final RelNode phase1Plan =
        planner.transform(0, desiredTraits, logicalWithSorts);

    final String beforeStr = dumpText(phase1Plan);
    System.out.println("=== Q9 BEFORE (order-based pipeline) ===");
    System.out.println(beforeStr);
    writeDotFile("q9_before", phase1Plan);

    // Discover all 5 join pipelines bottom-up (inner first) and register nested MergedIndexes.
    final Pipeline pipelineTree = MergedIndexTestUtil.buildPipelineTree(phase1Plan);
    final List<Pipeline> pipelines = MergedIndexTestUtil.flattenPipelines(pipelineTree).stream()
        .filter(p -> p.sources.size() >= 2)
        .collect(Collectors.toList());
    assertThat("Expected 5 join pipelines for Q9", pipelines.size(), is(5));

    // ── Verify assembly subtrees ──────────────────────────────────────────
    for (int i = 0; i < pipelines.size(); i++) {
      final Pipeline.AssemblySubtree asm = pipelines.get(i).findAssemblySubtree();
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
    for (int i = 0; i < pipelines.size(); i++) {
      final Pipeline p = pipelines.get(i);
      new MergedIndex(p);
      // Use logical join (SQL types) for IVM derivation — matches StreamRules expectations
      final Join ljQ9 = logicalJoinsQ9.get(i);
      p.mergedIndex.setMaintenancePlan(deriveIncrementalPlan(
          List.of(ljQ9.getLeft(), ljQ9.getRight())));
    }

    printMaintenancePlans("Q9", pipelines);
    for (int i = 0; i < pipelines.size(); i++) {
      writeDotFile("q9_maintenance_" + i, pipelines.get(i).mergedIndex.getMaintenancePlan());
    }

    // ── Phase 2: incremental MI registration with multi-stage HEP ────────
    // Each pass registers ONE level's MI and replaces that level's boundary
    // Sorts with per-source MIScans. Registering incrementally ensures each
    // pass only matches the current level's Sorts (deeper Sorts were already
    // replaced in prior passes).
    final HepProgram hepPass = HepProgram.builder()
        .addRuleInstance(
            EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
        .build();
    RelNode current = phase1Plan;
    for (Pipeline p : pipelines) {
      MergedIndexRegistry.register(p.mergedIndex);
      final HepPlanner hp = new HepPlanner(hepPass);
      hp.setRoot(current);
      current = hp.findBestExp();
    }
    final RelNode phase2Plan = current;

    final String afterStr = dumpText(phase2Plan);
    System.out.println("=== Q9 AFTER (merged index plan) ===");
    System.out.println(afterStr);
    writeDotFile("q9_after", phase2Plan);

    // ── Assert ────────────────────────────────────────────────────────────
    // Per-source MI scan architecture: MergeJoins stay, boundary Sorts replaced.
    assertThat(afterStr, containsString("EnumerableMergeJoin"));
    assertThat(afterStr, containsString("EnumerableMergedIndexScan"));
    // No EnumerableMergedIndexJoin (obsolete under per-source architecture)
    assertThat(afterStr, not(containsString("EnumerableMergedIndexJoin")));
    // Non-boundary Sorts may remain (GROUP BY, ORDER BY):
    // 1. Before the Aggregate: Sort(n_name ASC, o_year ASC) on GROUP BY keys,
    //    injected by injectSortsBeforeSortBasedOps.
    // 2. After the Aggregate: Sort(n_name ASC, o_year DESC) for ORDER BY.
    //    This cannot be dropped because the Aggregate output is sorted
    //    (n_name ASC, o_year ASC) which differs in direction from the required
    //    (n_name ASC, o_year DESC).
    // All pipelines have maintenance plans; each has exactly 2 LogicalDelta branches.
    for (Pipeline p : pipelines) {
      assertThat("Q9 pipeline missing maintenance plan",
          p.mergedIndex.getMaintenancePlan() != null, is(true));
      final String m = dumpText(p.mergedIndex.getMaintenancePlan());
      assertThat(MergedIndexTestUtil.countOccurrences(m, "LogicalDelta"), is(2));
    }
  }

  // ── IVM helpers ──────────────────────────────────────────────────────────

  /**
   * Derives the incremental maintenance plan for a merged-index pipeline.
   *
   * <p>A merged index stores records from each source independently, interleaved by
   * sort key. Each source independently inserts its records at update time — no join
   * against any other source is needed at the MI level. The maintenance plan is
   * therefore a union of independent delta streams, one per sorted input:
   *
   * <pre>
   *   LogicalUnion(all=true)
   *     LogicalDelta(sortedInputs.get(0))
   *     LogicalDelta(sortedInputs.get(1))
   *     ...
   * </pre>
   *
   * <p>For nested pipelines (outer MI), the left sorted input wraps the entire inner
   * pipeline (e.g., {@code Sort(inner_join_result)}). {@code LogicalDelta} over this
   * node means "run the inner pipeline for changed keys and emit the assembled delta,"
   * which is the Phase 2 propagation defined by the inner pipeline's own operators.
   * No additional join against the right source is added here.
   *
   * <p>{@code sortedInputs} are the sort nodes immediately feeding the pipeline
   * operator (join, aggregate, etc.) — the boundaries of the interesting-ordering
   * pipeline that defines this MI.
   */
  private static RelNode deriveIncrementalPlan(List<RelNode> sortedInputs) {
    final List<RelNode> branches = sortedInputs.stream()
        .map(LogicalDelta::create)
        .collect(Collectors.toList());
    return LogicalUnion.create(branches, true);
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
