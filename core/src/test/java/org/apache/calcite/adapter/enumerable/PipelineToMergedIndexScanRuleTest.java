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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.materialize.MergedIndex;
import org.apache.calcite.materialize.MergedIndexRegistry;
import org.apache.calcite.materialize.Pipeline;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
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

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Integration test for {@link PipelineToMergedIndexScanRule}.
 *
 * <p>Verifies a two-phase optimizer run:
 * <ol>
 *   <li>Phase 1 converts a logical plan (with explicit LogicalSort nodes pre-injected
 *       at join inputs) into the enumerable pipeline:
 *       {@code EnumerableMergeJoin(EnumerableSort(Scan), EnumerableSort(Scan))}</li>
 *   <li>After registering a {@link MergedIndex}, phase 2 replaces the entire
 *       pipeline with a single {@link EnumerableMergedIndexScan}.</li>
 * </ol>
 *
 * <p>LogicalSort nodes are pre-injected because {@code ENUMERABLE_SORT_RULE}
 * converts existing {@code LogicalSort} nodes to {@code EnumerableSort}; it
 * does not create sorts from scratch to satisfy merge-join's ordering needs.
 */
public class PipelineToMergedIndexScanRuleTest {

  @AfterEach
  void clearRegistry() {
    MergedIndexRegistry.clear();
  }

  /**
   * A merge-join pipeline over tables sorted by join key is replaced by a
   * single merged-index scan once a matching {@link MergedIndex} is registered.
   */
  @Test void pipelineReplacedByMergedIndexScan() throws Exception {
    // Tables A(k, x) and B(k, y) joined on the key column k.
    final String sql =
        "SELECT \"a\".\"x\", \"b\".\"y\""
            + " FROM \"A\" \"a\""
            + " JOIN \"B\" \"b\" ON \"a\".\"k\" = \"b\".\"k\"";

    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus defSchema = rootSchema.add("s", new TwoTableSchema());

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(defSchema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(
            // Phase 1: convert to the order-based enumerable pipeline.
            Programs.of(RuleSets.ofList(
                EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_RULE,
                EnumerableRules.ENUMERABLE_SORT_RULE,
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE)))
        .build();

    Planner planner = Frameworks.getPlanner(config);
    SqlNode parsed = planner.parse(sql);
    SqlNode validated = planner.validate(parsed);
    RelRoot root = planner.rel(validated);

    // Inject LogicalSort at each sort-based operator input so that
    // ENUMERABLE_SORT_RULE can convert them to EnumerableSort nodes in phase 1.
    RelNode logicalWithSorts =
        MergedIndexTestUtil.injectSortsBeforeSortBasedOps(root.rel);

    RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1 ────────────────────────────────────────────────────────────
    RelNode phase1Plan = planner.transform(0, desiredTraits, logicalWithSorts);

    // ── Discover pipeline and register merged index ────────────────────────
    Pipeline pipelineTree = MergedIndexTestUtil.buildPipelineTree(phase1Plan);
    List<Pipeline> pipelines =
        MergedIndexTestUtil.flattenPipelines(pipelineTree).stream()
            .filter(p -> p.sources.size() >= 2)
            .collect(Collectors.toList());
    assertThat("Expected 1 join pipeline", pipelines.size(), is(1));

    Pipeline p = pipelines.get(0);
    new MergedIndex(p);
    MergedIndexRegistry.register(p.mergedIndex);

    // ── Phase 2 ────────────────────────────────────────────────────────────
    HepProgram hepProgram = HepProgram.builder()
        .addRuleInstance(EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
        .build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(phase1Plan);
    RelNode phase2Plan = hepPlanner.findBestExp();

    String planStr = RelOptUtil.dumpPlan(
        "", phase2Plan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);

    assertThat(planStr, containsString("EnumerableMergedIndexScan"));
    assertThat(planStr, not(containsString("EnumerableMergeJoin")));
    assertThat(planStr, not(containsString("EnumerableSort")));
  }

  /**
   * Verifies assembly subtree identification for the aggregate-above-join case.
   *
   * <p>This test uses a simple 2-table schema where the planner places the
   * aggregate above the join (both share the same key {@code k}). The
   * assembly subtree for the join pipeline is just {MergeJoin} — the
   * SortedAggregate sits above the LCA and is a "remaining operator."
   *
   * <p>Actual rel tree (from Phase 1 plan):
   * <pre>
   *   SortedAggregate(k)                        ← remaining operator
   *     MergeJoin(k)
   *       Sort(k) → Scan(A)                     ← boundary Sort (source 0)
   *       Sort(k) → Scan(B)                     ← boundary Sort (source 1)
   * </pre>
   *
   * <p>The multi-operator case (assembly = {MergeJoin, SortedAggregate}) arises
   * naturally in Q3-OL's inner pipeline where the SortedAggregate feeds one
   * side of the MergeJoin without an intermediate Sort. This happens because
   * {@code injectSortsBeforeSortBasedOps} uses {@code inputAlreadySorted} to
   * skip redundant Sorts when the aggregate output is already sorted on the
   * join key. See {@code MergedIndexTpchPlanTest.tpchQ3OrdersLineitem} for
   * that case.
   *
   * <p>When the aggregate is above the join (as in this test), a second
   * Pipeline rooted at the aggregate is also constructed to verify that
   * the LCA correctly identifies the MergeJoin even when the pipeline root
   * is a different operator.
   */
  @Test void multiOperatorAssemblySubtree() throws Exception {
    // Query: aggregate A by key k, then join with B on k.
    final String sql =
        "SELECT \"b\".\"y\", SUM(\"a\".\"x\") AS sx"
            + " FROM \"A\" \"a\""
            + " JOIN \"B\" \"b\" ON \"a\".\"k\" = \"b\".\"k\""
            + " GROUP BY \"a\".\"k\", \"b\".\"y\"";

    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus defSchema = rootSchema.add("s", new TwoTableSchema());

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(defSchema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(
            Programs.of(RuleSets.ofList(
                EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_RULE,
                EnumerableRules.ENUMERABLE_SORT_RULE,
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE)))
        .build();

    Planner planner = Frameworks.getPlanner(config);
    SqlNode parsed = planner.parse(sql);
    SqlNode validated = planner.validate(parsed);
    RelRoot root = planner.rel(validated);

    RelNode logicalWithSorts =
        MergedIndexTestUtil.injectSortsBeforeSortBasedOps(root.rel);

    RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode phase1Plan = planner.transform(0, desiredTraits, logicalWithSorts);

    String planStr = RelOptUtil.dumpPlan(
        "", phase1Plan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    System.out.println("=== Multi-operator assembly BEFORE ===");
    System.out.println(planStr);

    // ── Case 1: Aggregate above join (standard planner output) ────────────
    // Use pipeline discovery to find the join pipeline.
    Pipeline pipelineTree = MergedIndexTestUtil.buildPipelineTree(phase1Plan);
    List<Pipeline> pipelines =
        MergedIndexTestUtil.flattenPipelines(pipelineTree).stream()
            .filter(p -> p.sources.size() >= 2)
            .collect(Collectors.toList());
    assertThat("Expected 1 join pipeline", pipelines.size(), is(1));

    Pipeline pJoin = pipelines.get(0);
    Pipeline.AssemblySubtree asm1 = pJoin.findAssemblySubtree();
    assertThat("Assembly subtree should exist", asm1 != null, is(true));
    assertThat("LCA should be MergeJoin",
        asm1.lca, instanceOf(EnumerableMergeJoin.class));
    assertThat("Assembly should contain only MergeJoin",
        asm1.nodes.size(), is(1));
    assertThat("Should have 2 boundary sorts",
        asm1.boundarySorts.size(), is(2));

    // ── Case 2: Hypothetical multi-operator assembly ──────────────────────
    // Construct a Pipeline rooted at the aggregate (parent of the join).
    // The assembly subtree should still be {MergeJoin} because the
    // SortedAggregate doesn't directly consume a boundary Sort.
    RelNode aggNode = findSortedAggregate(phase1Plan);
    if (aggNode != null) {
      Pipeline pAgg = new Pipeline(aggNode, pJoin.sources,
          pJoin.sharedCollation, pJoin.sharedCollation, 10.0);
      Pipeline.AssemblySubtree asm2 = pAgg.findAssemblySubtree();
      assertThat("Aggregate-rooted assembly subtree should exist",
          asm2 != null, is(true));
      assertThat("LCA should be MergeJoin even with Aggregate as root",
          asm2.lca, instanceOf(EnumerableMergeJoin.class));
      assertThat("Assembly should contain only MergeJoin",
          asm2.nodes.size(), is(1));
      assertThat("Should have 2 boundary sorts",
          asm2.boundarySorts.size(), is(2));
    }
  }

  /** Finds the first {@link EnumerableSortedAggregate} in the tree. */
  private static @Nullable RelNode findSortedAggregate(RelNode node) {
    if (node instanceof EnumerableSortedAggregate) {
      return node;
    }
    for (RelNode input : node.getInputs()) {
      RelNode found = findSortedAggregate(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Two-table schema with {@code A(k INT, x INT)} and {@code B(k INT, y INT)}.
   * No collation statistics are reported so the tables are unsorted by default.
   */
  private static class TwoTableSchema extends AbstractSchema {
    @Override protected Map<String, Table> getTableMap() {
      return ImmutableMap.of(
          "A", new SimpleTable(
              factory -> new RelDataTypeFactory.Builder(factory)
                  .add("k", factory.createJavaType(int.class))
                  .add("x", factory.createJavaType(int.class))
                  .build(),
              List.of(new Object[]{1, 10}, new Object[]{2, 20})),
          "B", new SimpleTable(
              factory -> new RelDataTypeFactory.Builder(factory)
                  .add("k", factory.createJavaType(int.class))
                  .add("y", factory.createJavaType(int.class))
                  .build(),
              List.of(new Object[]{1, 100}, new Object[]{2, 200})));
    }

    private static class SimpleTable extends AbstractTable
        implements ScannableTable {

      private final Function<RelDataTypeFactory, RelDataType> typeBuilder;
      private final List<Object[]> data;

      SimpleTable(
          Function<RelDataTypeFactory, RelDataType> typeBuilder,
          List<Object[]> data) {
        this.typeBuilder = typeBuilder;
        this.data = data;
      }

      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeBuilder.apply(typeFactory);
      }

      @Override public Statistic getStatistic() {
        return Statistics.UNKNOWN;
      }

      @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(data);
      }
    }
  }
}
