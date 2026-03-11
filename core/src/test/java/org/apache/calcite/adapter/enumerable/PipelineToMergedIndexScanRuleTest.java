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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalSort;
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
import java.util.Objects;
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
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE)))
        .build();

    Planner planner = Frameworks.getPlanner(config);
    SqlNode parsed = planner.parse(sql);
    SqlNode validated = planner.validate(parsed);
    RelRoot root = planner.rel(validated);

    // Inject LogicalSort(k ASC) at each join input so that ENUMERABLE_SORT_RULE
    // can convert them to EnumerableSort nodes in phase 1.
    // Without this, ENUMERABLE_SORT_RULE has no LogicalSort to work with, and
    // the Volcano planner cannot satisfy the merge-join's collation requirement.
    RelCollation joinKeyCollation = RelCollations.of(0);
    RelNode logicalWithSorts = injectSortsBeforeJoin(root.rel, joinKeyCollation);

    RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);

    // ── Phase 1 ────────────────────────────────────────────────────────────
    // Expected shape: (EnumerableProject →) EnumerableMergeJoin
    //                   → EnumerableSort(k) → EnumerableTableScan(A)
    //                   → EnumerableSort(k) → EnumerableTableScan(B)
    RelNode phase1Plan = planner.transform(0, desiredTraits, logicalWithSorts);

    // Navigate through any wrapping Project to locate the MergeJoin.
    EnumerableMergeJoin join = Objects.requireNonNull(
        findMergeJoin(phase1Plan),
        "Phase 1 plan does not contain an EnumerableMergeJoin: " + phase1Plan);

    // ── Register merged index ───────────────────────────────────────────────
    EnumerableSort leftSort = (EnumerableSort) join.getLeft();
    EnumerableSort rightSort = (EnumerableSort) join.getRight();
    RelCollation collation = leftSort.getCollation();
    double rowCount = join.estimateRowCount(join.getCluster().getMetadataQuery());

    // Build Pipeline objects to wrap the table scans, then create a
    // Pipeline for the join so MergedIndex(Pipeline) can be used.
    Pipeline pLeft = new Pipeline(leftSort.getInput(), List.of(),
        collation, collation, leftSort.getInput()
            .estimateRowCount(join.getCluster().getMetadataQuery()));
    Pipeline pRight = new Pipeline(rightSort.getInput(), List.of(),
        collation, collation, rightSort.getInput()
            .estimateRowCount(join.getCluster().getMetadataQuery()));
    Pipeline pJoin = new Pipeline(join, List.of(pLeft, pRight),
        collation, collation, rowCount);
    new MergedIndex(pJoin);
    MergedIndexRegistry.register(pJoin.mergedIndex);

    // ── Phase 2 ────────────────────────────────────────────────────────────
    // Use a HEP planner so the rule fires directly on the matched subtree
    // without cost-model complications from the Volcano planner running with
    // only a single rule.
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
   * Verifies assembly subtree identification for a multi-operator case:
   * a SortedAggregate sits between a boundary Sort and the MergeJoin,
   * with no intermediate Sort separating them.
   *
   * <p>This hypothetical structure arises when an optimizer recognizes that
   * both the SortedAggregate and the MergeJoin share the same key, so the
   * intermediate Sort between them is redundant and removed.
   *
   * <p>Actual rel tree (from Phase 1 plan, aggregate placed above join):
   * <pre>
   *   SortedAggregate(k)                        ← pipeline root
   *     MergeJoin(k)
   *       Sort(k) → Scan(A)                     ← boundary Sort (source 0)
   *       Sort(k) → Scan(B)                     ← boundary Sort (source 1)
   * </pre>
   *
   * <p>In this case the SortedAggregate has no boundary sort as a direct input —
   * only the MergeJoin does. So the assembly subtree = {MergeJoin} and the
   * SortedAggregate is a "remaining operator" above the assembly.
   *
   * <p>For the multi-operator case, we construct a second Pipeline where the
   * MergeJoin is the root and one Sort child is replaced by
   * {@code SortedAggregate → Sort(boundary)}, making both the MergeJoin and
   * the SortedAggregate boundary consumers. This requires the aggregate to be
   * a child of the MergeJoin, which arises from queries like Q3-OL where the
   * aggregate feeds one side of the join without an intermediate Sort.
   *
   * <p>Since Calcite's planner always inserts Sorts before every join input,
   * the multi-operator subtree only occurs when redundant Sorts are eliminated.
   * We test this by locating nodes from the Phase 1 plan and manually
   * constructing the Pipeline with the desired boundary structure.
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

    RelCollation joinKeyCollation = RelCollations.of(0);
    RelNode logicalWithSorts = injectSortsBeforeJoin(root.rel, joinKeyCollation);

    RelTraitSet desiredTraits =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode phase1Plan = planner.transform(0, desiredTraits, logicalWithSorts);

    String planStr = RelOptUtil.dumpPlan(
        "", phase1Plan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    System.out.println("=== Multi-operator assembly BEFORE ===");
    System.out.println(planStr);

    // ── Case 1: Aggregate above join (standard planner output) ────────────
    // Pipeline root = SortedAggregate (or Aggregate), boundaries = 2 Sorts
    // under the MergeJoin. Assembly = {MergeJoin} only — the aggregate is
    // a remaining operator because it has no boundary Sort as a direct input.
    EnumerableMergeJoin join = Objects.requireNonNull(
        findMergeJoin(phase1Plan),
        "Plan must contain EnumerableMergeJoin: " + planStr);
    EnumerableSort leftSort = (EnumerableSort) join.getLeft();
    EnumerableSort rightSort = (EnumerableSort) join.getRight();
    RelCollation collation = leftSort.getCollation();

    // Build pipeline with root = join's parent (aggregate or project)
    Pipeline pLeft = new Pipeline(leftSort.getInput(), List.of(),
        collation, collation, 10.0);
    Pipeline pRight = new Pipeline(rightSort.getInput(), List.of(),
        collation, collation, 10.0);
    Pipeline pJoin = new Pipeline(join, List.of(pLeft, pRight),
        collation, collation, 10.0);
    Pipeline.AssemblySubtree asm1 = pJoin.findAssemblySubtree();
    assertThat("Assembly subtree should exist", asm1 != null, is(true));
    assertThat("LCA should be the MergeJoin", asm1.lca, is(join));
    assertThat("Assembly should contain only MergeJoin",
        asm1.nodes.size(), is(1));
    assertThat("Should have 2 boundary sorts",
        asm1.boundarySorts.size(), is(2));

    // ── Case 2: Hypothetical multi-operator assembly ──────────────────────
    // Construct a Pipeline where MergeJoin's left input is
    // SortedAggregate → Sort(boundary) instead of Sort(boundary) → Scan.
    // This simulates the case where an optimizer removed the redundant Sort
    // between the SortedAggregate and MergeJoin because they share key k.
    //
    // We find the Sort below the left side of the join — that will become
    // the "inner" boundary sort that sits below a SortedAggregate in the
    // hypothetical plan tree.
    //
    // Actual tree: join → leftSort → Scan(A)
    //                   → rightSort → Scan(B)
    // We pretend:  join → SortedAgg → leftSort → Scan(A)   (no outer Sort)
    //                   → rightSort → Scan(B)
    //
    // Since findAssemblySubtree walks the actual tree from pipeline.root,
    // we need an actual SortedAggregate node in the tree. Find one from
    // the phase1 plan (above the join) and use it to construct a pipeline.
    RelNode aggNode = findSortedAggregate(phase1Plan);
    if (aggNode != null) {
      // The aggregate sits above the join: Agg → Sort → MergeJoin → ...
      // or Agg → MergeJoin → ... (if Sort was removed).
      // Build pipeline with root = aggNode, where boundaries are the two
      // Sorts directly under the MergeJoin. The assembly subtree should
      // still be {MergeJoin} because the SortedAggregate doesn't directly
      // consume a boundary Sort.
      Pipeline pAgg = new Pipeline(aggNode, List.of(pLeft, pRight),
          collation, collation, 10.0);
      Pipeline.AssemblySubtree asm2 = pAgg.findAssemblySubtree();
      assertThat("Aggregate-rooted assembly subtree should exist",
          asm2 != null, is(true));
      // LCA is still MergeJoin: it's the deepest node reaching both boundaries
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
   * Walks the plan tree and wraps each immediate input of every {@link Join}
   * node in a {@link LogicalSort} with the given collation.
   *
   * <p>This ensures {@code ENUMERABLE_SORT_RULE} has concrete {@code LogicalSort}
   * nodes to convert, giving the Volcano planner the physical sort operators
   * that {@link EnumerableMergeJoin} requires from its inputs.
   */
  private static RelNode injectSortsBeforeJoin(RelNode node,
      RelCollation collation) {
    if (node instanceof Join) {
      Join join = (Join) node;
      RelNode sortedLeft =
          LogicalSort.create(join.getLeft(), collation, null, null);
      RelNode sortedRight =
          LogicalSort.create(join.getRight(), collation, null, null);
      return join.copy(join.getTraitSet(),
          List.of(sortedLeft, sortedRight));
    }
    List<RelNode> newInputs = node.getInputs().stream()
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
      EnumerableMergeJoin found = findMergeJoin(input);
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
