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
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

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
    RelOptTable tableA = ((EnumerableTableScan) leftSort.getInput()).getTable();
    RelOptTable tableB = ((EnumerableTableScan) rightSort.getInput()).getTable();
    RelCollation collation = leftSort.getCollation();
    double rowCount = join.estimateRowCount(join.getCluster().getMetadataQuery());
    MergedIndexRegistry.register(
        new MergedIndex(List.of(tableA, tableB), collation, rowCount));

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
