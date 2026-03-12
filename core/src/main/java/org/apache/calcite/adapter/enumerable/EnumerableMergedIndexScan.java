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

import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.materialize.MergedIndex;
import org.apache.calcite.materialize.Pipeline;
import org.apache.calcite.materialize.TaggedRowSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical relational operator that reads a {@link MergedIndex} —
 * a multi-table interleaved B-tree — and produces the joined (and optionally
 * aggregated) output of all participating tables in a single sequential pass.
 *
 * <p>This operator replaces an entire pipeline of:
 * <pre>
 *   EnumerableSort(A) ──→ EnumerableMergeJoin ──→ (EnumerableSortedAggregate)
 *   EnumerableSort(B) ──→/
 * </pre>
 * The merged index physically pre-sorts records from all tables by the shared
 * key, so no run-time sorting, merging, or aggregation is needed.
 *
 * <p>Cost model: {@code rowCount=ΣTᵢ, cpu=ΣTᵢ*0.1, io=ΣTᵢ} — always cheaper
 * than the sum of sort + merge-join costs for any non-trivial dataset.
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
public class EnumerableMergedIndexScan extends AbstractRelNode
    implements EnumerableRel {

  /** The merged index that backs this scan. */
  public final MergedIndex mergedIndex;

  /**
   * The joined output row type (all fields from all participating tables
   * concatenated, matching the row type of the pipeline being replaced).
   */
  private final RelDataType rowType;

  /** Creates an EnumerableMergedIndexScan. Use {@link #create} instead. */
  protected EnumerableMergedIndexScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      MergedIndex mergedIndex,
      RelDataType rowType) {
    super(cluster, traitSet);
    this.mergedIndex = mergedIndex;
    this.rowType = rowType;
    assert getConvention() instanceof EnumerableConvention;
  }

  /**
   * Creates an {@code EnumerableMergedIndexScan}.
   *
   * @param cluster    query planning cluster
   * @param mergedIndex the merged index to scan
   * @param rowType    the joined output row type (from the pipeline being replaced)
   */
  public static EnumerableMergedIndexScan create(
      RelOptCluster cluster,
      MergedIndex mergedIndex,
      RelDataType rowType) {
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replace(mergedIndex.getCollation());
    return new EnumerableMergedIndexScan(cluster, traitSet, mergedIndex, rowType);
  }

  @Override protected RelDataType deriveRowType() {
    return rowType;
  }

  /**
   * Cost model: the merged index scan costs only O(ΣTᵢ) I/O and O(ΣTᵢ)
   * CPU (record assembly), eliminating the O(N log N) sort costs.
   */
  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double rc = mergedIndex.getRowCount();
    return planner.getCostFactory().makeCost(rc, rc * 0.1, rc);
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    return mergedIndex.getRowCount();
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    List<String> sourceDescriptions = new ArrayList<>();
    for (int i = 0; i < mergedIndex.getSources().size(); i++) {
      Pipeline child = mergedIndex.getSources().get(i);
      RelCollation tc = child.boundaryCollation;
      if (child.mergedIndex != null) {
        sourceDescriptions.add("view(" + child.mergedIndex.getCollation() + ")");
      } else {
        RelOptTable t = MergedIndex.findLeafScan(child.root);
        if (t != null) {
          int keyIdx = tc.getFieldCollations().get(0).getFieldIndex();
          String keyName = t.getRowType().getFieldList().get(keyIdx).getName();
          sourceDescriptions.add(t.getQualifiedName() + ":" + keyName);
        } else {
          sourceDescriptions.add("unknown");
        }
      }
    }
    return super.explainTerms(pw)
        .item("tables", sourceDescriptions)
        .item("collation", mergedIndex.getCollation());
  }

  /**
   * Code generation: stashes the {@link MergedIndex} instance and generates
   * a call to {@link MergedIndex#scanData()} which returns the pre-populated
   * tagged row stream as {@code Enumerable<Object[]>}.
   *
   * <p>The PhysType uses the tagged row schema (from {@link TaggedRowSchema
   * #toRelDataType}) so that downstream operators see the correct field count.
   * The plan-level {@code deriveRowType()} still returns the join row type for
   * compatibility with existing plan-only tests; Assembly (Subtask 4) will
   * reconcile the two.
   */
  @Override public Result implement(EnumerableRelImplementor implementor,
      Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final TaggedRowSchema schema = mergedIndex.getTaggedRowSchema();
    final RelDataType taggedType =
        schema.toRelDataType(implementor.getTypeFactory());
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), taggedType,
            JavaRowFormat.ARRAY);

    // Stash the MergedIndex for runtime access (same pattern as
    // EnumerableInterpreter stashing a RelNode).
    final Expression miExpr =
        implementor.stash(mergedIndex, MergedIndex.class);

    // Generate: mergedIndex.scanData()
    builder.add(
        Expressions.return_(null,
            Expressions.call(miExpr,
                Types.lookupMethod(MergedIndex.class, "scanData"))));

    return implementor.result(physType, builder.toBlock());
  }
}
