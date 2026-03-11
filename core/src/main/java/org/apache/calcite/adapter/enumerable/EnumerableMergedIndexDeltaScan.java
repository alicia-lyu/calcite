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
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.materialize.MergedIndex;
import org.apache.calcite.materialize.Pipeline;
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
import org.apache.calcite.util.BuiltInMethod;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical relational operator representing a <em>delta stream</em> of new
 * records arriving from a {@link MergedIndex}; used in incremental maintenance
 * plans.
 *
 * <p>Produced by {@link DeltaToMergedIndexDeltaScanRule} when it sees a
 * {@code LogicalDelta} whose input is an {@link EnumerableMergedIndexScan}.
 * The delta scan represents the stream of newly inserted rows that need to be
 * merged into the index during an incremental view maintenance step.
 *
 * <p>Cost model: slightly higher than {@link EnumerableMergedIndexScan} to
 * discourage accidental substitution in query plans.
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
public class EnumerableMergedIndexDeltaScan extends AbstractRelNode
    implements EnumerableRel {

  /** The merged index whose delta stream this operator represents. */
  public final MergedIndex mergedIndex;

  private final RelDataType rowType;

  /** Creates an EnumerableMergedIndexDeltaScan. Use {@link #create} instead. */
  protected EnumerableMergedIndexDeltaScan(
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
   * Creates an {@code EnumerableMergedIndexDeltaScan}.
   *
   * @param cluster     query planning cluster
   * @param mergedIndex the merged index whose delta stream to produce
   * @param rowType     the output row type (same as the base scan's row type)
   */
  public static EnumerableMergedIndexDeltaScan create(
      RelOptCluster cluster,
      MergedIndex mergedIndex,
      RelDataType rowType) {
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replace(mergedIndex.getCollation());
    return new EnumerableMergedIndexDeltaScan(cluster, traitSet, mergedIndex, rowType);
  }

  @Override protected RelDataType deriveRowType() {
    return rowType;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double rc = mergedIndex.getRowCount();
    return planner.getCostFactory().makeCost(rc, rc * 0.2, rc);
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    return mergedIndex.getRowCount();
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    List<String> descs = new ArrayList<>();
    for (int i = 0; i < mergedIndex.getSources().size(); i++) {
      Pipeline child = mergedIndex.getSources().get(i);
      RelCollation tc = child.boundaryCollation;
      if (child.mergedIndex != null) {
        descs.add("delta:view(" + child.mergedIndex.getCollation() + ")");
      } else {
        RelOptTable t = MergedIndex.findLeafScan(child.root);
        if (t != null) {
          int keyIdx = tc.getFieldCollations().get(0).getFieldIndex();
          String keyName = t.getRowType().getFieldList().get(keyIdx).getName();
          descs.add("delta:" + t.getQualifiedName() + ":" + keyName);
        } else {
          descs.add("delta:unknown");
        }
      }
    }
    return super.explainTerms(pw)
        .item("tables", descs)
        .item("collation", mergedIndex.getCollation());
  }

  /**
   * Code generation (proof-of-concept): returns an empty enumerable.
   * A production implementation would stream newly-inserted delta records
   * from the merged-index storage layer.
   */
  @Override public Result implement(EnumerableRelImplementor implementor,
      Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.preferArray());
    builder.add(
        Expressions.return_(null,
            Expressions.call(BuiltInMethod.EMPTY_ENUMERABLE.method)));
    return implementor.result(physType, builder.toBlock());
  }
}
