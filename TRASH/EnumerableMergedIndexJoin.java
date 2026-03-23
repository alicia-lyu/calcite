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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.BuiltInMethod;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical operator that assembles join output from a
 * {@link EnumerableMergedIndexScan} by computing the Cartesian product of
 * co-located record groups within each sort-key group.
 *
 * <p>Used only for the outermost pipeline at query time. For example, in a
 * 3-table plan (LINEITEM ⋈ ORDERS ⋈ CUSTOMER), the inner pipeline
 * (LINEITEM+ORDERS on orderkey) is replaced by a leaf
 * {@link EnumerableMergedIndexScan}; the outer pipeline (inner_view+CUSTOMER
 * on custkey) is replaced by this join node wrapping a second scan.
 *
 * <p>At query time, the outer merged index scan streams co-located groups of
 * (inner-join-result rows, CUSTOMER rows) by {@code custkey}; this operator
 * assembles the Cartesian product within each group (Algorithm 1 from the
 * paper). Inner join + aggregation are performed at maintenance time and
 * pre-stored in the outer merged index.
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
public class EnumerableMergedIndexJoin extends SingleRel implements EnumerableRel {

  /** The merged index backing the scan child of this join. */
  public final MergedIndex mergedIndex;

  /** The join type (typically INNER for equi-joins). */
  public final JoinRelType joinType;

  /** Creates an EnumerableMergedIndexJoin. Use {@link #create} instead. */
  protected EnumerableMergedIndexJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      MergedIndex mergedIndex,
      JoinRelType joinType) {
    super(cluster, traitSet, input);
    this.mergedIndex = mergedIndex;
    this.joinType = joinType;
    assert getConvention() instanceof EnumerableConvention;
  }

  /**
   * Creates an {@code EnumerableMergedIndexJoin}.
   *
   * @param cluster     query planning cluster
   * @param mergedIndex the outer merged index
   * @param joinType    join type (typically INNER)
   * @param input       the {@link EnumerableMergedIndexScan} child
   */
  public static EnumerableMergedIndexJoin create(
      RelOptCluster cluster,
      MergedIndex mergedIndex,
      JoinRelType joinType,
      RelNode input) {
    RelTraitSet traits = cluster.traitSetOf(EnumerableConvention.INSTANCE)
        .replace(mergedIndex.getCollation());
    return new EnumerableMergedIndexJoin(cluster, traits, input, mergedIndex, joinType);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableMergedIndexJoin(getCluster(), traitSet,
        sole(inputs), mergedIndex, joinType);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    List<String> sourceDescs = new ArrayList<>();
    for (int i = 0; i < mergedIndex.getSources().size(); i++) {
      Pipeline child = mergedIndex.getSources().get(i);
      if (child.mergedIndex != null) {
        sourceDescs.add("view(" + child.mergedIndex.getCollation() + ")");
      } else {
        RelOptTable t = MergedIndex.findLeafScan(child.root);
        if (t != null) {
          int keyIdx = child.boundaryCollation.getFieldCollations().get(0).getFieldIndex();
          String keyName = t.getRowType().getFieldList().get(keyIdx).getName();
          sourceDescs.add(t.getQualifiedName() + ":" + keyName);
        } else {
          sourceDescs.add("unknown");
        }
      }
    }
    return super.explainTerms(pw)
        .item("sources", sourceDescs)
        .item("joinType", joinType)
        .item("collation", mergedIndex.getCollation());
  }

  /**
   * Code generation (proof-of-concept stub): returns an empty enumerable.
   * A real implementation would apply Algorithm 1 from the paper — computing
   * the Cartesian product of co-located record groups streamed from the scan.
   */
  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());
    builder.add(
        Expressions.return_(null,
            Expressions.call(BuiltInMethod.EMPTY_ENUMERABLE.method)));
    return implementor.result(physType, builder.toBlock());
  }
}
