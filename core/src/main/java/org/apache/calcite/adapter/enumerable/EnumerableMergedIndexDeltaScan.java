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
import org.apache.calcite.materialize.MergedIndexScanGroup;
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

/**
 * Physical relational operator representing a <em>delta stream</em> of new
 * records arriving from one source of a {@link MergedIndex}; used in
 * incremental maintenance plans.
 *
 * <p>Under the "Transparent Per-Source MI Scans" architecture, each boundary
 * Sort in a pipeline is replaced independently by a per-source delta scan that
 * returns <b>source-native rows</b> (the row type of just that one source).
 * Parent operators (MergeJoin, SortedAggregate) remain in the plan unchanged.
 *
 * <p>All per-source delta scans within the same pipeline share a
 * {@link MergedIndexScanGroup} instance, signifying that they share one
 * physical I/O pass over the merged index's delta buffer.
 *
 * <p>Produced by {@link DeltaToMergedIndexDeltaScanRule} when it sees a
 * {@code LogicalDelta} whose input is an {@link EnumerableMergedIndexScan}.
 * The delta scan represents the stream of newly inserted rows that need to be
 * merged into the index during an incremental view maintenance step.
 *
 * <p>Cost model: slightly higher cpu than {@link EnumerableMergedIndexScan}
 * (0.2 vs 0.1) to discourage accidental substitution in query plans.
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
public class EnumerableMergedIndexDeltaScan extends AbstractRelNode
    implements EnumerableRel {

  /** The merged index whose delta stream this operator represents. */
  public final MergedIndex mergedIndex;

  /** Which source (0..N-1) this delta scan filters for. */
  public final int sourceIndex;

  /** Shared physical scan reference — sibling scans share the same instance. */
  public final MergedIndexScanGroup scanGroup;

  private final RelDataType rowType;

  /** Creates an EnumerableMergedIndexDeltaScan. Use a {@code create} factory instead. */
  protected EnumerableMergedIndexDeltaScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      MergedIndex mergedIndex,
      int sourceIndex,
      MergedIndexScanGroup scanGroup,
      RelDataType rowType) {
    super(cluster, traitSet);
    this.mergedIndex = mergedIndex;
    this.sourceIndex = sourceIndex;
    this.scanGroup = scanGroup;
    this.rowType = rowType;
    assert getConvention() instanceof EnumerableConvention;
  }

  /**
   * Creates a per-source {@code EnumerableMergedIndexDeltaScan}.
   *
   * <p>The delta scan returns rows matching the source pipeline's row type at
   * {@code sourceIndex}, with collation matching the source's boundary
   * collation.
   *
   * @param cluster     query planning cluster
   * @param mergedIndex the merged index whose delta stream to produce
   * @param sourceIndex which source (0..N-1) this delta scan filters for
   * @param scanGroup   shared physical scan reference
   */
  public static EnumerableMergedIndexDeltaScan create(
      RelOptCluster cluster,
      MergedIndex mergedIndex,
      int sourceIndex,
      MergedIndexScanGroup scanGroup) {
    Pipeline source = mergedIndex.getSources().get(sourceIndex);
    RelCollation boundaryCollation = source.boundaryCollation;
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replace(boundaryCollation);
    RelDataType rowType = source.root.getRowType();
    return new EnumerableMergedIndexDeltaScan(cluster, traitSet, mergedIndex,
        sourceIndex, scanGroup, rowType);
  }

  /**
   * Creates an {@code EnumerableMergedIndexDeltaScan} with explicit row type.
   *
   * @deprecated Use {@link #create(RelOptCluster, MergedIndex, int, MergedIndexScanGroup)}.
   *     This overload exists for backward compatibility with existing callers.
   */
  @Deprecated
  public static EnumerableMergedIndexDeltaScan create(
      RelOptCluster cluster,
      MergedIndex mergedIndex,
      RelDataType rowType) {
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replace(mergedIndex.getCollation());
    return new EnumerableMergedIndexDeltaScan(cluster, traitSet, mergedIndex,
        0, new MergedIndexScanGroup(mergedIndex), rowType);
  }

  @Override protected RelDataType deriveRowType() {
    return rowType;
  }

  /**
   * Cost model for a per-source delta scan over a merged index.
   *
   * <p>Cost formula (mirrors {@link EnumerableMergedIndexScan} but with higher
   * cpu to discourage accidental use in query plans):
   * <ul>
   *   <li><b>rowCount</b> = totalRows / sourceCount (only this source's rows)
   *   <li><b>cpu</b> = sourceRows * 0.2 (slightly higher than full scan)
   *   <li><b>io</b> = totalRows (full index scan, shared across siblings)
   * </ul>
   */
  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double totalRows = mergedIndex.getRowCount();
    double sourceRows = totalRows / scanGroup.sourceCount;
    return planner.getCostFactory().makeCost(sourceRows, sourceRows * 0.2,
        totalRows);
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    return mergedIndex.getRowCount() / scanGroup.sourceCount;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    Pipeline source = mergedIndex.getSources().get(sourceIndex);
    String srcDesc;
    if (source.mergedIndex != null) {
      srcDesc = "delta:view(" + source.mergedIndex.getCollation() + ")";
    } else {
      RelOptTable t = MergedIndex.findLeafScan(source.root);
      if (t != null) {
        RelCollation tc = source.boundaryCollation;
        int keyIdx = tc.getFieldCollations().get(0).getFieldIndex();
        String keyName = t.getRowType().getFieldList().get(keyIdx).getName();
        srcDesc = "delta:" + t.getQualifiedName() + ":" + keyName;
      } else {
        srcDesc = "delta:unknown";
      }
    }
    return super.explainTerms(pw)
        .item("mi", mergedIndex.getCollation())
        .item("source", srcDesc)
        .item("sourceIndex", sourceIndex);
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
