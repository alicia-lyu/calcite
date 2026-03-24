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

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Physical relational operator that reads one source's rows from a
 * {@link MergedIndex} — a multi-table interleaved B-tree.
 *
 * <p>Under the "Transparent Per-Source MI Scans" architecture, each boundary
 * Sort in a pipeline is replaced independently by a per-source MI scan that
 * returns <b>source-native rows</b> (the row type of just that one source).
 * Parent operators (MergeJoin, SortedAggregate) remain in the plan unchanged.
 *
 * <p>All per-source scans within the same pipeline share a
 * {@link MergedIndexScanGroup} instance, signifying that they share one
 * physical I/O pass over the merged index. The co-locality benefits are
 * handled by the buffer manager (not implemented in Calcite).
 *
 * <p>Cost model: source's share of total MI rows for cpu/rowCount; full MI
 * size for I/O (the entire index is scanned, shared across siblings).
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
public class EnumerableMergedIndexScan extends AbstractRelNode
    implements EnumerableRel {

  /** The merged index that backs this scan. */
  public final MergedIndex mergedIndex;

  /** Which source (0..N-1) this scan filters for. */
  public final int sourceIndex;

  /** Shared physical scan reference — sibling scans share the same instance. */
  public final MergedIndexScanGroup scanGroup;

  /**
   * Optional row type override for backward compatibility with the deprecated
   * {@link #create(RelOptCluster, MergedIndex, RelDataType)} factory. When
   * non-null, {@link #deriveRowType()} returns this instead of deriving from
   * the source pipeline.
   */
  private final @Nullable RelDataType rowTypeOverride;

  /** Creates an EnumerableMergedIndexScan. Use a {@code create} factory instead. */
  protected EnumerableMergedIndexScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      MergedIndex mergedIndex,
      int sourceIndex,
      MergedIndexScanGroup scanGroup,
      @Nullable RelDataType rowTypeOverride) {
    super(cluster, traitSet);
    this.mergedIndex = mergedIndex;
    this.sourceIndex = sourceIndex;
    this.scanGroup = scanGroup;
    this.rowTypeOverride = rowTypeOverride;
    assert getConvention() instanceof EnumerableConvention;
  }

  /**
   * Creates a per-source {@code EnumerableMergedIndexScan}.
   *
   * <p>The scan returns rows matching the source pipeline's row type at
   * {@code sourceIndex}, with collation matching the source's boundary
   * collation.
   *
   * @param cluster     query planning cluster
   * @param mergedIndex the merged index to scan
   * @param sourceIndex which source (0..N-1) this scan filters for
   * @param scanGroup   shared physical scan reference
   */
  public static EnumerableMergedIndexScan create(
      RelOptCluster cluster,
      MergedIndex mergedIndex,
      int sourceIndex,
      MergedIndexScanGroup scanGroup) {
    Pipeline source = mergedIndex.getSources().get(sourceIndex);
    RelCollation boundaryCollation = source.boundaryCollation;
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replace(boundaryCollation);
    return new EnumerableMergedIndexScan(cluster, traitSet, mergedIndex,
        sourceIndex, scanGroup, null);
  }

  /**
   * Creates an {@code EnumerableMergedIndexScan} with explicit row type.
   *
   * @deprecated Use {@link #create(RelOptCluster, MergedIndex, int, MergedIndexScanGroup)}.
   *     This overload exists for backward compatibility with existing tests and
   *     rules that expect the old joined-row-type semantics.
   */
  @Deprecated
  public static EnumerableMergedIndexScan create(
      RelOptCluster cluster,
      MergedIndex mergedIndex,
      RelDataType rowType) {
    return new EnumerableMergedIndexScan(cluster,
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replace(mergedIndex.getCollation()),
        mergedIndex, 0, new MergedIndexScanGroup(mergedIndex), rowType);
  }

  @Override protected RelDataType deriveRowType() {
    if (rowTypeOverride != null) {
      return rowTypeOverride;
    }
    return mergedIndex.getSources().get(sourceIndex).root.getRowType();
  }

  /**
   * Cost model for a per-source scan over a merged index.
   *
   * <p>A merged index stores records from N sources interleaved by a shared
   * sort key. At query time, a single sequential I/O pass reads the entire
   * index, but each per-source scan only processes its own rows (filtering
   * by domain tag). Multiple per-source scans within the same pipeline share
   * one physical I/O pass via their common {@link MergedIndexScanGroup}.
   *
   * <p>Cost formula:
   * <ul>
   *   <li><b>rowCount</b> = totalRows / sourceCount (only this source's rows)
   *   <li><b>cpu</b> = sourceRows * 0.1 (tag filtering is cheap)
   *   <li><b>io</b> = totalRows (full index scan, shared across siblings)
   * </ul>
   *
   * <p><b>Design decision — why IO is NOT divided by N:</b> each per-source
   * scan reports the full index size as its IO cost. This is intentional:
   * dividing by N would under-count physical IO when the planner evaluates
   * a single scan in isolation. The shared-scan benefit is a buffer-manager
   * property (co-located pages are read once and served to all consumers)
   * that cannot be expressed in Calcite's per-operator cost model. In
   * practice, the Volcano planner compares this scan's cost against the
   * Sort-then-TableScan alternative; even with full IO, the merged index
   * scan wins because it eliminates the O(N log N) sort CPU cost.
   *
   * <p><b>Future refinement:</b> {@link MergedIndexScanGroup} is a
   * placeholder for a cost-sharing model where sibling scans collectively
   * amortize IO. This would require extending {@code RelOptCost} with a
   * group-aware summation (not currently supported by Calcite).
   */
  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double totalRows = mergedIndex.getRowCount();
    double sourceRows = totalRows / scanGroup.sourceCount;
    return planner.getCostFactory().makeCost(sourceRows, sourceRows * 0.1,
        totalRows);
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    return mergedIndex.getRowCount() / scanGroup.sourceCount;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    Pipeline source = mergedIndex.getSources().get(sourceIndex);
    String srcDesc;
    if (source.mergedIndex != null) {
      srcDesc = "view(" + source.mergedIndex.getCollation() + ")";
    } else {
      RelOptTable t = MergedIndex.findLeafScan(source.root);
      srcDesc = (t != null) ? t.getQualifiedName().toString() : "unknown";
    }
    return super.explainTerms(pw)
        .item("mi", mergedIndex.getCollation())
        .item("source", srcDesc)
        .item("sourceIndex", sourceIndex);
  }

  /**
   * Stub implementation: returns an empty enumerable. Real execution happens
   * in a separate storage system with actual B-trees/LSM-trees, not in
   * Calcite. Calcite is used purely for plan generation and cost modeling.
   */
  @Override public Result implement(EnumerableRelImplementor implementor,
      Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final PhysType physType = PhysTypeImpl.of(
        implementor.getTypeFactory(), deriveRowType(), JavaRowFormat.ARRAY);
    builder.add(Expressions.return_(null,
        Expressions.call(BuiltInMethod.EMPTY_ENUMERABLE.method)));
    return implementor.result(physType, builder.toBlock());
  }
}
