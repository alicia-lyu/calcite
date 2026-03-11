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
package org.apache.calcite.materialize;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Descriptor for a <em>merged index</em> — a multi-table interleaved B-tree
 * that stores records from several tables/views sorted by a shared key.
 *
 * <p>A merged index on tables (A, B) with collation {@code k ASC} physically
 * interleaves rows from A and B ordered by {@code k}, so that a single
 * sequential scan can execute an entire merge-join pipeline without any
 * run-time sorting.
 *
 * <p>Sources may be base tables ({@link RelOptTable}) or inner pipeline views
 * (other {@code MergedIndex} instances), supporting multi-level interesting
 * ordering pipelines.
 *
 * <p>All information is derived from the backing {@link Pipeline}. Getter
 * methods delegate to the pipeline's fields.
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
public class MergedIndex {

  /**
   * The pipeline that this merged index backs. This is the only stored field;
   * all other information is accessed via getter methods that delegate to
   * the pipeline.
   */
  public final Pipeline pipeline;

  /**
   * Incremental maintenance plan derived by applying {@code DeltaJoinTransposeRule}
   * to this pipeline's join node. Null until set by the test/caller.
   *
   * <p>Future work: move to {@code Pipeline.physicalPlan}.
   */
  private @Nullable RelNode maintenancePlan;

  /**
   * Creates a MergedIndex from a {@link Pipeline}. Sources, collation, and
   * row count are derived from the pipeline's structure, eliminating field
   * duplication. Also sets the pipeline's back-reference to this index.
   *
   * @param pipeline the pipeline backing this merged index
   */
  public MergedIndex(Pipeline pipeline) {
    this.pipeline = pipeline;
    pipeline.mergedIndex = this;
  }

  // -- Getter methods delegating to Pipeline ----------------------------------

  /** Returns the shared sort collation of the pipeline. */
  public RelCollation getCollation() {
    return pipeline.sharedCollation;
  }

  /** Returns the estimated total row count across all tables. */
  public double getRowCount() {
    return pipeline.rowCount;
  }

  /** Returns the child pipelines (sources at Sort boundaries). */
  public ImmutableList<Pipeline> getSources() {
    return pipeline.sources;
  }

  /** Returns per-source boundary collations. */
  public ImmutableList<RelCollation> getSourceCollations() {
    return pipeline.sources.stream()
        .map(c -> c.boundaryCollation)
        .collect(ImmutableList.toImmutableList());
  }

  public void setMaintenancePlan(RelNode plan) {
    this.maintenancePlan = plan;
  }

  public @Nullable RelNode getMaintenancePlan() {
    return maintenancePlan;
  }

  /**
   * Drills through single-input operators to find a leaf
   * {@link org.apache.calcite.adapter.enumerable.EnumerableTableScan}.
   */
  public static @Nullable RelOptTable findLeafScan(RelNode node) {
    if (node instanceof org.apache.calcite.adapter.enumerable.EnumerableTableScan) {
      return ((org.apache.calcite.adapter.enumerable.EnumerableTableScan) node)
          .getTable();
    }
    if (node.getInputs().size() == 1) {
      return findLeafScan(node.getInputs().get(0));
    }
    return null;
  }

  /**
   * Returns {@code true} when this index's collation is a prefix of
   * {@code required}, meaning the index can satisfy the required ordering
   * without additional sorting.
   *
   * <p>For example, an index on {@code (k ASC)} satisfies a requirement for
   * {@code (k ASC)} or {@code (k ASC, x ASC)} (the index key is a prefix),
   * but does NOT satisfy {@code (x ASC)} alone.
   *
   * <p>TODO: FD extension — if index collation is [A] and required is [A, B]
   * where A→B is a functional dependency (e.g. o_orderkey → o_orderdate),
   * this should return true. Currently uses exact prefix matching only.
   */
  public boolean satisfies(RelCollation required) {
    List<RelFieldCollation> indexFields =
        pipeline.sharedCollation.getFieldCollations();
    List<RelFieldCollation> requiredFields = required.getFieldCollations();
    if (indexFields.isEmpty()) {
      return false;
    }
    // The index collation must be a prefix of the required collation.
    if (indexFields.size() > requiredFields.size()) {
      return false;
    }
    for (int i = 0; i < indexFields.size(); i++) {
      RelFieldCollation idxField = indexFields.get(i);
      RelFieldCollation reqField = requiredFields.get(i);
      if (idxField.getFieldIndex() != reqField.getFieldIndex()) {
        return false;
      }
      if (idxField.direction != reqField.direction) {
        return false;
      }
    }
    return true;
  }

  @Override public String toString() {
    return "MergedIndex{sources=" + pipeline.sources
        + ", collation=" + pipeline.sharedCollation + "}";
  }
}
