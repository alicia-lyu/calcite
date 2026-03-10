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
import java.util.stream.Stream;

/**
 * Descriptor for a <em>merged index</em> — a multi-table interleaved B-tree
 * that stores records from several tables sorted by a shared key.
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
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
public class MergedIndex {

  /**
   * Each element is a {@link RelOptTable} (base table) or a
   * {@link MergedIndex} (inner pipeline view).
   */
  public final ImmutableList<Object> sources;

  /**
   * The participating base tables, in pipeline order (flat list expanding
   * any inner {@link MergedIndex} views to their constituent tables).
   */
  public final ImmutableList<RelOptTable> tables;

  /**
   * Per-source sort collation: {@code tableCollations.get(i)} is the collation
   * of the {@link org.apache.calcite.adapter.enumerable.EnumerableSort} that
   * feeds source {@code i} into the pipeline.
   */
  public final ImmutableList<RelCollation> tableCollations;

  /**
   * The shared sort key used by all operators in the pipeline
   * (merge joins and sorted aggregations).
   */
  public final RelCollation collation;

  /**
   * Estimated total row count across all tables in the index.
   * Used by the cost model in {@code EnumerableMergedIndexScan}.
   */
  public final double rowCount;

  /**
   * Incremental maintenance plan derived by applying {@code DeltaJoinTransposeRule}
   * to this pipeline's join node. Null for manually registered indexes that do not
   * go through the Pipeline discovery path.
   */
  public @Nullable RelNode maintenancePlan;

  /**
   * Constructor for base-table-only pipelines (most common case).
   *
   * @param tables           base tables in pipeline order
   * @param tableCollations  per-table sort collations
   * @param collation        shared sort key of the pipeline
   * @param rowCount         estimated total row count
   */
  public MergedIndex(List<RelOptTable> tables,
      List<RelCollation> tableCollations,
      RelCollation collation, double rowCount) {
    this.sources = tables.stream().map(t -> (Object) t)
        .collect(ImmutableList.toImmutableList());
    this.tables = ImmutableList.copyOf(tables);
    this.tableCollations = ImmutableList.copyOf(tableCollations);
    this.collation = collation;
    this.rowCount = rowCount;
  }

  /**
   * Static factory for pipelines with mixed base-table / view sources.
   * Use this when any source is a {@link MergedIndex} inner pipeline view.
   *
   * @param sources          each element is a {@link RelOptTable} or a
   *                         {@link MergedIndex} (inner pipeline view)
   * @param sourceCollations per-source sort collations
   * @param collation        shared sort key of the pipeline
   * @param rowCount         estimated total row count
   */
  public static MergedIndex of(List<Object> sources,
      List<RelCollation> sourceCollations,
      RelCollation collation, double rowCount) {
    ImmutableList<RelOptTable> tables = sources.stream()
        .flatMap(s -> s instanceof MergedIndex
            ? ((MergedIndex) s).tables.stream()
            : Stream.of((RelOptTable) s))
        .collect(ImmutableList.toImmutableList());
    return new MergedIndex(
        ImmutableList.copyOf(sources), tables,
        ImmutableList.copyOf(sourceCollations), collation, rowCount);
  }

  /** Private constructor used by {@link #of}. */
  private MergedIndex(ImmutableList<Object> sources,
      ImmutableList<RelOptTable> tables,
      ImmutableList<RelCollation> tableCollations,
      RelCollation collation, double rowCount) {
    this.sources = sources;
    this.tables = tables;
    this.tableCollations = tableCollations;
    this.collation = collation;
    this.rowCount = rowCount;
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
  public void setMaintenancePlan(RelNode plan) { this.maintenancePlan = plan; }

  public @Nullable RelNode getMaintenancePlan() { return maintenancePlan; }

  public boolean satisfies(RelCollation required) {
    List<RelFieldCollation> indexFields = collation.getFieldCollations();
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
    return "MergedIndex{tables=" + tables + ", collation=" + collation + "}";
  }
}
