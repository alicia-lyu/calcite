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

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Descriptor for a <em>merged index</em> — a multi-table interleaved B-tree
 * that stores records from several tables sorted by a shared key.
 *
 * <p>A merged index on tables (A, B) with collation {@code k ASC} physically
 * interleaves rows from A and B ordered by {@code k}, so that a single
 * sequential scan can execute an entire merge-join pipeline without any
 * run-time sorting.
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
public class MergedIndex {

  /** The participating tables, in pipeline order. */
  public final ImmutableList<RelOptTable> tables;

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

  public MergedIndex(List<RelOptTable> tables, RelCollation collation,
      double rowCount) {
    this.tables = ImmutableList.copyOf(tables);
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
   */
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
