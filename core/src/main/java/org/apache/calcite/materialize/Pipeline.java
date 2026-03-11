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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Describes an <em>order-based pipeline</em> — a maximal connected subgraph
 * of a query plan whose operators all share a compatible sort order.
 *
 * <p>Pipelines are identified by {@link org.apache.calcite.adapter.enumerable.EnumerableSort}
 * boundaries: each Sort marks the transition between two pipelines. The Sort's
 * collation is the <em>boundary collation</em> — how the pipeline delivers data
 * to its parent.
 *
 * <p>A Pipeline's {@link #sources} are child pipelines (cut at Sort boundaries).
 * After discovery, a {@link MergedIndex} may be created for this pipeline and
 * stored in the {@link #mergedIndex} field.
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
public class Pipeline {

  /** Topmost operator in this pipeline (inclusive). */
  public final RelNode root;

  /** Child pipelines at Sort boundaries. */
  public final ImmutableList<Pipeline> sources;

  /**
   * Sort order shared by all operators in this pipeline. Derived from
   * the first boundary Sort's collation (the ordering that the pipeline's
   * internal operators, e.g. MergeJoin, actually share).
   */
  public final RelCollation sharedCollation;

  /**
   * Collation of the Sort above this pipeline — how this pipeline
   * delivers data to its parent.
   */
  public final RelCollation boundaryCollation;

  /** Estimated output row count at root. */
  public final double rowCount;

  /**
   * The merged index backing this pipeline, set after registration.
   * Null for pipelines that have not (yet) been matched to a merged index.
   */
  public @Nullable MergedIndex mergedIndex;

  // future work: physicalPlan // query-time or update-time

  public Pipeline(RelNode root, List<Pipeline> sources,
      RelCollation sharedCollation, RelCollation boundaryCollation,
      double rowCount) {
    this.root = root;
    this.sources = ImmutableList.copyOf(sources);
    this.sharedCollation = sharedCollation;
    this.boundaryCollation = boundaryCollation;
    this.rowCount = rowCount;
  }
}
