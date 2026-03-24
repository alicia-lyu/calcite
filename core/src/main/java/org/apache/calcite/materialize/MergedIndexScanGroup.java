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

/**
 * Shared metadata object representing a single physical scan over a
 * {@link MergedIndex}. All per-source {@code EnumerableMergedIndexScan} nodes
 * that belong to the same pipeline hold a reference to the same
 * {@code MergedIndexScanGroup} instance, signifying that they share one
 * physical I/O pass over the merged index.
 *
 * <p><b>Why this class exists:</b> a merged index with N sources produces N
 * separate {@code EnumerableMergedIndexScan} nodes in the plan (one per
 * source), but these N nodes correspond to one physical sequential scan of
 * the B-tree. The scan group makes this sharing explicit so that:
 * <ul>
 *   <li>The cost model in {@code EnumerableMergedIndexScan.computeSelfCost()}
 *       can reference {@link #sourceCount} to split row counts.
 *   <li>Future cost-sharing refinements (amortized IO across siblings) have a
 *       natural home — extend this class rather than adding cross-references
 *       between scan operators.
 *   <li>Plan output can show group identity (e.g., same label for sibling scans).
 * </ul>
 *
 * <p>Deliberately minimal for now — contains only the merged index reference
 * and source count. Future work: track cumulative IO, support group-aware
 * cost summation in the planner.
 *
 * <p>See: "Storing and Indexing Multiple Tables by Interesting Orderings",
 * Wenhui Lyu &amp; Goetz Graefe, VLDB 2026.
 */
public class MergedIndexScanGroup {

  /** The merged index backing this scan group. */
  public final MergedIndex mergedIndex;

  /** Number of sources (per-source scans) sharing this physical scan. */
  public final int sourceCount;

  /**
   * Creates a scan group for the given merged index.
   *
   * @param mergedIndex the merged index that all per-source scans read from
   */
  public MergedIndexScanGroup(MergedIndex mergedIndex) {
    this.mergedIndex = mergedIndex;
    this.sourceCount = mergedIndex.getSources().size();
  }
}
