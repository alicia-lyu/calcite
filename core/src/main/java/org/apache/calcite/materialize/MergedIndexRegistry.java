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
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Static singleton registry that maps a set of tables to the
 * {@link MergedIndex} instances that cover them.
 *
 * <p>The registry is process-global and thread-safe (registrations are
 * synchronized). It is intended for testing and proof-of-concept scenarios;
 * a production implementation would integrate with the schema/catalog layer.
 */
public final class MergedIndexRegistry {

  private static final List<MergedIndex> INDEXES = new ArrayList<>();

  private MergedIndexRegistry() {
  }

  /** Registers a merged index. */
  public static synchronized void register(MergedIndex index) {
    INDEXES.add(index);
  }

  /**
   * Finds a registered {@link MergedIndex} whose pipeline sources match
   * exactly {@code querySources} (in any order) and whose collation satisfies
   * {@code required}.
   *
   * <p>Query sources may be {@link RelOptTable} (matched by qualified name
   * against the leaf scan of a Pipeline source) or {@link MergedIndex}
   * (matched by identity against a Pipeline source's {@code mergedIndex}).
   *
   * @param querySources the sources extracted from the query pipeline
   * @param required     the collation that the pipeline requires
   * @return the first matching index, or empty if none
   */
  public static synchronized Optional<MergedIndex> findFor(
      List<Object> querySources, RelCollation required) {
    for (MergedIndex index : INDEXES) {
      if (sourcesMatch(index.pipeline.sources, querySources)
          && index.satisfies(required)) {
        return Optional.of(index);
      }
    }
    return Optional.empty();
  }

  /**
   * Finds a registered {@link MergedIndex} containing {@code sortInput} as one
   * of its pipeline sources, with a boundary collation matching {@code required}.
   *
   * <p>Matching strategy (robust against HEP node copying):
   * <ol>
   *   <li><b>Identity</b> (fast path): {@code source.root == sortInput}.
   *       Works for leaf {@code TableScan} nodes which HEP does not copy.
   *   <li><b>Base table</b>: both the source root and sortInput are drilled
   *       to their leaf {@code TableScan} and compared by qualified name.
   *   <li><b>View source</b> (source has a {@code mergedIndex}): searches
   *       the sortInput subtree for an {@code EnumerableMergedIndexScan}
   *       referencing the same {@code MergedIndex} instance.
   * </ol>
   *
   * <p>Collation is checked per-source: the source's {@code boundaryCollation}
   * must match the Sort's collation, because field indices are relative to
   * each source's own row type (not the MI's shared collation).
   *
   * @param sortInput the node directly below the Sort boundary
   * @param required  the collation of the Sort being matched
   * @return a match describing the merged index and source position, or empty
   */
  public static synchronized Optional<SourceMatch> findForSource(
      RelNode sortInput, RelCollation required) {
    for (MergedIndex index : INDEXES) {
      List<Pipeline> sources = index.pipeline.sources;
      for (int i = 0; i < sources.size(); i++) {
        Pipeline source = sources.get(i);
        if (!collationMatches(source.boundaryCollation, required)) {
          continue;
        }
        if (sourceMatchesSortInput(source, sortInput)) {
          return Optional.of(new SourceMatch(index, i));
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Checks whether a pipeline source structurally matches a Sort's input.
   *
   * <ol>
   *   <li>Identity match (leaf TableScans survive HEP unchanged).
   *   <li>Leaf TableScan qualified-name match (for base-table sources whose
   *       root is a non-leaf operator like Project or Aggregate).
   *   <li>MergedIndex identity match (for view sources after inner pipeline
   *       Sorts have been replaced by MIScans in a prior HEP pass).
   * </ol>
   */
  private static boolean sourceMatchesSortInput(Pipeline source,
      RelNode sortInput) {
    // Fast path: identity (works for leaf TableScans and same-planner nodes)
    if (source.root == sortInput) {
      return true;
    }
    // Base table source: compare leaf TableScan qualified names.
    // source.root is from the original Phase 1 plan (no HepRelVertex).
    // sortInput is from HEP (may contain HepRelVertex children).
    if (source.mergedIndex == null) {
      final RelOptTable sourceLeaf = MergedIndex.findLeafScan(source.root);
      final RelOptTable inputLeaf = findLeafScanHep(sortInput);
      if (sourceLeaf != null && inputLeaf != null) {
        return sourceLeaf.getQualifiedName()
            .equals(inputLeaf.getQualifiedName());
      }
    }
    // View source: search sortInput subtree for a MIScan with matching MI
    if (source.mergedIndex != null) {
      return containsMergedIndex(sortInput, source.mergedIndex);
    }
    return false;
  }

  /** Unwraps a {@link HepRelVertex} to its current rel. */
  private static RelNode unwrap(RelNode node) {
    return node instanceof HepRelVertex
        ? ((HepRelVertex) node).getCurrentRel() : node;
  }

  /**
   * Like {@link MergedIndex#findLeafScan} but unwraps {@link HepRelVertex}
   * at each level, so it works inside a HEP planner graph.
   */
  private static @org.checkerframework.checker.nullness.qual.Nullable
      RelOptTable findLeafScanHep(RelNode node) {
    final RelNode n = unwrap(node);
    if (n instanceof
        org.apache.calcite.adapter.enumerable.EnumerableTableScan) {
      return ((org.apache.calcite.adapter.enumerable.EnumerableTableScan) n)
          .getTable();
    }
    if (n.getInputs().size() == 1) {
      return findLeafScanHep(n.getInputs().get(0));
    }
    return null;
  }

  /**
   * Searches the subtree rooted at {@code node} for an
   * {@code EnumerableMergedIndexScan} referencing {@code mi}.
   * Unwraps {@link HepRelVertex} at each level.
   */
  private static boolean containsMergedIndex(RelNode node, MergedIndex mi) {
    final RelNode n = unwrap(node);
    if (n instanceof
        org.apache.calcite.adapter.enumerable.EnumerableMergedIndexScan) {
      if (((org.apache.calcite.adapter.enumerable.EnumerableMergedIndexScan)
          n).mergedIndex == mi) {
        return true;
      }
    }
    for (RelNode input : n.getInputs()) {
      if (containsMergedIndex(input, mi)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether a source's boundary collation matches the Sort's collation.
   * Both must have the same field indices and directions (exact match, not
   * prefix), because the Sort was derived from this source during discovery.
   */
  private static boolean collationMatches(RelCollation boundary,
      RelCollation required) {
    List<RelFieldCollation> bFields = boundary.getFieldCollations();
    List<RelFieldCollation> rFields = required.getFieldCollations();
    if (bFields.size() != rFields.size()) {
      return false;
    }
    for (int i = 0; i < bFields.size(); i++) {
      if (bFields.get(i).getFieldIndex() != rFields.get(i).getFieldIndex()) {
        return false;
      }
      if (bFields.get(i).direction != rFields.get(i).direction) {
        return false;
      }
    }
    return true;
  }

  /** Removes all registered indexes. Useful for test isolation. */
  public static synchronized void clear() {
    INDEXES.clear();
  }

  /** Result of a single-source lookup in the registry. */
  public static class SourceMatch {
    /** The merged index containing the source. */
    public final MergedIndex mergedIndex;
    /** Position of the source within the merged index's pipeline sources. */
    public final int sourceIndex;

    SourceMatch(MergedIndex mergedIndex, int sourceIndex) {
      this.mergedIndex = mergedIndex;
      this.sourceIndex = sourceIndex;
    }
  }

  /**
   * Checks whether the registered index's Pipeline sources match the query's
   * extracted sources (RelOptTable or MergedIndex objects).
   */
  private static boolean sourcesMatch(List<Pipeline> indexSources,
      List<Object> querySources) {
    if (indexSources.size() != querySources.size()) {
      return false;
    }
    outer:
    for (Pipeline pipelineSrc : indexSources) {
      for (Object querySrc : querySources) {
        if (pipelineMatchesSource(pipelineSrc, querySrc)) {
          continue outer;
        }
      }
      return false;
    }
    return true;
  }

  /**
   * Matches a Pipeline child source against a query-extracted source.
   *
   * <ul>
   *   <li>If the Pipeline child has a {@code mergedIndex} and the query source
   *       is a {@code MergedIndex}: identity match.
   *   <li>If the query source is a {@code RelOptTable}: find the leaf table
   *       scan in the Pipeline child's root and compare qualified names.
   * </ul>
   */
  private static boolean pipelineMatchesSource(Pipeline pipelineSrc,
      Object querySrc) {
    if (querySrc instanceof MergedIndex && pipelineSrc.mergedIndex != null) {
      return pipelineSrc.mergedIndex == querySrc;
    }
    if (querySrc instanceof RelOptTable) {
      final RelOptTable leafTable = MergedIndex.findLeafScan(pipelineSrc.root);
      if (leafTable != null) {
        return leafTable.getQualifiedName()
            .equals(((RelOptTable) querySrc).getQualifiedName());
      }
    }
    return false;
  }
}
