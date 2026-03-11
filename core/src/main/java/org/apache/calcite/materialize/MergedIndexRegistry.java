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

  /** Removes all registered indexes. Useful for test isolation. */
  public static synchronized void clear() {
    INDEXES.clear();
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
