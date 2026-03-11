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
import java.util.stream.Collectors;

/**
 * Static singleton registry that maps a set of tables to the
 * {@link MergedIndex} instances that cover them.
 *
 * <p>Usage:
 * <pre>{@code
 * MergedIndexRegistry.register(new MergedIndex(List.of(tableA, tableB), collation, rowCount));
 * Optional<MergedIndex> idx = MergedIndexRegistry.findFor(List.of(tableA, tableB), collation);
 * }</pre>
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
   * Finds a registered {@link MergedIndex} whose sources match exactly
   * {@code sources} (in any order) and whose collation satisfies
   * {@code required}.
   *
   * <p>Sources may be {@link RelOptTable} (matched by qualified name) or
   * {@link MergedIndex} (matched by object identity).
   *
   * @param sources  the sources participating in the pipeline
   * @param required the collation that the pipeline requires
   * @return the first matching index, or empty if none
   */
  public static synchronized Optional<MergedIndex> findFor(
      List<Object> sources, RelCollation required) {
    for (MergedIndex index : INDEXES) {
      if (sourcesMatch(index.sources, sources) && index.satisfies(required)) { // index.sources -> index.pipeline.sources
        return Optional.of(index);
      }
    }
    return Optional.empty();
  }

  /** Removes all registered indexes. Useful for test isolation. */
  public static synchronized void clear() {
    INDEXES.clear();
  }

  private static boolean sourcesMatch(List<Object> a, List<Object> b) {
    if (a.size() != b.size()) {
      return false;
    }
    outer:
    for (Object ai : a) {
      for (Object bi : b) {
        if (sourceEquals(ai, bi)) {
          continue outer;
        }
      }
      return false;
    }
    return true;
  }

  // sources become pipelines in the future
  private static boolean sourceEquals(Object a, Object b) {
    if (a instanceof RelOptTable && b instanceof RelOptTable) {
      return ((RelOptTable) a).getQualifiedName()
          .equals(((RelOptTable) b).getQualifiedName());
    }
    if (a instanceof MergedIndex && b instanceof MergedIndex) {
      return a == b; // identity: same Java object
    }
    return false;
  }
}
