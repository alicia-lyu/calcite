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

import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  /**
   * Returns {@code true} if {@code node} is a pipeline-separating Sort.
   *
   * <p>A boundary Sort is an {@link EnumerableSort} with a non-empty
   * collation and no FETCH/OFFSET (LimitSort carries row-count semantics
   * and is not a pipeline boundary).
   */
  public static boolean isBoundarySort(RelNode node) {
    if (!(node instanceof EnumerableSort)) {
      return false;
    }
    final EnumerableSort sort = (EnumerableSort) node;
    if (sort.fetch != null || sort.offset != null) {
      return false;
    }
    return !sort.getCollation().getFieldCollations().isEmpty();
  }

  /**
   * Identifies the assembly subtree — the minimal connected subgraph of
   * operators that will be absorbed into {@code EnumerableMergedIndexAssemble}.
   *
   * <p>The assembly subtree spans from the LCA of all boundary-Sort consumers
   * down to (but not including) the boundary Sorts themselves. Operators above
   * the LCA are "remaining operators" that execute normally on the assembly's
   * joined output.
   *
   * @return the assembly subtree description, or null if no boundary sorts found
   */
  public @Nullable AssemblySubtree findAssemblySubtree() {
    // Step 1: Find boundary sorts by walking from root down
    Set<RelNode> boundarySorts = new LinkedHashSet<>();
    collectBoundarySorts(root, boundarySorts);
    if (boundarySorts.isEmpty()) {
      return null;
    }

    // Step 2: Post-order count of reachable boundary sorts per node
    Map<RelNode, Integer> descendantCount = new IdentityHashMap<>();
    markDescendants(root, boundarySorts, descendantCount);

    // Step 3: Find LCA — deepest node whose count == total boundary sorts
    int total = boundarySorts.size();
    RelNode lca = findLCA(root, total, descendantCount);

    // Step 4: Collect nodes on paths from LCA to boundary sorts
    Set<RelNode> subtreeNodes = new LinkedHashSet<>();
    collectSubtreeNodes(lca, boundarySorts, descendantCount, subtreeNodes);

    return new AssemblySubtree(lca,
        ImmutableSet.copyOf(subtreeNodes),
        ImmutableSet.copyOf(boundarySorts));
  }

  /**
   * Walks the tree from {@code node} down, collecting {@link EnumerableSort}
   * nodes that satisfy {@link #isBoundarySort}. Does NOT recurse below
   * boundary sorts (they belong to child pipelines).
   */
  private static void collectBoundarySorts(RelNode node,
      Set<RelNode> result) {
    for (RelNode input : node.getInputs()) {
      if (isBoundarySort(input)) {
        result.add(input);
      } else {
        collectBoundarySorts(input, result);
      }
    }
  }

  /**
   * Post-order traversal counting how many boundary sorts are reachable
   * from each node. If an input IS a boundary sort, it contributes 1 but
   * we don't recurse into it (it belongs to a child pipeline).
   */
  private static int markDescendants(RelNode node,
      Set<RelNode> boundarySorts, Map<RelNode, Integer> counts) {
    int count = 0;
    for (RelNode input : node.getInputs()) {
      if (boundarySorts.contains(input)) {
        count++;
      } else {
        count += markDescendants(input, boundarySorts, counts);
      }
    }
    counts.put(node, count);
    return count;
  }

  /**
   * Finds the LCA: the deepest node whose descendant count equals the total
   * number of boundary sorts. Walks top-down: if exactly one child carries
   * all counts, recurse into it; if multiple children carry counts, this
   * node is the LCA.
   */
  private static RelNode findLCA(RelNode node, int total,
      Map<RelNode, Integer> counts) {
    for (RelNode input : node.getInputs()) {
      Integer inputCount = counts.get(input);
      if (inputCount != null && inputCount == total) {
        return findLCA(input, total, counts);
      }
    }
    // No single child carries all boundary sorts — this is the LCA
    return node;
  }

  /**
   * From the LCA, collects all nodes on paths to boundary sorts.
   * A node is included if it has descendantCount > 0. Boundary sorts
   * themselves are excluded (they belong to child pipelines).
   */
  private static void collectSubtreeNodes(RelNode node,
      Set<RelNode> boundarySorts, Map<RelNode, Integer> counts,
      Set<RelNode> result) {
    result.add(node);
    for (RelNode input : node.getInputs()) {
      if (boundarySorts.contains(input)) {
        // Boundary sort — don't include it, don't recurse
        continue;
      }
      Integer inputCount = counts.get(input);
      if (inputCount != null && inputCount > 0) {
        collectSubtreeNodes(input, boundarySorts, counts, result);
      }
    }
  }

  /**
   * The assembly subtree — the minimal set of operators absorbed into
   * {@code EnumerableMergedIndexAssemble}.
   *
   * <p>Contains the LCA (assembly subtree root), all nodes on paths from
   * the LCA to the boundary sorts, and the boundary sorts for reference.
   */
  public static class AssemblySubtree {
    /** The root of the assembly subtree (LCA of all boundary consumers). */
    public final RelNode lca;

    /** All operators absorbed into assembly (from LCA down to boundary sorts, exclusive). */
    public final ImmutableSet<RelNode> nodes;

    /** The boundary sorts at the edges of this pipeline (for reference). */
    public final ImmutableSet<RelNode> boundarySorts;

    AssemblySubtree(RelNode lca, ImmutableSet<RelNode> nodes,
        ImmutableSet<RelNode> boundarySorts) {
      this.lca = lca;
      this.nodes = nodes;
      this.boundarySorts = boundarySorts;
    }
  }

  // ── Pipeline discovery (moved from MergedIndexTestUtil) ─────────────────

  /**
   * Builds a pipeline tree from the Phase 1 physical plan by identifying
   * {@link EnumerableSort} operators as boundaries between pipelines.
   *
   * @param planRoot the Phase 1 physical plan root
   * @return the root pipeline of the pipeline tree
   */
  public static Pipeline buildTree(RelNode planRoot) {
    return buildPipeline(planRoot);
  }

  /**
   * Post-order flattening of this pipeline tree: leaves first, root last.
   * Returns all pipelines with at least one source (both multi-source
   * merged indexes and single-source indexed views).
   */
  public List<Pipeline> flatten() {
    final List<Pipeline> result = new ArrayList<>();
    flattenPostOrder(this, result);
    return result;
  }

  /**
   * Safely extracts the first {@link RelCollation} from a node's trait set.
   * Uses {@code getTraits()} to avoid the {@code RelCompositeTrait} cast bug.
   */
  public static @Nullable RelCollation safeGetCollation(RelNode node) {
    final List<RelCollation> collations =
        node.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
    if (collations == null || collations.isEmpty()) {
      return null;
    }
    return collations.get(0);
  }

  /**
   * Recursively builds a single pipeline rooted at {@code node}.
   * Collects child pipelines by finding Sort boundaries among descendants.
   */
  private static Pipeline buildPipeline(RelNode node) {
    final List<Pipeline> childPipelines = new ArrayList<>();
    collectChildPipelines(node, childPipelines);
    final RelCollation collation;
    if (!childPipelines.isEmpty()
        && !childPipelines.get(0).boundaryCollation.getFieldCollations()
            .isEmpty()) {
      collation = childPipelines.get(0).boundaryCollation;
    } else {
      collation = inferCollation(node);
    }
    final double rowCount =
        node.estimateRowCount(node.getCluster().getMetadataQuery());
    return new Pipeline(node, childPipelines, collation, collation, rowCount);
  }

  /**
   * Recurses into {@code node}'s inputs looking for Sort boundaries.
   * When hitting an {@link EnumerableSort} boundary, creates a child Pipeline
   * rooted at the Sort's input. Otherwise continues recursing.
   */
  private static void collectChildPipelines(RelNode node,
      List<Pipeline> result) {
    for (RelNode input : node.getInputs()) {
      if (isBoundarySort(input)) {
        final EnumerableSort sort = (EnumerableSort) input;
        final RelNode below = sort.getInput();
        final Pipeline child = buildPipeline(below);
        result.add(new Pipeline(below, child.sources,
            child.sharedCollation, sort.getCollation(), child.rowCount));
      } else {
        collectChildPipelines(input, result);
      }
    }
  }

  /**
   * Infers the shared collation for a pipeline rooted at {@code node}.
   * First checks the node's own trait set, then drills through single-input
   * operators to find an authoritative Sort.
   */
  private static RelCollation inferCollation(RelNode node) {
    final RelCollation c = safeGetCollation(node);
    if (c != null && !c.getFieldCollations().isEmpty()) {
      return c;
    }
    RelNode cur = node;
    while (cur.getInputs().size() == 1 && !(cur instanceof Sort)) {
      cur = cur.getInputs().get(0);
    }
    if (cur instanceof Sort) {
      return ((Sort) cur).getCollation();
    }
    final RelCollation deep = safeGetCollation(cur);
    if (deep != null && !deep.getFieldCollations().isEmpty()) {
      return deep;
    }
    return RelCollations.EMPTY;
  }

  private static void flattenPostOrder(Pipeline p, List<Pipeline> result) {
    for (Pipeline child : p.sources) {
      flattenPostOrder(child, result);
    }
    if (p.sources.size() >= 1) {
      result.add(p);
    }
  }
}
