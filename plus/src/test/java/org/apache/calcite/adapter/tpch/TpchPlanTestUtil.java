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
package org.apache.calcite.adapter.tpch;

import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableMergedIndexDeltaScan;
import org.apache.calcite.adapter.enumerable.EnumerableMergedIndexScan;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.materialize.MergedIndex;
import org.apache.calcite.materialize.Pipeline;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalPipelineOutputScan;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shared TPC-H plan test utilities: DOT visualization and plan-rewriting helpers.
 *
 * <p>All methods are {@code static}; this class is not instantiated.
 * Used by {@link MergedIndexTpchPlanTest} and
 * {@code MergedIndexSinglePipelineTpchPlanTest}.
 */
final class TpchPlanTestUtil {

  private TpchPlanTestUtil() {}

  // ── IVM helpers ────────────────────────────────────────────────────────────

  /**
   * If the root is a Sort (ORDER BY) whose input chain contains an Aggregate
   * with a Sort (GROUP BY) on the same key fields but different directions,
   * propagates the ORDER BY directions to the GROUP BY sort.
   *
   * <p>The ORDER BY Sort is <em>removed</em> from the plan after direction
   * propagation — the GROUP BY sort already produces output in the correct
   * order, so the ORDER BY is redundant. Removing it reduces the pipeline
   * count by one (one fewer boundary sort → one fewer indexed view pipeline).
   * The caller must strip the collation from {@code desiredTraits} before
   * passing to Volcano, since the root output no longer has a Sort node.
   *
   * <p>This fixes Q9: the GROUP BY sort {@code (n_name ASC, o_year ASC)}
   * becomes {@code (n_name ASC, o_year DESC)} matching the ORDER BY, and
   * the redundant ORDER BY sort is eliminated.
   *
   * @param node the plan root (potentially a Sort)
   * @return the plan with propagated directions, or unchanged if no match
   */
  static RelNode propagateOrderByDirection(RelNode node) {
    if (!(node instanceof Sort)) {
      return node;
    }
    final Sort orderBy = (Sort) node;
    if (orderBy.fetch != null || orderBy.offset != null) {
      return node;
    }

    // Drill through single-input operators to find the Aggregate
    RelNode cur = orderBy.getInput();
    final List<RelNode> chain = new ArrayList<>();
    while (cur != null && !(cur instanceof Aggregate)
        && cur.getInputs().size() == 1) {
      chain.add(cur);
      cur = cur.getInputs().get(0);
    }
    if (!(cur instanceof Aggregate)) {
      return node;
    }

    final Aggregate agg = (Aggregate) cur;
    final RelNode aggInput = agg.getInput();
    if (!(aggInput instanceof Sort)) {
      return node;
    }

    final Sort groupBy = (Sort) aggInput;
    final List<RelFieldCollation> obFields =
        orderBy.getCollation().getFieldCollations();
    final List<RelFieldCollation> gbFields =
        groupBy.getCollation().getFieldCollations();
    if (obFields.size() != gbFields.size()) {
      return node;
    }

    // Check same key fields and whether directions differ
    boolean sameKeys = true;
    boolean sameDirections = true;
    for (int i = 0; i < obFields.size(); i++) {
      if (obFields.get(i).getFieldIndex() != gbFields.get(i).getFieldIndex()) {
        sameKeys = false;
        break;
      }
      if (obFields.get(i).getDirection() != gbFields.get(i).getDirection()) {
        sameDirections = false;
      }
    }
    if (!sameKeys || sameDirections) {
      return node; // fields don't match, or directions already match
    }

    // Build new GROUP BY collation with ORDER BY directions
    final List<RelFieldCollation> newGbFields = new ArrayList<>();
    for (int i = 0; i < gbFields.size(); i++) {
      newGbFields.add(new RelFieldCollation(
          gbFields.get(i).getFieldIndex(),
          obFields.get(i).getDirection()));
    }
    final RelNode newGroupBy = LogicalSort.create(
        groupBy.getInput(), RelCollations.of(newGbFields), null, null);
    RelNode result = agg.copy(agg.getTraitSet(), List.of(newGroupBy));

    // Rebuild intermediate nodes (between ORDER BY and Aggregate)
    for (int i = chain.size() - 1; i >= 0; i--) {
      final RelNode n = chain.get(i);
      result = n.copy(n.getTraitSet(), List.of(result));
    }
    // Drop the ORDER BY Sort: after direction propagation the GROUP BY sort
    // already produces output in (n_name ASC, o_year DESC) order, making the
    // ORDER BY redundant. Removing it reduces the pipeline count by one
    // (one fewer boundary sort → one fewer indexed view pipeline).
    return result;
  }

  /** Prints maintenance plans for all pipelines to stdout. */
  static void printMaintenancePlans(String label, List<Pipeline> pipelines) {
    System.out.println("=== " + label + " MAINTENANCE PLANS ===");
    for (int i = 0; i < pipelines.size(); i++) {
      System.out.println("-- Level " + i + ": " + pipelines.get(i).mergedIndex);
      final RelNode plan = pipelines.get(i).mergedIndex.getMaintenancePlan();
      System.out.println(plan != null ? dumpText(plan) : "  (none)");
    }
  }

  // ── DOT file writers ────────────────────────────────────────────────────────

  /**
   * Writes two Graphviz DOT plan files to {@code test-dot-output/}:
   *
   * <ul>
   *   <li>{@code <name>.dot} — plain format: full first-line explain label, no colors.
   *   <li>{@code <name>_color.dot} — presentation format: color-coded nodes with
   *       shortened labels using actual column names instead of {@code $N} indices.
   * </ul>
   */
  static void writeDotFile(String name, RelNode rel) {
    writeDotFile("test-dot-output", name, rel);
  }

  /** Like {@link #writeDotFile(String, RelNode)} but writes to {@code baseDir}. */
  static void writeDotFile(String baseDir, String name, RelNode rel) {
    writeDotToFile(baseDir, name + ".dot", dumpDot(rel));
    writeDotToFile(baseDir, name + "_color.dot", dumpDotColor(rel));
  }

  /**
   * Like {@link #writeDotFile(String, RelNode)} but annotates the color DOT
   * with pipeline-boundary clusters when {@code rootPipeline} is non-null.
   * The plain {@code .dot} file is unchanged.
   */
  static void writeDotFile(String name, RelNode rel,
      @org.checkerframework.checker.nullness.qual.Nullable Pipeline rootPipeline) {
    writeDotFile("test-dot-output", name, rel, rootPipeline);
  }

  /**
   * Like {@link #writeDotFile(String, RelNode, Pipeline)} but writes to {@code baseDir}.
   */
  static void writeDotFile(String baseDir, String name, RelNode rel,
      @org.checkerframework.checker.nullness.qual.Nullable Pipeline rootPipeline) {
    writeDotToFile(baseDir, name + ".dot", dumpDot(rel));
    if (rootPipeline != null) {
      writeDotToFile(baseDir, name + "_color.dot",
          dumpDotColorWithPipelines(rel, rootPipeline));
    } else {
      writeDotToFile(baseDir, name + "_color.dot", dumpDotColor(rel));
    }
  }

  /**
   * Writes tree-mode DOT files (no identity dedup) for {@code rel}.
   * Unlike {@link #writeDotFile}, this never merges shared nodes —
   * each traversal visit gets a fresh DOT node ID.
   */
  static void writeDotFileTree(String name, RelNode rel) {
    writeDotFileTree("test-dot-output", name, rel);
  }

  /** Like {@link #writeDotFileTree(String, RelNode)} but writes to {@code baseDir}. */
  static void writeDotFileTree(String baseDir, String name, RelNode rel) {
    writeDotToFile(baseDir, name + ".dot", dumpDotTree(rel));
    writeDotToFile(baseDir, name + "_color.dot", dumpDotColorTree(rel));
  }

  static void writeDotToFile(String filename, String content) {
    writeDotToFile("test-dot-output", filename, content);
  }

  /** Writes {@code content} to {@code baseDir/filename}, creating directories as needed. */
  static void writeDotToFile(String baseDir, String filename, String content) {
    final java.nio.file.Path file = java.nio.file.Paths.get(baseDir, filename);
    try {
      java.nio.file.Files.createDirectories(file.getParent());
      java.nio.file.Files.writeString(file, content);
      System.out.println("DOT written -> " + file.toAbsolutePath());
    } catch (java.io.IOException e) {
      System.err.println("Failed to write DOT file " + filename + ": " + e.getMessage());
    }
  }

  // ── Text dump ──────────────────────────────────────────────────────────────

  static String dumpText(RelNode rel) {
    return RelOptUtil.dumpPlan("", rel, SqlExplainFormat.TEXT,
        SqlExplainLevel.EXPPLAN_ATTRIBUTES);
  }

  // ── Plain DOT ──────────────────────────────────────────────────────────────

  /**
   * Plain DOT: each node gets its full first-line explain label and no color.
   * Every node has a unique integer ID so visually identical siblings
   * (e.g., two {@code EnumerableSort(sort0=[$0])} nodes) are never merged.
   */
  static String dumpDot(RelNode root) {
    final StringBuilder sb = new StringBuilder("digraph {\n  rankdir=BT;\n");
    final IdentityHashMap<RelNode, Integer> ids = new IdentityHashMap<>();
    final int[] counter = {0};
    dumpDotNode(root, sb, ids, counter);
    sb.append("}\n");
    return sb.toString();
  }

  static int dumpDotNode(RelNode node, StringBuilder sb,
      IdentityHashMap<RelNode, Integer> ids, int[] counter) {
    if (ids.containsKey(node)) {
      return ids.get(node);
    }
    final int id = counter[0]++;
    ids.put(node, id);
    final String explain = RelOptUtil.dumpPlan("", node,
        SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES).trim();
    final String label = explain.lines().findFirst()
        .orElse(node.getClass().getSimpleName()).trim().replace("\"", "'");
    sb.append("  n").append(id).append(" [label=\"").append(label).append("\"];\n");
    final List<RelNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      final int childId = dumpDotNode(inputs.get(i), sb, ids, counter);
      sb.append("  n").append(childId).append(" -> n").append(id)
          .append(" [label=\"").append(i).append("\"];\n");
    }
    return id;
  }

  // ── Color DOT ──────────────────────────────────────────────────────────────

  /**
   * Colorful DOT: nodes are color-coded by operator type and labeled with
   * short, human-readable names — actual column names instead of {@code $N}
   * field-index references, and without the {@code Enumerable} prefix.
   *
   * <p>Color legend:
   * <ul>
   *   <li>MergedIndexScan — light green (#90EE90)
   *   <li>MergedIndexJoin — lime green (#32CD32)
   *   <li>MergeJoin — gold (#FFD700)
   *   <li>Sort / LimitSort — light salmon (#FFA07A)
   *   <li>TableScan — light blue (#ADD8E6)
   *   <li>Aggregate — plum (#DDA0DD)
   *   <li>Project — light gray (#D3D3D3)
   *   <li>Filter — peach (#FFDAB9)
   * </ul>
   *
   * <p>Two-phase approach:
   * <ol>
   *   <li>Phase A: walk the plan collecting node-definition strings and edge strings.
   *   <li>Phase B: group {@link EnumerableMergedIndexScan} nodes by shared
   *       {@link MergedIndex} identity; emit one {@code subgraph cluster_mi_N} per
   *       group of 2+ scans, then emit remaining nodes and all edges.
   * </ol>
   */
  static String dumpDotColor(RelNode root) {
    // Phase A: collect IDs, node defs, edge defs.
    final IdentityHashMap<RelNode, Integer> ids = new IdentityHashMap<>();
    final int[] counter = {0};
    final Map<Integer, String> nodeDefs = new HashMap<>();
    final List<String> edgeDefs = new ArrayList<>();
    collectNodesAndEdges(root, ids, counter, nodeDefs, edgeDefs);

    // Group MIScan node IDs by shared MergedIndex identity (groups of 2+ only).
    final IdentityHashMap<MergedIndex, List<Integer>> miGroups = new IdentityHashMap<>();
    for (Map.Entry<RelNode, Integer> entry : ids.entrySet()) {
      if (entry.getKey() instanceof EnumerableMergedIndexScan) {
        final EnumerableMergedIndexScan scan = (EnumerableMergedIndexScan) entry.getKey();
        miGroups.computeIfAbsent(scan.mergedIndex, k -> new ArrayList<>())
            .add(entry.getValue());
      }
      if (entry.getKey() instanceof EnumerableMergedIndexDeltaScan) {
        final EnumerableMergedIndexDeltaScan scan =
            (EnumerableMergedIndexDeltaScan) entry.getKey();
        miGroups.computeIfAbsent(scan.mergedIndex, k -> new ArrayList<>())
            .add(entry.getValue());
      }
    }

    // Collect node IDs that belong to a cluster (2+ scans sharing one MI).
    final IdentityHashMap<Integer, Boolean> clustered = new IdentityHashMap<>();
    final List<Map.Entry<MergedIndex, List<Integer>>> clusterEntries = new ArrayList<>();
    for (Map.Entry<MergedIndex, List<Integer>> e : miGroups.entrySet()) {
      if (e.getValue().size() >= 2) {
        clusterEntries.add(e);
        for (int nid : e.getValue()) {
          clustered.put(nid, Boolean.TRUE);
        }
      }
    }

    // Phase B: emit DOT.
    final StringBuilder sb = new StringBuilder(
        "digraph {\n  rankdir=BT;\n  node [fontname=\"Helvetica\"];\n");

    // Emit MI clusters.
    for (int i = 0; i < clusterEntries.size(); i++) {
      final List<Integer> nodeIds = clusterEntries.get(i).getValue();
      sb.append("  subgraph cluster_mi_").append(i).append(" {\n");
      sb.append("    label=\"Shared Merged Index\";\n");
      sb.append("    style=dashed; color=\"#228B22\"; fontname=\"Helvetica\";\n");
      for (int nid : nodeIds) {
        final String def = nodeDefs.get(nid);
        if (def != null) {
          sb.append("  ").append(def);
        }
      }
      sb.append("  }\n");
    }

    // Emit non-clustered nodes.
    for (Map.Entry<Integer, String> e : nodeDefs.entrySet()) {
      if (!clustered.containsKey(e.getKey())) {
        sb.append(e.getValue());
      }
    }

    // All edges outside clusters.
    for (String edge : edgeDefs) {
      sb.append(edge);
    }

    sb.append("}\n");
    return sb.toString();
  }

  // ── Tree-mode DOT ──────────────────────────────────────────────────────────

  /**
   * Renders a RelNode graph as a DOT string in tree mode (no dedup).
   * Each visit to a shared node produces a separate DOT node,
   * ensuring the output is always a tree even if the input is a DAG.
   *
   * <p>Motivation: {@code LogicalTableScan.copy()} returns {@code this},
   * so identity-based dedup in {@link #dumpDotColor} cannot be fixed by
   * copying leaf nodes. Per-visit IDs bypass the problem entirely.
   */
  static String dumpDotTree(RelNode rel) {
    final StringBuilder sb = new StringBuilder("digraph {\n  rankdir=BT;\n");
    final int[] counter = {0};
    dumpDotTreeNode(rel, sb, counter);
    sb.append("}\n");
    return sb.toString();
  }

  /** Recursively renders nodes in tree mode — no identity dedup.
   * Returns the node ID assigned to this visit. */
  static int dumpDotTreeNode(RelNode node, StringBuilder sb, int[] counter) {
    final int id = counter[0]++;
    final String explain = RelOptUtil.dumpPlan("", node,
        SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES).trim();
    final String label = explain.lines().findFirst()
        .orElse(node.getClass().getSimpleName()).trim().replace("\"", "'");
    sb.append("  n").append(id).append(" [label=\"").append(label)
        .append("\\n#").append(id).append("\"];\n");
    final List<RelNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      final int childId = dumpDotTreeNode(inputs.get(i), sb, counter);
      sb.append("  n").append(childId).append(" -> n").append(id).append(";\n");
    }
    return id;
  }

  /**
   * Renders a RelNode graph as a colorful DOT string in tree mode (no dedup).
   * Each visit to a shared node produces a separate DOT node,
   * ensuring the output is always a tree even if the input is a DAG.
   */
  static String dumpDotColorTree(RelNode rel) {
    final StringBuilder sb = new StringBuilder(
        "digraph {\n  rankdir=BT;\n  node [fontname=\"Helvetica\"];\n");
    final int[] counter = {0};
    dumpDotColorTreeNode(rel, sb, counter);
    sb.append("}\n");
    return sb.toString();
  }

  /** Recursively renders nodes in tree mode with colors — no identity dedup.
   * Returns the node ID assigned to this visit. */
  static int dumpDotColorTreeNode(RelNode node, StringBuilder sb, int[] counter) {
    final int id = counter[0]++;
    final String explain = RelOptUtil.dumpPlan("", node,
        SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES).trim();
    final String firstLine = explain.lines().findFirst()
        .orElse(node.getClass().getSimpleName()).trim();
    final String label = nodeLabel(node, firstLine);
    final String color = nodeColor(node);
    sb.append("  n").append(id)
        .append(" [label=\"").append(label).append("\"")
        .append(", style=filled, fillcolor=\"").append(color).append("\"")
        .append("];\n");
    final List<RelNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      final RelNode child = skipTransparent(inputs.get(i));
      final int childId = dumpDotColorTreeNode(child, sb, counter);
      final String edgeLabel = inputs.size() > 1 ? (i == 0 ? "left" : "right") : "";
      sb.append("  n").append(childId).append(" -> n").append(id);
      if (!edgeLabel.isEmpty()) {
        sb.append(" [label=\"").append(edgeLabel).append("\"]");
      }
      sb.append(";\n");
    }
    return id;
  }

  // ── Pipeline-annotated DOT helpers ─────────────────────────────────────────

  /**
   * Generates a colorful DOT string with {@code subgraph cluster_N} boxes
   * drawn around each pipeline's nodes.
   *
   * <p>Two-phase approach:
   * <ol>
   *   <li>Phase A: walk the plan collecting node-definition strings and
   *       edge strings, keyed by integer node ID.
   *   <li>Phase B: group node IDs by pipeline, emit one {@code subgraph cluster_N}
   *       per pipeline, then emit all edges outside the clusters.
   * </ol>
   */
  static String dumpDotColorWithPipelines(RelNode root, Pipeline rootPipeline) {
    // Phase A: collect IDs, node defs, edge defs.
    final IdentityHashMap<RelNode, Integer> ids = new IdentityHashMap<>();
    final int[] counter = {0};
    final Map<Integer, String> nodeDefs = new HashMap<>();
    final List<String> edgeDefs = new ArrayList<>();
    collectNodesAndEdges(root, ids, counter, nodeDefs, edgeDefs);

    // Build pipeline assignment map (node → pipeline).
    final IdentityHashMap<RelNode, Pipeline> pipelineMap =
        assignNodesToPipelines(rootPipeline);

    // Group node IDs by pipeline (using identity of Pipeline objects).
    final List<Pipeline> pipelineList = rootPipeline.flatten();
    final IdentityHashMap<Pipeline, List<Integer>> pipelineNodeIds =
        new IdentityHashMap<>();
    for (Pipeline p : pipelineList) {
      pipelineNodeIds.put(p, new ArrayList<>());
    }
    final List<Integer> ungrouped = new ArrayList<>();
    for (Map.Entry<RelNode, Integer> entry : ids.entrySet()) {
      final Pipeline p = pipelineMap.get(entry.getKey());
      if (p != null && pipelineNodeIds.containsKey(p)) {
        pipelineNodeIds.get(p).add(entry.getValue());
      } else {
        ungrouped.add(entry.getValue());
      }
    }

    // Phase B: emit DOT.
    final StringBuilder sb = new StringBuilder(
        "digraph {\n  rankdir=BT;\n  node [fontname=\"Helvetica\"];\n");

    for (int i = 0; i < pipelineList.size(); i++) {
      final Pipeline p = pipelineList.get(i);
      final List<Integer> nodeIds = pipelineNodeIds.get(p);
      if (nodeIds == null || nodeIds.isEmpty()) {
        continue;
      }
      sb.append("  subgraph cluster_").append(i).append(" {\n");
      sb.append("    label=\"").append(pipelineLabel(p, i)).append("\";\n");
      sb.append("    style=dashed; color=\"#666666\"; fontname=\"Helvetica\";\n");
      for (int nid : nodeIds) {
        final String def = nodeDefs.get(nid);
        if (def != null) {
          sb.append("  ").append(def);
        }
      }
      sb.append("  }\n");
    }

    // Ungrouped nodes (root pipeline nodes above flatten'd pipelines, if any)
    for (int nid : ungrouped) {
      final String def = nodeDefs.get(nid);
      if (def != null) {
        sb.append(def);
      }
    }

    // All edges outside clusters.
    for (String edge : edgeDefs) {
      sb.append(edge);
    }

    sb.append("}\n");
    return sb.toString();
  }

  /**
   * Builds a map from each {@link RelNode} in the plan to the {@link Pipeline}
   * whose cluster it belongs to. Only pipelines with at least one source
   * (i.e., appearing in {@link Pipeline#flatten()}) receive clusters.
   * Leaf pipelines (0 sources — bare table scans) are absorbed into their
   * parent pipeline's cluster.
   *
   * <p>Algorithm: recurse children-first so parent clusters do not override
   * child claims. For each pipeline {@code p}:
   * <ol>
   *   <li>Walk down from {@code p.root}.
   *   <li>At boundary sorts, assign the Sort itself to {@code p}, then look
   *       one level below:
   *       <ul>
   *         <li>If the child below the Sort is the root of a <em>non-leaf</em>
   *             child pipeline, stop — that subtree is claimed by the child.
   *         <li>If the child is a <em>leaf</em> pipeline root (0 sources),
   *             absorb it: assign its Sort + TableScan to {@code p}.
   *       </ul>
   *   <li>Skip nodes already claimed ({@code map.containsKey} guard).
   * </ol>
   */
  static IdentityHashMap<RelNode, Pipeline> assignNodesToPipelines(
      Pipeline rootPipeline) {
    final IdentityHashMap<RelNode, Pipeline> map = new IdentityHashMap<>();

    // Collect the roots of non-leaf child pipelines — these get their own cluster.
    final Set<RelNode> nonLeafChildRoots =
        Collections.newSetFromMap(new IdentityHashMap<>());
    collectNonLeafRoots(rootPipeline, nonLeafChildRoots);

    // Process pipelines that appear in flatten() — children first (post-order).
    final List<Pipeline> ordered = rootPipeline.flatten();
    for (Pipeline p : ordered) {
      assignPipelineNodes(p, p.root, map, nonLeafChildRoots);
    }
    return map;
  }

  /** Collects root nodes of all pipelines that have at least one source. */
  static void collectNonLeafRoots(Pipeline p, Set<RelNode> result) {
    if (p.sources.size() > 0) {
      result.add(p.root);
    }
    for (Pipeline child : p.sources) {
      collectNonLeafRoots(child, result);
    }
  }

  /**
   * Recursively walks the plan rooted at {@code node}, assigning each visited
   * node to pipeline {@code p} unless already claimed or blocked by a non-leaf
   * child pipeline boundary.
   */
  static void assignPipelineNodes(Pipeline p, RelNode node,
      IdentityHashMap<RelNode, Pipeline> map,
      Set<RelNode> nonLeafChildRoots) {
    if (map.containsKey(node)) {
      return; // already claimed by a child pipeline
    }
    map.put(node, p);

    for (RelNode input : node.getInputs()) {
      if (Pipeline.isBoundarySort(input)) {
        // Boundary Sort and everything below it stays OUTSIDE all clusters.
        // Do NOT assign the sort or its children to any pipeline.
        continue;
      } else {
        assignPipelineNodes(p, input, map, nonLeafChildRoots);
      }
    }
  }

  /**
   * Phase-A walk: collects node-definition strings (keyed by ID) and
   * edge-definition strings. Reuses {@link #nodeLabel} and {@link #nodeColor}.
   */
  static int collectNodesAndEdges(RelNode node,
      IdentityHashMap<RelNode, Integer> ids, int[] counter,
      Map<Integer, String> nodeDefs, List<String> edgeDefs) {
    if (ids.containsKey(node)) {
      return ids.get(node);
    }
    final int id = counter[0]++;
    ids.put(node, id);
    final String explain = RelOptUtil.dumpPlan("", node,
        SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES).trim();
    final String firstLine = explain.lines().findFirst()
        .orElse(node.getClass().getSimpleName()).trim();
    final String label = nodeLabel(node, firstLine);
    final String color = nodeColor(node);
    nodeDefs.put(id,
        "  n" + id
        + " [label=\"" + label + "\""
        + ", style=filled, fillcolor=\"" + color + "\""
        + "];\n");
    final List<RelNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      final RelNode child = skipTransparent(inputs.get(i));
      final int childId = collectNodesAndEdges(
          child, ids, counter, nodeDefs, edgeDefs);
      final String edgeLabel = inputs.size() > 1 ? (i == 0 ? "left" : "right") : "";
      final StringBuilder edge = new StringBuilder();
      edge.append("  n").append(childId).append(" -> n").append(id);
      if (!edgeLabel.isEmpty()) {
        edge.append(" [label=\"").append(edgeLabel).append("\"]");
      }
      edge.append(";\n");
      edgeDefs.add(edge.toString());
    }
    return id;
  }

  // ── Node label and color ────────────────────────────────────────────────────

  /**
   * Returns a short, presentation-friendly label for a colorful DOT node.
   *
   * <p>Strips the {@code Enumerable} prefix. Resolves {@code $N} field-index
   * references to actual column names using the node's input row type.
   * Drops internal attributes like {@code sort0=}, {@code dir0=}, {@code joinType=}.
   */
  static String nodeLabel(RelNode node, String firstLine) {
    // LogicalPipelineOutputScan placeholders inserted by replaceChildBoundaries
    // represent the output of a child pipeline's merged index view.
    if (node instanceof LogicalPipelineOutputScan) {
      return "ChildViewOutput";
    }
    final int parenIdx = firstLine.indexOf('(');
    final String fullCls = parenIdx >= 0 ? firstLine.substring(0, parenIdx) : firstLine;
    final String cls = fullCls.startsWith("Enumerable")
        ? fullCls.substring("Enumerable".length()) : fullCls;
    if (parenIdx < 0 || !firstLine.endsWith(")")) {
      return cls.replace("\"", "'");
    }
    final String inner = firstLine.substring(parenIdx + 1, firstLine.length() - 1);

    // TableScan: show only the table name (last element of the qualified name).
    if (fullCls.contains("TableScan")) {
      final int lb = inner.lastIndexOf(", ");
      final String name = (lb >= 0 ? inner.substring(lb + 2) : inner)
          .replace("[", "").replace("]", "");
      return (cls + "\\n" + name).replace("\"", "'");
    }

    // MergedIndexScan / MergedIndexJoin: show table list, drop collation attribute.
    if (fullCls.contains("MergedIndex")) {
      final int collationIdx = inner.indexOf(", collation");
      final String tables = collationIdx >= 0 ? inner.substring(0, collationIdx) : inner;
      final String attrs = tables.replace("], ", "]\\n").replace(", [", "\\n[");
      return (cls + "\\n" + attrs).replace("\"", "'");
    }

    // MergeJoin: resolve join keys to "LEFT_COL = RIGHT_COL" using splitJoinCondition.
    if (node instanceof EnumerableMergeJoin) {
      final EnumerableMergeJoin mj = (EnumerableMergeJoin) node;
      final List<Integer> lk = new ArrayList<>();
      final List<Integer> rk = new ArrayList<>();
      RelOptUtil.splitJoinCondition(mj.getLeft(), mj.getRight(),
          mj.getCondition(), lk, rk, new ArrayList<>());
      final List<org.apache.calcite.rel.type.RelDataTypeField> lf =
          mj.getLeft().getRowType().getFieldList();
      final List<org.apache.calcite.rel.type.RelDataTypeField> rf =
          mj.getRight().getRowType().getFieldList();
      final StringBuilder cond = new StringBuilder();
      for (int i = 0; i < lk.size(); i++) {
        if (i > 0) {
          cond.append("\\n");
        }
        cond.append(lk.get(i) < lf.size() ? lf.get(lk.get(i)).getName() : "$" + lk.get(i));
        cond.append(" = ");
        cond.append(rk.get(i) < rf.size() ? rf.get(rk.get(i)).getName() : "$" + rk.get(i));
      }
      return (cls + "\\n" + cond).replace("\"", "'");
    }

    // Sort / LimitSort: resolve sort fields to "COL_NAME ASC/DESC", one per line.
    if (node instanceof Sort && !node.getInputs().isEmpty()) {
      final Sort sort = (Sort) node;
      final List<org.apache.calcite.rel.type.RelDataTypeField> fields =
          node.getInputs().get(0).getRowType().getFieldList();
      final String keys = sort.getCollation().getFieldCollations().stream()
          .map(fc -> {
            final String fname = fc.getFieldIndex() < fields.size()
                ? fields.get(fc.getFieldIndex()).getName() : "$" + fc.getFieldIndex();
            final String dir =
                fc.getDirection() == RelFieldCollation.Direction.DESCENDING ? " DESC" : " ASC";
            return fname + dir;
          })
          .collect(Collectors.joining("\\n"));
      final String fetchStr = sort.fetch != null ? "\\nLIMIT " + sort.fetch : "";
      return (cls + "\\n" + keys + fetchStr).replace("\"", "'");
    }

    // Aggregate / SortedAggregate: resolve group-by indices to column names.
    if (fullCls.contains("Aggregate") && !node.getInputs().isEmpty()) {
      final List<org.apache.calcite.rel.type.RelDataTypeField> fields =
          node.getInputs().get(0).getRowType().getFieldList();
      final int grpStart = inner.indexOf("group=[{");
      final int grpEnd = grpStart >= 0 ? inner.indexOf("}]", grpStart) : -1;
      if (grpStart >= 0 && grpEnd >= 0) {
        final String body = inner.substring(grpStart + "group=[{".length(), grpEnd).trim();
        final String resolved = body.isEmpty() ? "(none)"
            : java.util.Arrays.stream(body.split(",\\s*"))
                .map(s -> {
                  try {
                    final int idx = Integer.parseInt(s.trim());
                    return idx < fields.size() ? fields.get(idx).getName() : "$" + idx;
                  } catch (NumberFormatException e) {
                    return s.trim();
                  }
                })
                .collect(Collectors.joining(", "));
        return (cls + "\\ngroup: " + resolved).replace("\"", "'");
      }
    }

    // Default: just the short class name (Project, Filter, etc.).
    return cls.replace("\"", "'");
  }

  /** Returns a fill color for the colorful DOT node based on operator type. */
  static String nodeColor(RelNode node) {
    final String cls = node.getClass().getSimpleName();
    // Physical operators (query/creation plans)
    if (cls.contains("MergedIndexScan")) return "#90EE90"; // light green
    if (cls.contains("MergedIndexJoin")) return "#32CD32"; // lime green
    if (cls.contains("MergeJoin"))       return "#FFD700"; // gold
    // Logical operators (maintenance plans)
    if (cls.contains("Delta"))           return "#FF6347"; // tomato — change stream
    if (cls.contains("Union"))           return "#FFFFE0"; // light yellow — set union
    if (cls.contains("Join"))            return "#FFD700"; // gold — same concept as MergeJoin
    if (cls.contains("Values"))          return "#98FB98"; // pale green — child view output
    // Shared operators
    if (cls.contains("LimitSort"))       return "#FFA07A"; // light salmon
    if (cls.contains("Sort"))            return "#FFA07A"; // light salmon
    if (cls.contains("TableScan"))       return "#ADD8E6"; // light blue
    if (cls.contains("Aggregate"))       return "#DDA0DD"; // plum
    if (cls.contains("Project"))         return "#D3D3D3"; // light gray
    if (cls.contains("Filter"))          return "#FFDAB9"; // peach
    return "white";
  }

  // ── Pipeline label helpers ─────────────────────────────────────────────────

  /**
   * Returns the DOT cluster label for a pipeline.
   *
   * <ul>
   *   <li>Multi-source (>=2): {@code "Pipeline N: MI(TABLE1, TABLE2)"}
   *   <li>Single-source (==1): {@code "Pipeline N: Indexed View"}
   * </ul>
   */
  static String pipelineLabel(Pipeline p, int index) {
    final String key = collationKeyNames(p);
    if (p.sources.size() >= 2) {
      final StringBuilder tables = new StringBuilder();
      for (int i = 0; i < p.sources.size(); i++) {
        if (i > 0) {
          tables.append(", ");
        }
        final Pipeline src = p.sources.get(i);
        if (src.sources.isEmpty()) {
          // Leaf source: resolve table name via findLeafScan.
          final org.apache.calcite.plan.RelOptTable relOptTable =
              MergedIndex.findLeafScan(src.root);
          if (relOptTable != null) {
            final List<String> qname = relOptTable.getQualifiedName();
            tables.append(qname.get(qname.size() - 1));
          } else {
            tables.append("src").append(i);
          }
        } else {
          tables.append("inner_view");
        }
      }
      return "Pipeline " + index + ": MI(" + tables + ")";
    } else {
      return "Pipeline " + index + ": Indexed View";
    }
  }

  /**
   * Returns a comma-separated string of sort-key column names for a pipeline,
   * with " DESC" appended for descending fields.
   *
   * <p>The {@code sharedCollation} field indices are derived from the boundary
   * Sort's collation, which references the Sort's <em>input</em> row type
   * (i.e., the source pipeline root's row type), not the pipeline root's row
   * type. We resolve against the first source's row type when sources exist,
   * otherwise fall back to {@code p.root.getRowType()}.
   */
  static String collationKeyNames(Pipeline p) {
    // Use the first source's row type to correctly resolve field indices,
    // since sharedCollation is derived from the boundary Sort over the source.
    final List<org.apache.calcite.rel.type.RelDataTypeField> fields =
        p.sources.isEmpty()
            ? p.root.getRowType().getFieldList()
            : p.sources.get(0).root.getRowType().getFieldList();
    return p.sharedCollation.getFieldCollations().stream()
        .map(fc -> {
          final String name = fc.getFieldIndex() < fields.size()
              ? fields.get(fc.getFieldIndex()).getName()
              : "$" + fc.getFieldIndex();
          final String dir =
              fc.getDirection() == RelFieldCollation.Direction.DESCENDING
                  ? " DESC" : "";
          return name + dir;
        })
        .collect(Collectors.joining(", "));
  }

  // ── Transparent node skip ──────────────────────────────────────────────────

  /**
   * Drills through {@link EnumerableProject} nodes, returning the first
   * non-Project input. Projects are "transparent" in the DOT visualization —
   * they add no information about pipeline structure and are omitted to reduce
   * visual clutter.
   */
  static RelNode skipTransparent(RelNode node) {
    while (node instanceof EnumerableProject) {
      node = ((EnumerableProject) node).getInput();
    }
    return node;
  }
}
