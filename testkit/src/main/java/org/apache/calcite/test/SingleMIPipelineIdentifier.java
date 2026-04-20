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
package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Identifies which sets of base tables can be stored in a single merged index
 * by analyzing sort requirements in a logical plan.
 *
 * <p>The algorithm has five steps:
 * <ol>
 *   <li>{@link #enumerateRequirements}: collect per-table sort requirements
 *       from all sort-based operators (Join, Aggregate, Sort).</li>
 *   <li>{@link #buildEquivalenceClasses}: union-find over equi-join conditions
 *       to identify which (table, column) pairs are always co-sorted.</li>
 *   <li>{@link #consolidate}: for each table, keep only maximal collations,
 *       reordering compound keys when permitted.</li>
 *   <li>{@link #findConsistentSubsets}: enumerate table combinations whose
 *       per-table collations form a valid prefix chain via equivalence classes.
 *   </li>
 *   <li>{@link #rank}: sort candidates by table count (descending).</li>
 * </ol>
 *
 * <p>The primary entry point is {@link #identify(RelNode)}, which executes
 * all five steps and returns a ranked list of {@link Candidate} objects.
 */
public final class SingleMIPipelineIdentifier {

  private SingleMIPipelineIdentifier() {
  }

  // ── Public entry point ───────────────────────────────────────────────────

  /**
   * Runs all five algorithm steps and returns ranked merged-index candidates
   * for the given logical plan.
   */
  public static List<Candidate> identify(RelNode plan) {
    final List<SortRequirement> raw = enumerateRequirements(plan);
    final UnionFind<TableCol> uf = buildEquivalenceClasses(plan);
    final List<SortRequirement> consolidated = consolidate(raw);
    final List<Candidate> candidates = findConsistentSubsets(consolidated, uf);
    return rank(candidates);
  }

  // ── Step 1: Enumerate sort requirements ─────────────────────────────────

  /**
   * Walks the logical plan and emits one {@link SortRequirement} per
   * (operator, side) that can be served by a merged index.
   *
   * <ul>
   *   <li><b>LogicalJoin</b>: for each side whose keys all trace to one base
   *       table, emit a requirement with {@code reorderable=true}.</li>
   *   <li><b>LogicalAggregate</b>: if all group-set columns trace to one base
   *       table, emit a requirement with {@code reorderable=true}.</li>
   *   <li><b>LogicalSort</b>: if all collation columns trace to one base
   *       table, emit a requirement with {@code reorderable=false} (ORDER BY
   *       keys cannot be reordered).</li>
   * </ul>
   */
  public static List<SortRequirement> enumerateRequirements(RelNode plan) {
    final List<SortRequirement> result = new ArrayList<>();
    enumerateRequirementsImpl(plan, result);
    return result;
  }

  private static void enumerateRequirementsImpl(
      RelNode node, List<SortRequirement> out) {
    // Recurse into children first so requirements are collected bottom-up.
    for (RelNode child : node.getInputs()) {
      enumerateRequirementsImpl(child, out);
    }

    if (node instanceof Join) {
      final Join join = (Join) node;
      final List<Integer> leftKeys = new ArrayList<>();
      final List<Integer> rightKeys = new ArrayList<>();
      RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(),
          join.getCondition(), leftKeys, rightKeys, new ArrayList<>());
      if (leftKeys.isEmpty()) {
        return; // non-equi or cross join
      }
      // Trace left keys through join.getLeft() (not through the join itself).
      emitJoinSideRequirement(join, join.getLeft(), leftKeys, out);
      emitJoinSideRequirement(join, join.getRight(), rightKeys, out);

    } else if (node instanceof Aggregate) {
      final Aggregate agg = (Aggregate) node;
      if (agg.getGroupSet().isEmpty()) {
        return;
      }
      final List<Integer> groupIndices = agg.getGroupSet().asList();
      final RelOptTable table =
          traceAllToSameTable(agg.getInput(), groupIndices);
      if (table == null) {
        return;
      }
      final List<Integer> baseIndices =
          traceIndicesToBase(agg.getInput(), groupIndices);
      if (baseIndices == null) {
        return;
      }
      final RelCollation collation = collationOf(baseIndices);
      out.add(new SortRequirement(table, collation, node, true));

    } else if (node instanceof Sort) {
      final Sort sort = (Sort) node;
      final List<RelFieldCollation> fieldCollations =
          sort.getCollation().getFieldCollations();
      if (fieldCollations.isEmpty()) {
        return;
      }
      final List<Integer> indices = fieldCollations.stream()
          .map(RelFieldCollation::getFieldIndex)
          .collect(Collectors.toList());
      final RelOptTable table = traceAllToSameTable(sort.getInput(), indices);
      if (table == null) {
        return;
      }
      final List<Integer> baseIndices =
          traceIndicesToBase(sort.getInput(), indices);
      if (baseIndices == null) {
        return;
      }
      // Preserve original sort directions for ORDER BY (not reorderable).
      final List<RelFieldCollation> baseFieldCols = new ArrayList<>();
      for (int i = 0; i < baseIndices.size(); i++) {
        baseFieldCols.add(new RelFieldCollation(
            baseIndices.get(i), fieldCollations.get(i).getDirection(),
            fieldCollations.get(i).nullDirection));
      }
      final RelCollation collation = RelCollations.of(baseFieldCols);
      out.add(new SortRequirement(table, collation, node, false));
    }
  }

  /** Emits a requirement for one side of a join if all keys trace to one table. */
  private static void emitJoinSideRequirement(
      Join join, RelNode side, List<Integer> keys, List<SortRequirement> out) {
    final RelOptTable table = traceAllToSameTable(side, keys);
    if (table == null) {
      return;
    }
    final List<Integer> baseIndices = traceIndicesToBase(side, keys);
    if (baseIndices == null) {
      return;
    }
    final RelCollation collation = collationOf(baseIndices);
    out.add(new SortRequirement(table, collation, join, true));
  }

  // ── Step 2: Build equivalence classes ────────────────────────────────────

  /**
   * Walks all {@link Join} nodes and merges (table, column) pairs that are
   * equi-joined into equivalence classes using union-find.
   *
   * <p>Two columns in different tables belong to the same class when the
   * query always co-sorts them — i.e., they appear on opposite sides of an
   * equi-join condition.
   */
  public static UnionFind<TableCol> buildEquivalenceClasses(RelNode plan) {
    final UnionFind<TableCol> uf = new UnionFind<>();
    buildEquivImpl(plan, uf);
    return uf;
  }

  private static void buildEquivImpl(RelNode node, UnionFind<TableCol> uf) {
    for (RelNode child : node.getInputs()) {
      buildEquivImpl(child, uf);
    }
    if (!(node instanceof Join)) {
      return;
    }
    final Join join = (Join) node;
    final List<Integer> leftKeys = new ArrayList<>();
    final List<Integer> rightKeys = new ArrayList<>();
    RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(),
        join.getCondition(), leftKeys, rightKeys, new ArrayList<>());
    for (int i = 0; i < leftKeys.size(); i++) {
      final RelColumnOrigin lo =
          traceToBaseTable(join.getLeft(), leftKeys.get(i));
      final RelColumnOrigin ro =
          traceToBaseTable(join.getRight(), rightKeys.get(i));
      if (lo == null || ro == null) {
        continue;
      }
      final TableCol leftTc = new TableCol(
          lo.getOriginTable().getQualifiedName(), lo.getOriginColumnOrdinal());
      final TableCol rightTc = new TableCol(
          ro.getOriginTable().getQualifiedName(), ro.getOriginColumnOrdinal());
      uf.union(leftTc, rightTc);
    }
  }

  // ── Step 3: Consolidate requirements per table ────────────────────────────

  /**
   * Groups requirements by table, then for each table retains only maximal
   * collations (removing any that are a strict prefix of another). When a
   * shorter collation can be turned into a prefix of a longer one via
   * reordering (both must be reorderable), the longer collation is rewritten
   * with the shorter as a prefix.
   */
  public static List<SortRequirement> consolidate(
      List<SortRequirement> requirements) {
    // Group by table qualified name.
    final Map<List<String>, List<SortRequirement>> byTable = new HashMap<>();
    for (SortRequirement req : requirements) {
      byTable.computeIfAbsent(
          req.table.getQualifiedName(), k -> new ArrayList<>()).add(req);
    }

    final List<SortRequirement> result = new ArrayList<>();
    for (List<SortRequirement> group : byTable.values()) {
      result.addAll(consolidateGroup(group));
    }
    return result;
  }

  private static List<SortRequirement> consolidateGroup(
      List<SortRequirement> group) {
    // Try pairwise: if A is a prefix of B, drop A.
    // If reorderable B can be reordered so A is a prefix, update B.
    final List<SortRequirement> working = new ArrayList<>(group);

    // Outer loop: for each pair, see if consolidation applies.
    boolean changed = true;
    while (changed) {
      changed = false;
      outer:
      for (int i = 0; i < working.size(); i++) {
        for (int j = 0; j < working.size(); j++) {
          if (i == j) {
            continue;
          }
          final SortRequirement shorter = working.get(i);
          final SortRequirement longer = working.get(j);
          if (shorter.fieldIndices().size() >= longer.fieldIndices().size()) {
            continue;
          }
          // Check if shorter is already a prefix of longer.
          if (isPrefixOf(shorter.fieldIndices(), longer.fieldIndices())) {
            working.remove(i);
            changed = true;
            break outer;
          }
          // If longer is reorderable, try reordering it so shorter is a prefix.
          if (longer.reorderable) {
            final List<Integer> reordered =
                tryReorderAsPrefixed(shorter.fieldIndices(),
                    longer.fieldIndices());
            if (reordered != null) {
              working.set(j, new SortRequirement(
                  longer.table, collationOf(reordered),
                  longer.operator, longer.reorderable));
              working.remove(i);
              changed = true;
              break outer;
            }
          }
        }
      }
    }
    return working;
  }

  /**
   * Tries to reorder {@code full} so that {@code prefix} appears at the front.
   * Returns the reordered list, or null if no such reordering exists.
   *
   * <p>For TPC-H scale (≤2 column compound keys) this checks all permutations.
   * For larger keys, an exponential search is used — acceptable because merged
   * index keys rarely exceed 3 columns in practice.
   */
  private static @Nullable List<Integer> tryReorderAsPrefixed(
      List<Integer> prefix, List<Integer> full) {
    // full must contain all elements of prefix.
    if (!full.containsAll(prefix)) {
      return null;
    }
    // Build the reordered list: prefix columns first, then remaining in
    // their original relative order.
    final List<Integer> reordered = new ArrayList<>(prefix);
    for (int col : full) {
      if (!prefix.contains(col)) {
        reordered.add(col);
      }
    }
    // Verify prefix is indeed a prefix of the reordering.
    if (isPrefixOf(prefix, reordered)) {
      return reordered;
    }
    return null;
  }

  // ── Step 4: Find consistent subsets ──────────────────────────────────────

  /**
   * Enumerates all non-empty subsets of tables whose chosen collations form a
   * valid prefix chain when columns are mapped to their equivalence-class
   * representatives.
   *
   * <p>Only combinations where at least two tables share an equivalence class
   * are considered multi-table candidates; single-table candidates are
   * included as degenerate cases.
   *
   * <p>TODO: add pre-aggregation support (skip single-table candidates whose
   * collation comes only from a GROUP BY on that table alone).
   */
  public static List<Candidate> findConsistentSubsets(
      List<SortRequirement> requirements, UnionFind<TableCol> uf) {
    // Deduplicate requirements per (table, collation) to avoid redundant
    // candidates from multiple operators requiring the same collation.
    final List<SortRequirement> deduped = deduplicateByTableAndCollation(
        requirements);

    // Group by table qualified name.
    final Map<List<String>, List<SortRequirement>> byTable = new HashMap<>();
    for (SortRequirement req : deduped) {
      byTable.computeIfAbsent(
          req.table.getQualifiedName(), k -> new ArrayList<>()).add(req);
    }

    final List<List<String>> tableNames = new ArrayList<>(byTable.keySet());
    final int n = tableNames.size();
    final List<Candidate> candidates = new ArrayList<>();

    // Enumerate all non-empty subsets.
    for (int mask = 1; mask < (1 << n); mask++) {
      final List<List<SortRequirement>> groups = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        if ((mask & (1 << i)) != 0) {
          groups.add(byTable.get(tableNames.get(i)));
        }
      }
      // Enumerate one collation choice per table.
      enumerateChoices(groups, 0, new ArrayList<>(), uf, candidates);
    }
    return candidates;
  }

  /** Recursively enumerates one requirement per table group and checks chains. */
  private static void enumerateChoices(
      List<List<SortRequirement>> groups,
      int depth,
      List<SortRequirement> chosen,
      UnionFind<TableCol> uf,
      List<Candidate> out) {
    if (depth == groups.size()) {
      if (isPrefixChain(chosen, uf)) {
        out.add(new Candidate(ImmutableList.copyOf(chosen)));
      }
      return;
    }
    for (SortRequirement req : groups.get(depth)) {
      chosen.add(req);
      enumerateChoices(groups, depth + 1, chosen, uf, out);
      chosen.remove(chosen.size() - 1);
    }
  }

  /**
   * Returns true if the chosen collations form a valid prefix chain when each
   * (table, column) pair is mapped to its equivalence-class representative.
   *
   * <p>A valid prefix chain means: when collations are sorted by length
   * descending, each shorter collation is a prefix of the longest.
   */
  private static boolean isPrefixChain(
      List<SortRequirement> chosen, UnionFind<TableCol> uf) {
    if (chosen.isEmpty()) {
      return false;
    }
    if (chosen.size() == 1) {
      return true; // A single table always forms a trivial chain.
    }

    // Map each collation to a list of equivalence-class representatives.
    final List<List<TableCol>> normalized = new ArrayList<>();
    for (SortRequirement req : chosen) {
      final List<TableCol> repr = new ArrayList<>();
      for (int col : req.fieldIndices()) {
        final TableCol tc = new TableCol(
            req.table.getQualifiedName(), col);
        repr.add(uf.find(tc));
      }
      normalized.add(repr);
    }

    // Sort by length descending; the longest is the reference chain.
    normalized.sort((a, b) -> b.size() - a.size());
    final List<TableCol> longest = normalized.get(0);

    for (int i = 1; i < normalized.size(); i++) {
      if (!isPrefixOf(normalized.get(i), longest)) {
        return false;
      }
    }
    return true;
  }

  // ── Step 5: Rank candidates ───────────────────────────────────────────────

  /**
   * Sorts candidates by table count descending (more tables = more
   * consolidation opportunity = higher priority).
   */
  public static List<Candidate> rank(List<Candidate> candidates) {
    return candidates.stream()
        .sorted()
        .collect(Collectors.toList());
  }

  // ── Column tracing helpers ────────────────────────────────────────────────

  /**
   * Traces a single field index through {@code input} to its base-table
   * origin. Returns null if the origin is ambiguous, derived, or the metadata
   * is unavailable.
   */
  static @Nullable RelColumnOrigin traceToBaseTable(RelNode input, int fieldIdx) {
    final RelMetadataQuery mq = input.getCluster().getMetadataQuery();
    final Set<RelColumnOrigin> origins = mq.getColumnOrigins(input, fieldIdx);
    if (origins == null || origins.size() != 1) {
      return null;
    }
    final RelColumnOrigin origin = origins.iterator().next();
    if (origin.isDerived()) {
      return null;
    }
    return origin;
  }

  /**
   * Traces all {@code indices} through {@code input}. Returns the shared
   * {@link RelOptTable} if every index traces to the same base table, or null
   * if any trace fails or tables differ.
   */
  private static @Nullable RelOptTable traceAllToSameTable(
      RelNode input, List<Integer> indices) {
    RelOptTable table = null;
    for (int idx : indices) {
      final RelColumnOrigin origin = traceToBaseTable(input, idx);
      if (origin == null) {
        return null;
      }
      if (table == null) {
        table = origin.getOriginTable();
      } else if (!table.getQualifiedName()
          .equals(origin.getOriginTable().getQualifiedName())) {
        return null;
      }
    }
    return table;
  }

  /**
   * Traces each index in {@code indices} through {@code input} and returns
   * the corresponding base-table column ordinals in the same order. Returns
   * null if any trace fails.
   */
  private static @Nullable List<Integer> traceIndicesToBase(
      RelNode input, List<Integer> indices) {
    final List<Integer> result = new ArrayList<>();
    for (int idx : indices) {
      final RelColumnOrigin origin = traceToBaseTable(input, idx);
      if (origin == null) {
        return null;
      }
      result.add(origin.getOriginColumnOrdinal());
    }
    return result;
  }

  /** Builds a collation from a list of field indices (all ASC, default null direction). */
  private static RelCollation collationOf(List<Integer> indices) {
    return RelCollations.of(
        indices.stream()
            .map(RelFieldCollation::new)
            .collect(Collectors.toList()));
  }

  /** Removes duplicate (table, collation) pairs, keeping one representative each. */
  private static List<SortRequirement> deduplicateByTableAndCollation(
      List<SortRequirement> requirements) {
    final Map<String, SortRequirement> seen = new HashMap<>();
    final List<SortRequirement> result = new ArrayList<>();
    for (SortRequirement req : requirements) {
      final String key =
          req.table.getQualifiedName() + ":" + req.collation;
      if (seen.putIfAbsent(key, req) == null) {
        result.add(req);
      }
    }
    return result;
  }

  // ── Generic prefix check for any list type ────────────────────────────────

  private static <T> boolean isPrefixOf(List<T> prefix, List<T> full) {
    if (prefix.size() > full.size()) {
      return false;
    }
    for (int i = 0; i < prefix.size(); i++) {
      if (!prefix.get(i).equals(full.get(i))) {
        return false;
      }
    }
    return true;
  }

  // ── Inner classes ─────────────────────────────────────────────────────────

  /**
   * A sort requirement traced to a base table.
   *
   * <p>{@code reorderable=true} for join/GROUP BY keys (the optimizer may
   * reorder them); {@code reorderable=false} for ORDER BY keys (direction and
   * order are fixed by the query).
   */
  public static final class SortRequirement {
    public final RelOptTable table;
    /** Field indices in the base table's row type. */
    public final RelCollation collation;
    /** The sort-based operator that generated this requirement. */
    public final RelNode operator;
    /** True when the optimizer may freely reorder the key columns. */
    public final boolean reorderable;

    public SortRequirement(RelOptTable table, RelCollation collation,
        RelNode operator, boolean reorderable) {
      this.table = Objects.requireNonNull(table, "table");
      this.collation = Objects.requireNonNull(collation, "collation");
      this.operator = Objects.requireNonNull(operator, "operator");
      this.reorderable = reorderable;
    }

    /** Returns the base-table column ordinals in collation order. */
    public List<Integer> fieldIndices() {
      return collation.getFieldCollations().stream()
          .map(RelFieldCollation::getFieldIndex)
          .collect(Collectors.toList());
    }

    @Override public boolean equals(@Nullable Object obj) {
      if (!(obj instanceof SortRequirement)) {
        return false;
      }
      final SortRequirement other = (SortRequirement) obj;
      return table.getQualifiedName().equals(other.table.getQualifiedName())
          && collation.equals(other.collation);
    }

    @Override public int hashCode() {
      return Objects.hash(table.getQualifiedName(), collation);
    }

    @Override public String toString() {
      return table.getQualifiedName().get(
          table.getQualifiedName().size() - 1)
          + collation.getFieldCollations().stream()
              .map(fc -> "[" + fc.getFieldIndex() + "]")
              .collect(Collectors.joining(",", "(", ")"))
          + (reorderable ? "" : " ORDER-BY");
    }
  }

  /**
   * A candidate merged index: one {@link SortRequirement} per participating
   * table, all sharing a common sort key (verified by prefix-chain check).
   *
   * <p>Candidates are ordered by table count descending so that the richest
   * consolidation opportunities sort first.
   */
  public static final class Candidate implements Comparable<Candidate> {
    /** One requirement per table in this candidate merged index. */
    public final ImmutableList<SortRequirement> requirements;

    public Candidate(ImmutableList<SortRequirement> requirements) {
      this.requirements = Objects.requireNonNull(requirements, "requirements");
    }

    /** Returns the number of tables in this candidate merged index. */
    public int tableCount() {
      return requirements.size();
    }

    @Override public int compareTo(Candidate other) {
      // Larger table counts sort first (descending).
      return Integer.compare(other.tableCount(), this.tableCount());
    }

    @Override public String toString() {
      return requirements.stream()
          .map(SortRequirement::toString)
          .collect(Collectors.joining(", ", "Candidate[", "]"));
    }
  }

  /**
   * A (qualified table name, column ordinal) pair used as a key in the
   * union-find equivalence classes.
   */
  public static final class TableCol {
    public final List<String> qualifiedName;
    public final int column;

    public TableCol(List<String> qualifiedName, int column) {
      this.qualifiedName =
          ImmutableList.copyOf(Objects.requireNonNull(qualifiedName, "qualifiedName"));
      this.column = column;
    }

    @Override public boolean equals(@Nullable Object obj) {
      if (!(obj instanceof TableCol)) {
        return false;
      }
      final TableCol other = (TableCol) obj;
      return qualifiedName.equals(other.qualifiedName) && column == other.column;
    }

    @Override public int hashCode() {
      return Objects.hash(qualifiedName, column);
    }

    @Override public String toString() {
      return qualifiedName.get(qualifiedName.size() - 1) + "." + column;
    }
  }

  /**
   * Simple map-based union-find (disjoint-set) data structure.
   *
   * <p>Supports {@link #union} and path-compressing {@link #find}.
   * Elements are added implicitly on first {@code find} or {@code union}.
   */
  public static final class UnionFind<T> {
    private final Map<T, T> parent = new HashMap<>();

    /** Finds the representative of {@code x}'s equivalence class. */
    public T find(T x) {
      parent.putIfAbsent(x, x);
      T root = x;
      while (!parent.get(root).equals(root)) {
        root = parent.get(root);
      }
      // Path compression: point all traversed nodes directly to the root.
      T current = x;
      while (!current.equals(root)) {
        final T next = parent.get(current);
        parent.put(current, root);
        current = next;
      }
      return root;
    }

    /** Merges the equivalence classes of {@code a} and {@code b}. */
    public void union(T a, T b) {
      final T ra = find(a);
      final T rb = find(b);
      if (!ra.equals(rb)) {
        parent.put(ra, rb);
      }
    }

    /** Returns true if {@code a} and {@code b} are in the same class. */
    public boolean connected(T a, T b) {
      return find(a).equals(find(b));
    }
  }
}
