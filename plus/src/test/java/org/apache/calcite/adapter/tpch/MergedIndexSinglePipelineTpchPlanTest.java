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

import org.apache.calcite.materialize.MergedIndexRegistry;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.SingleMIPipelineIdentifier;
import org.apache.calcite.test.SingleMIPipelineIdentifier.Candidate;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;


/**
 * Unit tests for {@link SingleMIPipelineIdentifier} on TPC-H logical plans.
 *
 * <p>Each test parses a TPC-H SQL string to a logical {@link RelNode} (no
 * Volcano optimization), calls {@link SingleMIPipelineIdentifier#identify},
 * prints all returned candidates, and asserts basic structural properties.
 *
 * <p>Tests run sequentially ({@link ExecutionMode#SAME_THREAD}) because
 * {@link MergedIndexRegistry} is a static singleton.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class MergedIndexSinglePipelineTpchPlanTest {

  private static final String OUTPUT_DIR = "plus/test-output-ind-ord";

  @AfterEach
  void clearRegistry() {
    MergedIndexRegistry.clear();
  }

  // ── Shared config and parse helper ───────────────────────────────────────

  /** Builds a FrameworkConfig backed by TPC-H schema at scale 0.01. */
  private static FrameworkConfig tpchConfig() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("TPCH", new TpchSchema(0.01, 0, 1, false));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(rootSchema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .build();
  }

  /**
   * Parses {@code sql} and returns the logical {@link RelNode} (before any
   * physical optimization). This is the correct input to
   * {@link SingleMIPipelineIdentifier#identify}.
   */
  private static RelNode parseToLogical(String sql) throws Exception {
    final Planner planner = Frameworks.getPlanner(tpchConfig());
    final SqlNode parsed = planner.parse(sql);
    final SqlNode validated = planner.validate(parsed);
    final RelRoot root = planner.rel(validated);
    return root.rel;
  }

  // ── Q12: ORDERS ⋈ LINEITEM by orderkey ───────────────────────────────────

  /**
   * TPC-H Q12: expects {@code {ORDERS, LINEITEM}} as the primary multi-table
   * candidate, keyed on {@code o_orderkey / l_orderkey}.
   *
   * <p>Expected candidates include (at least):
   * <ul>
   *   <li>2-table: ORDERS + LINEITEM (orderkey)</li>
   *   <li>1-table: ORDERS (orderkey)</li>
   *   <li>1-table: LINEITEM (orderkey)</li>
   *   <li>1-table: LINEITEM (l_shipmode) — from GROUP BY / ORDER BY</li>
   * </ul>
   */
  @Test void testIdentifyQ12() throws Exception {
    final String sql = "SELECT l.l_shipmode,"
        + " SUM(CASE WHEN o.o_orderpriority = '1-URGENT'"
        + "     THEN 1 ELSE 0 END) AS high_line_count,"
        + " SUM(CASE WHEN o.o_orderpriority <> '1-URGENT'"
        + "     THEN 1 ELSE 0 END) AS low_line_count"
        + " FROM tpch.orders o"
        + " JOIN tpch.lineitem l ON o.o_orderkey = l.l_orderkey"
        + " WHERE l.l_shipmode IN ('MAIL', 'SHIP')"
        + " AND l.l_commitdate < l.l_receiptdate"
        + " AND l.l_shipdate < l.l_commitdate"
        + " AND l.l_receiptdate >= DATE '1994-01-01'"
        + " AND l.l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR"
        + " GROUP BY l.l_shipmode"
        + " ORDER BY l.l_shipmode";

    final RelNode logical = parseToLogical(sql);
    final List<Candidate> candidates = SingleMIPipelineIdentifier.identify(logical);

    System.out.println("=== Q12 SingleMIPipelineIdentifier candidates ===");
    candidates.forEach(c -> System.out.println("  " + c));

    // The top-ranked candidate must be the 2-table ORDERS+LINEITEM join.
    assertThat("Q12 must produce at least one candidate",
        candidates.size(), greaterThanOrEqualTo(1));
    final int maxTables = candidates.get(0).tableCount();
    assertThat("Q12 top candidate must cover 2 tables (ORDERS+LINEITEM)",
        maxTables, is(2));

    // All 2-table candidates must involve ORDERS and LINEITEM.
    final List<Candidate> multiTable = candidates.stream()
        .filter(c -> c.tableCount() == 2)
        .collect(Collectors.toList());
    assertThat("Q12 must have at least one 2-table candidate",
        multiTable, hasSize(greaterThanOrEqualTo(1)));

    final boolean hasOrdersLineitem = multiTable.stream().anyMatch(c ->
        c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("ORDERS"))
        && c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("LINEITEM")));
    assertThat("Q12 must have ORDERS+LINEITEM as a multi-table candidate",
        hasOrdersLineitem, is(true));
  }

  // ── Q3: CUSTOMER ⋈ ORDERS ⋈ LINEITEM ─────────────────────────────────────

  /**
   * TPC-H Q3: three-table join with two distinct equi-join keys.
   *
   * <ul>
   *   <li>CUSTOMER ⋈ ORDERS on {@code c_custkey = o_custkey}</li>
   *   <li>ORDERS ⋈ LINEITEM on {@code o_orderkey = l_orderkey}</li>
   * </ul>
   *
   * <p>Expected multi-table candidates:
   * <ul>
   *   <li>{@code CUSTOMER + ORDERS} by custkey — the outer join pipeline</li>
   *   <li>{@code ORDERS + LINEITEM} by orderkey — the inner join pipeline</li>
   * </ul>
   *
   * <p>ORDERS cannot be in both simultaneously because its two join keys
   * ({@code o_custkey}, {@code o_orderkey}) are independent surrogates with no
   * prefix relationship. Hence the two candidates are separate, not a 3-table
   * candidate.
   */
  @Test void testIdentifyQ3() throws Exception {
    final String sql = "SELECT l.l_orderkey,"
        + " SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,"
        + " o.o_orderdate, o.o_shippriority"
        + " FROM tpch.customer c"
        + " JOIN tpch.orders o ON c.c_custkey = o.o_custkey"
        + " JOIN tpch.lineitem l ON l.l_orderkey = o.o_orderkey"
        + " WHERE c.c_mktsegment = 'BUILDING'"
        + " AND o.o_orderdate < DATE '1995-03-15'"
        + " AND l.l_shipdate > DATE '1995-03-15'"
        + " GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority"
        + " ORDER BY revenue DESC, o.o_orderdate";

    final RelNode logical = parseToLogical(sql);
    final List<Candidate> candidates = SingleMIPipelineIdentifier.identify(logical);

    System.out.println("=== Q3 SingleMIPipelineIdentifier candidates ===");
    candidates.forEach(c -> System.out.println("  " + c));

    assertThat("Q3 must produce at least one candidate",
        candidates.size(), greaterThanOrEqualTo(1));

    final List<Candidate> multiTable = candidates.stream()
        .filter(c -> c.tableCount() >= 2)
        .collect(Collectors.toList());
    assertThat("Q3 must have at least two 2-table candidates"
        + " (CUSTOMER+ORDERS and ORDERS+LINEITEM)",
        multiTable.size(), greaterThanOrEqualTo(2));

    // CUSTOMER + ORDERS candidate must exist.
    final boolean hasCustOrders = multiTable.stream().anyMatch(c ->
        c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("CUSTOMER"))
        && c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("ORDERS")));
    assertThat("Q3 must have CUSTOMER+ORDERS as a multi-table candidate",
        hasCustOrders, is(true));

    // ORDERS + LINEITEM candidate must exist.
    final boolean hasOrdersLineitem = multiTable.stream().anyMatch(c ->
        c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("ORDERS"))
        && c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("LINEITEM")));
    assertThat("Q3 must have ORDERS+LINEITEM as a multi-table candidate",
        hasOrdersLineitem, is(true));

    // ORDERS cannot bridge both keys: no 3-table CUSTOMER+ORDERS+LINEITEM.
    final boolean has3TableAll = multiTable.stream().anyMatch(c ->
        c.tableCount() >= 3
        && c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("CUSTOMER"))
        && c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("ORDERS"))
        && c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("LINEITEM")));
    assertThat("Q3 must NOT produce a 3-table CUSTOMER+ORDERS+LINEITEM candidate"
        + " (independent keys)",
        has3TableAll, is(false));
  }

  // ── Q9: 6-table join ──────────────────────────────────────────────────────

  /**
   * TPC-H Q9: six-table join with five distinct equi-join conditions.
   *
   * <p>Full candidate list produced by the identifier (verified by running):
   * <ul>
   *   <li>3-table: {@code LINEITEM([1],[2]) + PART([0]) + PARTSUPP([0],[1])}
   *       — PART's partkey ([0]) is a prefix of both LINEITEM's reordered
   *       (partkey,suppkey) and PARTSUPP's compound key (partkey,suppkey).</li>
   *   <li>2-table: {@code ORDERS([0]) + LINEITEM([0])} — orderkey</li>
   *   <li>2-table: {@code LINEITEM([1],[2]) + PART([0])} — partkey prefix</li>
   *   <li>2-table: {@code LINEITEM([1],[2]) + PARTSUPP([0],[1])}
   *       — (partkey,suppkey) match</li>
   *   <li>2-table: {@code PART([0]) + PARTSUPP([0],[1])}
   *       — partkey prefix of compound key</li>
   *   <li>2-table: {@code LINEITEM([2],[1]) + SUPPLIER([0])}
   *       — suppkey prefix of LINEITEM's (suppkey,partkey)</li>
   *   <li>2-table: {@code SUPPLIER([3]) + NATION([0])} — nationkey</li>
   * </ul>
   *
   * <p>The top-ranked candidate is the 3-table {LINEITEM, PART, PARTSUPP}
   * because PART's single-column partkey is a prefix of PARTSUPP's compound
   * key (partkey, suppkey), and LINEITEM is reordered to (partkey, suppkey)
   * to match. LINEITEM's original (suppkey, partkey) requirement also forms
   * separate 2-table candidates with SUPPLIER.
   */
  @Test void testIdentifyQ9() throws Exception {
    final String sql = "SELECT n.n_name,"
        + " EXTRACT(YEAR FROM o.o_orderdate) AS o_year,"
        + " SUM(l.l_extendedprice * (1 - l.l_discount)"
        + "     - ps.ps_supplycost * l.l_quantity) AS amount"
        + " FROM tpch.lineitem l"
        + " JOIN tpch.orders o ON l.l_orderkey = o.o_orderkey"
        + " JOIN tpch.part p ON l.l_partkey = p.p_partkey"
        + " JOIN tpch.partsupp ps"
        + "   ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey"
        + " JOIN tpch.supplier s ON l.l_suppkey = s.s_suppkey"
        + " JOIN tpch.nation n ON s.s_nationkey = n.n_nationkey"
        + " WHERE p.p_name LIKE '%green%'"
        + " GROUP BY n.n_name, EXTRACT(YEAR FROM o.o_orderdate)"
        + " ORDER BY n.n_name, o_year DESC";

    final RelNode logical = parseToLogical(sql);
    final List<Candidate> candidates = SingleMIPipelineIdentifier.identify(logical);

    System.out.println("=== Q9 SingleMIPipelineIdentifier candidates ===");
    candidates.forEach(c -> System.out.println("  " + c));

    assertThat("Q9 must produce at least one candidate",
        candidates.size(), greaterThanOrEqualTo(1));

    // Top candidate must be the 3-table {LINEITEM, PART, PARTSUPP}.
    assertThat("Q9 top candidate must cover 3 tables (LINEITEM+PART+PARTSUPP)",
        candidates.get(0).tableCount(), is(3));

    final boolean topHasLps = candidates.get(0).requirements.stream()
        .anyMatch(r -> r.table.getQualifiedName().toString().contains("LINEITEM"))
        && candidates.get(0).requirements.stream()
            .anyMatch(r -> r.table.getQualifiedName().toString().contains("PART"))
        && candidates.get(0).requirements.stream()
            .anyMatch(r -> r.table.getQualifiedName().toString().contains("PARTSUPP"));
    assertThat("Q9 top candidate must be LINEITEM+PART+PARTSUPP",
        topHasLps, is(true));

    final List<Candidate> multiTable = candidates.stream()
        .filter(c -> c.tableCount() >= 2)
        .collect(Collectors.toList());
    assertThat("Q9 must have at least 4 multi-table candidates"
        + " ({L,P,PS}, {L,O}, {L,S}, {S,N})",
        multiTable.size(), greaterThanOrEqualTo(4));

    // ORDERS + LINEITEM by orderkey.
    final boolean hasOrdersLineitem = multiTable.stream().anyMatch(c ->
        c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("ORDERS"))
        && c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("LINEITEM")));
    assertThat("Q9 must have ORDERS+LINEITEM candidate",
        hasOrdersLineitem, is(true));

    // 3-table LINEITEM + PART + PARTSUPP: PART's partkey ([0]) is a prefix of
    // PARTSUPP's compound (partkey, suppkey); LINEITEM is reordered to match.
    final boolean hasLineitemPartPartsupp = multiTable.stream().anyMatch(c ->
        c.tableCount() >= 3
        && c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("LINEITEM"))
        && c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("PART"))
        && c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("PARTSUPP")));
    assertThat("Q9 must have LINEITEM+PART+PARTSUPP as a 3-table candidate",
        hasLineitemPartPartsupp, is(true));

    // LINEITEM + SUPPLIER by suppkey prefix of LINEITEM's (suppkey,partkey).
    final boolean hasLineitemSupplier = multiTable.stream().anyMatch(c ->
        c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("LINEITEM"))
        && c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("SUPPLIER")));
    assertThat("Q9 must have LINEITEM+SUPPLIER candidate",
        hasLineitemSupplier, is(true));

    // SUPPLIER + NATION by nationkey.
    final boolean hasSupplierNation = multiTable.stream().anyMatch(c ->
        c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("SUPPLIER"))
        && c.requirements.stream().anyMatch(r ->
            r.table.getQualifiedName().toString().contains("NATION")));
    assertThat("Q9 must have SUPPLIER+NATION candidate",
        hasSupplierNation, is(true));
  }

  // ── Placeholder tests (not yet implemented) ───────────────────────────────

  /**
   * Single-MI substitution: ORDERS ⋈ LINEITEM inner pipeline only; CUSTOMER
   * merge-join on custkey remains at query time.
   *
   * <p>TODO: implement via {@code singleMIPlan} when that helper is complete.
   */
  @Test
  void tpchQ12OlMI() throws Exception {
    // TODO
  }

  /**
   * Single-MI substitution: ORDERS ⋈ LINEITEM inner pipeline only; CUSTOMER
   * join on custkey remains at query time.
   *
   * <p>TODO: implement via {@code singleMIPlan} when that helper is complete.
   */
  @Test
  void tpchQ3OlMI() throws Exception {
    // TODO
  }

  /**
   * Single-MI substitution: CUSTOMER ⋈ ORDERS inner pipeline only; LINEITEM
   * merge-join on orderkey remains at query time.
   *
   * <p>TODO: implement via {@code singleMIPlan} when that helper is complete.
   */
  @Test
  void tpchQ3CoMI() throws Exception {
    // TODO
  }

  /**
   * Single-MI substitution: ORDERS ⋈ LINEITEM leaf pipeline only for Q9;
   * remaining 4 joins at query time.
   *
   * <p>TODO: implement via {@code singleMIPlan} when that helper is complete.
   */
  @Test
  void tpchQ9OlMI() throws Exception {
    // TODO
  }

  /**
   * Single-MI substitution: LINEITEM ⋈ PARTSUPP compound-key pipeline for Q9.
   *
   * <p>TODO: implement via {@code singleMIPlan} when that helper is complete.
   */
  @Test
  void tpchQ9LpsMI() throws Exception {
    // TODO
  }

  /** TPC-H Q5 MI candidate analysis. TODO: identify and implement. */
  @Test
  void tpchQ5() throws Exception {
    // TODO
  }

  /** TPC-H Q7 MI candidate analysis. TODO: identify and implement. */
  @Test
  void tpchQ7() throws Exception {
    // TODO
  }
}
