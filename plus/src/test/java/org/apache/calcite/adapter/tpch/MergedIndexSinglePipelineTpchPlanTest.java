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

import org.apache.calcite.materialize.MergedIndex;
import org.apache.calcite.materialize.MergedIndexRegistry;
import org.apache.calcite.materialize.Pipeline;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;


/**
 * Single-MI TPC-H plan tests.
 *
 * <p>Each test demonstrates substituting exactly ONE merged index into a query
 * plan, leaving the remaining joins as regular operators at query time. MI
 * candidates are defined conceptually — by the set of tables and their shared
 * sort key — independent of how many pipelines the Volcano optimizer happens
 * to produce.
 *
 * <p>SQL rewrites follow the same pattern as {@link MergedIndexTpchPlanTest}:
 * explicit {@code JOIN … ON …} syntax and manual join-order control via
 * subqueries or left-deep trees to surface the desired interesting-ordering
 * chain.
 *
 * <p>Visualization helpers live in {@link TpchPlanTestUtil}.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class MergedIndexSinglePipelineTpchPlanTest {

  @AfterEach
  void clearRegistry() {
    MergedIndexRegistry.clear();
  }

  /**
   * Runs Phase 1 (Volcano), discovers all pipelines, creates exactly ONE
   * {@link MergedIndex} for the pipeline at {@code pipelineIndex} in post-order
   * {@link Pipeline#flatten()}, and runs a single HEP pass to substitute it.
   *
   * <p>The caller is responsible for rewriting the SQL so that the desired
   * pipeline (the MI candidate) appears at {@code pipelineIndex} in the
   * flattened list. This is the same technique used in
   * {@link MergedIndexTpchPlanTest}: explicit {@code JOIN … ON …} syntax and
   * subqueries or left-deep join order to surface the target interesting-ordering
   * chain. Index 0 is always the innermost leaf pipeline.
   *
   * <p>Phase 1 steps:
   * <ol>
   *   <li>{@code injectSortsBeforeSortBasedOps} — inject LogicalSort before joins/aggs
   *   <li>{@code hoistFiltersAboveBoundaries} — move predicates above pipeline boundaries
   *   <li>{@code planner.transform} — Volcano: logical to physical
   *   <li>{@code splitLimitSorts} — separate EnumerableLimitSort into Limit + Sort
   *   <li>{@code Pipeline.buildTree} + {@code captureLogicalRoots} — pipeline discovery
   * </ol>
   *
   * @param sql            SQL string (rewritten for desired join order)
   * @param config         framework config (rules, schema, trait defs)
   * @param pipelineIndex  index into {@link Pipeline#flatten()} identifying the
   *                       target pipeline (0 = innermost leaf)
   * @return phase-2 plan with the selected MI substituted; remaining joins intact
   */
  @SuppressWarnings("unused")
  private static RelNode singleMIPlan(String sql, FrameworkConfig config,
      int pipelineIndex) throws Exception {
    // TODO: implement
    // Sketch:
    //   Planner planner = Frameworks.getPlanner(config);
    //   RelRoot root = planner.rel(planner.validate(planner.parse(sql)));
    //   RelNode injected = MergedIndexTestUtil.injectSortsBeforeSortBasedOps(root.rel);
    //   RelNode logicalWithSorts = MergedIndexTestUtil.hoistFiltersAboveBoundaries(injected);
    //   RelTraitSet desired = root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);
    //   RelNode phase1Plan = MergedIndexTestUtil.splitLimitSorts(
    //       planner.transform(0, desired, logicalWithSorts));
    //   Pipeline rootPipeline = Pipeline.buildTree(phase1Plan);
    //   List<Pipeline> pipelines = rootPipeline.flatten();
    //   Pipeline.captureLogicalRoots(rootPipeline, logicalWithSorts);
    //   Pipeline selected = pipelines.get(pipelineIndex);
    //   new MergedIndex(selected);
    //   if (selected.logicalRoot != null) {
    //     selected.mergedIndex.setMaintenancePlan(
    //         MaintenancePlanConverter.deriveMaintenancePlan(selected));
    //   }
    //   MergedIndexRegistry.register(selected.mergedIndex);
    //   HepProgram hp = HepProgram.builder()
    //       .addRuleInstance(EnumerableRules.ENUMERABLE_PIPELINE_TO_MERGED_INDEX_SCAN_RULE)
    //       .build();
    //   HepPlanner hep = new HepPlanner(hp);
    //   hep.setRoot(phase1Plan);
    //   return hep.findBestExp();
    throw new UnsupportedOperationException("singleMIPlan not yet implemented");
  }

  // ── Q12: MI(ORDERS, LINEITEM) by o_orderkey ──────────────────────────────
  // Candidate: absorbs O-L join; Filter + SortedAggregate(l_shipmode) remain.
  // SQL: same structure as MergedIndexTpchPlanTest.tpchQ12 — no rewrite needed.
  //
  // BEFORE: Sort(l_shipmode) → SortedAgg → Filter
  //           → Sort(o_orderkey) → MergeJoin(ORDERS, LINEITEM)
  //         where the two input Sorts are replaced by MIScans
  // AFTER:  Sort(l_shipmode) → SortedAgg → Filter → MIScan(ORDERS, LINEITEM)
  //
  // TODO: fill in SQL, call singleMIPlan, assert plan shape, write DOTs to
  //       test-dot-output/single-mi/q12/
  @Test
  void tpchQ12OlMI() throws Exception {
    // TODO
  }

  // ── Q3 Candidate A: MI(ORDERS, LINEITEM) by o_orderkey ───────────────────
  // Absorbs inner O-L join. CUSTOMER merge-join on custkey remains at query time.
  // SQL rewrite: ORDERS ⋈ LINEITEM as inner join (subquery or left-deep).
  //
  // BEFORE: MergeJoin(custkey)
  //           ├─ Sort(custkey) → MergeJoin(orderkey) → [ORDERS, LINEITEM sorts]
  //           └─ Sort(custkey) → Scan(CUSTOMER)
  // AFTER:  MergeJoin(custkey)
  //           ├─ Sort(custkey) → MIScan(ORDERS, LINEITEM)
  //           └─ Sort(custkey) → Scan(CUSTOMER)
  //
  // TODO: rewrite SQL, call singleMIPlan, fill in assertions
  @Test
  void tpchQ3OlMI() throws Exception {
    // TODO
  }

  // ── Q3 Candidate B: MI(CUSTOMER, ORDERS) by o_custkey ────────────────────
  // Absorbs C-O join. LINEITEM merge-join on orderkey remains at query time.
  // SQL rewrite: CUSTOMER ⋈ ORDERS as inner join.
  //
  // TODO: rewrite SQL, call singleMIPlan, fill in assertions
  @Test
  void tpchQ3CoMI() throws Exception {
    // TODO
  }

  // ── Q9 Candidate A: MI(ORDERS, LINEITEM) by o_orderkey ───────────────────
  // Absorbs O-L leaf join. 4 remaining joins (PART, PARTSUPP, SUPPLIER, NATION)
  // execute at query time on different keys.
  // SQL rewrite: same left-deep order as MergedIndexTpchPlanTest.tpchQ9.
  // Uses TpchPlanTestUtil.propagateOrderByDirection for o_year DESC alignment.
  //
  // BEFORE: ... → MergeJoin(orderkey)
  //                 ├─ Sort → Scan(ORDERS)
  //                 └─ Sort → Scan(LINEITEM)
  // AFTER:  ... → MIScan(ORDERS, LINEITEM)  (4 outer joins unchanged)
  //
  // TODO: fill in SQL, call singleMIPlan, fill in assertions
  @Test
  void tpchQ9OlMI() throws Exception {
    // TODO
  }

  // ── Q9 Candidate B: MI(LINEITEM, PARTSUPP) by (l_partkey, l_suppkey) ─────
  // Absorbs LINEITEM ⋈ PARTSUPP compound-key join.
  // SQL rewrite: LINEITEM ⋈ PARTSUPP as inner join, partkey-first condition
  //   (ps_partkey = l_partkey AND ps_suppkey = l_suppkey).
  //
  // TODO: rewrite SQL, call singleMIPlan, fill in assertions
  @Test
  void tpchQ9LpsMI() throws Exception {
    // TODO
  }

  // ── Q5: TBD ──────────────────────────────────────────────────────────────
  // 6-table star join. Candidate MIs to be identified after SQL rewrite
  // analysis. Does Volcano choose merge joins for the star topology, or hash
  // joins? MI benefit depends on the planner's join strategy choice.
  //
  // TODO: identify candidates, rewrite SQL, call singleMIPlan
  @Test
  void tpchQ5() throws Exception {
    // TODO
  }

  // ── Q7: TBD — potential "MI not helpful" case ────────────────────────────
  // Contains a NATION self-join that may break interesting-ordering chains.
  // An honest evaluation candidate: if no pipeline benefits from MI, document
  // that finding as a bound on MI applicability.
  //
  // TODO: analyze, identify candidates if any, rewrite SQL
  @Test
  void tpchQ7() throws Exception {
    // TODO
  }
}
