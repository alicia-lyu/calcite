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
package org.apache.calcite.rel.logical;

import org.apache.calcite.materialize.Pipeline;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Logical leaf node representing a child pipeline's output in a parent
 * pipeline's maintenance plan.
 *
 * <p>When deriving an incremental maintenance plan, child pipeline subtrees
 * are replaced with this placeholder. It carries the child {@link Pipeline}
 * reference so that physical conversion rules can resolve it to the
 * appropriate merged index source.
 *
 * <p>At physical conversion time, a rule converts this to
 * {@code EnumerableMergedIndexScan} by resolving
 * {@code childPipeline.mergedIndex} and finding the source index
 * in the parent merged index.
 */
public class LogicalPipelineOutputScan extends AbstractRelNode {

  /** The child pipeline whose output this node represents. */
  public final Pipeline childPipeline;

  private final RelDataType rowType;

  protected LogicalPipelineOutputScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      Pipeline childPipeline,
      RelDataType rowType) {
    super(cluster, traitSet);
    this.childPipeline = childPipeline;
    this.rowType = rowType;
  }

  public static LogicalPipelineOutputScan create(
      RelOptCluster cluster,
      Pipeline childPipeline,
      RelDataType rowType) {
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalPipelineOutputScan(cluster, traitSet, childPipeline, rowType);
  }

  @Override protected RelDataType deriveRowType() {
    return rowType;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new LogicalPipelineOutputScan(getCluster(), traitSet, childPipeline, rowType);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("pipeline", childPipeline.toString());
  }
}
