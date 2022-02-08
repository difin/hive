/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.impala.calcite.rules;

import org.apache.commons.lang.BooleanUtils;
import com.google.common.collect.ImmutableSet;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRulesRegistry;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaTypeConverter;
import org.apache.impala.catalog.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rule to fix scalar functions that were resolved by Impala but need to run on Tez.
 * The idea for this rule is to keep the return type as an Impala return type so the
 * general structure of the Calcite RelNode types should stay the same.
 *
 * A specific example may help explain this. The return type for the "rank"  function in
 * Impala is BIGINT, but it is INT for Tez. The function will resolve to the signature
 * "BIGINT rank()". But the Tez resolver would have generated "INT rank()". To fix this,
 * this rule will change the RexNode to have the signature "BIGINT CAST( INT RANK())". The
 * whole RexNode remains a BIGINT, but a cast is done on the Tez rank function.
 * we need to
 *
 */
public class TezEngineScalarFixerRule extends RelOptRule {

  private static final Logger LOG = LoggerFactory.getLogger(TezEngineScalarFixerRule.class);

  public static Set<String> RANK_FUNCTIONS =
      ImmutableSet.<String> builder()
      .add("RANK")
      .add("DENSE_RANK")
      .build();

  private final ProjectFactory projectFactory;

  public static final TezEngineScalarFixerRule INSTANCE =
      new TezEngineScalarFixerRule();

  public TezEngineScalarFixerRule() {
    super(operand(HiveProject.class, any()));
    this.projectFactory = HiveRelFactories.HIVE_PROJECT_FACTORY;

  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveProject project = call.rel(0);

    HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);

    // If this operator has been visited already by the rule,
    // we do not need to apply the optimization
    if (registry != null && registry.getVisited(this).contains(project)) {
      return false;
    }

    RexVisitor<Boolean> visitor = new RexVisitorImpl<Boolean>(true) {;
      @Override
      public Boolean visitOver(RexOver call) {
        if (BooleanUtils.isTrue(super.visitOver(call))) {
          return true;
        }
        if (RANK_FUNCTIONS.contains(call.getOperator().getName().toUpperCase())) {
          LOG.debug("Found function " + call.getOperator().getName() + ", will apply " +
              "function resolution for Tez engine.");
          return true;
        }
        return false;
      }
    };

    for (RexNode rexNode : project.getProjects()) {
      if (BooleanUtils.isTrue(rexNode.accept(visitor))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final HiveProject oldProject = call.rel(0);

    // 0. Register that we have visited this operator in this rule
    HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);
    if (registry != null) {
      registry.registerVisited(this, oldProject);
    }
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(oldProject.getInput());

    RexVisitor<RexNode> visitor = new RexVisitorImpl<RexNode>(true) {
      RexBuilder rexBuilder = oldProject.getCluster().getRexBuilder();

      @Override
      public RexNode visitCall(RexCall call) {
        return call;
      }

      @Override
      public RexNode visitOver(RexOver call) {
        RexBuilder rexBuilder = oldProject.getCluster().getRexBuilder();
        RexNode processedNode = call;
        if (RANK_FUNCTIONS.contains(call.getOperator().getName().toUpperCase())) {
          // First, adjust original function from a BIGINT to an INT so it matches the Tez
          // signature.
          RelDataType tezReturnTypeForRank = ImpalaTypeConverter.getRelDataType(Type.INT,
              call.getType().isNullable());
          processedNode = rexBuilder.makeOver(tezReturnTypeForRank, call.getAggOperator(),
              call.getOperands(), call.getWindow().partitionKeys, call.getWindow().orderKeys,
              call.getWindow().getLowerBound(), call.getWindow().getUpperBound(),
              call.getWindow().isRows(), true /*allowPartial*/, false /*nullWhenCountZero*/,
              call.isDistinct(), call.ignoreNulls());
          // Then, put a cast around the tez function to make it a BIGINT again so it matches
          // the already compiled Calcite plan.
          processedNode = (RexCall) rexBuilder.makeCast(call.getType(), processedNode);
        }
        return processedNode;
      }
    };

    final List<RexNode> projList = new ArrayList<>();
    for (RexNode rexNode : oldProject.getProjects()) {
      RexNode r = rexNode.accept(visitor);
      if (r == null) {
        r = rexNode;
      }
      projList.add(r);
    }

    RelNode newProject = projectFactory.createProject(oldProject.getInput(), Collections.emptyList(), projList,
        oldProject.getRowType().getFieldNames());
    call.transformTo(newProject);
  }
}
