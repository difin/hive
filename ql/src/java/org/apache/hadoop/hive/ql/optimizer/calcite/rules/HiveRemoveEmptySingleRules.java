/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.Bug;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;

import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Iterables.concat;

/**
 * This class provides access to Calcite's {@link PruneEmptyRules}.
 * The instances of the rules use {@link org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelBuilder}.
 */
public class HiveRemoveEmptySingleRules extends PruneEmptyRules {

  public static final RelOptRule PROJECT_INSTANCE = new PruneEmptyRules.RemoveEmptySingleRule(
      HiveProject.class, project -> true, HiveRelFactories.HIVE_BUILDER, "HivePruneEmptyProject");
  public static final RelOptRule FILTER_INSTANCE = new PruneEmptyRules.RemoveEmptySingleRule(
      HiveFilter.class, hiveFilter -> true, HiveRelFactories.HIVE_BUILDER, "HivePruneEmptyFilter");
  public static final RelOptRule SORT_INSTANCE = new PruneEmptyRules.RemoveEmptySingleRule(
      HiveSortLimit.class, hiveSortLimit -> true, HiveRelFactories.HIVE_BUILDER, "HivePruneEmptySort");
  public static final RelOptRule SORT_FETCH_ZERO_INSTANCE = new RemoveSortFetchZeroRule();
  public static final RelOptRule AGGREGATE_INSTANCE = new PruneEmptyRules.RemoveEmptySingleRule(
      HiveAggregate.class, Aggregate::isNotGrandTotal, HiveRelFactories.HIVE_BUILDER, "HivePruneEmptyAggregate");

  public static final RelOptRule JOIN_LEFT_INSTANCE = new RemoveLeftEmptyJoinRule<>(HiveJoin.class);
  public static final RelOptRule SEMI_JOIN_LEFT_INSTANCE = new RemoveLeftEmptyJoinRule<>(HiveSemiJoin.class);
  public static final RelOptRule JOIN_RIGHT_INSTANCE = new RemoveRightEmptyJoinRule<>(HiveJoin.class);
  public static final RelOptRule SEMI_JOIN_RIGHT_INSTANCE = new RemoveRightEmptyJoinRule<>(HiveSemiJoin.class);
  public static final RelOptRule UNION_INSTANCE = new UnionEmptyPruneRule();

  public static class RemoveSortFetchZeroRule extends RelOptRule {

    public RemoveSortFetchZeroRule() {
      super(operand(HiveSortLimit.class, RelOptRule.any()), HiveRelFactories.HIVE_BUILDER, "PruneSortLimit0");
    }

    public void onMatch(RelOptRuleCall call) {
      Sort sort = call.rel(0);
      if (sort.fetch != null && !(sort.fetch instanceof RexDynamicParam) && RexLiteral.intValue(sort.fetch) == 0) {
        call.transformTo(call.builder().push(sort).empty().build());
      }
    }
  }

  /**
   * Improved version of Calcite's {@link PruneEmptyRules.JoinLeftEmptyRuleConfig}.
   * In case of right outer join if the left branch is empty the join operator can be removed
   * and take the right branch only.
   *
   * select * from (select * from emp where 1=0) right join dept
   * to
   * select null as emp.col0 ... null as emp.coln, dept.* from dept
   */
  public static class RemoveLeftEmptyJoinRule<R extends RelNode> extends RelOptRule {

    public RemoveLeftEmptyJoinRule(Class<R> clazz) {
      super(operand(clazz,
          operand(Values.class, none()),
          operand(RelNode.class, any())), HiveRelFactories.HIVE_BUILDER, "HivePruneEmptyJoin(left)");

      if (Bug.CALCITE_5294_FIXED) {
        throw new IllegalStateException(
            "Class RemoveLeftEmptyJoinRule is redundant after fix is merged into Calcite");
      }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      Values values = call.rel(1);
      return Values.isEmpty(values);
    }

    public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      final RelNode right = call.rel(2);
      final RelBuilder relBuilder = call.builder();
      if (join.getJoinType().generatesNullsOnLeft()) {
        // If "emp" is empty, "select * from emp right join dept" will have
        // the same number of rows as "dept", and null values for the
        // columns from "emp". The left side of the join can be removed.
        call.transformTo(padWithNulls(relBuilder, right, join.getRowType(), true));
        return;
      }
      call.transformTo(relBuilder.push(join).empty().build());
    }
  }

  /**
   * Improved version of Calcite's {@link PruneEmptyRules.JoinRightEmptyRuleConfig}.
   * In case of left outer join if the right branch is empty the join operator can be removed
   * and take the left branch only.
   *
   * select * from emp right join (select * from dept where 1=0)
   * to
   * select emp.*, null as dept.col0 ... null as dept.coln from emp
   */
  public static class RemoveRightEmptyJoinRule<R extends RelNode> extends RelOptRule {

    public RemoveRightEmptyJoinRule(Class<R> clazz) {
      super(operand(clazz,
          operand(RelNode.class, any()),
          operand(Values.class, none())), HiveRelFactories.HIVE_BUILDER, "HivePruneEmptyJoin(right)");

      if (Bug.CALCITE_5294_FIXED) {
        throw new IllegalStateException(
            "Class RemoveRightEmptyJoinRule is redundant after fix is merged into Calcite");
      }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      Values values = call.rel(2);
      return Values.isEmpty(values);
    }

    public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      final RelNode left = call.rel(1);
      final RelBuilder relBuilder = call.builder();
      if (join.getJoinType().generatesNullsOnRight()) {
        // If "dept" is empty, "select * from emp left join dept" will have
        // the same number of rows as "emp", and null values for the
        // columns from "dept". The right side of the join can be removed.
        call.transformTo(padWithNulls(relBuilder, left, join.getRowType(), false));
        return;
      }
      call.transformTo(relBuilder.push(join).empty().build());
    }
  }

  private static RelNode padWithNulls(RelBuilder builder, RelNode input, RelDataType resultType,
      boolean leftPadding) {
    int padding = resultType.getFieldCount() - input.getRowType().getFieldCount();
    List<RexNode> nullLiterals = Collections.nCopies(padding, builder.literal(null));
    builder.push(input);
    if (leftPadding) {
      builder.project(concat(nullLiterals, builder.fields()));
    } else {
      builder.project(concat(builder.fields(), nullLiterals));
    }
    return builder.convert(resultType, true).build();
  }

  public static final RelOptRule CORRELATE_RIGHT_INSTANCE = new CorrelateRightEmptyRule(Correlate.class);
  public static final RelOptRule CORRELATE_LEFT_INSTANCE = new CorrelateLeftEmptyRule(Correlate.class);

  /** Rule that prunes a correlate if left input is empty. */
  public static class CorrelateLeftEmptyRule extends RelOptRule {
    public CorrelateLeftEmptyRule(Class<? extends Correlate> clazz){
      super(operand(clazz,
          operand(Values.class, none()),
          operand(RelNode.class, any())), HiveRelFactories.HIVE_BUILDER, "HivePruneEmptyCorrelate(left)");
      if (Bug.CALCITE_5669_FIXED) {
        throw new IllegalStateException("Class is redundant after fix is merged into Calcite");
      }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      Values values = call.rel(1);
      return Values.isEmpty(values);
    }
    
    @Override
    public void onMatch(RelOptRuleCall call) {
      final Correlate corr = call.rel(0);
      call.transformTo(call.builder().push(corr).empty().build());
    }
  }

  /** Rule that prunes a correlate if right input is empty. */
  public static class CorrelateRightEmptyRule extends RelOptRule {
    public CorrelateRightEmptyRule(Class<? extends Correlate> clazz){
      super(operand(clazz,
          operand(RelNode.class, any()),
          operand(Values.class, none())), HiveRelFactories.HIVE_BUILDER, "HivePruneEmptyCorrelate(right)");
      if (Bug.CALCITE_5669_FIXED) {
        throw new IllegalStateException("Class is redundant after fix is merged into Calcite");
      }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      Values values = call.rel(2);
      return Values.isEmpty(values);
    }
    
    @Override
    public void onMatch(RelOptRuleCall call) {
      final Correlate corr = call.rel(0);
      final RelNode left = call.rel(1);
      final RelBuilder b = call.builder();
      final RelNode newRel;
      switch (corr.getJoinType()) {
      case LEFT:
        newRel = padWithNulls(b, left, corr.getRowType(), false);
        break;
      case INNER:
      case SEMI:
        newRel = b.push(corr).empty().build();
        break;
      case ANTI:
        newRel = left;
        break;
      default:
        throw new IllegalStateException("Correlate does not support " + corr.getJoinType());
      }
      call.transformTo(newRel);
    }
  }

  /**
   * Copy of {@link PruneEmptyRules.UnionEmptyPruneRuleConfig} but this version expects {@link Union}.
   */
  public static class UnionEmptyPruneRule extends RelOptRule {

    public UnionEmptyPruneRule() {
      super(operand(HiveUnion.class, any()), HiveRelFactories.HIVE_BUILDER, "UnionEmptyPruneRule");
      if (Bug.CALCITE_5293_FIXED) {
        throw new IllegalStateException(
            "Class UnionEmptyPruneRule is redundant after fix is merged into Calcite");
      }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Union union = call.rel(0);
      final List<RelNode> inputs = union.getInputs();
      assert inputs != null;
      final RelBuilder builder = call.builder();
      int nonEmptyInputs = 0;
      for (RelNode input : inputs) {
        if (!isEmpty(input)) {
          builder.push(input);
          nonEmptyInputs++;
        }
      }
      if (nonEmptyInputs == inputs.size()) {
        return;
      }
      if (nonEmptyInputs == 0) {
        builder.push(union).empty();
      } else {
        builder.union(union.all, nonEmptyInputs);
        builder.convert(union.getRowType(), true);
      }
      call.transformTo(builder.build());
    }
  }

  private static boolean isEmpty(RelNode node) {
    if (node instanceof Values) {
      return ((Values) node).getTuples().isEmpty();
    }
    if (node instanceof HepRelVertex) {
      return isEmpty(((HepRelVertex) node).getCurrentRel());
    }
    // Note: relation input might be a RelSubset, so we just iterate over the relations
    // in order to check if the subset is equivalent to an empty relation.
    if (!(node instanceof RelSubset)) {
      return false;
    }
    RelSubset subset = (RelSubset) node;
    for (RelNode rel : subset.getRels()) {
      if (isEmpty(rel)) {
        return true;
      }
    }
    return false;
  }
}
