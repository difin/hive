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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.impala.calcite.ImpalaTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.CalciteUDFInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSqlFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Rule to replace unsupported RexOver with supported syntax. For instance,
 * the percent_rank function isn't supported directly, but it can be calculated
 * through the rank() and count() functions which are supported.
 */
public class HiveImpalaRexOverRule extends RelOptRule {

  public static final HiveImpalaRexOverRule INSTANCE =
      new HiveImpalaRexOverRule();

  private final ProjectFactory projectFactory;

  private static final SqlOperator MIN_OPERATOR = createOperator("least",
      ImmutableList.of(SqlTypeName.BIGINT, SqlTypeName.BIGINT), SqlTypeName.BIGINT);

  private static final SqlOperator INT_DIVIDE_OPERATOR = createOperator("int_divide",
      ImmutableList.of(SqlTypeName.BIGINT, SqlTypeName.BIGINT), SqlTypeName.BIGINT);

  private HiveImpalaRexOverRule() {
    super(operand(Project.class, any()));
    this.projectFactory = HiveRelFactories.HIVE_PROJECT_FACTORY;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final RelNode input = project.getInput();
    final RexBuilder rexBuilder = project.getCluster().getRexBuilder();

    RexOverReplacer replacer = new RexOverReplacer(rexBuilder);

    List<RexNode> exprs = new ArrayList<>();
    for (RexNode r : project.getProjects()) {
      exprs.add(replacer.apply(r));
    }

    if (!replacer.replacedValue) {
      return;
    }

    RelNode newProject = projectFactory.createProject(
        input, Collections.emptyList(), exprs, project.getRowType().getFieldNames());

    call.transformTo(newProject);
  }

  private static class RexOverReplacer extends RexShuttle {
    public boolean replacedValue;
    private final RexBuilder rexBuilder;

    public RexOverReplacer(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitOver(RexOver over) {
      if (over.getOperator().getName().toLowerCase().equals("percent_rank")) {
        replacedValue = true;
        return replacePercentRank(over);
      }
      if (over.getOperator().getName().toLowerCase().equals("cume_dist")) {
        replacedValue = true;
        return replaceCumeDist(over);
      }
      if (over.getOperator().getName().toLowerCase().equals("ntile")) {
        replacedValue = true;
        return replaceNTile(over);
      }
      return super.visitOver(over);
    }

    /**
     * Rewrite cume_dist() to the following:
     *
     * cume_dist() over([partition by clause] order by clause)
     *    = ((Count - Rank) + 1)/Count
     * where,
     *  Rank = rank() over([partition by clause] order by clause DESC)
     *  Count = count() over([partition by clause])
     */
    public RexNode replaceCumeDist(RexOver over) {
      Set<SqlKind> descendingSet = ImmutableSet.of(SqlKind.DESCENDING);
      List<RexFieldCollation> descendingCollation = new ArrayList<>();
      for (RexFieldCollation collation : over.getWindow().orderKeys) {
        descendingCollation.add(new RexFieldCollation(collation.getKey(), descendingSet));
      }
      ImmutableList allDescendingCollation = ImmutableList.copyOf(descendingCollation);
      RelDataType bigintType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
      RexNode rankNode = rexBuilder.makeOver(bigintType, SqlStdOperatorTable.RANK,
          over.getOperands(), over.getWindow().partitionKeys, allDescendingCollation,
          over.getWindow().getLowerBound(), over.getWindow().getUpperBound(),
          over.getWindow().isRows(), true, false, over.isDistinct(), over.ignoreNulls());
      RexNode countNode = rexBuilder.makeOver(bigintType, SqlStdOperatorTable.COUNT, over.getOperands(),
          over.getWindow().partitionKeys, ImmutableList.of() /* orderKeys */,
          over.getWindow().getLowerBound(), over.getWindow().getUpperBound(),
          over.getWindow().isRows(), true, false, over.isDistinct(), over.ignoreNulls());
      RexNode one =
          rexBuilder.makeCast(bigintType, rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)));
      RexNode countMinusRank =
          rexBuilder.makeCall(SqlStdOperatorTable.MINUS, countNode, rankNode);
      RexNode countMinusRankPlusOne =
          rexBuilder.makeCall(SqlStdOperatorTable.PLUS, countMinusRank, one);
      RexNode numeratorDouble = rexBuilder.makeCast(over.type, countMinusRankPlusOne);
      RexNode denominatorDouble = rexBuilder.makeCast(over.type, countNode);
      return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, numeratorDouble, denominatorDouble);
    }

    /**
     * Rewrite percent_rank() to the following:
     *
     * percent_rank() over([partition by clause] order by clause)
     *    = case
     *        when Count = 1 then 0
     *        else (Rank - 1)/(Count - 1)
     * where,
     *  Rank = rank() over([partition by clause] order by clause)
     *  Count = count() over([partition by clause])
     */
    public RexNode replacePercentRank(RexOver over) {
      RelDataType bigintType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
      RexNode rankNode = rexBuilder.makeOver(bigintType, SqlStdOperatorTable.RANK,
          over.getOperands(), over.getWindow().partitionKeys, over.getWindow().orderKeys,
          over.getWindow().getLowerBound(), over.getWindow().getUpperBound(),
          over.getWindow().isRows(), true, false, over.isDistinct(), over.ignoreNulls());
      RexNode one =
          rexBuilder.makeCast(bigintType, rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)));
      RexNode rankNodeMinusOne = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, rankNode, one);
      RexNode rankDouble = rexBuilder.makeCast(over.type, rankNodeMinusOne);
      RexNode countNode = rexBuilder.makeOver(bigintType, SqlStdOperatorTable.COUNT,
          over.getOperands(), over.getWindow().partitionKeys, ImmutableList.of() /*orderKeys */,
          over.getWindow().getLowerBound(), over.getWindow().getUpperBound(),
          over.getWindow().isRows(), true, false, over.isDistinct(), over.ignoreNulls());
      RexNode countNodeMinusOne = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, countNode, one);
      RexNode countDouble = rexBuilder.makeCast(over.type, countNodeMinusOne);
      RexNode rankDivCount = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, rankDouble, countDouble);
      RexNode caseCountOne = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, countNode, one);
      RexNode zeroDouble = rexBuilder.makeCast(over.type, rexBuilder.makeExactLiteral(BigDecimal.valueOf(0)));
      return rexBuilder.makeCall(SqlStdOperatorTable.CASE, caseCountOne, zeroDouble, rankDivCount);
    }

    /**
     * Rewrite ntile() to the following:
     *
     * ntile(B) over([partition by clause] order by clause)
     *    = floor(min(Count, B) * (RowNumber - 1)/Count) + 1
     * where,
     *  RowNumber = row_number() over([partition by clause] order by clause)
     *  Count = count() over([partition by clause])
     */
    public RexNode replaceNTile(RexOver over) {
      RelDataType bigintType = rexBuilder.getTypeFactory().createTypeWithNullability(
          rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT), true);
      RexNode rowNumberNode = rexBuilder.makeOver(bigintType, SqlStdOperatorTable.ROW_NUMBER,
          new ArrayList<>(), over.getWindow().partitionKeys, over.getWindow().orderKeys,
          over.getWindow().getLowerBound(), over.getWindow().getUpperBound(),
          over.getWindow().isRows(), true, false, over.isDistinct(), over.ignoreNulls());
      ImmutableList<RexFieldCollation> orderKeys = ImmutableList.of();
      RexNode countNode = rexBuilder.makeOver(bigintType, SqlStdOperatorTable.COUNT,
          new ArrayList<>(), over.getWindow().partitionKeys, orderKeys,
          over.getWindow().getLowerBound(), over.getWindow().getUpperBound(),
          over.getWindow().isRows(), true, false, over.isDistinct(), over.ignoreNulls());
      RexNode one =
          rexBuilder.makeCast(bigintType, rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)));
      RexNode minCountNTile =
          rexBuilder.makeCall(bigintType, MIN_OPERATOR, Arrays.asList(countNode, over.getOperands().get(0)));
      RexNode rowNumberMinusOne =
          rexBuilder.makeCall(bigintType, SqlStdOperatorTable.MINUS, Arrays.asList(rowNumberNode, one));
      RexNode numeratorInt =
          rexBuilder.makeCall(bigintType, SqlStdOperatorTable.MULTIPLY,
              Arrays.asList(minCountNTile, rowNumberMinusOne));
      RexNode ntileMinusOne =
          rexBuilder.makeCall(bigintType, INT_DIVIDE_OPERATOR, Arrays.asList(numeratorInt, countNode));
      return rexBuilder.makeCall(bigintType, SqlStdOperatorTable.PLUS, Arrays.asList(ntileMinusOne, one));
    }
  }

  private static SqlOperator createOperator(String func, List<SqlTypeName> argTypes,
      SqlTypeName retType) {
    RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl()));
    List<RelDataType> rdtArgTypes = new ArrayList<>();
    for (SqlTypeName argType : argTypes) {
      rdtArgTypes.add(rexBuilder.getTypeFactory().createSqlType(argType));
    }
    RelDataType rdtRetType = rexBuilder.getTypeFactory().createSqlType(retType);
    CalciteUDFInfo udfInfo = CalciteUDFInfo.createUDFInfo(func, rdtArgTypes, rdtRetType);
    return new HiveSqlFunction(func, SqlKind.OTHER_FUNCTION, udfInfo.returnTypeInference,
        udfInfo.operandTypeInference, udfInfo.operandTypeChecker,
        SqlFunctionCategory.USER_DEFINED_FUNCTION, true /*deterministic*/,
        false /*runtimeConstant*/);
  }
}
