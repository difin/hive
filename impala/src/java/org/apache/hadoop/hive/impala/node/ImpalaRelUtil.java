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
package org.apache.hadoop.hive.impala.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaFunctionSignature;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.impala.funcmapper.AggFunctionDetails;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaFunctionHelper;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaTypeConverter;
import org.apache.hadoop.hive.impala.rex.ImpalaRexVisitor.ImpalaInferMappingRexVisitor;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This is a class to hold utility functions that assist with handling and
 * translation of {@link ImpalaPlanRel} nodes.
 */
public class ImpalaRelUtil {

  /**
   * Returns the aggregation function for the provided parameters. In Impala,
   * this could be either an aggregation or analytic function.
   */
  public static AggregateFunction getAggregateFunction(String currentDatabase,
      SqlAggFunction aggFunction, RelDataType retType,
      List<RelDataType> operandTypes) throws HiveException {

    String funcName = ImpalaFunctionHelper.getFuncName(AggFunctionDetails.getAllAggs(),
        aggFunction.getName(), currentDatabase);
    return getAggregateFunction(funcName, retType, operandTypes);
  }

  /**
   * Returns the aggregation function for the provided parameters. In Impala,
   * this could be either an aggregation or analytic function.
   */
  public static AggregateFunction getAggregateFunction(String funcName,
      RelDataType retType,
      List<RelDataType> operandTypes) throws HiveException {

    AggFunctionDetails funcDetails = AggFunctionDetails.get(funcName, operandTypes,
        retType);

    if (funcDetails == null) {
      throw new SemanticException("Could not find function \"" + funcName + "\"");
    }

    List<Type> argTypes = ImpalaTypeConverter.createImpalaTypes(operandTypes);
    Type impalaRetType = ImpalaTypeConverter.createImpalaType(retType);
    int intermediateTypePrecision = funcDetails.intermediateTypeLength != 0
        ? funcDetails.intermediateTypeLength
        : retType.getPrecision();
    Type intermediateType = ImpalaTypeConverter.createImpalaType(funcDetails.getIntermediateType(),
        intermediateTypePrecision, retType.getScale());

    return createAggFunction(funcDetails, funcDetails.getName(), argTypes, impalaRetType,
        intermediateType);
  }

  private static AggregateFunction createAggFunction(AggFunctionDetails funcDetails, String aggFuncName,
      List<Type> operandTypes, Type retType, Type intermediateType) {
    Preconditions.checkState(funcDetails.isAgg || funcDetails.isAnalyticFn);
    if (!funcDetails.isAgg) {
      return AggregateFunction
          .createAnalyticBuiltin(BuiltinsDb.getInstance(), aggFuncName, operandTypes, retType, intermediateType,
              funcDetails.initFnSymbol, funcDetails.updateFnSymbol, funcDetails.removeFnSymbol, funcDetails.getValueFnSymbol,
              funcDetails.finalizeFnSymbol);
    }
    // Use the createRewrittenBuiltin() method for grouping_id since it gets
    // rewritten internally in Impala.
    if (aggFuncName.equalsIgnoreCase("grouping_id")) {
      return AggregateFunction.createRewrittenBuiltin(BuiltinsDb.getInstance(),
          aggFuncName, operandTypes, retType, funcDetails.ignoresDistinct, funcDetails.isAnalyticFn,
          funcDetails.returnsNonNullOnEmpty);
    }
    // Some agg functions are used both in analytic functions and regular aggregations (e.g. count)
    // We can treat them both as a regular builtin.
    AggregateFunction agg = new AggregateFunction(funcDetails.getFunctionName(), operandTypes,
        retType, intermediateType, funcDetails.hdfsUri, funcDetails.updateFnSymbol,
        funcDetails.initFnSymbol, funcDetails.serializeFnSymbol, funcDetails.mergeFnSymbol,
        funcDetails.getValueFnSymbol, funcDetails.removeFnSymbol, funcDetails.finalizeFnSymbol);
    agg.setBinaryType(funcDetails.binaryType);
    return agg;
  }

  /**
   * Given an input and analyzer instance, translate a rex node into
   * an Impala expression.
   */
  protected static Expr getExpr(RexNode exp, Analyzer analyzer, ImpalaPlanRel input) {
    ImpalaInferMappingRexVisitor visitor = new ImpalaInferMappingRexVisitor(
        analyzer, ImmutableList.of(input), input.getCluster().getRexBuilder());
    return exp.accept(visitor);
  }

  /**
   * Given an input and analyzer instance, translate a rex node list into
   * an Impala expression list.
   */
  protected static List<Expr> getExprs(List<RexNode> exp, Analyzer analyzer, ImpalaPlanRel input) {
    ImpalaInferMappingRexVisitor visitor = new ImpalaInferMappingRexVisitor(
        analyzer, ImmutableList.of(input), input.getCluster().getRexBuilder());
    return exp.stream().map(e -> e.accept(visitor)).collect(Collectors.toList());
  }

  /**
   * Gather all Impala Hdfs table scans in the plan starting from a root node and populate
   * the supplied tableScans list
   */
  public static void gatherTableScans(ImpalaPlanRel rootRelNode, List<ImpalaHdfsScanRel> tableScans) {
    if (rootRelNode instanceof ImpalaHdfsScanRel) {
      tableScans.add((ImpalaHdfsScanRel) rootRelNode);
      return;
    }
    for (RelNode child : rootRelNode.getInputs()) {
      gatherTableScans((ImpalaPlanRel) child, tableScans);
    }
  }

}
