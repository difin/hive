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

package org.apache.hadoop.hive.impala.funcmapper;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.hadoop.hive.ql.parse.type.HiveFunctionHelper;
import org.apache.impala.catalog.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HiveGenericUDFFunctionResolver is the function resolver used for Hive
 * Generic functions. This ImpalaFunctionResolver serves as a wrapper around
 * the class that does the actual function resolving within Hive.
 */
public class HiveGenericUDFFunctionResolver implements ImpalaFunctionResolver {
  private final String func;

  private final List<RexNode> inputNodes;

  private final HiveFunctionHelper functionHelper;

  private final FunctionInfo functionInfo;

  private RelDataType retType;

  public HiveGenericUDFFunctionResolver(HiveFunctionHelper helper, FunctionInfo functionInfo,
      String func, List<RexNode> inputNodes) {
    this.func = func;
    this.inputNodes = inputNodes;
    this.functionHelper = helper;
    this.functionInfo = functionInfo;
  }

  @Override
  public ImpalaFunctionSignature getFunction(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap)
      throws SemanticException {
    return ImpalaFunctionSignature.createDummyFuncSignature(func);
  }

  @Override
  public List<RexNode> getConvertedInputs(ImpalaFunctionSignature function) throws HiveException {
    try {
      return functionHelper.convertInputs(functionInfo, transformOperands(inputNodes), retType);
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RelDataType getRetType(ImpalaFunctionSignature function, List<RexNode> operands) {
    try {
      this.retType = functionHelper.getReturnType(functionInfo, transformOperands(operands));
      // Impala does not allow null types to be returned, so we change it to a boolean.
      // (which is the same logic within Impala).
      if (this.retType.toString().toLowerCase().equals("null")) {
        this.retType = ImpalaTypeConverter.getRelDataType(Type.BOOLEAN, true);
      }
      return this.retType;
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
  }

  private List<RexNode> transformOperands(List<RexNode> operands) {
    if (!ScalarFunctionDetails.isHiveFunction(functionInfo.getDisplayName())) {
      return operands;
    }
    RexBuilder rexBuilder = functionHelper.getRexNodeExprFactory().getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    List<RexNode> result = new ArrayList<>(operands.size());

    for (RexNode operand: operands) {
      RexNode value = operand;
      if (isLiteralTypeSmallerThanInteger(operand)) {
        value = rexBuilder
            .makeLiteral(
                RexLiteral.intValue(operand),
                typeFactory.createSqlType(SqlTypeName.INTEGER),
                false
            );
      }
      result.add(value);
    }

    return result;
  }

  private boolean isLiteralTypeSmallerThanInteger(RexNode node) {
    if (!(node instanceof RexLiteral)) {
      return false;
    }
    return node.getType().getSqlTypeName() == SqlTypeName.TINYINT ||
        node.getType().getSqlTypeName() == SqlTypeName.SMALLINT;
  }

  @Override
  public RexNode createRexNode(ImpalaFunctionSignature function, List<RexNode> inputs,
      RelDataType returnDataType) {
    try {
      return functionHelper.getExpression(func, functionInfo, inputs, returnDataType);
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
  }

  public static ImpalaFunctionResolver create(FunctionHelper helper, String func,
      List<RexNode> inputNodes) throws SemanticException {
    HiveFunctionHelper hiveFunctionHelper =
        new HiveFunctionHelper(helper.getRexNodeExprFactory().getRexBuilder());
    FunctionInfo functionInfo = hiveFunctionHelper.getFunctionInfo(func);
    if (functionInfo == null) {
      return null;
    }
    return new HiveGenericUDFFunctionResolver(hiveFunctionHelper, functionInfo, func, inputNodes);

  }
}
