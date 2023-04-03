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
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FloorIntervalFunctionResolver extends ImpalaFunctionResolverImpl {

  private static final String floorFuncText = "FLOOR";
  private final FunctionInfo floorFunctionInfo;
  private RelDataType floorReturnType;
  private List<RexNode> intervalExpr;

  public FloorIntervalFunctionResolver(FunctionHelper helper, String func,
                                       List<RexNode> inputNodes) {
    super(helper, func, inputNodes);
    try {
      floorFunctionInfo = helper.getFunctionInfo(floorFuncText);
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RelDataType getRetType(ImpalaFunctionSignature funcSig, List<RexNode> operands) {
    String intervalFunctionText = func.toLowerCase().split("_")[1];
    FunctionInfo intervalFunctionInfo;
    try {
      intervalFunctionInfo = helper.getFunctionInfo(intervalFunctionText);
      RelDataType intervalReturnType = helper.getReturnType(intervalFunctionInfo, operands);
      List<RexNode> newIntervalInputs = helper.convertInputs(
          intervalFunctionInfo, operands, intervalReturnType
      );
      intervalExpr = Collections.singletonList(helper.getExpression(
          intervalFunctionText, intervalFunctionInfo, newIntervalInputs, intervalReturnType
      ));
      floorReturnType = helper.getReturnType(floorFunctionInfo, intervalExpr);
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
    return floorReturnType;
  }

  @Override
  public List<RexNode> getConvertedInputs(ImpalaFunctionSignature candidate)
      throws HiveException {
    return helper.convertInputs(floorFunctionInfo, intervalExpr, floorReturnType);
  }

  @Override
  public RexNode createRexNode(ImpalaFunctionSignature candidate, List<RexNode> inputs,
                               RelDataType returnDataType) {
    try {
      return helper.getExpression(floorFuncText, floorFunctionInfo, inputs, floorReturnType);
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ImpalaFunctionSignature getFunction(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap
  ) throws SemanticException {
    return null;
  }
}
