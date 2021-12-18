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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.impala.catalog.Type;

import java.util.List;
import java.util.Map;

/**
 * A cast function resolver created from a RexCall specifically used when
 * the syntax includes the "format" keyword.
 */
public class CastFormatFunctionResolver extends ImpalaFunctionResolverImpl {

  private final RelDataType returnType;

  private final RelDataType intermediateType;

  /**
   * The arguments passed into the cast_format function are:
   *    0: the HiveParser token type
   *    1: the column that will be cast
   *    2: the format of the column (e.g. 'YYYY/MM/DD').
   */
  CastFormatFunctionResolver(FunctionHelper helper, List<RexNode> inputNodes) {
    super(helper, SqlStdOperatorTable.CAST, "cast", inputNodes.subList(1,3));

    int token = RexLiteral.intValue(inputNodes.get(0));
    switch(token) {
      case HiveParser.TOK_DATE:
        returnType = ImpalaTypeConverter.getRelDataType(Type.DATE, true);
        intermediateType = null;
        break;
      case HiveParser.TOK_TIMESTAMP:
        returnType = ImpalaTypeConverter.getRelDataType(Type.TIMESTAMP, true);
        intermediateType = null;
        break;
      case HiveParser.TOK_STRING:
        returnType = ImpalaTypeConverter.getRelDataType(Type.STRING, true);
        intermediateType = null;
        break;
      case HiveParser.TOK_CHAR:
        // When converting to the format CHAR, Impala requires that the type
        // first be converted into a STRING.
        returnType = ImpalaTypeConverter.createCharType(
            rexBuilder.getTypeFactory(), RexLiteral.intValue(inputNodes.get(3)));
        intermediateType = ImpalaTypeConverter.getRelDataType(Type.STRING, true);
        break;
      case HiveParser.TOK_VARCHAR:
        returnType = ImpalaTypeConverter.getRelDataType(Type.STRING, true);
        intermediateType = null;
        break;
      default:
        throw new RuntimeException("Invalid token passed for format: " + token);
    }
  }

  @Override
  public RelDataType getRetType(ImpalaFunctionSignature funcSig, List<RexNode> operands) {
    Preconditions.checkNotNull(returnType);
    // Need to override the getRetType method since it is derived from one of the input
    // parameters from the cast_format function.
    return returnType;
  }

  @Override
  public RexNode createRexNode(ImpalaFunctionSignature candidate, List<RexNode> inputs,
      RelDataType retType) {
    if (intermediateType == null) {
      return super.createRexNode(candidate, inputs, returnType);
    }
    RexNode intermediateRexNode = rexBuilder.makeCall(intermediateType, op, inputs);
    RexNode r = rexBuilder.makeCall(returnType, op, ImmutableList.of(intermediateRexNode));
    return r;
  }
}
