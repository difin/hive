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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.List;
import java.util.Map;

/**
 * Function resolver for the sketch functions.
 */
public class SketchFunctionResolver extends ImpalaFunctionResolverImpl {

  public SketchFunctionResolver(FunctionHelper helper, String func, List<RexNode> inputNodes) {
    super(helper, func, inputNodes);
  }

  /**
   * Override of getFunction. The ds_*_sketch functions currently do not allow upcasting
   * (with the exception of "kll" functions).
   */
  @Override
  public ImpalaFunctionSignature getFunction(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap)
      throws SemanticException {
    ImpalaFunctionSignature funcSig = super.getFunction(functionDetailsMap);
    List<RelDataType> funcSigArgTypes = funcSig.getArgTypes();
    // The function signature may have less arguments if it hasVarArgs, but should never
    // have more.
    Preconditions.checkArgument(funcSigArgTypes.size() <= argTypes.size());
    for (int i = 0; i < funcSigArgTypes.size(); ++i) {
      SqlTypeName funcSigType = funcSigArgTypes.get(i).getSqlTypeName();
      SqlTypeName argType = argTypes.get(i).getSqlTypeName();
      if (funcSigType.equals(argType)) {
        continue;
      }
      if (SqlTypeName.CHAR_TYPES.contains(funcSigType) &&
          SqlTypeName.CHAR_TYPES.contains(argType)) {
        continue;
      }
      throw new SemanticException("No matching function with signature: " + toString());
    }
    return funcSig;
  }

  /** 
   * Returns true if the given function name should use the resolver.
   * Currently, the resolver is used for all dll*sketch* functions except
   * for the "kll" functions. So this includes "dll", "theta", "cpc", and
   * by default any new ones that are added.
   */
  public static boolean needsResolver(String func) {
    return func.startsWith("ds_") && func.contains("_sketch") && !func.contains("_kll_");
  }
}
