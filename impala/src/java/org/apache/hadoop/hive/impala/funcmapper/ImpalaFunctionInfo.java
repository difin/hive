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

import org.apache.hadoop.hive.ql.exec.FunctionInfo;

/**
 * Impala's version of the FunctionInfo class.  Stores state that is passed
 * into the FunctionHelper object.
 */
public class ImpalaFunctionInfo extends FunctionInfo {

  private ImpalaFunctionResolver funcResolver;

  private ImpalaFunctionSignature impalaFunctionSignature;

  public ImpalaFunctionInfo(String funcName) {
    super(funcName, "");
  }

  public void setFunctionResolver(ImpalaFunctionResolver funcResolver) {
    this.funcResolver = funcResolver;
  }

  public ImpalaFunctionResolver getFunctionResolver() {
    return funcResolver;
  }

  public void setImpalaFunctionSignature(ImpalaFunctionSignature ifs) {
    impalaFunctionSignature = ifs;
  }

  public ImpalaFunctionSignature getImpalaFunctionSignature() {
    return impalaFunctionSignature;
  }

  @Override
  public boolean isGenericUDAFResolver() {
    return AggFunctionDetails.isAggFunction(getDisplayName());
  }

  /**
   * isGenericUDF returns true when the function is a scalar. The logic
   * below is not fool-proof. There are two checks here.  It should be
   * in the ScalarFunctionDetails. However, there are some CAST functions that
   * exist in Hive but do not exist in Impala, and therefore are not in the
   * ScalarFunctionDetails. To rectify this, there is also a call to the
   * IMPALA_OPERATOR_MAP which can identify the relevant Calcite operator
   * type. In this case, the operator will be CAST which is not an aggregate
   * function.
   *
   * The isGenericUDF function is only called in a special case for CDPD-46776,
   * so this logic should work. The one worry here is that there may be some
   * scalar functions that slip through the cracks with this call. It is possible
   * a future change might be needed so that all scalar functions are definitely
   * returning true with this call, but because of the limited use of isGenericUDF
   * the below logic should be good enough.
   */
  @Override
  public boolean isGenericUDF() {
    return ScalarFunctionDetails.isScalarFunction(getDisplayName()) ||
        !ImpalaOperatorTable.IMPALA_OPERATOR_MAP.get(getDisplayName().toUpperCase()).isAggregator();
  }
}
