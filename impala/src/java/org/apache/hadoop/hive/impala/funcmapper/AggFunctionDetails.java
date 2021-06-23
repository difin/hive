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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import com.google.gson.reflect.TypeToken;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TPrimitiveType;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Contains details for Aggregation functions.  These functions are retrieved from the Impala
 * frontend and shared library and are stored in the SCALAR_FUNCTIONS_MAP.
 * Implements FunctionDetails because ImpalaFunctionSignature can be used
 * as a key for both AggFunctionDetails and ScalarFunctionDetails.
 */
public class AggFunctionDetails implements FunctionDetails {

  public final String dbName;
  public final String fnName;
  public final String impalaFnName;
  @Expose(serialize=false,deserialize=false)
  private final Type impalaRetType;
  @Expose(serialize=false,deserialize=false)
  private final List<Type> impalaArgTypes;
  @Expose(serialize=false,deserialize=false)
  private final Type impalaIntermediateType;
  public final int intermediateTypeLength;
  public final boolean isAnalyticFn;
  public final boolean isPersistent;
  public final String updateFnSymbol;
  public final String initFnSymbol;
  public final String mergeFnSymbol;
  public final String finalizeFnSymbol;
  public final String getValueFnSymbol;
  public final String removeFnSymbol;
  public final String serializeFnSymbol;
  public final boolean ignoresDistinct;
  public final boolean returnsNonNullOnEmpty;
  public final boolean isAgg;
  public final TFunctionBinaryType binaryType;
  private final ImpalaFunctionSignature ifs;

  // Set of all aggregate functions available in Impala
  static final Set<String> AGG_BUILTINS = new HashSet<>();
  // Set of all analytic functions available in Impala
  static final Set<String> ANALYTIC_BUILTINS = new HashSet<>();
  // Map containing an aggregate Impala signature to the details associated with the signature.
  // A signature consists of the function name, the operand types and the return type.
  static final Map<ImpalaFunctionSignature, AggFunctionDetails> AGG_BUILTINS_MAP = Maps.newHashMap();

  /**
   * Fetch the functions from the Impala frontend and return them as a AggFunctionDetails list.
   */
  public static List<AggFunctionDetails> createAggFunctionDetailsFromImpala() {
    List<AggFunctionDetails> result = new ArrayList<>();
    // Retrieve all functions from the Impala front-end
    Map<String, List<Function>> functions = BuiltinsDb.getInstance().getAllFunctions();
    Set<String> allFnNames = new HashSet<>();
    for (String impalaFnName : functions.keySet()) {
      for (Function func : functions.get(impalaFnName)) {
        // The 'grouping' function is found in the 'agg' functions within Impala, but
        // within Hive, we treat it as if it's scalar. So we ignore the function here.
        // CDPD-28066:  Aggregate grouping function not supported. It's an overloaded
        // function that is used both in analytical functions and in aggregate functions..
        if (func.functionName().equals("grouping")) {
          continue;
        }
        if (func instanceof AggregateFunction) {
          AggFunctionDetails afd = new AggFunctionDetails((AggregateFunction) func);
          result.add(afd);
        }
      }
    }
    return result;
  }

  /**
   * Add all functions from the Impala shared library into static structures to be used
   * at compilation time.
   */
  public static void addFunctionsFromImpala(List<AggFunctionDetails> afdList) {
    // This method should only be called once at initialization.
    Preconditions.checkState(AGG_BUILTINS_MAP.isEmpty());
    List<ImpalaFunctionSignature> ifsList = new ArrayList<>();
    for (AggFunctionDetails afd : afdList) {
      AGG_BUILTINS_MAP.put(afd.ifs, afd);
      if (afd.isAgg) {
        AGG_BUILTINS.add(afd.fnName.toUpperCase());
      }
      if (afd.isAnalyticFn) {
        ANALYTIC_BUILTINS.add(afd.fnName.toUpperCase());
      }
      ifsList.add(afd.ifs);
    }

    ImpalaFunctionSignature.populateCastCheckBuiltins(ifsList);
  }

  public AggFunctionDetails(AggregateFunction func) {
    dbName = func.dbName();
    fnName = func.functionName();
    impalaFnName = func.functionName();
    impalaRetType = func.getReturnType();
    impalaArgTypes = new ArrayList<Type>(Arrays.asList(func.getArgs()));
    isPersistent = func.isPersistent();
    binaryType = func.getBinaryType();
    impalaIntermediateType = (func.getIntermediateType() == null)
        ? impalaRetType : func.getIntermediateType();
    intermediateTypeLength = (func.getIntermediateType() == null)
        ? 0 : ((ScalarType)func.getIntermediateType()).getSlotSize();
    isAnalyticFn = func.isAnalyticFn();
    updateFnSymbol = func.getUpdateFnSymbol();
    initFnSymbol = func.getInitFnSymbol();
    mergeFnSymbol = func.getMergeFnSymbol();
    getValueFnSymbol = func.getValueFnSymbol();
    finalizeFnSymbol = func.getFinalizeFnSymbol();
    removeFnSymbol = func.getRemoveFnSymbol();
    serializeFnSymbol = func.getSerializeFnSymbol();
    ignoresDistinct = func.ignoresDistinct();
    returnsNonNullOnEmpty = func.returnsNonNullOnEmpty();
    isAgg = func.isAggregateFn();
    ifs = ImpalaFunctionSignature.create(fnName, getArgTypes(), getRetType(), false, false);
  }

  public AggFunctionDetails(AggFunctionWrapper func) {
    dbName = func.dbName();
    fnName = func.functionName();
    impalaFnName = func.functionName();
    impalaRetType = func.getReturnType();
    impalaArgTypes = func.getArgTypes();
    isPersistent = func.isPersistent();
    binaryType = func.getBinaryType();
    impalaIntermediateType = (func.getIntermediateType() == null)
        ? impalaRetType : func.getIntermediateType();

    intermediateTypeLength = func.getIntermediateTypeLength();

    isAnalyticFn = func.isAnalyticFn();
    updateFnSymbol = func.getUpdateFnSymbol();
    initFnSymbol = func.getInitFnSymbol();
    mergeFnSymbol = func.getMergeFnSymbol();
    getValueFnSymbol = func.getValueFnSymbol();
    finalizeFnSymbol = func.getFinalizeFnSymbol();
    removeFnSymbol = func.getRemoveFnSymbol();
    serializeFnSymbol = func.getSerializeFnSymbol();
    ignoresDistinct = func.ignoresDistinct();
    returnsNonNullOnEmpty = func.returnsNonNullOnEmpty();
    isAgg = func.isAggregateFn();
    ifs = ImpalaFunctionSignature.create(fnName, getArgTypes(), getRetType(), false, false);
  }

  public static Collection<AggFunctionDetails> getAllFuncDetails() {
    return AGG_BUILTINS_MAP.values();
  }

  public List<Type> getArgTypes() {
    return impalaArgTypes;
  }

  public Type getRetType() {
    return impalaRetType;
  }

  public Type getIntermediateType() {
    return impalaIntermediateType;
  }

  @Override
  public ImpalaFunctionSignature getSignature() {
    return ifs;
  }

  /**
   * Retrieve function details about an agg function given a signature
   * containing the function name, return type, and operand types.
   */
  public static AggFunctionDetails get(String name, List<RelDataType> operandTypes,
       RelDataType retType) {

    ImpalaFunctionSignature sig = ImpalaFunctionSignature.fetch(AGG_BUILTINS_MAP,
        name, operandTypes, retType);

    if (sig != null) {
      return AGG_BUILTINS_MAP.get(sig);
    }
    return null;
  }

  public static boolean isAggFunction(String fnName) {
    return AGG_BUILTINS.contains(fnName.toUpperCase());
  }

  public static AggFunctionDetails get(String name, List<Type> operandTypes,
      Type retType, boolean hasVarArgs) {

    ImpalaFunctionSignature sig = ImpalaFunctionSignature.create(name.toLowerCase(), operandTypes, retType,
        hasVarArgs, null);

    if (sig != null) {
      return AGG_BUILTINS_MAP.get(sig);
    }
    return null;
  }
}
