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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import com.google.gson.reflect.TypeToken;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TPrimitiveType;

import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains details for Aggregation functions.  These functions are retrieved from the Impala
 * frontend and shared library and are stored in the SCALAR_FUNCTIONS_MAP.
 * Implements FunctionDetails because ImpalaFunctionSignature can be used
 * as a key for both AggFunctionDetails and ScalarFunctionDetails.
 */
public class AggFunctionDetails implements FunctionDetails {
  protected static final Logger LOG = LoggerFactory.getLogger(AggFunctionDetails.class);

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
  public final HdfsUri hdfsUri;
  private final ImpalaFunctionSignature ifs;

  // Set of all aggregate functions available in Impala
  private static final Set<String> AGG_BUILTINS = new HashSet<>();
  // Set of all analytic functions available in Impala
  private static final Set<String> ANALYTIC_BUILTINS = new HashSet<>();
  // Map containing an aggregate Impala signature to the details associated with the signature.
  // A signature consists of the function name, the operand types and the return type.
  private static final Map<ImpalaFunctionSignature, AggFunctionDetails> AGG_BUILTINS_MAP = Maps.newHashMap();

  // Set of all agg functions available in Impala
  // This contains both the builtins and the UDFs
  private static volatile Set<String> ALL_AGG_FUNCS = new HashSet<>();

  // Map containing an agg Impala signature to the details associated with the signature.
  // A signature consists of the function name, the operand types and the return type.
  // This contains both the builtins and the UDFs
  private static volatile Map<ImpalaFunctionSignature, AggFunctionDetails>
      ALL_AGG_MAP = Maps.newHashMap();
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
          AggFunctionDetails afd =
              new AggFunctionDetails(func.functionName(), (AggregateFunction) func);
          // Some of the sketch functions (e.g. data_kll_sketch) have a function defined in
          // the builtins, but there is no "init" function (or others) rendering it
          // unusable, so we do not add these to our list of functions.
          if (!(afd.initFnSymbol == null && func.functionName().contains("sketch"))) {
            result.add(afd);
          }
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
        AGG_BUILTINS.add(afd.fnName.toLowerCase());
      }
      if (afd.isAnalyticFn) {
        ANALYTIC_BUILTINS.add(afd.fnName.toLowerCase());
      }
      ifsList.add(afd.ifs);
    }

    // Also add functions that don't map directly into Impala (which are stored in a "json"
    // resources file.
    for (NonImpalaFunction nif : NonImpalaFunction.getNonImpalaFunctionsFromFile("/impala_aggs.json")) {
      ImpalaFunctionSignature ifs = ImpalaFunctionSignature.create(nif.fnName, nif.getArgTypes(),
          nif.getRetType(), false, false);
      AggFunctionDetails afd = new AggFunctionDetails(nif);
      AGG_BUILTINS_MAP.put(afd.ifs, afd);
      if (nif.isAnalyticFn) {
        ANALYTIC_BUILTINS.add(nif.fnName.toLowerCase());
      }
      if (nif.isAgg) {
        AGG_BUILTINS.add(nif.fnName.toLowerCase());
      }
      ifsList.add(ifs);
    }

    ImpalaFunctionSignature.populateCastCheckFunctions(ifsList);
    //XXX: CDPD-30169: support for analytic UDFs (we probably need
    // to copy over ANALYTIC_BUILTINS to support this.)
    ALL_AGG_FUNCS = ImmutableSet.copyOf(AGG_BUILTINS);
    ALL_AGG_MAP = ImmutableMap.copyOf(AGG_BUILTINS_MAP);

  }

  public static void addUDFs(List<Function> functions) {
    List<ImpalaFunctionSignature> ifsList = new ArrayList<>();
    Set<String> allFuncs = new HashSet<>(AGG_BUILTINS);
    Map<ImpalaFunctionSignature, AggFunctionDetails> allFuncsMap =
        new HashMap<>(AGG_BUILTINS_MAP);

    for (Function func : functions) {
      if (!(func instanceof AggregateFunction)) {
        continue;
      }
      // udf function name is saved with the database
      String fullFunctionName = func.getFunctionName().toString();
      LOG.info("Registering function " + fullFunctionName);
      fullFunctionName = fullFunctionName.toLowerCase();
      AggFunctionDetails afd =
          new AggFunctionDetails(fullFunctionName, (AggregateFunction) func);
      allFuncsMap.put(afd.ifs, afd);
      allFuncs.add(fullFunctionName);
      ifsList.add(afd.ifs);
    }
    ImpalaFunctionSignature.populateCastCheckFunctions(ifsList);
    ALL_AGG_FUNCS = ImmutableSet.copyOf(allFuncs);
    ALL_AGG_MAP = ImmutableMap.copyOf(allFuncsMap);
    LOG.info("Done Registering functions ");
  }

  public AggFunctionDetails(String funcName, AggregateFunction func) {
    dbName = func.dbName();
    fnName = funcName;
    impalaFnName = func.functionName();
    impalaRetType = func.getReturnType();
    impalaArgTypes = new ArrayList<Type>(Arrays.asList(func.getArgs()));
    isPersistent = func.isPersistent();
    binaryType = func.getBinaryType();
    hdfsUri = func.getLocation();
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
    ifs = ImpalaFunctionSignature.create(fnName.toLowerCase(), getArgTypes(), getRetType(),
        false, false);
  }

  public AggFunctionDetails(AggFunctionWrapper func) {
    dbName = func.dbName();
    fnName = func.functionName();
    impalaFnName = func.functionName();
    impalaRetType = func.getReturnType();
    impalaArgTypes = func.getArgTypes();
    isPersistent = func.isPersistent();
    binaryType = func.getBinaryType();
    hdfsUri = null;
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
    ifs = ImpalaFunctionSignature.create(fnName.toLowerCase(), getArgTypes(), getRetType(),
        false, false);
  }

  public AggFunctionDetails(NonImpalaFunction func) {
    dbName = func.dbName;
    fnName = func.fnName;
    impalaFnName = func.fnName;
    impalaRetType = func.getRetType();
    impalaArgTypes = func.getArgTypes();
    isPersistent = false;
    binaryType = null;
    hdfsUri = null;
    impalaIntermediateType = impalaRetType;

    intermediateTypeLength = 0;

    isAnalyticFn = func.isAnalyticFn;
    updateFnSymbol = null;
    initFnSymbol = null;
    mergeFnSymbol = null;
    getValueFnSymbol = null;
    finalizeFnSymbol = null;
    removeFnSymbol = null;
    serializeFnSymbol = null;
    ignoresDistinct = false;
    returnsNonNullOnEmpty = false;
    isAgg = func.isAgg;
    ifs = ImpalaFunctionSignature.create(fnName, getArgTypes(), getRetType(), false, false);
  }

  public static Collection<AggFunctionDetails> getAllFuncDetails() {
    return ALL_AGG_MAP.values();
  }

  public List<Type> getArgTypes() {
    return impalaArgTypes;
  }

  public String getName() {
    return impalaFnName;
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

  public FunctionName getFunctionName() {
    return new FunctionName(dbName, impalaFnName);
  }

  public static int fetchFunctionInfo(DataOutputStream outStream, String func)
      throws IOException, SemanticException {
    return 0;
  }

  /**
   * Retrieve function details about an agg function given a signature
   * containing the function name, return type, and operand types.
   */
  public static AggFunctionDetails get(String name, List<RelDataType> operandTypes,
       RelDataType retType) {

    ImpalaFunctionSignature sig = ImpalaFunctionSignature.fetch(ALL_AGG_MAP,
        name, operandTypes, retType);

    if (sig != null) {
      return ALL_AGG_MAP.get(sig);
    }
    return null;
  }

  public static boolean isAggFunction(String name) {
    return ALL_AGG_FUNCS.contains(name.toLowerCase());
  }

  public static boolean isAggFunction(String name, String db) {
    return ALL_AGG_FUNCS.contains(name.toLowerCase()) || ALL_AGG_FUNCS.contains(db + "." + name);
  }

  public static boolean isAnalyticFunction(String name) {
    return ANALYTIC_BUILTINS.contains(name.toLowerCase());
  }

  public static AggFunctionDetails get(String name, List<Type> operandTypes,
      Type retType, boolean hasVarArgs) {

    ImpalaFunctionSignature sig = ImpalaFunctionSignature.create(name.toLowerCase(), operandTypes, retType,
        hasVarArgs, null);

    if (sig != null) {
      return ALL_AGG_MAP.get(sig);
    }
    return null;
  }

  public static AggFunctionDetails get(ImpalaFunctionSignature ifs) {
    return ALL_AGG_MAP.get(ifs);
  }

  public static Set<String> getAllAggs() {
    return ALL_AGG_FUNCS;
  }

  public static Map<ImpalaFunctionSignature, AggFunctionDetails>  getAggsMap() {
    return ALL_AGG_MAP;
  }
}
