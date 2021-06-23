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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import com.google.gson.reflect.TypeToken;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TPrimitiveType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Contains details for Scalar functions.  These functions are retrieved from the Impala
 * frontend and shared library and are stored in the SCALAR_FUNCTIONS_MAP.
 * There are also some functions that are supported in SQL that do not have an Impala function
 * equivalent. One example is "between", which gets converted into a "<" and ">" operation. These
 * are stored in the "impala_scalars.json" resource file, and the names are kept in the
 * SCALAR_BUILTINS String list.
 * For testing, the Impala ".so" library does not exist so the functions are retrieved via a "json"
 * file. This can be found in the qtest-impala/src/test/resources directory.
 */
public class ScalarFunctionDetails implements FunctionDetails {

  public final String dbName;

  public final String fnName;

  public final String impalaFnName;

  @Expose(serialize=false,deserialize=false)
  public final Type impalaRetType;

  @Expose(serialize=false,deserialize=false)
  public final List<Type> impalaArgTypes;

  public final String symbolName;

  public final String prepareFnSymbol;

  public final String closeFnSymbol;

  public final boolean hasVarArgs;

  public final boolean isPersistent;

  public final boolean castUp;

  public final TFunctionBinaryType binaryType;

  public final HdfsUri hdfsUri;

  public final boolean retTypeAlwaysNullable;

  public final ImpalaFunctionSignature ifs;

  // Set of all scalar functions available in Impala
  static final Set<String> SCALAR_BUILTINS = new HashSet<>();

  // Map containing a scalar  Impala signature to the details associated with the signature.
  // A signature consists of the function name, the operand types and the return type.
  static final Map<ImpalaFunctionSignature, ScalarFunctionDetails>
      SCALAR_BUILTINS_MAP = Maps.newHashMap();

  /**
   * Fetch the functions from the Impala frontend and return them as a ScalarFunctionDetails list.
   */
  public static List<ScalarFunctionDetails> createScalarFunctionDetailsFromImpala() {
    List<ScalarFunctionDetails> result = new ArrayList<>();
    Map<String, List<Function>> functions = BuiltinsDb.getInstance().getAllFunctions();
    for (String impalaFnName : functions.keySet()) {
      // The functions.get() method retrieves all the functions related to a given function
      // name with its different parameters.
      for (Function func : functions.get(impalaFnName)) {
        // Skip over the non-scalar functions.
        if (!(func instanceof ScalarFunction)) {
          continue;
        }
        // The getFunctionNames() handles the fact that there may be multiple Calcite function
        // names mapping to the same Impala function. For instance, both "or" and "||" map to
        // Impala's "or".
        for (String fnName : getFunctionNames(func.functionName())) {
          ScalarFunctionWrapper funcWrapper = new ScalarFunctionWrapperImpl(func);
          ScalarFunctionDetails sfd = new ScalarFunctionDetails(fnName, funcWrapper);
          result.add(sfd);
        }
      }
    }
    return result;
  }

  /**
   * Add all functions from the Impala shared library into static structures to be used
   * at compilation time.
   */
  public static void addFunctionsFromImpala(List<ScalarFunctionDetails> sfdList) {
    Preconditions.checkState(SCALAR_BUILTINS_MAP.isEmpty());
    List<ImpalaFunctionSignature> ifsList = new ArrayList<>();
    for (ScalarFunctionDetails sfd : sfdList) {
      SCALAR_BUILTINS_MAP.put(sfd.ifs, sfd);
      SCALAR_BUILTINS.add(sfd.fnName.toUpperCase());
      ifsList.add(sfd.ifs);
    }

    // Also add functions that don't map directly into Impala (which are stored in a "json"
    // resources file.
    for (NonImpalaFunction nif : NonImpalaFunction.getNonImpalaFunctionsFromFile()) {
      ImpalaFunctionSignature ifs = ImpalaFunctionSignature.create(nif.fnName, nif.getArgTypes(),
          nif.getRetType(), nif.hasVarArgs, nif.retTypeAlwaysNullable);
      SCALAR_BUILTINS.add(nif.fnName.toUpperCase());
      ifsList.add(ifs);
    }

    ImpalaFunctionSignature.populateCastCheckBuiltins(ifsList);
  }

  public static Set<String> getFunctionNames(String impalaFnName) {
    return FunctionDetailStatics.IMPALA_FUNCTION_MAP.containsKey(impalaFnName)
        ? FunctionDetailStatics.IMPALA_FUNCTION_MAP.get(impalaFnName)
        : ImmutableSet.of(impalaFnName);
  }

  public static Collection<ScalarFunctionDetails> getAllFuncDetails() {
    return SCALAR_BUILTINS_MAP.values();
  }

  public ScalarFunctionDetails(String fnName, ScalarFunctionWrapper func) {
    this.fnName = fnName;

    this.dbName = func.dbName();

    this.impalaFnName = func.functionName();

    this.impalaRetType = ImpalaTypeConverter.getNormalizedType(func.getRetType());

    this.impalaArgTypes = ImpalaTypeConverter.getNormalizedTypeList(func.getArgTypes());

    this.symbolName = func.getSymbolName();

    this.prepareFnSymbol = func.getPrepareFnSymbol();

    this.closeFnSymbol = func.getCloseFnSymbol();

    this.hasVarArgs = FunctionDetailStatics.OVERRIDE_HAS_VAR_ARGS_FUNCS.contains(fnName)
        ? true
        : func.hasVarArgs();

    this.isPersistent = func.isPersistent();

    this.binaryType = func.getBinaryType();

    this.hdfsUri = func.getLocation();

    this.castUp = fnName.startsWith("cast")
        ? isSupportedCast(impalaArgTypes.get(0), impalaRetType)
        : false;

    this.retTypeAlwaysNullable =
        FunctionDetailStatics.RET_TYPE_ALWAYS_NULLABLE_FUNCS.contains(fnName);

    this.ifs = ImpalaFunctionSignature.create(fnName, getArgTypes(), getRetType(),
          hasVarArgs, retTypeAlwaysNullable);
  }

  public List<Type> getArgTypes() {
     return impalaArgTypes;
  }

  public Type getRetType() {
     return impalaRetType;
  }

  @Override
  public ImpalaFunctionSignature getSignature() {
    return ifs;
  }

  private boolean isSupportedCast(Type toCast, Type fromCast) {
    return FunctionDetailStatics.SUPPORTED_IMPLICIT_CASTS.contains(
        Pair.of(ImpalaTypeConverter.getNormalizedType(toCast),
            ImpalaTypeConverter.getNormalizedType(fromCast)));
  }

  /**
   * Shortcut for getting the ScalarFunctionDetails when the Impala operand types,
   * the return type, and the function name are hardcoded.
   */
  public static ScalarFunctionDetails get(String name, List<Type> operandTypes,
       Type retType) {

    ImpalaFunctionSignature sig = ImpalaFunctionSignature.create(name,
        operandTypes, retType, false, null);

    return SCALAR_BUILTINS_MAP.get(sig);
  }

  public static ScalarFunctionDetails get(String name, List<RelDataType> operandTypes,
       RelDataType retType) {

    ImpalaFunctionSignature sig = ImpalaFunctionSignature.fetch(
        SCALAR_BUILTINS_MAP, name, operandTypes, retType);

    return SCALAR_BUILTINS_MAP.get(sig);
  }

  /**
   * Class used to read from the json file and hold the functions that exist and are supported
   * in the SQL language by Impala but no direct Impala function exists. One example is "between"
   * which gets translated into the "<" and a ">" functions.
   */
  public static class NonImpalaFunction {
    public String dbName;

    public String fnName;

    private TPrimitiveType retType;

    private TPrimitiveType[] argTypes;

    public boolean hasVarArgs;

    public boolean retTypeAlwaysNullable;

    public static List<NonImpalaFunction> getNonImpalaFunctionsFromFile() {
      Reader reader =
          new InputStreamReader(ImpalaFunctionSignature.class.getResourceAsStream("/impala_scalars.json"));
      Gson gson = new Gson();
      java.lang.reflect.Type nonImpalaFunctionType =
          new TypeToken<ArrayList<NonImpalaFunction>>(){}.getType();
      return gson.fromJson(reader, nonImpalaFunctionType);
    }

    public void setFnName(String fnName) {
      this.fnName = fnName;
    }

    public void setRetType(TPrimitiveType retType) {
      this.retType = retType;
    }

    public void setArgTypes(TPrimitiveType[] argTypes) {
      this.argTypes = argTypes;
    }

    public void setHasVarArgs(boolean hasVarArgs) {
      this.hasVarArgs = hasVarArgs;
    }

    public void setRetTypeAlwaysAllowsNulls(boolean retTypeAlwaysNullable) {
      this.retTypeAlwaysNullable = retTypeAlwaysNullable;
    }

    public List<Type> getArgTypes() {
      return (argTypes != null)
          ? ImpalaTypeConverter.getImpalaTypesList(argTypes)
          : Lists.newArrayList();
    }

    public Type getRetType() {
      return ImpalaTypeConverter.getImpalaType(retType);
    }
  }

  public static class ScalarFunctionWrapperImpl implements ScalarFunctionWrapper {
    private final ScalarFunction func;

    public ScalarFunctionWrapperImpl(Function func) {
      this.func = (ScalarFunction) func;
    }

    public String dbName() {
      return func.dbName();
    }

    public String functionName() {
      return func.functionName();
    }

    public Type getRetType() {
      return func.getReturnType();
    }

    public List<Type> getArgTypes() {
      return new ArrayList<Type>(Arrays.asList(func.getArgs()));
    }

    public String getSymbolName() {
      return func.getSymbolName();
    }

    public String getPrepareFnSymbol() {
      return func.getPrepareFnSymbol();
    }

    public String getCloseFnSymbol() {
      return func.getCloseFnSymbol();
    }

    public boolean hasVarArgs() {
      return func.hasVarArgs();
    }

    public boolean isPersistent() {
      return func.isPersistent();
    }

    public TFunctionBinaryType getBinaryType() {
      return func.getBinaryType();
    }

    public HdfsUri getLocation() {
      return func.getLocation();
    }
  }
}
