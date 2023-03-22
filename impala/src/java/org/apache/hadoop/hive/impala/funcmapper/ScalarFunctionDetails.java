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
import com.google.common.collect.Maps;
import com.google.gson.annotations.Expose;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Registry;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TFunctionBinaryType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  protected static final Logger LOG = LoggerFactory.getLogger(ScalarFunctionDetails.class);

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

  // The isStateful boolean describes whether the function may change its output across
  // calls within the same query. For instance, the now() function will return different
  // values for each call. This is necessary to track because constant folding is not
  // allowed on these functions. Also, since we do not know what kind of code a user
  // created UDF will contain, we will default the flag to be turned on for these functions.
  public final boolean isStateful;

  // Set of all scalar functions available in Impala
  // This contains just the builtins.
  private static final Set<String> SCALAR_BUILTINS = new HashSet<>();

  // Map containing a scalar  Impala signature to the details associated with the signature.
  // A signature consists of the function name, the operand types and the return type.
  // This contains just the builtins.
  private static final Map<ImpalaFunctionSignature, ScalarFunctionDetails>
      SCALAR_BUILTINS_MAP = Maps.newHashMap();

  // Set of all scalar functions available in Impala
  // This contains both the builtins and the UDFs
  private static volatile Set<String> ALL_SCALARS_FUNCS = new HashSet<>();

  // Map containing a scalar Impala signature to the details associated with the signature.
  // A signature consists of the function name, the operand types and the return type.
  // This contains both the builtins and the UDFs
  private static volatile Map<ImpalaFunctionSignature, ScalarFunctionDetails>
      ALL_SCALARS_MAP = ImmutableMap.of();

  private static volatile Map<String, List<String>> HIVE_UDF_MAP = new HashMap<>();

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
          ScalarFunctionWrapper funcWrapper = new ScalarFunctionWrapperImpl(func, false);
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
      SCALAR_BUILTINS.add(sfd.fnName.toLowerCase());
      ifsList.add(sfd.ifs);
    }

    // Also add functions that don't map directly into Impala (which are stored in a "json"
    // resources file.
    for (NonImpalaFunction nif : NonImpalaFunction.getNonImpalaFunctionsFromFile("/impala_scalars.json")) {
      ImpalaFunctionSignature ifs = ImpalaFunctionSignature.createFuncSignatureForStorage(
          nif.fnName.toLowerCase(), nif.getArgTypes(), nif.getRetType(), nif.hasVarArgs,
          nif.retTypeAlwaysNullable);
      SCALAR_BUILTINS.add(nif.fnName.toLowerCase());
      ifsList.add(ifs);
    }

    ImpalaFunctionSignature.populateCastCheckFunctions(ifsList);
    // make a copy.  SCALAR_BUILTINS will only contain builtins whereas ALL_SCALAR will
    // contain builtins and UDFs.
    ALL_SCALARS_FUNCS = ImmutableSet.copyOf(SCALAR_BUILTINS);
    ALL_SCALARS_MAP = ImmutableMap.copyOf(SCALAR_BUILTINS_MAP);
  }

  public static void addUDFs(List<Function> functions) {
    List<ImpalaFunctionSignature> ifsList = new ArrayList<>();
    Set<String> allFuncs = new HashSet<>(SCALAR_BUILTINS);
    Map<ImpalaFunctionSignature, ScalarFunctionDetails> allFuncsMap =
      new HashMap<>(SCALAR_BUILTINS_MAP);

    for (Function func : functions) {
      if (!(func instanceof ScalarFunction)) {
        continue;
      }
      // udf function name is saved with the database
      String fullFunctionName = func.getFunctionName().toString();
      LOG.info("Registering function " + fullFunctionName);
      fullFunctionName = fullFunctionName.toLowerCase();
      ScalarFunctionWrapper funcWrapper = new ScalarFunctionWrapperImpl(func, true);
      ScalarFunctionDetails sfd = new ScalarFunctionDetails(fullFunctionName, funcWrapper);
      allFuncsMap.put(sfd.ifs, sfd);
      allFuncs.add(fullFunctionName);
      ifsList.add(sfd.ifs);
    }
    ImpalaFunctionSignature.populateCastCheckFunctions(ifsList);
    ALL_SCALARS_FUNCS = ImmutableSet.copyOf(allFuncs);
    ALL_SCALARS_MAP = ImmutableMap.copyOf(allFuncsMap);
    LOG.info("Done Registering functions ");
  }

  public static Set<String> getFunctionNames(String impalaFnName) {
    return FunctionDetailStatics.IMPALA_FUNCTION_MAP.containsKey(impalaFnName)
        ? FunctionDetailStatics.IMPALA_FUNCTION_MAP.get(impalaFnName)
        : ImmutableSet.of(impalaFnName);
  }

  public static Collection<ScalarFunctionDetails> getAllFuncDetails() {
    return ALL_SCALARS_MAP.values();
  }

  public ScalarFunctionDetails(String fnName, ScalarFunctionWrapper func) {
    this.fnName = fnName;

    this.dbName = func.dbName();

    this.impalaFnName = func.functionName();

    this.impalaRetType = func.getRetType();

    this.impalaArgTypes = func.getArgTypes();

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

    this.ifs = ImpalaFunctionSignature.createFuncSignatureForStorage(fnName.toLowerCase(),
        getArgTypes(), getRetType(), hasVarArgs, retTypeAlwaysNullable);

    // All functions defined by the user are considered to be stateful and avoid constant
    // folding for safety purposes.
    this.isStateful = func.isUDF() ||
        FunctionDetailStatics.STATEFUL_FUNCS.contains(fnName.toLowerCase());
  }

  public ScalarFunctionDetails(String fnName, List<Type> argTypes, Type retType, String hdfsUri, String className) {
    this.fnName = fnName;

    this.dbName = "";

    this.impalaFnName = fnName;

    this.impalaRetType = retType;

    this.impalaArgTypes = argTypes;

    this.symbolName = className;

    this.prepareFnSymbol = null;

    this.closeFnSymbol = null;

    this.hasVarArgs = false;

    this.isPersistent = false;

    this.binaryType = TFunctionBinaryType.JAVA;

    this.hdfsUri = new HdfsUri(hdfsUri);

    this.castUp = false;

    this.retTypeAlwaysNullable = false;

    this.ifs = null;

    this.isStateful = true;
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

  public boolean isStateful() {
    return isStateful;
  }

  private boolean isSupportedCast(Type toCast, Type fromCast) {
    return FunctionDetailStatics.SUPPORTED_IMPLICIT_CASTS.contains(
        Pair.of(ImpalaTypeConverter.getNormalizedType(toCast),
            ImpalaTypeConverter.getNormalizedType(fromCast)));
  }

  public static ScalarFunctionDetails get(String name, List<RelDataType> operandTypes,
       RelDataType retType) {

    ImpalaFunctionSignature sig = ImpalaFunctionSignature.fetch(
        ALL_SCALARS_MAP, name, operandTypes, retType);

    ScalarFunctionDetails sfd = get(sig);
    if (sfd != null) {
      return sfd;
    }

    // Look to see if this is a Hive Generic function. Hive Generic functions do not
    // have stored signatures since Hive signatures are more lenient with types allowed.
    // For instance, the "+" operator in Impala forces both parameters to be of the same
    // type whereas Hive allows a mix and match approach, like adding a TINYINT to an INT.
    // Because of this, the ScalarFunctionDetail structure is created on the fly and resolved
    // by the HiveFunctionResolver.
    if (HIVE_UDF_MAP.containsKey(name)) {
      sfd = new ScalarFunctionDetails(name,
          ImpalaTypeConverter.getNormalizedImpalaTypes(operandTypes),
          ImpalaTypeConverter.getNormalizedImpalaType(retType),
          HIVE_UDF_MAP.get(name).get(1),
          HIVE_UDF_MAP.get(name).get(0));
    }
    return sfd;
  }

  public static ScalarFunctionDetails get(ImpalaFunctionSignature ifs) {
    ScalarFunctionDetails sfd = ALL_SCALARS_MAP.get(ifs);
    if (sfd != null) {
      return sfd;
    }

    return null;
  }

  public static Set<String> getAllScalars() {
    return ALL_SCALARS_FUNCS;
  }

  public static Map<ImpalaFunctionSignature, ScalarFunctionDetails> getScalarsMap() {
    return ALL_SCALARS_MAP;
  }

  public static boolean isScalarFunction(String name) {
    return ALL_SCALARS_FUNCS.contains(name.toLowerCase()) ||
        HIVE_UDF_MAP.containsKey(name.toLowerCase());
  }

  private static class ScalarFunctionWrapperImpl implements ScalarFunctionWrapper {
    private final ScalarFunction func;
    private final boolean isUDF;

    public ScalarFunctionWrapperImpl(Function func, boolean isUDF) {
      this.func = (ScalarFunction) func;
      this.isUDF = isUDF;
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

    public boolean isUDF() {
      return isUDF;
    }
  }

  /**
   * Add the builtin in hive UDFs to the map. The Hive UDFs should all be in the
   * ql.udf.generic package and must have an annotation defined.
   */
  public static void addHiveUDFs() throws HiveException {
    // load generic UDFs under org.apache.hadoop.hive.ql.udf.generic
    Set<String> functionsFromFunctionRegistry = FunctionRegistry.getFunctionNames();
    for (String funcName: functionsFromFunctionRegistry) {
      Class<?> funcClass = FunctionRegistry.getFunctionInfo(funcName).getFunctionClass();
      if (funcClass.getPackage().getName().contains("org.apache.hadoop.hive.ql.udf")) {
        HIVE_UDF_MAP.put(funcName, Arrays.asList(funcClass.getName(), ""));
      }
    }

    // load permanent functions created with CREATE FUNCTION and stored in HMS
    List<org.apache.hadoop.hive.metastore.api.Function> permanentFunctions = Hive.get().getAllFunctions();
    for (org.apache.hadoop.hive.metastore.api.Function func: permanentFunctions) {
      List<ResourceUri> resources = func.getResourceUris();
      if (resources.size() != 1) {
        continue;
      }
      //getFunctionName doesn't return db. For db.func, it returns func
      HIVE_UDF_MAP.put(func.getFunctionName(), Arrays.asList(func.getClassName(), resources.get(0).getUri()));
    }

    // load temporary functions from Session Registry
    Registry sessionRegistry = SessionState.getRegistry();
    if (sessionRegistry != null) {
      Set<String> temporaryFunctions = sessionRegistry.getCurrentFunctionNames();
      for (String funcName: temporaryFunctions) {
        FunctionInfo funcInfo = sessionRegistry.getFunctionInfo(funcName);
        FunctionInfo.FunctionResource[] resources = funcInfo.getResources();
        if (resources.length != 1) {
          continue;
        }
        // put only if it's not replacing a permanent function
        HIVE_UDF_MAP.putIfAbsent(funcName, Arrays.asList(funcInfo.getFunctionClass().getName(),
                resources[0].getResourceURI()));
      }
    }

    // needed for hplsql
    HIVE_UDF_MAP.put("hplsql", Arrays.asList("org.apache.hive.hplsql.udf.Udf", ""));
  }

  /**
   * Returns true if it is a hive function and similar function isn't present in
   * Impala.
   *
   * For example, it returns false for + as it is a builtin Impala function, although
   * we have a Hive UDF for it - org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus
   * @param function name
   * @return boolean
   */
  public static boolean isHiveFunction(String name) {
    String funcName = name.toLowerCase();
    return HIVE_UDF_MAP.containsKey(funcName) &&
        !SCALAR_BUILTINS.contains(funcName);
  }
}
