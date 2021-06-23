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
package org.apache.hadoop.hive.cli;

import java.io.File;
import java.util.List;

import org.apache.hadoop.hive.cli.control.CliAdapter;
import org.apache.hadoop.hive.cli.control.CliConfigs;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.impala.funcmapper.AggFunctionDetails;
import org.apache.hadoop.hive.impala.funcmapper.AggFunctionWrapper;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaFunctionSignature;
import org.apache.hadoop.hive.impala.funcmapper.ScalarFunctionWrapper;
import org.apache.hadoop.hive.impala.funcmapper.ScalarFunctionDetails;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaTypeConverter;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TPrimitiveType;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import com.google.gson.reflect.TypeToken;
import java.io.Reader;
import java.util.ArrayList;
import java.io.InputStreamReader;
import com.google.common.base.Preconditions;
@RunWith(Parameterized.class)
public class TestImpalaCliDriver {

  static {
    ScalarFunctionDetails.addFunctionsFromImpala(
        TestScalarFunctionWrapper.createScalarFunctionDetailsFromTestFile());
    AggFunctionDetails.addFunctionsFromImpala(
        TestAggNonImpalaFunction.createAggFunctionDetailsFromTestFile());
    BuiltinsDb.getInstance(ImpalaBuiltinsDb.LOADER);
  }

  static CliAdapter adapter = new CliConfigs.ImpalaCliConfig().getCliAdapter();

  @Parameters(name = "{0}")
  public static List<Object[]> getParameters() throws Exception {
    return adapter.getParameters();
  }

  @ClassRule
  public static TestRule cliClassRule = adapter.buildClassRule();

  @Rule
  public TestRule cliTestRule = adapter.buildTestRule();

  private String name;
  private File qfile;

  public TestImpalaCliDriver(String name, File qfile) {
    this.name = name;
    this.qfile = qfile;
  }

  @Test
  public void testCliDriver() throws Exception {
    adapter.runTest(name, qfile);
  }

  private static class TestScalarFunctionWrapper implements ScalarFunctionWrapper {

    public String dbName;

    public String fnName;

    private TPrimitiveType retType;

    private TPrimitiveType[] argTypes;

    public boolean hasVarArgs;

    public boolean retTypeAlwaysNullable;

    public String impalaFnName;

    public String symbolName;

    public String prepareFnSymbol;

    public String closeFnSymbol;

    public boolean userVisible;

    public boolean isAgg;

    public boolean isPersistent;

    public TFunctionBinaryType binaryType;

    public String hdfsUriLoc;

    public HdfsUri hdfsUri;

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

    public void setImpalaFnName(String impalaFnName) {
      this.impalaFnName = impalaFnName;
    }

    public void setSymbolName(String symbolName) {
      this.symbolName = symbolName;
    }

    public void setPrepareFnSymbol(String prepareFnSymbol) {
      this.prepareFnSymbol = prepareFnSymbol;
    }

    public void setCloseFnSymbol(String closeFnSymbol) {
      this.closeFnSymbol = closeFnSymbol;
    }

    public void setUserVisible(boolean userVisible) {
      this.userVisible = userVisible;
    }

    public void setIsAgg(boolean isAgg) {
      this.isAgg = isAgg;
    }

    public void setIsPersistent(boolean isAgg) {
      this.isAgg = isAgg;
    }

    public void setBinaryType(TFunctionBinaryType binaryType) {
      this.binaryType = binaryType;
    }

    public void setHdfsUriLoc(String hdfsUriLoc) {
      this.hdfsUriLoc = hdfsUriLoc;
      this.hdfsUri = new HdfsUri(hdfsUriLoc);
    }

    public List<Type> getArgTypes() {
      return (argTypes != null)
          ? ImpalaTypeConverter.getImpalaTypesList(argTypes)
          : Lists.newArrayList();
    }

    public Type getRetType() {
      return ImpalaTypeConverter.getImpalaType(retType);
    }
    public static List<ScalarFunctionDetails> createScalarFunctionDetailsFromTestFile() {
      List<ScalarFunctionDetails> result = new ArrayList<>();
      Reader reader =
          new InputStreamReader(ImpalaFunctionSignature.class.getResourceAsStream("/test_impala_scalars.json"));
      Gson gson = new Gson();
      java.lang.reflect.Type funcDetailsType =
          new TypeToken<ArrayList<TestScalarFunctionWrapper>>(){}.getType();
      List<TestScalarFunctionWrapper> funcs = gson.fromJson(reader, funcDetailsType);
      for (TestScalarFunctionWrapper func : funcs) {
        result.add(new ScalarFunctionDetails(func.fnName, func));
      }
      return result;
    }

    public String dbName() {
      return dbName;
    }

    public String functionName() {
      return impalaFnName;
    }

    public String getSymbolName() {
      return symbolName;
    }

    public String getPrepareFnSymbol() {
      return prepareFnSymbol;
    }

    public String getCloseFnSymbol() {
      return closeFnSymbol;
    }

    public boolean hasVarArgs() {
      return hasVarArgs;
    }

    public boolean isPersistent() {
      return isPersistent;
    }

    public TFunctionBinaryType getBinaryType() {
      return binaryType;
    }

    public HdfsUri getLocation() {
      return hdfsUri;
    }
  }

  private static class TestAggNonImpalaFunction implements AggFunctionWrapper {
    public String fnName;
    public String impalaFnName;
    private TPrimitiveType retType;
    private TPrimitiveType[] argTypes;
    private TPrimitiveType intermediateType;
    @Expose(serialize=false,deserialize=false)
    private Type impalaRetType;
    @Expose(serialize=false,deserialize=false)
    private List<Type> impalaArgTypes;
    @Expose(serialize=false,deserialize=false)
    private Type impalaIntermediateType;
    public int intermediateTypeLength;
    public boolean isAnalyticFn;
    public String updateFnSymbol;
    public String initFnSymbol;
    public String mergeFnSymbol;
    public String finalizeFnSymbol;
    public String valueFnSymbol;
    public String removeFnSymbol;
    public String serializeFnSymbol;
    public boolean ignoresDistinct;
    public boolean returnsNonNullOnEmpty;
    public boolean isAgg;
    public boolean isPersistent;
    public TFunctionBinaryType binaryType;

    public String dbName() {
      return BuiltinsDb.NAME;
    }

    public String functionName() {
      return impalaFnName;
    }

    public boolean isPersistent() {
      return isPersistent;
    }

    public TFunctionBinaryType getBinaryType() {
      return binaryType;
    }

    public boolean isAnalyticFn() {
      return isAnalyticFn;
    }

    public String getUpdateFnSymbol() {
      return updateFnSymbol;
    }

    public String getInitFnSymbol() {
      return initFnSymbol;
    }

    public String getMergeFnSymbol() {
      return mergeFnSymbol;
    }

    public String getValueFnSymbol() {
      return valueFnSymbol;
    }

    public String getFinalizeFnSymbol() {
      return finalizeFnSymbol;
    }

    public String getRemoveFnSymbol() {
      return removeFnSymbol;
    }

    public String getSerializeFnSymbol() {
      return serializeFnSymbol;
    }

    public boolean ignoresDistinct() {
      return ignoresDistinct;
    }

    public boolean returnsNonNullOnEmpty() {
      return returnsNonNullOnEmpty;
    }

    public boolean isAggregateFn() {
      return isAgg;
    }

    public List<Type> getArgTypes() {
      if (impalaArgTypes == null) {
        impalaArgTypes = (argTypes != null)
            ? ImpalaTypeConverter.getImpalaTypesList(argTypes)
            : Lists.newArrayList();
      }
      return impalaArgTypes;
    }

    public Type getReturnType() {
      if (impalaRetType == null) {
        impalaRetType = ImpalaTypeConverter.getImpalaType(retType);
      }
      return impalaRetType;
    }

    public Type getIntermediateType() {
      if (intermediateType == null) {
        return getReturnType();
      }
      //XXX: fix this up
      Type impalaIntermediateType = ImpalaTypeConverter.getImpalaType(intermediateType);
      // The only case where intermediateTypeLength is set is for FIXED_UDA_INTERMEDIATE.
      if (intermediateTypeLength > 0) {
        Preconditions.checkState(intermediateType == TPrimitiveType.FIXED_UDA_INTERMEDIATE);
        impalaIntermediateType =
            ImpalaTypeConverter.createImpalaType(impalaIntermediateType, intermediateTypeLength, 0);
      }
      return impalaIntermediateType;
    }

    public int getIntermediateTypeLength() {
      return intermediateTypeLength;
    }

    public void setFnName(String fnName) {
      this.fnName = fnName;
    }

    public void setImpalaFnName(String impalaFnName) {
      this.impalaFnName = impalaFnName;
    }

    public void setRetType(TPrimitiveType retType) {
      this.retType = retType;
    }

    public void setArgTypes(TPrimitiveType[] argTypes) {
      this.argTypes = argTypes;
    }

    public void setIntermediateType(TPrimitiveType intermediateType) {
      this.intermediateType = intermediateType;
    }

    public void setIntermediateTypeLength(int intermediateTypeLength) {
      this.intermediateTypeLength = intermediateTypeLength;
    }

    public void setIsAnalyticFn(boolean isAnalyticFn) {
      this.isAnalyticFn = isAnalyticFn;
    }

    public void setUpdateFnSymbol(String updateFnSymbol) {
      this.updateFnSymbol = updateFnSymbol;
    }

    public void setInitFnSymbol(String initFnSymbol) {
      this.initFnSymbol = initFnSymbol;
    }

    public void setMergeFnSymbol(String mergeFnSymbol) {
      this.mergeFnSymbol = mergeFnSymbol;
    }

    public void setFinalizeFnSymbol(String finalizeFnSymbol) {
      this.finalizeFnSymbol = finalizeFnSymbol;
    }

    public void setGetValueFnSymbol(String getValueFnSymbol) {
      this.valueFnSymbol = valueFnSymbol;
    }

    public void setRemoveFnSymbol(String removeFnSymbol) {
      this.removeFnSymbol = removeFnSymbol;
    }

    public void setIgnoresDistinct(boolean ignoresDistinct) {
      this.ignoresDistinct = ignoresDistinct;
    }

    public void setReturnsNonNullOnEmpty(boolean returnsNonNullOnEmpty) {
      this.returnsNonNullOnEmpty = returnsNonNullOnEmpty;
    }

    public void setIsAgg(boolean isAgg) {
      this.isAgg = isAgg;
    }

    public void setBinaryType(TFunctionBinaryType binaryType) {
      this.binaryType = binaryType;
    }

    public static List<AggFunctionDetails> createAggFunctionDetailsFromTestFile() {
      List<AggFunctionDetails> result = new ArrayList<>();
      Reader reader =
          new InputStreamReader(ImpalaFunctionSignature.class.getResourceAsStream("/test_impala_aggs.json"));
      Gson gson = new Gson();
      java.lang.reflect.Type funcDetailsType = new TypeToken<ArrayList<TestAggNonImpalaFunction>>(){}.getType();
      List<TestAggNonImpalaFunction> funcs = gson.fromJson(reader, funcDetailsType);
      for (TestAggNonImpalaFunction func : funcs) {
        result.add(new AggFunctionDetails(func));
      }
      return result;
    }
  }
}
