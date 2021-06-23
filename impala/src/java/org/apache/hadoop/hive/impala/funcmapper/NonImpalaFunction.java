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

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TPrimitiveType;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * Class used to read from the json file and hold the functions that exist and are supported
 * in the SQL language by Impala but no direct Impala function exists. One example is "between"
 * which gets translated into the "<" and a ">" functions.
 */
public class NonImpalaFunction {
  public String dbName;

  public String fnName;

  private TPrimitiveType retType;

  private TPrimitiveType[] argTypes;

  public boolean hasVarArgs;

  public boolean retTypeAlwaysNullable;

  public boolean isAgg;

  public boolean isAnalyticFn;

  public static List<NonImpalaFunction> getNonImpalaFunctionsFromFile(String fileName) {
    Reader reader =
        new InputStreamReader(ImpalaFunctionSignature.class.getResourceAsStream(fileName));
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

  public void setIsAgg(boolean isAgg) {
    this.isAgg = isAgg;
  }

  public void setIsAnalyticFn(boolean isAnalyticFn) {
    this.isAnalyticFn = isAnalyticFn;
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
