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

package org.apache.hadoop.hive.impala;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConf.ResultMethod;
import org.apache.hadoop.hive.impala.exec.ImpalaQueryOperator;
import org.apache.hadoop.hive.impala.exec.ImpalaStreamingFetchOperator;
import org.apache.hadoop.hive.impala.exec.ImpalaTask;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ddl.function.show.ShowFunctionsOperation;
import org.apache.hadoop.hive.ql.engine.EngineRuntimeHelper;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TaskFactory.TaskTuple;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TaskCompiler;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.impala.funcmapper.AggFunctionDetails;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaFunctionSignature;
import org.apache.hadoop.hive.impala.funcmapper.ScalarFunctionDetails;
import org.apache.hadoop.hive.impala.exec.ImpalaSessionImpl;
import org.apache.hadoop.hive.impala.exec.ImpalaSessionManager;
import org.apache.hadoop.hive.impala.parse.ImpalaCompiler;
import org.apache.hadoop.hive.impala.parse.ImpalaFetchWork;
import org.apache.hadoop.hive.impala.plan.ImpalaQueryDesc;
import org.apache.hadoop.hive.impala.work.ImpalaWork;
import org.apache.hadoop.mapred.JobConf;
import org.apache.impala.hive.executor.HiveJavaFunction;
import org.apache.impala.hive.executor.HiveJavaFunctionFactory;
import org.apache.impala.hive.executor.HiveJavaFunctionFactoryImpl;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.util.FunctionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImpalaRuntimeHelper implements EngineRuntimeHelper {
  protected static final Logger LOG = LoggerFactory.getLogger(ImpalaRuntimeHelper.class);

  public List<TaskTuple<? extends Serializable>> getTaskTuples() {
    return Lists.newArrayList(new TaskTuple<ImpalaWork>(ImpalaWork.class, ImpalaTask.class));
  }

  public IdentityHashMap<Class<? extends OperatorDesc>,
      Class<? extends Operator<? extends OperatorDesc>>> getOperatorVecs() {
    IdentityHashMap<Class<? extends OperatorDesc>,
        Class<? extends Operator<? extends OperatorDesc>>> opVecs =
            new IdentityHashMap<>();
    opVecs.put(ImpalaQueryDesc.class, ImpalaQueryOperator.class);
    return opVecs;
  }

  public FetchOperator createFetchOperator(HiveConf conf, FetchWork work, JobConf job,
      Operator<?> operator, List<VirtualColumn> vcCols, Schema resultSchema,
      HiveOperation hiveOp) throws HiveException {
    return (work instanceof ImpalaFetchWork) ||
        (hiveOp == HiveOperation.QUERY && conf.getResultMethod() == ResultMethod.STREAMING)
      ? new ImpalaStreamingFetchOperator(work, job, operator, vcCols, resultSchema)
      : new FetchOperator(work, job, operator, vcCols);
  }

  @Override
  public synchronized void reloadFunctions(List<Function> functions, HiveConf conf, IMetaStoreClient msc) {
    List<org.apache.impala.catalog.Function> impalaFunctions =
        getHiveUDFs(functions, conf);
    impalaFunctions.addAll(getNativeUDFs(msc));
    ScalarFunctionDetails.addUDFs(impalaFunctions);
    AggFunctionDetails.addUDFs(impalaFunctions);
  }

  @Override
  public int fetchFunctions(DataOutputStream outStream, String pattern)
      throws IOException {
    Set<String> allFuncs = new HashSet<String>(ScalarFunctionDetails.getAllScalars());
    allFuncs.addAll(AggFunctionDetails.getAllAggs());
    Set<String> matchedFuncs = (pattern == null)
        ? allFuncs
        : FunctionRegistry.getFunctionNamesByLikePattern(pattern, allFuncs);
    return ShowFunctionsOperation.printFunctions(outStream, matchedFuncs);
  }

  @Override
  public int fetchFunctionInfo(DataOutputStream outStream, String func, boolean isExtended)
      throws IOException, SemanticException {
    if (!ImpalaFunctionSignature.CAST_CHECK_FUNCS_INSTANCE.containsKey(func)) {
      outStream.writeBytes("Function '" + func+ "' does not exist.");
      return 1;
    }

    List<ImpalaFunctionSignature> ifsList =
        ImpalaFunctionSignature.CAST_CHECK_FUNCS_INSTANCE.get(func);
    for (ImpalaFunctionSignature ifs : ifsList) {
      outStream.writeBytes(ifs.toString());
      outStream.writeBytes(":");
      if (ScalarFunctionDetails.isScalarFunction(func)) {
        ScalarFunctionDetails sfd = ScalarFunctionDetails.get(ifs);
        outStream.writeBytes(sfd.binaryType.toString());
      } else {
        AggFunctionDetails afd = AggFunctionDetails.get(ifs);
        outStream.writeBytes(afd.binaryType.toString());
      }
      outStream.write(Utilities.newLineCode);
    }
    return 0;
  }

  public TaskCompiler getCompiler(HiveConf conf) {
    long fetchSize = conf.getLongVar(HiveConf.ConfVars.HIVE_IMPALA_FETCH_SIZE);
    return new ImpalaCompiler(fetchSize);
  }

  private List<org.apache.impala.catalog.Function> getHiveUDFs(
      List<Function> hiveFunctions, HiveConf conf) {
    String localLibPath;
    try {
      localLibPath = getLocalLibPath(conf);
    } catch (HiveException e) {
      LOG.info("Not adding Hive UDF functions; Failed to retrieve localLibPath");
      return new ArrayList<>();
    }

    List<org.apache.impala.catalog.Function> allFunctions = new ArrayList<>();
    for (org.apache.hadoop.hive.metastore.api.Function hiveFunc : hiveFunctions) {
      String db = hiveFunc.getDbName();
      try {
        HiveJavaFunctionFactory factory = new HiveJavaFunctionFactoryImpl(localLibPath);
        HiveJavaFunction javaFunc = factory.create(hiveFunc);
        allFunctions.addAll(javaFunc.extract());
      } catch (Exception e) {
        LOG.info("Failed to add udf " + hiveFunc.getFunctionName() + ": " + e);
      }
    }

    return allFunctions;
  }

  private List<org.apache.impala.catalog.Function> getNativeUDFs(IMetaStoreClient msc) {
    List<Database> allDbs = getAllDatabases(msc);
    List<org.apache.impala.catalog.Function> allFunctions = new ArrayList<>();
    for (Database db : allDbs) {
      allFunctions.addAll(FunctionUtils.deserializeNativeFunctionsFromDbParams(db.getParameters()));
    }
    return allFunctions;
  }

  private List<Database> getAllDatabases(IMetaStoreClient msc) {
    List<Database> databases = new ArrayList<>();
    try {
      List<String> databaseStrings = msc.getAllDatabases();
      for (String databaseString : databaseStrings) {
        databases.add(msc.getDatabase(databaseString));
      }
    } catch (Exception e) {
      LOG.info("Could not retrieve all databases for fetching functions.");
    }
    return databases;
  }

  /**
   * Get the Local Lib Path.  Kind of a heavy operation.  Need to connect to Impala
   * Server to get this information. This is only done when reloading functions right
   * now. If other things are needed from Impala, this probably should be refactored.
   */
  private static String getLocalLibPath(HiveConf conf) throws HiveException {
    if (conf.getBoolVar(ConfVars.HIVE_IN_TEST)) {
      return "/tmp";
    }

    ImpalaSessionImpl session = ImpalaSessionManager.getInstance().getSession(conf);
    TBackendGflags config = session.getBackendConfig();
    if (config == null) {
      throw new HiveException("Backend Config could not be retrieved from Impala.");
    }
    return config.getLocal_library_path();
  }
}
