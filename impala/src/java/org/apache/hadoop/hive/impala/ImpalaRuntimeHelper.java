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
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ddl.function.show.ShowFunctionsOperation;
import org.apache.hadoop.hive.ql.engine.EngineRuntimeHelper;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TaskFactory.TaskTuple;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.KuduStorageFormatDescriptor;
import org.apache.hadoop.hive.kudu.KuduStorageHandler;
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
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.hive.executor.HiveJavaFunction;
import org.apache.impala.hive.executor.HiveJavaFunctionFactory;
import org.apache.impala.hive.executor.HiveJavaFunctionFactoryImpl;
import org.apache.impala.service.KuduCatalogOpExecutor;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.EventSequence;
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

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.EXTERNAL_TABLE_PURGE;

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
    try {
      List<org.apache.impala.catalog.Function> impalaFunctions =
          getHiveUDFs(functions, conf);
      impalaFunctions.addAll(getNativeUDFs(msc));
      ScalarFunctionDetails.addUDFs(impalaFunctions);
      AggFunctionDetails.addUDFs(impalaFunctions);
      ScalarFunctionDetails.addHiveUDFs();
    } catch (HiveException e) {
      LOG.warn("Reload functions failed for UnifiedAnalytics, some UDFs may not be visible.");
    }
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

  @Override
  public void dropDatabase(List<String> tableNameList, List<org.apache.hadoop.hive.metastore.api.Table> tables)
      throws HiveException {
    for (org.apache.hadoop.hive.metastore.api.Table msTbl : tables) {
      boolean isSynchronizedKuduTable = msTbl != null && KuduTable.isKuduTable(msTbl) &&
          KuduTable.isSynchronizedTable(msTbl);
      if (isSynchronizedKuduTable) {
        // drop table only if it is managed table or purge property set to true
        // drop external table shouldn't drop base kudu table
        if (msTbl.getTableType().equalsIgnoreCase(TableType.MANAGED_TABLE.toString()) ||
            Boolean.parseBoolean(msTbl.getParameters().get(EXTERNAL_TABLE_PURGE))) {
          try {
            LOG.info("Table {} is being dropped from kudu", msTbl.getTableName());
            KuduCatalogOpExecutor.dropTable(msTbl, /* if exists */ true);
          } catch (Exception e) {
            throw new HiveException(e);
          }
        }
      }
    }
  }

  @Override
  public void createTable(Table tbl, DDLOperationContext context, CreateTableDesc desc)
      throws HiveException {
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getTTable();
    // set default table properties if they aren't supplied using TBLPROPERTIES()
    if (!tbl.getParameters().containsKey(KuduStorageHandler.KUDU_TABLE_NAME_KEY)) {
      tbl.getParameters().put(KuduStorageHandler.KUDU_TABLE_NAME_KEY, msTbl.getDbName() + "." + msTbl.getTableName());
    }
    if (!tbl.getParameters().containsKey(KuduStorageHandler.KUDU_MASTER_ADDRS_KEY)) {
      String masterAddresses = HiveConf.getVar(context.getConf(),
          HiveConf.ConfVars.HIVE_KUDU_MASTER_ADDRESSES_DEFAULT);
      tbl.getParameters().put(KuduStorageHandler.KUDU_MASTER_ADDRS_KEY, masterAddresses);
    }
    if (!tbl.getParameters().containsKey(KuduTable.KEY_STORAGE_HANDLER)) {
      tbl.getParameters().put(KuduTable.KEY_STORAGE_HANDLER, KuduStorageFormatDescriptor.KUDU_STORAGE_HANDLER);
    }
    msTbl.setParameters(tbl.getParameters());

    TCreateTableParams createTableParams = new TCreateTableParams();
    createTableParams.setTable_name(new TTableName(msTbl.getDbName(), msTbl.getTableName()));
    createTableParams.setIs_external(org.apache.impala.catalog.Table.isExternalTable(msTbl));
    List<String> primary_key_column_names = new ArrayList<>();
    for (SQLPrimaryKey pk: desc.getPrimaryKeys()) {
      primary_key_column_names.add(pk.getColumn_name());
    }
    createTableParams.setPrimary_key_column_names(primary_key_column_names);

    List<TColumn> columns = new ArrayList<>(msTbl.getSd().getColsSize());
    for (FieldSchema column: msTbl.getSd().getCols()) {
      columns.add(new TColumn(column.getName(), Type.parseColumnType(column.getType()).toThrift()));
    }
    createTableParams.setColumns(columns);
    // CDPD-62070: Hardcode this field to true until we figure out how to create a Kudu
    // table with a non-unique primary key.
    createTableParams.setIs_primary_key_unique(true);

    try {
      // Return if we are in Hive's q test. This allows us to exercise the corresponding
      // code path up to this point in the q test.
      if (context.getConf().getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) return;
      EventSequence catalogTimeline = new EventSequence("Catalog Server Operation");
      KuduCatalogOpExecutor.createSynchronizedTable(catalogTimeline, msTbl, createTableParams);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropTable(Table table, DDLOperationContext context) throws HiveException {
    org.apache.hadoop.hive.metastore.api.Table msTbl = table.getTTable();
    boolean isSynchronizedKuduTable = msTbl != null && KuduTable.isKuduTable(msTbl) &&
        KuduTable.isSynchronizedTable(msTbl);
    // if kudu table, drop it from kudu too
    if (isSynchronizedKuduTable) {
      // drop table only if it is managed table or purge property set to true
      // drop external table shouldn't drop base kudu table
      if (msTbl.getTableType().equalsIgnoreCase(TableType.MANAGED_TABLE.toString()) ||
          Boolean.parseBoolean(msTbl.getParameters().get(EXTERNAL_TABLE_PURGE))) {
        try {
          LOG.info("Table {} is being dropped from kudu", msTbl.getTableName());
          // Return if we are in Hive's q test. This allows us to exercise the
          // corresponding code path up to this point in the q test.
          if (context.getConf().getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) return;
          KuduCatalogOpExecutor.dropTable(msTbl, /* if exists */ true);
        } catch (Exception e) {
          throw new HiveException(e);
        }
      }
    }
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
