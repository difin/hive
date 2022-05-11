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

package org.apache.hadoop.hive.ql.engine.internal;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HMSConverter;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.function.desc.DescFunctionOperation;
import org.apache.hadoop.hive.ql.ddl.function.show.ShowFunctionsOperation;
import org.apache.hadoop.hive.ql.engine.EngineCompileHelper;
import org.apache.hadoop.hive.ql.engine.EngineEventSequence;
import org.apache.hadoop.hive.ql.engine.EngineQueryHelper;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.parse.MapReduceCompiler;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TaskCompiler;
import org.apache.hadoop.hive.ql.parse.TezCompiler;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeEngineCompileHelper implements EngineCompileHelper {
  protected static final Logger LOG = LoggerFactory.getLogger(NativeEngineCompileHelper.class);

  public HMSConverter getHMSConverter() {
    return null;
  }

  public void reloadFunctions(List<Function> functions, HiveConf conf, IMetaStoreClient msc) {
  }

  public int fetchFunctions(DataOutputStream outStream, String pattern)
      throws IOException {
    return ShowFunctionsOperation.execute(outStream, pattern);
  }

  public int fetchFunctionInfo(DataOutputStream outStream, String func, boolean isExtended) 
      throws IOException, SemanticException {
    return DescFunctionOperation.execute(outStream, func, isExtended);
  }

  public EngineEventSequence getEventSequence(String event) {
    return new DummyEventSequence();
  }

  public EngineQueryHelper getQueryHelper(HiveConf conf, String dbname, String username,
                                             HiveTxnManager txnMgr, Context ctx,
                                             QueryState queryState) throws SemanticException {
    return null;
  }

  public EngineQueryHelper resetQueryHelper(
      EngineQueryHelper queryHelper) throws SemanticException {
    return null;
  }

  public RelDataTypeSystem getRelDataTypeSystem() {
    return new HiveTypeSystemImpl();
  }

  public TaskCompiler getCompiler(HiveConf conf) {
    switch (conf.getRuntime()) {
      case MR:
        return new MapReduceCompiler();
      case TEZ:
        return new TezCompiler();
      case INVALID_RUNTIME:
        throw new UnsupportedOperationException("Invalid execution engine specified.");
    }

    throw new UnsupportedOperationException("Invalid execution engine specified.");
  }
}
