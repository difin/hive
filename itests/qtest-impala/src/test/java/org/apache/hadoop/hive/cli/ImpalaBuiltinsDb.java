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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.impala.funcmapper.AggFunctionDetails;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaFunctionUtil;
import org.apache.hadoop.hive.impala.funcmapper.ScalarFunctionDetails;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Db;

import java.util.Collections;

/**
 * ImpalaBuiltinsDb class.  This class will serve as the replacement INSTANCE on the Hive
 * side for the BuiltinsDb on the Impala side.  The Impala side will instantiate the INSTANCE
 * by calling back to the ImpalaBuiltinsDbLoader in this class.
 */
public class ImpalaBuiltinsDb extends Db {

  private static final String BUILTINS_DB_COMMENT = "System database for Impala builtin functions";

  public static final ImpalaBuiltinsDbLoader LOADER = new ImpalaBuiltinsDbLoader();

  public ImpalaBuiltinsDb() {
    super (BuiltinsDb.NAME, new org.apache.hadoop.hive.metastore.api.Database(BuiltinsDb.NAME,
        BUILTINS_DB_COMMENT, "", Collections.<String,String>emptyMap()));
    setIsSystemDb(true);
    try {
      for (ScalarFunctionDetails functionDetails : ScalarFunctionDetails.getAllFuncDetails()) {
        addFunction(ImpalaFunctionUtil.create(functionDetails));
      }
      for (AggFunctionDetails functionDetails : AggFunctionDetails.getAllFuncDetails()) {
        addFunction(ImpalaFunctionUtil.create(functionDetails));
      }
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  public static Db getInstance() {
    return BuiltinsDb.getInstance();
  }

  private static class ImpalaBuiltinsDbLoader implements BuiltinsDb.BuiltinsDbLoader {
    public Db getBuiltinsDbInstance() {
      return new ImpalaBuiltinsDb();
    }
  }
}
