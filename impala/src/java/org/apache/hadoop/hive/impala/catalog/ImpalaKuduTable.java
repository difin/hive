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
package org.apache.hadoop.hive.impala.catalog;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;

public class ImpalaKuduTable extends KuduTable {

  /**
   * ImpalaKuduTable constructor
   * @param msTbl MetaStoreTable
   * @param db    Database
   * @param name  Database name
   * @param owner Database owner
   * @throws HiveException
   * @throws MetaException
   */
  public ImpalaKuduTable(Table msTbl, Db db, String name, String owner)
          throws HiveException, MetaException {
    super(msTbl, db, name, owner);
    try {
      setTableStats(msTbl);
      // Load metadata from Kudu
      try {
        loadSchemaFromKudu();
      } catch (ImpalaRuntimeException e) {
        // TableLoadingException extends CatalogException, which in turn extends
        // ImpalaException.
        throw new TableLoadingException("Error loading metadata for Kudu table " +
            name, e);
      }
      // Load from HMS
      IMetaStoreClient msc = Hive.get().getMSC();
      loadAllColumnStats(msc);
    } catch (ImpalaException e) {
      throw new HiveException(e);
    }
  }

}
