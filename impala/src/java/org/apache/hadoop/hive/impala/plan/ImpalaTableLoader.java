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
package org.apache.hadoop.hive.impala.plan;

import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.impala.catalog.ImpalaKuduTable;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.impala.catalog.ImpalaHdfsTable;
import org.apache.hadoop.hive.impala.prune.ImpalaBasicHdfsTable;
import org.apache.hadoop.hive.impala.prune.ImpalaBasicHdfsTable.TableWithPartitionNames;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.util.EventSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.impala.catalog.KuduTable.isKuduTable;

/**
 * This class manages the loading of table and partitions metadata (which includes file
 * metadata for all files within that partition) within a single query. A table may be
 * referenced multiple times in a query and each instance may have its own set of pruned
 * partitions (for partition tables). This centralized loader ensures that for each table
 * the list of partitions are combined into a unique set before asking Impala to load the
 * metadata for those partitions. For un-partitioned tables, the entire table's metadata
 * is loaded.
 */
public class ImpalaTableLoader {
  private static final Logger LOG = LoggerFactory.getLogger(ImpalaTableLoader.class);

  private final Map<org.apache.impala.catalog.Table, Set<String>> tablePartitionMap;
  private final Map<String, org.apache.impala.catalog.Table> tableMap;
  private final Map<String, org.apache.hadoop.hive.metastore.api.Database> dbMap;
  private final EventSequence timeline;
  private final ImpalaQueryContext queryContext;

  public ImpalaTableLoader(EventSequence timeline, ImpalaQueryContext queryContext) {
    this.tablePartitionMap = new HashMap<>();
    this.tableMap = new HashMap<>();
    this.dbMap = new HashMap<>();
    this.timeline = timeline;
    this.queryContext = queryContext;
  }

  /**
   * creates and returns new impala table
   * @param db Database
   * @param conf Hive configuration
   * @param msTbl Metastore table
   * @return table object from tableMap (of type ImpalaHdfsTable or ImpalaKuduTable)
   * @throws HiveException
   * @throws MetaException
   */
  public Table loadImpalaTable(Hive db, HiveConf conf, org.apache.hadoop.hive.metastore.api.Table msTbl)
          throws HiveException, MetaException {
    org.apache.hadoop.hive.metastore.api.Database msDb = dbMap.get(msTbl.getDbName());
    if (msDb == null) {
      // cache the Database object to avoid rpc to the metastore for future requests
      msDb = db.getDatabase(msTbl.getDbName());
      dbMap.put(msTbl.getDbName(), msDb);
    }
    String fqn = getFullyQualifiedName(msTbl);
    Table impalaTable = tableMap.get(fqn);
    if (impalaTable != null) {
      return impalaTable;
    }

    org.apache.impala.catalog.Db impalaDb = new Db(msTbl.getDbName(), msDb);
    impalaTable = isKuduTable(msTbl)
        ? new ImpalaKuduTable(msTbl, impalaDb, msTbl.getTableName(), msTbl.getOwner())
        : new ImpalaHdfsTable(conf, msTbl, impalaDb, msTbl.getTableName(), msTbl.getOwner());
    tableMap.put(fqn, impalaTable);
    tablePartitionMap.put(impalaTable, new HashSet<>());
    return impalaTable;
  }

  /**
   * Fetches impala table from tableMap
   * @param table Metastore table
   * @return table object from tableMap (of type ImpalaHdfsTable or ImpalaKuduTable)
   */
  public Table getImpalaTable(org.apache.hadoop.hive.metastore.api.Table table) {
    return tableMap.get(getFullyQualifiedName(table));
  }

  private static String getFullyQualifiedName(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return msTbl.getDbName() + "." + msTbl.getTableName();
  }

  public void loadTablesAndPartitions(Hive db, ValidTxnWriteIdList txnWriteIdList)
          throws HiveException {
    timeline.markEvent("Metadata load started, loading " + queryContext.getBasicTables().size()
            + " tables.");
    for (TableWithPartitionNames tableWithNames : queryContext.getBasicTables()) {
      ImpalaBasicHdfsTable basicTable = tableWithNames.getTable();
      LOG.info("Loading metadata for table " + basicTable.getName());
      try {
        ValidWriteIdList validWriteIdList = null;
        if (txnWriteIdList != null) {
          // Lets get this specific table's write id list
          validWriteIdList = txnWriteIdList.getTableValidWriteIdList(basicTable.getFullName());
        }

        // load only one type of table (ImpalaHdfsTable or ImpalaKuduTable) in tableMap,
        // depending on the type of query
        Table impalaTable = isKuduTable(basicTable.getMetaStoreTable())
            ? new ImpalaKuduTable(basicTable.getMetaStoreTable(), basicTable.getDb(), basicTable.getName(),
            basicTable.getOwnerUser())
            : ImpalaHdfsTable.create(queryContext.getConf(), basicTable, tableWithNames.getPartitionNames(),
            validWriteIdList);
        tableMap.put(getFullyQualifiedName(basicTable.getMetaStoreTable()), impalaTable);
      } catch (ImpalaException|MetaException e) {
        timeline.markEvent("Metadata load failed for table " + basicTable.getName() +
                ". Completed" + " for " + tableMap.entrySet().size() + " tables.");
        throw new HiveException(e);
      }
    }
    timeline.markEvent("Metadata load finished. loaded-tables=" + tableMap.entrySet().size());
  }

}
