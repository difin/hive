/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.compaction;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context.RewritePolicy;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergCompactionQueryBuilder {

  private static final String TABLE_NAME = "db.table";
  private static final String SELECT_COLUMNS = "a, b";
  private static final String FILE_SIZE_PREDICATE = VirtualColumn.FILE_PATH.getName() +
      " in (select file_path from db.table.files where file_size_in_bytes < 1000)";
  private static final String ORDER_BY = "order by a";

  @Test
  public void testUnpartitionedTableQuery() {
    HiveConf conf = new HiveConf();
    String query = IcebergCompactionQueryBuilder.forTable(TABLE_NAME, conf)
        .selectColumns(SELECT_COLUMNS)
        .fileSizePredicate(FILE_SIZE_PREDICATE)
        .orderByClause(ORDER_BY)
        .unpartitionedTable()
        .build();

    Assert.assertEquals("insert overwrite table db.table select a, b from db.table where " + FILE_SIZE_PREDICATE +
        " order by a", query);
    Assert.assertEquals(RewritePolicy.FULL_TABLE.name(), HiveConf.getVar(conf, HiveConf.ConfVars.REWRITE_POLICY));
  }

  @Test
  public void testUnpartitionedTableQueryWithoutOptionalClauses() {
    HiveConf conf = new HiveConf();
    String query = IcebergCompactionQueryBuilder.forTable(TABLE_NAME, conf)
        .selectColumns("*")
        .unpartitionedTable()
        .build();

    Assert.assertEquals("insert overwrite table db.table select * from db.table  ", query);
  }

  @Test
  public void testEvolvedPartitionSpecsQuery() {
    HiveConf conf = new HiveConf();
    String query = IcebergCompactionQueryBuilder.forTable(TABLE_NAME, conf)
        .selectColumns(SELECT_COLUMNS)
        .fileSizePredicate(FILE_SIZE_PREDICATE)
        .orderByClause(ORDER_BY)
        .evolvedPartitionSpecs(2)
        .build();

    Assert.assertEquals("insert overwrite table db.table select a, b from db.table " +
            "where " + VirtualColumn.PARTITION_SPEC_ID.getName() + " != 2 and " +
            VirtualColumn.FILE_PATH.getName() + " is not null and " + FILE_SIZE_PREDICATE + " order by a",
        query);
    Assert.assertEquals(RewritePolicy.PARTITION.name(), HiveConf.getVar(conf, HiveConf.ConfVars.REWRITE_POLICY));
  }

  @Test
  public void testSinglePartitionQuery() {
    HiveConf conf = new HiveConf();
    String partitionPredicate = "`partition`.`dept_id` = 1 AND `partition`.`city` = 'London'";
    String query = IcebergCompactionQueryBuilder.forTable(TABLE_NAME, conf)
        .selectColumns(SELECT_COLUMNS)
        .fileSizePredicate(FILE_SIZE_PREDICATE)
        .orderByClause(ORDER_BY)
        .singlePartition("dept_id=1/city=London", partitionPredicate, 1)
        .build();

    Assert.assertEquals("INSERT OVERWRITE TABLE db.table SELECT a, b FROM db.table WHERE " +
            VirtualColumn.FILE_PATH.getName() + " IN " +
            "(SELECT FILE_PATH FROM db.table.FILES WHERE " + partitionPredicate + " AND SPEC_ID = 1) AND " +
            FILE_SIZE_PREDICATE + " order by a",
        query);
    Assert.assertEquals(RewritePolicy.PARTITION.name(), HiveConf.getVar(conf, HiveConf.ConfVars.REWRITE_POLICY));
    Assert.assertEquals("dept_id=1/city=London", conf.get(IcebergCompactionService.PARTITION_PATH));
    Assert.assertFalse(HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED));
    Assert.assertFalse(HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CONVERT_JOIN));
  }
}
