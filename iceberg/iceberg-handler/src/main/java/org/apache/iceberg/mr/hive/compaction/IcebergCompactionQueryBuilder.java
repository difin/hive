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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context.RewritePolicy;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;

/**
 * Builds INSERT OVERWRITE queries used by Iceberg compaction.
 */
final class IcebergCompactionQueryBuilder {

  enum Scope {
    UNPARTITIONED_TABLE,
    EVOLVED_PARTITION_SPECS,
    SINGLE_PARTITION
  }

  private final String tableName;
  private final HiveConf conf;
  private String selectColumns;
  private String fileSizePredicate;
  private String orderByClause = "";
  private Scope scope;
  private int currentSpecId;
  private String partitionPath;
  private String partitionPredicate;
  private int partitionSpecId;

  private IcebergCompactionQueryBuilder(String tableName, HiveConf conf) {
    this.tableName = tableName;
    this.conf = conf;
  }

  static IcebergCompactionQueryBuilder forTable(String tableName, HiveConf conf) {
    return new IcebergCompactionQueryBuilder(tableName, conf);
  }

  IcebergCompactionQueryBuilder selectColumns(String columns) {
    this.selectColumns = columns;
    return this;
  }

  IcebergCompactionQueryBuilder fileSizePredicate(String predicate) {
    this.fileSizePredicate = predicate;
    return this;
  }

  IcebergCompactionQueryBuilder orderByClause(String clause) {
    this.orderByClause = clause == null ? "" : clause;
    return this;
  }

  IcebergCompactionQueryBuilder unpartitionedTable() {
    this.scope = Scope.UNPARTITIONED_TABLE;
    return this;
  }

  IcebergCompactionQueryBuilder evolvedPartitionSpecs(int specId) {
    this.scope = Scope.EVOLVED_PARTITION_SPECS;
    this.currentSpecId = specId;
    return this;
  }

  IcebergCompactionQueryBuilder singlePartition(String path, String predicate, int specId) {
    this.scope = Scope.SINGLE_PARTITION;
    this.partitionPath = path;
    this.partitionPredicate = predicate;
    this.partitionSpecId = specId;
    return this;
  }

  String build() {
    switch (scope) {
      case UNPARTITIONED_TABLE:
        return buildUnpartitionedTableQuery();
      case EVOLVED_PARTITION_SPECS:
        return buildEvolvedPartitionSpecsQuery();
      case SINGLE_PARTITION:
        return buildSinglePartitionQuery();
      default:
        throw new IllegalStateException("Compaction query scope is not set");
    }
  }

  private String buildUnpartitionedTableQuery() {
    HiveConf.setVar(conf, ConfVars.REWRITE_POLICY, RewritePolicy.FULL_TABLE.name());
    return String.format("insert overwrite table %1$s select %2$s from %1$s %3$s %4$s",
        tableName, selectColumns, whereClause(fileSizePredicate), orderByClause);
  }

  private String buildEvolvedPartitionSpecsQuery() {
    HiveConf.setVar(conf, ConfVars.REWRITE_POLICY, RewritePolicy.PARTITION.name());
    // A single filter on a virtual column causes errors during compilation,
    // added another filter on file_path as a workaround.
    return String.format("insert overwrite table %1$s select %2$s from %1$s " +
            "where %3$s != %4$d and %5$s is not null %6$s %7$s",
        tableName, selectColumns,
        VirtualColumn.PARTITION_SPEC_ID.getName(), currentSpecId,
        VirtualColumn.FILE_PATH.getName(), andClause(fileSizePredicate), orderByClause);
  }

  private String buildSinglePartitionQuery() {
    HiveConf.setBoolVar(conf, ConfVars.HIVE_CONVERT_JOIN, false);
    conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    HiveConf.setVar(conf, ConfVars.REWRITE_POLICY, RewritePolicy.PARTITION.name());
    conf.set(IcebergCompactionService.PARTITION_PATH, new Path(partitionPath).toString());

    return String.format("INSERT OVERWRITE TABLE %1$s SELECT %2$s FROM %1$s WHERE %3$s IN " +
            "(SELECT FILE_PATH FROM %1$s.FILES WHERE %4$s AND SPEC_ID = %5$d) %6$s %7$s",
        tableName, selectColumns, VirtualColumn.FILE_PATH.getName(), partitionPredicate, partitionSpecId,
        andClause(fileSizePredicate, true), orderByClause);
  }

  private static String whereClause(String predicate) {
    return predicate == null ? "" : "where " + predicate;
  }

  private static String andClause(String predicate) {
    return andClause(predicate, false);
  }

  private static String andClause(String predicate, boolean uppercase) {
    return predicate == null ? "" : (uppercase ? "AND " : "and ") + predicate;
  }
}
