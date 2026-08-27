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

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.DriverUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.RowLineageUtils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorContext;
import org.apache.hadoop.hive.ql.txn.compactor.QueryCompactor;
import org.apache.hive.iceberg.org.apache.orc.storage.common.TableName;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.mr.hive.compaction.evaluator.CompactionEvaluator;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergQueryCompactor extends QueryCompactor  {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergQueryCompactor.class.getName());

  @Override
  public boolean run(CompactorContext context) throws IOException, HiveException, InterruptedException {

    String compactTableName = TableName.getDbTable(context.getTable().getDbName(), context.getTable().getTableName());
    Map<String, String> tblProperties = context.getTable().getParameters();
    LOG.debug("Initiating compaction for the {} table", compactTableName);

    HiveConf conf = new HiveConf(context.getConf());
    CompactionInfo ci = context.getCompactionInfo();

    String compactionQuery = buildCompactionQuery(context, compactTableName, conf);

    SessionState sessionState = setupQueryCompactionSession(conf, ci, tblProperties);

    String compactionTarget = "table " + HiveUtils.unparseIdentifier(compactTableName) +
        (ci.partName != null ? ", partition " + HiveUtils.unparseIdentifier(ci.partName) : "");

    try {
      DriverUtils.runOnDriver(sessionState.getConf(), sessionState, compactionQuery);
      LOG.info("Completed compaction for {}", compactionTarget);
      return true;
    } catch (HiveException e) {
      LOG.error("Failed compacting {}", compactionTarget, e);
      throw e;
    } finally {
      RowLineageUtils.disableRowLineage(sessionState);
      sessionState.setCompaction(false);
    }
  }

  private String buildCompactionQuery(CompactorContext context, String compactTableName, HiveConf conf)
      throws HiveException {
    CompactionInfo ci = context.getCompactionInfo();
    org.apache.hadoop.hive.ql.metadata.Table table = Hive.get(conf).getTable(context.getTable().getDbName(),
        context.getTable().getTableName());
    Table icebergTable = IcebergTableUtil.getTable(conf, table.getTTable());
    String orderBy = ci.orderByClause == null ? "" : ci.orderByClause;
    String fileSizePredicate = buildMinorFileSizePredicate(ci, compactTableName, conf, table);
    String columnsList = buildSelectColumns(table, icebergTable, ci, compactTableName, conf);

    IcebergCompactionQueryBuilder queryBuilder = IcebergCompactionQueryBuilder.forTable(compactTableName, conf)
        .selectColumns(columnsList)
        .fileSizePredicate(fileSizePredicate)
        .orderByClause(orderBy);

    String compactionQuery;
    if (ci.partName == null) {
      compactionQuery = buildFullTableCompactionQuery(queryBuilder, icebergTable);
    } else {
      compactionQuery = buildPartitionCompactionQuery(queryBuilder, ci, icebergTable);
    }

    LOG.info("Compaction query: {}", compactionQuery);
    return compactionQuery;
  }

  private static String buildFullTableCompactionQuery(IcebergCompactionQueryBuilder queryBuilder, Table icebergTable)
      throws HiveException {
    if (!icebergTable.spec().isPartitioned()) {
      return queryBuilder.unpartitionedTable().build();
    }

    if (icebergTable.specs().size() > 1) {
      return queryBuilder.evolvedPartitionSpecs(icebergTable.spec().specId()).build();
    }

    // Partitioned table without partition evolution with partition spec as null in the compaction request - this
    // code branch is not supposed to be reachable
    throw new HiveException(ErrorMsg.COMPACTION_NO_PARTITION);
  }

  private static String buildPartitionCompactionQuery(
      IcebergCompactionQueryBuilder queryBuilder, CompactionInfo ci, Table icebergTable) throws HiveException {
    try {
      PartitionSpec spec = IcebergTableUtil.getPartitionSpec(icebergTable, ci.partName);
      String partitionPredicate = IcebergPartitionPredicateBuilder.build(ci, spec);
      return queryBuilder.singlePartition(ci.partName, partitionPredicate, spec.specId()).build();
    } catch (MetaException e) {
      throw new HiveException(e);
    }
  }

  private static String buildMinorFileSizePredicate(
      CompactionInfo ci, String compactTableName, HiveConf conf, org.apache.hadoop.hive.ql.metadata.Table table) {
    if (ci.type != CompactionType.MINOR) {
      return null;
    }

    long fileSizeInBytesThreshold = CompactionEvaluator.getFragmentSizeBytes(table.getParameters());
    conf.setLong(CompactorContext.COMPACTION_FILE_SIZE_THRESHOLD, fileSizeInBytesThreshold);
    // IOW query containing a join with Iceberg .files metadata table fails with exception that Iceberg AVRO format
    // doesn't support vectorization, hence disabling it in this case.
    conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, false);

    return String.format("%1$s in (select file_path from %2$s.files where file_size_in_bytes < %3$d)",
        VirtualColumn.FILE_PATH.getName(), compactTableName, fileSizeInBytesThreshold);
  }

  private static String buildSelectColumns(
      org.apache.hadoop.hive.ql.metadata.Table table,
      Table icebergTable,
      CompactionInfo ci,
      String compactTableName,
      HiveConf conf) {
    String columnsList = "*";
    if (!RowLineageUtils.supportsRowLineage(table)) {
      return columnsList;
    }

    RowLineageUtils.enableRowLineage(conf);
    LOG.debug("Row lineage flag set for compaction of table {}", compactTableName);
    if (ci.isMajorCompaction() && ci.partName == null) {
      columnsList = buildSelectColumnList(icebergTable, conf) + RowLineageUtils.getRowLineageColumnsForCompaction();
    } else {
      columnsList = columnsList + RowLineageUtils.getRowLineageColumnsForCompaction();
    }
    return columnsList;
  }

  /**
   * Builds a comma-separated SELECT list from the Iceberg table schema.
   */
  private static String buildSelectColumnList(Table icebergTable, HiveConf conf) {
    return icebergTable.schema().columns().stream()
        .map(Types.NestedField::name)
        .map(col -> HiveUtils.unparseIdentifier(col, conf))
        .collect(Collectors.joining(", "));
  }
}
