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

package org.apache.hadoop.hive.ql.ddl.table.storage.compact;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.stream.Collectors;

/**
 * Operation process of compacting a table.
 */
public class AlterTableCompactOperation extends DDLOperation<AlterTableCompactDesc> {
  public AlterTableCompactOperation(DDLOperationContext context, AlterTableCompactDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    Table table = context.getDb().getTable(desc.getTableName());
    if (!AcidUtils.isTransactionalTable(table) && !AcidUtils.isNonNativeAcidTable(table)) {
      throw new HiveException(ErrorMsg.NONACID_COMPACTION_NOT_SUPPORTED, table.getDbName(), table.getTableName());
    }
    
    if (table.getStorageHandler() != null) {
      Optional<ErrorMsg> error = table.getStorageHandler().isEligibleForCompaction(table, desc.getPartitionSpec());
      if (error.isPresent()) {
        throw new HiveException(error.get(), table.getDbName(), table.getTableName());
      }
    }

    List<Partition> partitions = getPartitions(table);

    if (partitions.isEmpty()) {
      CompactionResponse compactionResponse = compact(table, null);
      parseCompactionResponse(compactionResponse, table, null);
    } else { // Check for eligible partitions and initiate compaction
      for (Partition partition : partitions) {
        CompactionResponse compactionResponse = compact(table, partition.getName());
        parseCompactionResponse(compactionResponse, table, partition.getName());
      }
      // If Iceberg table had partition evolution, it will create compaction request without partition specification,
      // and it will compact all files from old partition specs, besides compacting partitions of current spec in parallel.
      if (desc.getPartitionSpec() == null && 
          DDLUtils.isIcebergTable(table) && table.getStorageHandler().hasUndergonePartitionEvolution(table)) {
        CompactionResponse compactionResponse = compact(table, null);
        parseCompactionResponse(compactionResponse, table, null);
      }
    }

    return 0;
  }

  private void parseCompactionResponse(CompactionResponse compactionResponse, Table table, String partitionName)
      throws HiveException {
    if (compactionResponse == null) {
      context.getConsole().printInfo(
          "Not enough deltas to initiate compaction for table=" + table.getTableName() + "partition=" + partitionName);
      return;
    }
    if (!compactionResponse.isAccepted()) {
      if (compactionResponse.isSetErrormessage()) {
        throw new HiveException(ErrorMsg.COMPACTION_REFUSED, table.getDbName(), table.getTableName(),
            partitionName == null ? "" : " partition(" + partitionName + ")", compactionResponse.getErrormessage());
      }
      context.getConsole().printInfo(
          "Compaction already enqueued with id " + compactionResponse.getId() + "; State is " + compactionResponse.getState());
      return;
    }
    context.getConsole().printInfo("Compaction enqueued with id " + compactionResponse.getId());
    if (desc.isBlocking() && compactionResponse.isAccepted()) {
      waitForCompactionToFinish(compactionResponse);
    }
  }

  private List<Partition> getPartitions(Table table) throws HiveException {
    List<Partition> partitions = new ArrayList<>();
    if (desc.getPartitionSpec() == null) {
      if (table.isPartitioned()) { // Compaction can only be done on the whole table if the table is non-partitioned.
        throw new HiveException(ErrorMsg.COMPACTION_NO_PARTITION);
      }
      if ((DDLUtils.isIcebergTable(table) && table.getStorageHandler().isPartitioned(table))) {
        partitions = context.getDb().getPartitions(table);
      }
    } else {
      Map<String, String> partitionSpec = desc.getPartitionSpec();
      partitions = context.getDb().getPartitions(table, partitionSpec);
      if (partitions.isEmpty()) {
        throw new HiveException(ErrorMsg.INVALID_PARTITION_SPEC);
      }
      // This validates that the partition spec given in the compaction command matches exactly one partition 
      // in the table, not a partial partition spec.
      partitions = partitions.stream().filter(part -> part.getSpec().size() == partitionSpec.size()).collect(Collectors.toList());
      if (partitions.size() != 1) {
        throw new HiveException(ErrorMsg.TOO_MANY_COMPACTION_PARTITIONS);
      }
    }
    return partitions;
  }

  private CompactionResponse compact(Table table, String partitionName) throws HiveException {
    CompactionRequest req = new CompactionRequest(table.getDbName(), table.getTableName(),
        CompactionType.valueOf(desc.getCompactionType().toUpperCase()));
    req.setPartitionname(partitionName);
    req.setPoolName(desc.getPoolName());
    req.setProperties(desc.getProperties());
    req.setInitiatorId(JavaUtils.hostname() + "-" + HiveMetaStoreClient.MANUALLY_INITIATED_COMPACTION);
    req.setInitiatorVersion(HiveMetaStoreClient.class.getPackage().getImplementationVersion());
    req.setOrderByClause(desc.getOrderByClause());
    if (desc.getNumberOfBuckets() > 0) {
      req.setNumberOfBuckets(desc.getNumberOfBuckets());
    }
    CompactionResponse resp = context.getDb().compact(req);
    if (resp.isAccepted()) {
      context.getConsole().printInfo("Compaction enqueued with id " + resp.getId());
    } else {
      context.getConsole().printInfo("Compaction already enqueued with id " + resp.getId() + "; State is " +
          resp.getState());
    }
    return resp;
  }

  private void waitForCompactionToFinish(CompactionResponse resp) throws HiveException {
    StringBuilder progressDots = new StringBuilder();
    long waitTimeMs = 1000;
    long waitTimeOut = HiveConf.getLongVar(context.getConf(), HiveConf.ConfVars.HIVE_COMPACTOR_WAIT_TIMEOUT);
    wait: while (true) {
      //double wait time until 5min
      waitTimeMs = waitTimeMs*2;
      waitTimeMs = Math.min(waitTimeMs, waitTimeOut);
      try {
        Thread.sleep(waitTimeMs);
      } catch (InterruptedException ex) {
        context.getConsole().printInfo("Interrupted while waiting for compaction with id=" + resp.getId());
        break;
      }

      //this could be expensive when there are a lot of compactions....
      //todo: update to search by ID once HIVE-13353 is done
      ShowCompactResponse allCompactions = context.getDb().showCompactions();
      for (ShowCompactResponseElement compaction : allCompactions.getCompacts()) {
        if (resp.getId() != compaction.getId()) {
          continue;
        }

        switch (compaction.getState()) {
          case TxnStore.WORKING_RESPONSE:
          case TxnStore.INITIATED_RESPONSE:
            //still working
            context.getConsole().printInfo(progressDots.toString());
            progressDots.append(".");
            continue wait;
          default:
            //done
            context.getConsole().printInfo("Compaction with id " + resp.getId() + " finished with status: " +
                compaction.getState());
            break wait;
        }
      }
    }
  }
}
