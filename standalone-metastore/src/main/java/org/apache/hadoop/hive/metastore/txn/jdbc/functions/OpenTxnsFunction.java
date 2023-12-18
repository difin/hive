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
package org.apache.hadoop.hive.metastore.txn.jdbc.functions;

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.OpenTxnEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.TxnIdHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.TargetTxnIdListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class OpenTxnsFunction implements TransactionalFunction<List<Long>> {

  private static final Logger LOG = LoggerFactory.getLogger(OpenTxnsFunction.class);

  private final OpenTxnRequest rqst;
  private final List<TransactionalMetaStoreEventListener> transactionalListeners;

  public OpenTxnsFunction(OpenTxnRequest rqst, List<TransactionalMetaStoreEventListener> transactionalListeners) {
    this.rqst = rqst;
    this.transactionalListeners = transactionalListeners;
  }

  @Override
  public List<Long> execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    DatabaseProduct dbProduct = jdbcResource.getDatabaseProduct();
    int numTxns = rqst.getNum_txns();
    // Make sure the user has not requested an insane amount of txns.
    int maxTxns = MetastoreConf.getIntVar(jdbcResource.getConf(), MetastoreConf.ConfVars.TXN_MAX_OPEN_BATCH);
    if (numTxns > maxTxns) {
      numTxns = maxTxns;
    }
    
    TxnType txnType = rqst.isSetTxn_type() ? rqst.getTxn_type() : TxnType.DEFAULT;
    boolean isReplayedReplTxn = txnType == TxnType.REPL_CREATED;
    boolean isHiveReplTxn = rqst.isSetReplPolicy() && txnType == TxnType.DEFAULT;
    if (isReplayedReplTxn) {
      assert rqst.isSetReplPolicy();
      List<Long> targetTxnIdList = jdbcResource.execute(new TargetTxnIdListHandler(rqst.getReplPolicy(), rqst.getReplSrcTxnIds()));

      if (!targetTxnIdList.isEmpty()) {
        if (targetTxnIdList.size() != rqst.getReplSrcTxnIds().size()) {
          LOG.warn("target txn id number {} is not matching with source txn id number {}",
              targetTxnIdList, rqst.getReplSrcTxnIds());
        }
        LOG.info("Target transactions {} are present for repl policy : {} and Source transaction id : {}",
            targetTxnIdList, rqst.getReplPolicy(), rqst.getReplSrcTxnIds().toString());
        return targetTxnIdList;
      }
    }

    long first = jdbcResource.execute(new TxnIdHandler(0, true, jdbcResource.getSqlGenerator()));
    jdbcResource.getJdbcTemplate().update("UPDATE \"NEXT_TXN_ID\" SET \"NTXN_NEXT\" = :next",
        new MapSqlParameterSource().addValue("next", (first + numTxns)));
    
    long minOpenTxnId = 0;
    if (TxnHandler.ConfVars.useMinHistoryLevel()) {
      minOpenTxnId = new MinOpenTxnIdWaterMarkFunction().execute(jdbcResource);
    }


    List<Long> txnIds = new ArrayList<>(numTxns);

    List<String> rows = new ArrayList<>();
    List<String> params = new ArrayList<>();
    params.add(rqst.getUser());
    params.add(rqst.getHostname());
    List<List<String>> paramsList = new ArrayList<>(numTxns);

    for (long i = first; i < first + numTxns; i++) {
      txnIds.add(i);
      rows.add(i + "," + TxnStatus.OPEN + "," + getEpochFn(dbProduct) + ","
          + getEpochFn(dbProduct) + ",?,?," + txnType.getValue());
      paramsList.add(params);
    }
    
    jdbcResource.getJdbcTemplate().getJdbcTemplate().execute((ConnectionCallback<Void>) con -> {
      List<PreparedStatement> statements = jdbcResource.getSqlGenerator().createInsertValuesPreparedStmt(con,
          "\"TXNS\" (\"TXN_ID\", \"TXN_STATE\", \"TXN_STARTED\", \"TXN_LAST_HEARTBEAT\", "
              + "\"TXN_USER\", \"TXN_HOST\", \"TXN_TYPE\")",
          rows, paramsList);
      for (PreparedStatement pst : statements) {
        pst.execute();
        pst.close();
      }
      return null;
    });
    
    int maxBatchSize = MetastoreConf.getIntVar(jdbcResource.getConf(), MetastoreConf.ConfVars.JDBC_MAX_BATCH_SIZE);
    addTxnToMinHistoryLevel(jdbcResource.getJdbcTemplate().getJdbcTemplate(), maxBatchSize, txnIds, minOpenTxnId);

    if (isReplayedReplTxn) {
      List<String> rowsRepl = new ArrayList<>();

      params.clear();
      paramsList.clear();
      params.add(rqst.getReplPolicy());
      for (int i = 0; i < numTxns; i++) {
        rowsRepl.add("?," + rqst.getReplSrcTxnIds().get(i) + "," + txnIds.get(i));
        paramsList.add(params);
      }

      jdbcResource.getJdbcTemplate().getJdbcTemplate().execute((ConnectionCallback<Void>) con -> {
        List<PreparedStatement> statements = jdbcResource.getSqlGenerator().createInsertValuesPreparedStmt(con,
            "\"REPL_TXN_MAP\" (\"RTM_REPL_POLICY\", \"RTM_SRC_TXN_ID\", \"RTM_TARGET_TXN_ID\")", rowsRepl,
            paramsList);
        for (PreparedStatement pst : statements) {
          pst.execute();
          pst.close();
        }
        return null;
      });      
    }

    if (transactionalListeners != null && !isHiveReplTxn) {
      MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
          EventMessage.EventType.OPEN_TXN, new OpenTxnEvent(txnIds, txnType), jdbcResource.getConnection(), jdbcResource.getSqlGenerator());
    }
    return txnIds;
  }

  /**
   * Add min history level entry for each generated txn record
   * @param jdbcTemplate {@link NamedParameterJdbcTemplate} to use for command execution
   * @param txnIds new transaction ids
   * @deprecated Remove this method when min_history_level table is dropped
   * @throws SQLException ex
   */
  @Deprecated
  private void addTxnToMinHistoryLevel(JdbcTemplate jdbcTemplate, int batchSize, List<Long> txnIds, long minOpenTxnId) {
    if (!TxnHandler.ConfVars.useMinHistoryLevel()) {
      return;
    }
    String sql = "INSERT INTO \"MIN_HISTORY_LEVEL\" (\"MHL_TXNID\", \"MHL_MIN_OPEN_TXNID\") VALUES(?, ?)";
    LOG.debug("Going to execute insert batch: <{}>", sql);

    jdbcTemplate.batchUpdate(sql, txnIds, batchSize, (ps, argument) -> {
      ps.setLong(1, argument);
      ps.setLong(2, minOpenTxnId);
    });
    
    LOG.info("Added entries to MIN_HISTORY_LEVEL for current txns: ({}) with min_open_txn: {}", txnIds, minOpenTxnId);
  }

}
