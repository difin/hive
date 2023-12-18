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
package org.apache.hadoop.hive.metastore.txn.jdbc.queries;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.entities.OpenTxn;
import org.apache.hadoop.hive.metastore.txn.entities.OpenTxnList;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.ArrayList;
import java.util.List;

public class GetOpenTxnsListHandler implements TransactionalFunction<OpenTxnList> {

  private static final Logger LOG = LoggerFactory.getLogger(GetOpenTxnsListHandler.class);


  //language=SQL
  public static final String OPEN_TXNS_QUERY = "SELECT \"TXN_ID\", \"TXN_STATE\", \"TXN_TYPE\" FROM \"TXNS\""
      + " WHERE \"TXN_ID\" <= :hwm ORDER BY \"TXN_ID\"";
  //language=SQL
  public static final String OPEN_TXNS_INFO_QUERY = "SELECT \"TXN_ID\", \"TXN_STATE\", \"TXN_TYPE\", \"TXN_USER\", \"TXN_HOST\","
      + " \"TXN_STARTED\", \"TXN_LAST_HEARTBEAT\" FROM \"TXNS\" WHERE \"TXN_ID\" <= :hwm ORDER BY \"TXN_ID\"";
  
 
  private final boolean infoFields;

  public GetOpenTxnsListHandler(boolean infoFields) {
    this.infoFields = infoFields;
  }
  
  @Override
  public OpenTxnList execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    /**
     * This method can run at READ_COMMITTED as long as long as
     * {@link #openTxns(org.apache.hadoop.hive.metastore.api.OpenTxnRequest)} is atomic.
     * More specifically, as long as advancing TransactionID in NEXT_TXN_ID is atomic with
     * adding corresponding entries into TXNS.  The reason is that any txnid below HWM
     * is either in TXNS and thus considered open (Open/Aborted) or it's considered Committed.
     */
    long hwm = jdbcResource.execute(new TxnIdHandler(-1, false, null));
    List<OpenTxn> txnInfos = jdbcResource.getJdbcTemplate().query(
        String.format(infoFields ? OPEN_TXNS_INFO_QUERY : OPEN_TXNS_QUERY, TxnUtils.getEpochFn(jdbcResource.getDatabaseProduct())),
        new MapSqlParameterSource().addValue("hwm", hwm),
        rs -> {
          List<OpenTxn> txnInfos1 = new ArrayList<>();
          while (rs.next()) {
            long txnId = rs.getLong(1);
            TxnStatus state = TxnStatus.fromString(rs.getString(2));
            OpenTxn txnInfo = new OpenTxn(txnId, state, TxnType.findByValue(rs.getInt(3)));
            if (infoFields) {
              txnInfo.setUser(rs.getString(4));
              txnInfo.setHost(rs.getString(5));
              txnInfo.setStartedTime(rs.getLong(6));
              txnInfo.setLastHeartBeatTime(rs.getLong(7));
            }
            txnInfos1.add(txnInfo);
          }
          return txnInfos1; 
        }
    );
    LOG.debug("Got OpenTxnList with hwm: {} and openTxnList size {}.", hwm, txnInfos.size());
    return new OpenTxnList(hwm, txnInfos);
  }

}
