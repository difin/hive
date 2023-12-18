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

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.TxnIdHandler;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Objects;

public class MinOpenTxnIdWaterMarkFunction implements TransactionalFunction<Long> {


  public MinOpenTxnIdWaterMarkFunction() {
  }

  @Override
  public Long execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    long highestAllocatedTxnId = jdbcResource.execute(new TxnIdHandler(-1, false, null));
    return Objects.requireNonNull(jdbcResource.getJdbcTemplate().query(
        "SELECT MIN(\"TXN_ID\") FROM \"TXNS\" WHERE \"TXN_STATE\"= :status",
        new MapSqlParameterSource().addValue("status", TxnStatus.OPEN.getSqlConst(), Types.CHAR),
        (ResultSet rs) -> {
          if (!rs.next()) {
            throw new IllegalStateException("Scalar query returned no rows?!?!!");
          }
          long lowestOpenTxnId = rs.getLong(1);

          if (rs.wasNull()) {
            //if here then there are no Open txns and  highestAllocatedTxnId must be
            //resolved (i.e. committed or aborted), either way
            //there are no open txns with id <= highestAllocatedTxnId
            //the +1 is there because "delete ..." below has < (which is correct for the case when
            //there is an open txn
            //Concurrency: even if new txn starts (or starts + commits) it is still true that
            //there are no currently open txns that overlap with any committed txn with
            //commitId <= commitHighWaterMark (as set on next line).  So plain READ_COMMITTED is enough.
            return highestAllocatedTxnId + 1;
          } else {
            return lowestOpenTxnId;
          }
        }));
  }

}
