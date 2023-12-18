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

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TxnIdHandler implements QueryHandler<Long> {
  
  private final int offset;
  private final boolean addForUpdateClause;
  private final SQLGenerator sqlGenerator;

  public TxnIdHandler(int offset, boolean addForUpdateClause, SQLGenerator sqlGenerator) {
    this.offset = offset;
    this.addForUpdateClause = addForUpdateClause;
    this.sqlGenerator = sqlGenerator;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    String sql = "SELECT \"NTXN_NEXT\" + :offset FROM \"NEXT_TXN_ID\"";
    if (addForUpdateClause) { 
     sql = sqlGenerator.addForUpdateClause(sql);
    }
    return sql;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource().addValue("offset", offset);
  }

  @Override
  public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
    if (rs.next()) {
      long id = rs.getLong(1);
      if(!rs.wasNull()) {
        return id;
      }
    }
    throw new EmptyResultDataAccessException("Could not allocate next txn id", 1);
  }
}
