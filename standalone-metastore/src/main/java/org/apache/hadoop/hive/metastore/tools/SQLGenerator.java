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
package org.apache.hadoop.hive.metastore.tools;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Helper class that generates SQL queries with syntax specific to target DB
 * todo: why throw MetaException?
 */
@VisibleForTesting
public final class SQLGenerator {
  static final private Logger LOG = LoggerFactory.getLogger(SQLGenerator.class.getName());
  private final DatabaseProduct dbProduct;

  private final Configuration conf;

  public SQLGenerator(DatabaseProduct dbProduct, Configuration conf) {
    this.dbProduct = dbProduct;
    this.conf = conf;
  }

  /**
   * Generates "Insert into T(a,b,c) values(1,2,'f'),(3,4,'c')" for appropriate DB
   *
   * @param tblColumns   e.g. "T(a,b,c)"
   * @param rows         e.g. list of Strings like 3,4,'d'
   * @param paramsList   List of parameters which in turn is list of Strings to be set in PreparedStatement object
   * @return List PreparedStatement objects for fully formed INSERT INTO ... statements
   */
  public List<PreparedStatement> createInsertValuesPreparedStmt(Connection dbConn,
                                                                String tblColumns, List<String> rows,
                                                                List<List<String>> paramsList)
          throws SQLException {
    if (rows == null || rows.size() == 0) {
      return Collections.emptyList();
    }
    assert((paramsList == null) || (rows.size() == paramsList.size()));

    List<Integer> rowsCountInStmts = new ArrayList<>();
    List<String> insertStmts = createInsertValuesStmt(tblColumns, rows, rowsCountInStmts);
    assert(insertStmts.size() == rowsCountInStmts.size());

    List<PreparedStatement> preparedStmts = new ArrayList<>();
    int paramsListFromIdx = 0;
    try {
      for (int stmtIdx = 0; stmtIdx < insertStmts.size(); stmtIdx++) {
        String sql = insertStmts.get(stmtIdx);
        PreparedStatement pStmt = prepareStmtWithParameters(dbConn, sql, null);
        if (paramsList != null) {
          int paramIdx = 1;
          int paramsListToIdx = paramsListFromIdx + rowsCountInStmts.get(stmtIdx);
          for (int paramsListIdx = paramsListFromIdx; paramsListIdx < paramsListToIdx; paramsListIdx++) {
            List<String> params = paramsList.get(paramsListIdx);
            for (int i = 0; i < params.size(); i++, paramIdx++) {
              pStmt.setString(paramIdx, params.get(i));
            }
          }
          paramsListFromIdx = paramsListToIdx;
        }
        preparedStmts.add(pStmt);
      }
    } catch (SQLException e) {
      for (PreparedStatement pst : preparedStmts) {
        pst.close();
      }
      throw e;
    }
    return preparedStmts;
  }

  /**
   * Generates "Insert into T(a,b,c) values(1,2,'f'),(3,4,'c')" for appropriate DB
   *
   * @param tblColumns e.g. "T(a,b,c)"
   * @param rows       e.g. list of Strings like 3,4,'d'
   * @return fully formed INSERT INTO ... statements
   */
  public List<String> createInsertValuesStmt(String tblColumns, List<String> rows) {
    return createInsertValuesStmt(tblColumns, rows, null);
  }

  /**
   * Generates "Insert into T(a,b,c) values(1,2,'f'),(3,4,'c')" for appropriate DB
   *
   * @param tblColumns e.g. "T(a,b,c)"
   * @param rows       e.g. list of Strings like 3,4,'d'
   * @param rowsCountInStmts Output the number of rows in each insert statement returned.
   * @return fully formed INSERT INTO ... statements
   */
  private List<String> createInsertValuesStmt(String tblColumns, List<String> rows, List<Integer> rowsCountInStmts) {
    if (rows == null || rows.size() == 0) {
      return Collections.emptyList();
    }
    List<String> insertStmts = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    int numRowsInCurrentStmt = 0;
    switch (dbProduct) {
    case ORACLE:
      if (rows.size() > 1) {
        //http://www.oratable.com/oracle-insert-all/
        //https://livesql.oracle.com/apex/livesql/file/content_BM1LJQ87M5CNIOKPOWPV6ZGR3.html
        for (int numRows = 0; numRows < rows.size(); numRows++) {
          if (numRows % MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_MAX_ELEMENTS_VALUES_CLAUSE) == 0) {
            if (numRows > 0) {
              sb.append(" select * from dual");
              insertStmts.add(sb.toString());
              if (rowsCountInStmts != null) {
                rowsCountInStmts.add(numRowsInCurrentStmt);
              }
              numRowsInCurrentStmt = 0;
            }
            sb.setLength(0);
            sb.append("insert all ");
          }
          sb.append("into ").append(tblColumns).append(" values(").append(rows.get(numRows))
              .append(") ");
          numRowsInCurrentStmt++;
        }
        sb.append("select * from dual");
        insertStmts.add(sb.toString());
        if (rowsCountInStmts != null) {
          rowsCountInStmts.add(numRowsInCurrentStmt);
        }
        return insertStmts;
      }
      //fall through
    case DERBY:
    case MYSQL:
    case POSTGRES:
    case SQLSERVER:
      for (int numRows = 0; numRows < rows.size(); numRows++) {
        if (numRows % MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_MAX_ELEMENTS_VALUES_CLAUSE) == 0) {
          if (numRows > 0) {
            insertStmts.add(sb.substring(0, sb.length() - 1));//exclude trailing comma
            if (rowsCountInStmts != null) {
              rowsCountInStmts.add(numRowsInCurrentStmt);
            }
            numRowsInCurrentStmt = 0;
          }
          sb.setLength(0);
          sb.append("insert into ").append(tblColumns).append(" values");
        }
        sb.append('(').append(rows.get(numRows)).append("),");
        numRowsInCurrentStmt++;
      }
      insertStmts.add(sb.substring(0, sb.length() - 1));//exclude trailing comma
      if (rowsCountInStmts != null) {
        rowsCountInStmts.add(numRowsInCurrentStmt);
      }
      return insertStmts;
    default:
      String msg = "Unrecognized database product name <" + dbProduct + ">";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }
  }

  /**
   * Given a {@code selectStatement}, decorated it with FOR UPDATE or semantically equivalent
   * construct.  If the DB doesn't support, return original select.
   */
  public String addForUpdateClause(String selectStatement) throws MetaException {
    switch (dbProduct) {
    case DERBY:
      //https://db.apache.org/derby/docs/10.1/ref/rrefsqlj31783.html
      //sadly in Derby, FOR UPDATE doesn't meant what it should
      return selectStatement;
    case MYSQL:
      //http://dev.mysql.com/doc/refman/5.7/en/select.html
    case ORACLE:
      //https://docs.oracle.com/cd/E17952_01/refman-5.6-en/select.html
    case POSTGRES:
      //http://www.postgresql.org/docs/9.0/static/sql-select.html
      return selectStatement + " for update";
    case SQLSERVER:
      //https://msdn.microsoft.com/en-us/library/ms189499.aspx
      //https://msdn.microsoft.com/en-us/library/ms187373.aspx
      String modifier = " with (updlock)";
      int wherePos = selectStatement.toUpperCase().indexOf(" WHERE ");
      if (wherePos < 0) {
        return selectStatement + modifier;
      }
      return selectStatement.substring(0, wherePos) + modifier +
          selectStatement.substring(wherePos, selectStatement.length());
    default:
      String msg = "Unrecognized database product name <" + dbProduct + ">";
      LOG.error(msg);
      throw new MetaException(msg);
    }
  }

  /**
   * Suppose you have a query "select a,b from T" and you want to limit the result set
   * to the first 5 rows.  The mechanism to do that differs in different DBs.
   * Make {@code noSelectsqlQuery} to be "a,b from T" and this method will return the
   * appropriately modified row limiting query.
   * <p>
   * Note that if {@code noSelectsqlQuery} contains a join, you must make sure that
   * all columns are unique for Oracle.
   */
  public String addLimitClause(int numRows, String noSelectsqlQuery) throws MetaException {
    switch (dbProduct) {
    case DERBY:
      //http://db.apache.org/derby/docs/10.7/ref/rrefsqljoffsetfetch.html
      return "select " + noSelectsqlQuery + " fetch first " + numRows + " rows only";
    case MYSQL:
      //http://www.postgresql.org/docs/7.3/static/queries-limit.html
    case POSTGRES:
      //https://dev.mysql.com/doc/refman/5.0/en/select.html
      return "select " + noSelectsqlQuery + " limit " + numRows;
    case ORACLE:
      //newer versions (12c and later) support OFFSET/FETCH
      return "select * from (select " + noSelectsqlQuery + ") where rownum <= " + numRows;
    case SQLSERVER:
      //newer versions (2012 and later) support OFFSET/FETCH
      //https://msdn.microsoft.com/en-us/library/ms189463.aspx
      return "select TOP(" + numRows + ") " + noSelectsqlQuery;
    default:
      String msg = "Unrecognized database product name <" + dbProduct + ">";
      LOG.error(msg);
      throw new MetaException(msg);
    }
  }

  /**
   * Make PreparedStatement object with list of String type parameters to be set.
   * It is assumed the input sql string have the number of "?" equal to number of parameters
   * passed as input.
   * @param dbConn - Connection object
   * @param sql - SQL statement with "?" for input parameters.
   * @param parameters - List of String type parameters to be set in PreparedStatement object
   * @return PreparedStatement type object
   * @throws SQLException
   */
  public PreparedStatement prepareStmtWithParameters(Connection dbConn, String sql, List<String> parameters)
          throws SQLException {
    PreparedStatement pst = dbConn.prepareStatement(addEscapeCharacters(sql));
    if ((parameters == null) || parameters.isEmpty()) {
      return pst;
    }
    try {
      for (int i = 1; i <= parameters.size(); i++) {
        pst.setString(i, parameters.get(i - 1));
      }
    } catch (SQLException e) {
      pst.close();
      throw e;
    }
    return pst;
  }



  /**
   * Oracle SQL query that creates or replaces view HMS_SUMMARY.
   */
  private static final String CREATE_METADATASUMMARY_ORACLE = "CREATE OR REPLACE VIEW METADATASUMMARYALL AS SELECT a.TBL_ID, a.TBL_NAME, a.OWNER as \"CTLG\", a.TBL_TYPE, a.CREATE_TIME, a.DB_ID, a.SD_ID, b.NAME, c.INPUT_FORMAT, c.IS_COMPRESSED, c.LOCATION, c.OUTPUT_FORMAT,c.SERDE_ID, d.SLIB, TO_CHAR(e.PARAM_VALUE) as \"PARAM_VAL\", count(j.COLUMN_NAME) as \"TOTAL_COLUMN_COUNT\", jj.ARRAY_COLUMN_COUNT, jj.STRUCT_COLUMN_COUNT, jj.MAP_COLUMN_COUNT, k.PARTITION_KEY_NAME as \"PARTITION_COLUMN\", m.PARTITION_CNT, CAST(CAST(q.NUM_FILES AS VARCHAR2(200)) AS NUMBER) as \"num_files\", CAST(q.TOTAL_SIZE AS NUMBER) as \"total_size\", CAST(q.NUM_ROWS AS NUMBER) as \"num_rows\" FROM TBLS a left JOIN DBS b on a.DB_ID = b.DB_ID left JOIN SDS c on a.SD_ID = c.SD_ID LEFT JOIN SERDES d on c.SERDE_ID = d.SERDE_ID left JOIN (select SERDE_ID,PARAM_KEY,PARAM_VALUE from SERDE_PARAMS where PARAM_KEY = 'field.delim') e on c.SERDE_ID = e.SERDE_ID left join COLUMNS_V2 j on c.CD_ID = j.CD_ID LEFT JOIN (SELECT CD_ID, sum(CASE WHEN TYPE_NAME like 'array%' THEN 1 ELSE 0 END) AS \"ARRAY_COLUMN_COUNT\", sum(CASE WHEN TYPE_NAME like 'struct%' THEN 1 ELSE 0 END) AS \"STRUCT_COLUMN_COUNT\", sum(CASE WHEN TYPE_NAME like 'map%' THEN 1 ELSE 0 END) AS \"MAP_COLUMN_COUNT\" from COLUMNS_V2 group by CD_ID) jj on jj.CD_ID=c.CD_ID left JOIN(select TBL_ID, LISTAGG(PKEY_NAME, ',') as PARTITION_KEY_NAME from PARTITION_KEYS group by TBL_ID) k on a.TBL_ID = k.TBL_ID left JOIN (select SERDE_ID,PARAM_KEY,PARAM_VALUE from SERDE_PARAMS where PARAM_KEY = 'serialization.format') f on c.SERDE_ID = f.SERDE_ID left join (select TBL_ID,PARAM_KEY,PARAM_VALUE from TABLE_PARAMS where PARAM_KEY = 'comment') g on a.TBL_ID = g.TBL_ID left JOIN (select TBL_ID, PARAM_KEY,PARAM_VALUE from TABLE_PARAMS where PARAM_KEY = 'transient_lastDdlTime') h on a.TBL_ID = h.TBL_ID left join (select TBL_ID,COUNT(PART_ID) as PARTITION_CNT from PARTITIONS group by TBL_ID) m on a.TBL_ID = m.TBL_ID Left join (SELECT aa.TBL_ID, aa.NUM_FILES + case when bb.NUM_FILES is not null then bb.NUM_FILES else 0 end AS \"NUM_FILES\", aa.NUM_ROWS + case when bb.NUM_ROWS is not null then bb.NUM_ROWS else 0 end AS \"NUM_ROWS\", aa.TOTAL_SIZE + case when bb.TOTAL_SIZE is not null then bb.TOTAL_SIZE else 0 end  AS \"TOTAL_SIZE\" from (select TBL_ID, max(CASE PARAM_KEY WHEN 'numFiles' THEN CAST(CAST(PARAM_VALUE AS VARCHAR2(200)) AS NUMBER) ELSE 0 END) AS \"NUM_FILES\", max(CASE PARAM_KEY WHEN 'numRows' THEN CAST(CAST(PARAM_VALUE AS VARCHAR2(200)) AS NUMBER) ELSE 0 END) AS \"NUM_ROWS\", max(CASE PARAM_KEY WHEN 'totalSize' THEN CAST(CAST(PARAM_VALUE AS VARCHAR2(200)) AS NUMBER) ELSE 0 END) AS \"TOTAL_SIZE\" from TABLE_PARAMS group by TBL_ID) aa left join (SELECT y.TBL_ID, SUM(x.NUM_FILES) AS \"NUM_FILES\", SUM(x.NUM_ROWS) AS \"NUM_ROWS\", SUM(x.TOTAL_SIZE) AS \"TOTAL_SIZE\" FROM PARTITIONS y left JOIN (SELECT PART_ID, max(CASE PARAM_KEY WHEN 'numFiles' THEN CAST(CAST(PARAM_VALUE AS VARCHAR2(200)) AS NUMBER) ELSE 0 END) AS \"NUM_FILES\", max(CASE PARAM_KEY WHEN 'numRows' THEN CAST(CAST(PARAM_VALUE AS VARCHAR2(200)) AS NUMBER) ELSE 0 END) AS \"NUM_ROWS\", max(CASE PARAM_KEY WHEN 'totalSize' THEN CAST(CAST(PARAM_VALUE AS VARCHAR2(200)) AS NUMBER) ELSE 0 END) AS \"TOTAL_SIZE\" FROM PARTITION_PARAMS group by PART_ID)x ON y.PART_ID=x.PART_ID group by y.TBL_ID) bb on aa.TBL_ID = bb.TBL_ID) q on a.TBL_ID = q.TBL_ID group by a.TBL_ID, a.TBL_NAME, a.OWNER, a.TBL_TYPE, a.CREATE_TIME, a.DB_ID, a.SD_ID, b.NAME, c.INPUT_FORMAT, c.IS_COMPRESSED, c.LOCATION, c.OUTPUT_FORMAT,c.SERDE_ID, d.SLIB, TO_CHAR(e.PARAM_VALUE), jj.ARRAY_COLUMN_COUNT, jj.STRUCT_COLUMN_COUNT,jj.MAP_COLUMN_COUNT, k.PARTITION_KEY_NAME, m.PARTITION_CNT,q.NUM_FILES, q.TOTAL_SIZE, q.NUM_ROWS";

  /**
   * MySQL query that creates or replaces view HMS_SUMMARY.
   */
  private static final String CREATE_METADATASUMMARY_MYSQL = "CREATE OR REPLACE VIEW METADATASUMMARYALL AS SELECT a.TBL_ID, a.TBL_NAME, a.OWNER as \"CTLG\", a.TBL_TYPE, a.CREATE_TIME, a.DB_ID, a.SD_ID, b.NAME, c.INPUT_FORMAT, c.IS_COMPRESSED, c.LOCATION, c.OUTPUT_FORMAT,c.SERDE_ID, d.SLIB, e.PARAM_VALUE, count(j.COLUMN_NAME) as \"TOTAL_COLUMN_COUNT\", jj.ARRAY_COLUMN_COUNT as \"ARRAY_COLUMN_COUNT\", jj.STRUCT_COLUMN_COUNT as \"STRUCT_COLUMN_COUNT\", jj.MAP_COLUMN_COUNT as \"MAP_COLUMN_COUNT\", k.PARTITION_KEY_NAME as \"PARTITION_COLUMN\", m.PARTITION_CNT, CAST(CAST(q.NUM_FILES AS CHAR(200)) AS SIGNED) as NUM_FILES, CAST(q.TOTAL_SIZE AS SIGNED) as TOTAL_SIZE, CAST(q.NUM_ROWS AS SIGNED) as NUM_ROWS, q.WRITE_FORMAT_DEFAULT, q.TRANSACTIONAL_PROPERTIES FROM TBLS a left JOIN DBS b on a.DB_ID = b.DB_ID left JOIN SDS c on a.SD_ID = c.SD_ID LEFT JOIN SERDES d on c.SERDE_ID = d.SERDE_ID left JOIN (select SERDE_ID,PARAM_KEY,PARAM_VALUE from SERDE_PARAMS where PARAM_KEY = 'field.delim') e on c.SERDE_ID = e.SERDE_ID left join COLUMNS_V2 j on c.CD_ID = j.CD_ID LEFT JOIN (SELECT CD_ID, sum(CASE WHEN TYPE_NAME like 'array%' THEN 1 ELSE 0 END) AS \"ARRAY_COLUMN_COUNT\", sum(CASE WHEN TYPE_NAME like 'struct%' THEN 1 ELSE 0 END) AS \"STRUCT_COLUMN_COUNT\", sum(CASE WHEN TYPE_NAME like 'map%' THEN 1 ELSE 0 END) AS \"MAP_COLUMN_COUNT\" from COLUMNS_V2 group by CD_ID) jj on jj.CD_ID=c.CD_ID left JOIN(select TBL_ID, GROUP_CONCAT(PKEY_NAME, ',') as PARTITION_KEY_NAME from PARTITION_KEYS group by TBL_ID) k on a.TBL_ID = k.TBL_ID left JOIN (select SERDE_ID,PARAM_KEY,PARAM_VALUE from SERDE_PARAMS where PARAM_KEY = 'serialization.format') f on c.SERDE_ID = f.SERDE_ID left join (select TBL_ID,PARAM_KEY,PARAM_VALUE from TABLE_PARAMS where PARAM_KEY = 'comment') g on a.TBL_ID = g.TBL_ID left JOIN (select TBL_ID, PARAM_KEY,PARAM_VALUE from TABLE_PARAMS where PARAM_KEY = 'transient_lastDdlTime') h on a.TBL_ID = h.TBL_ID left join (select TBL_ID,COUNT(PART_ID) as PARTITION_CNT from PARTITIONS group by TBL_ID) m on a.TBL_ID = m.TBL_ID Left join (SELECT aa.TBL_ID, aa.NUM_FILES + case when bb.NUM_FILES is not null then bb.NUM_FILES else 0 end AS \"NUM_FILES\", aa.NUM_ROWS + case when bb.NUM_ROWS is not null then bb.NUM_ROWS else 0 end AS \"NUM_ROWS\", aa.TOTAL_SIZE + case when bb.TOTAL_SIZE is not null then bb.TOTAL_SIZE else 0 end AS \"TOTAL_SIZE\", aa.WRITE_FORMAT_DEFAULT, aa.TRANSACTIONAL_PROPERTIES from (select u.TBL_ID, NUM_FILES, NUM_ROWS, TOTAL_SIZE, WRITE_FORMAT_DEFAULT, TRANSACTIONAL_PROPERTIES from (select TBL_ID, max(CASE PARAM_KEY WHEN 'numFiles' THEN CAST(CAST(PARAM_VALUE AS CHAR(200)) AS SIGNED) ELSE 0 END) AS \"NUM_FILES\", max(CASE PARAM_KEY WHEN 'numRows' THEN CAST(CAST(PARAM_VALUE AS CHAR(200)) AS SIGNED) ELSE 0 END) AS \"NUM_ROWS\", max(CASE PARAM_KEY WHEN 'totalSize' THEN CAST(CAST(PARAM_VALUE AS CHAR(200)) AS SIGNED) ELSE 0 END) AS \"TOTAL_SIZE\" from TABLE_PARAMS group by TBL_ID) u left join (select TBL_ID, CAST(PARAM_VALUE AS CHAR(200)) as \"WRITE_FORMAT_DEFAULT\" from TABLE_PARAMS where PARAM_KEY = 'write.format.default') v on u.TBL_ID = v.TBL_ID left join (select TBL_ID, CAST(PARAM_VALUE AS CHAR(200)) as \"TRANSACTIONAL_PROPERTIES\" from TABLE_PARAMS where PARAM_KEY = 'transactional_properties') w on u.TBL_ID = w.TBL_ID) aa left join (SELECT y.TBL_ID, SUM(x.NUM_FILES) AS \"NUM_FILES\", SUM(x.NUM_ROWS) AS \"NUM_ROWS\", SUM(x.TOTAL_SIZE) AS \"TOTAL_SIZE\" FROM PARTITIONS y left JOIN (SELECT PART_ID, max(CASE PARAM_KEY WHEN 'numFiles' THEN CAST(CAST(PARAM_VALUE AS CHAR(200)) AS SIGNED) ELSE 0 END) AS \"NUM_FILES\", max(CASE PARAM_KEY WHEN 'numRows' THEN CAST(CAST(PARAM_VALUE AS CHAR(200)) AS SIGNED) ELSE 0 END) AS \"NUM_ROWS\", max(CASE PARAM_KEY WHEN 'totalSize' THEN CAST(CAST(PARAM_VALUE AS CHAR(200)) AS SIGNED) ELSE 0 END) AS \"TOTAL_SIZE\" FROM PARTITION_PARAMS group by PART_ID)x ON y.PART_ID=x.PART_ID group by y.TBL_ID) bb on aa.TBL_ID = bb.TBL_ID) q on a.TBL_ID = q.TBL_ID group by a.TBL_ID, a.TBL_NAME, a.OWNER, a.TBL_TYPE, a.CREATE_TIME, a.DB_ID, a.SD_ID, b.NAME, c.INPUT_FORMAT, c.IS_COMPRESSED, c.LOCATION, c.OUTPUT_FORMAT,c.SERDE_ID, d.SLIB, e.PARAM_VALUE, jj.ARRAY_COLUMN_COUNT, jj.STRUCT_COLUMN_COUNT,jj.MAP_COLUMN_COUNT, k.PARTITION_KEY_NAME, m.PARTITION_CNT,q.NUM_FILES, q.TOTAL_SIZE, q.NUM_ROWS, q.WRITE_FORMAT_DEFAULT, q.TRANSACTIONAL_PROPERTIES;";

  /**
   * Postgres SQL query that creates or replaces view HMS_SUMMARY.
   */
  private static final String CREATE_METADATASUMMARY_POSTGRES = "CREATE OR REPLACE VIEW \"METADATASUMMARYALL\" AS SELECT a.\"TBL_ID\", a.\"TBL_NAME\", a.\"OWNER\" as \"CTLG\", a.\"TBL_TYPE\", a.\"CREATE_TIME\", a.\"DB_ID\", a.\"SD_ID\", b.\"NAME\", c.\"INPUT_FORMAT\", c.\"IS_COMPRESSED\", c.\"LOCATION\", c.\"OUTPUT_FORMAT\",c.\"SERDE_ID\", d.\"SLIB\", e.\"PARAM_VALUE\", count(j.\"COLUMN_NAME\") as \"TOTAL_COLUMN_COUNT\", jj.\"ARRAY_COLUMN_COUNT\", jj.\"STRUCT_COLUMN_COUNT\", jj.\"MAP_COLUMN_COUNT\", k.\"PARTITION_KEY_NAME\" as \"PARTITION_COLUMN\", m.\"PARTITION_CNT\", CAST(CAST(q.\"NUM_FILES\" AS CHAR(200)) AS BIGINT), CAST(q.\"TOTAL_SIZE\" AS BIGINT), CAST(q.\"NUM_ROWS\" AS BIGINT), q.\"WRITE_FORMAT_DEFAULT\", q.\"TRANSACTIONAL_PROPERTIES\" FROM \"TBLS\" a LEFT JOIN \"DBS\" b on a.\"DB_ID\" = b.\"DB_ID\" LEFT JOIN \"SDS\" c on a.\"SD_ID\" = c.\"SD_ID\" LEFT JOIN \"SERDES\" d on c.\"SERDE_ID\" = d.\"SERDE_ID\" LEFT JOIN (select \"SERDE_ID\", \"PARAM_KEY\", \"PARAM_VALUE\" from \"SERDE_PARAMS\" where \"PARAM_KEY\" = 'field.delim') e on c.\"SERDE_ID\" = e.\"SERDE_ID\" LEFT JOIN \"COLUMNS_V2\" j on c.\"CD_ID\" = j.\"CD_ID\" LEFT JOIN (SELECT \"CD_ID\", sum(CASE WHEN \"TYPE_NAME\" like 'array%' THEN 1 ELSE 0 END) AS \"ARRAY_COLUMN_COUNT\", sum(CASE WHEN \"TYPE_NAME\" like 'struct%' THEN 1 ELSE 0 END) AS \"STRUCT_COLUMN_COUNT\", sum(CASE WHEN \"TYPE_NAME\" like 'map%' THEN 1 ELSE 0 END) AS \"MAP_COLUMN_COUNT\" from \"COLUMNS_V2\" group by \"CD_ID\") jj on jj.\"CD_ID\" = c.\"CD_ID\" LEFT JOIN (select \"TBL_ID\", string_agg(\"PKEY_NAME\", ',') as \"PARTITION_KEY_NAME\" from \"PARTITION_KEYS\" group by \"TBL_ID\") k on a.\"TBL_ID\" = k.\"TBL_ID\" LEFT JOIN (select \"SERDE_ID\", \"PARAM_KEY\", \"PARAM_VALUE\" from \"SERDE_PARAMS\" where \"PARAM_KEY\" = 'serialization.format') f on c.\"SERDE_ID\" = f.\"SERDE_ID\" LEFT JOIN (select \"TBL_ID\",\"PARAM_KEY\",\"PARAM_VALUE\" from \"TABLE_PARAMS\" where \"PARAM_KEY\" = 'comment') g on a.\"TBL_ID\" = g.\"TBL_ID\" LEFT JOIN (select \"TBL_ID\", \"PARAM_KEY\",\"PARAM_VALUE\" from \"TABLE_PARAMS\" where \"PARAM_KEY\" = 'transient_lastDdlTime') h on a.\"TBL_ID\" = h.\"TBL_ID\" LEFT JOIN (select \"TBL_ID\",COUNT(\"PART_ID\") as \"PARTITION_CNT\" from \"PARTITIONS\" group by \"TBL_ID\") m on a.\"TBL_ID\" = m.\"TBL_ID\" LEFT JOIN (SELECT aa.\"TBL_ID\", aa.\"NUM_FILES\" + case when bb.\"NUM_FILES\" is not null then bb.\"NUM_FILES\" else 0 end AS \"NUM_FILES\", aa.\"NUM_ROWS\" + case when bb.\"NUM_ROWS\" is not null then bb.\"NUM_ROWS\" else 0 end AS \"NUM_ROWS\", aa.\"TOTAL_SIZE\" + case when bb.\"TOTAL_SIZE\" is not null then bb.\"TOTAL_SIZE\" else 0 end AS \"TOTAL_SIZE\", aa.\"WRITE_FORMAT_DEFAULT\", aa.\"TRANSACTIONAL_PROPERTIES\" from (select u.\"TBL_ID\", \"NUM_FILES\", \"NUM_ROWS\", \"TOTAL_SIZE\", \"WRITE_FORMAT_DEFAULT\", \"TRANSACTIONAL_PROPERTIES\" from (select \"TBL_ID\", max(CASE \"PARAM_KEY\" WHEN 'numFiles' THEN CAST(CAST(\"PARAM_VALUE\" AS CHAR(200)) AS BIGINT) ELSE 0 END) AS \"NUM_FILES\", max(CASE \"PARAM_KEY\" WHEN 'numRows' THEN CAST(CAST(\"PARAM_VALUE\" AS CHAR(200)) AS BIGINT) ELSE 0 END) AS \"NUM_ROWS\", max(CASE \"PARAM_KEY\" WHEN 'totalSize' THEN CAST(CAST(\"PARAM_VALUE\" AS CHAR(200)) AS BIGINT) ELSE 0 END) AS \"TOTAL_SIZE\" from \"TABLE_PARAMS\" group by \"TBL_ID\") u left join (select \"TBL_ID\", CAST(\"PARAM_VALUE\" AS CHAR(200)) as \"WRITE_FORMAT_DEFAULT\" from \"TABLE_PARAMS\" where \"PARAM_KEY\" = 'write.format.default') v on u.\"TBL_ID\" = v.\"TBL_ID\" left join (select \"TBL_ID\", CAST(\"PARAM_VALUE\" AS CHAR(200)) as \"TRANSACTIONAL_PROPERTIES\" from \"TABLE_PARAMS\" where \"PARAM_KEY\" = 'transactional_properties') w on u.\"TBL_ID\" = w.\"TBL_ID\") aa left join (SELECT y.\"TBL_ID\", SUM(x.\"NUM_FILES\") AS \"NUM_FILES\", SUM(x.\"NUM_ROWS\") AS \"NUM_ROWS\", SUM(x.\"TOTAL_SIZE\") AS \"TOTAL_SIZE\" FROM \"PARTITIONS\" y left join (SELECT \"PART_ID\", max(CASE \"PARAM_KEY\" WHEN 'numFiles' THEN CAST(CAST(\"PARAM_VALUE\" AS CHAR(200)) AS BIGINT) ELSE 0 END) AS \"NUM_FILES\", max(CASE \"PARAM_KEY\" WHEN 'numRows' THEN CAST(CAST(\"PARAM_VALUE\" AS CHAR(200)) AS BIGINT) ELSE 0 END) AS \"NUM_ROWS\", max(CASE \"PARAM_KEY\" WHEN 'totalSize' THEN CAST(CAST(\"PARAM_VALUE\" AS CHAR(200)) AS BIGINT) ELSE 0 END) AS \"TOTAL_SIZE\" FROM \"PARTITION_PARAMS\" group by \"PART_ID\") x ON y.\"PART_ID\" = x.\"PART_ID\" group by y.\"TBL_ID\") bb on aa.\"TBL_ID\" = bb.\"TBL_ID\") q on a.\"TBL_ID\" = q.\"TBL_ID\" group by a.\"TBL_ID\", a.\"TBL_NAME\", a.\"OWNER\", a.\"TBL_TYPE\", a.\"CREATE_TIME\", a.\"DB_ID\", a.\"SD_ID\", b.\"NAME\", c.\"INPUT_FORMAT\", c.\"IS_COMPRESSED\", c.\"LOCATION\", c.\"OUTPUT_FORMAT\",c.\"SERDE_ID\", d.\"SLIB\", e.\"PARAM_VALUE\", jj.\"ARRAY_COLUMN_COUNT\", jj.\"STRUCT_COLUMN_COUNT\",jj.\"MAP_COLUMN_COUNT\", k.\"PARTITION_KEY_NAME\", m.\"PARTITION_CNT\",q.\"NUM_FILES\", q.\"TOTAL_SIZE\", q.\"NUM_ROWS\", q.\"WRITE_FORMAT_DEFAULT\", q.\"TRANSACTIONAL_PROPERTIES\";";

  /**
   * Create or replace a view that stores all the info regarding metastore summary.
   * @return
   */
  public List<String> getCreateQueriesForMetastoreSummary() {
    List<String> queries = new ArrayList();
    switch (dbProduct) {
    case MYSQL:
      queries.add(CREATE_METADATASUMMARY_MYSQL);
      break;
    case DERBY:
      return null;
    case SQLSERVER:
      return null;
    case ORACLE:
      queries.add(CREATE_METADATASUMMARY_ORACLE);
      break;
    case POSTGRES:
      queries.add(CREATE_METADATASUMMARY_POSTGRES);
      break;
    }
    return queries;
  }

  public String getSelectQueryForMetastoreSummary() {
    if (dbProduct == DatabaseProduct.DERBY || dbProduct == DatabaseProduct.MYSQL || dbProduct == DatabaseProduct.ORACLE) {
      return "select * from METADATASUMMARYALL";
    } else if (dbProduct == DatabaseProduct.POSTGRES){
      return "select * from \"METADATASUMMARYALL\"";
    }
    return null;
  }

  public DatabaseProduct getDbProduct() {
    return dbProduct;
  }

  // This is required for SQL executed directly. If the SQL has double quotes then some dbs tend to
  // remove the escape characters and store the variable without double quote.
  public String addEscapeCharacters(String s) {
    if (dbProduct ==  DatabaseProduct.MYSQL) {
      return s.replaceAll("\\\\", "\\\\\\\\");
    }
    return s;
  }

  /**
   * Gets the multiple row insert query for the given table with specified columns and row format
   * @param tableName table name to be used in query
   * @param columns comma separated column names string
   * @param rowFormat values format string used in the insert query. Format is like (?,?...?) and the number of
   *                  question marks in the format is equal to number of column names in the columns argument
   * @param batchCount number of rows in the query
   * @return database specific multiple row insert query
   */
  public String getBatchInsertQuery(String tableName, String columns, String rowFormat, int batchCount) {
    StringBuilder sb = new StringBuilder();
    String fixedPart = tableName + " " + columns + " values ";
    String row;
    if (dbProduct == DatabaseProduct.ORACLE) {
      sb.append("insert all ");
      row = "into " + fixedPart + rowFormat + " ";
    } else {
      sb.append("insert into " + fixedPart);
      row = rowFormat + ',';
    }
    for (int i = 0; i < batchCount; i++) {
      sb.append(row);
    }
    if (dbProduct == DatabaseProduct.ORACLE) {
      sb.append("select * from dual ");
    }
    sb.setLength(sb.length() - 1);
    return sb.toString();
  }

  /**
   * Gets the boolean value specific to database for the given input
   * @param val boolean value
   * @return database specific value
   */
  public Object getBoolean(boolean val) {
    if (dbProduct == DatabaseProduct.DERBY) {
      return val ? "Y" : "N";
    }
    return val;
  }
}
