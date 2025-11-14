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

package org.apache.hadoop.hive.metastore;

import javax.sql.DataSource;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;

/** Database product infered via JDBC. */
public enum DatabaseProduct {
  DERBY, MYSQL, POSTGRES, ORACLE, SQLSERVER, OTHER;
  static final private Logger LOG = LoggerFactory.getLogger(DatabaseProduct.class.getName());
  public static final String DERBY_NAME = "derby";
  public static final String SQL_SERVER_NAME = "sqlserver";
  public static final String MYSQL_NAME = "mysql";
  public static final String MARIADB_NAME = "mariadb";
  public static final String POSTGRESQL_NAME = "postgresql";
  public static final String ORACLE_NAME = "oracle";
  public static final String UNDEFINED_NAME = "other";

  private String productName;

  private String dbVersion;

  private Pair<Integer, Integer> versionNums;

  /**
   * Determine the database product type
   * @param productName string to defer database connection
   * @return database product type
   */
  public static DatabaseProduct determineDatabaseProduct(String productName) {
    return determineDatabaseProduct(productName, null, null);
  }

  public static DatabaseProduct determineDatabaseProduct(String productName,
      String version, Pair<Integer, Integer> versionNums) {
    DatabaseProduct databaseProduct = OTHER;
    if (productName == null) {
      return databaseProduct;
    }
    productName = productName.toLowerCase();
    if (productName.contains(DERBY_NAME)) {
      databaseProduct = DERBY;
    } else if (productName.contains(SQL_SERVER_NAME)) {
      databaseProduct = SQLSERVER;
    } else if (productName.contains(MYSQL_NAME) || productName.contains(MARIADB_NAME)) {
      databaseProduct = MYSQL;
    } else if (productName.contains(ORACLE_NAME)) {
      databaseProduct = ORACLE;
    } else if (productName.contains(POSTGRESQL_NAME)) {
      databaseProduct = POSTGRES;
    }
    databaseProduct.productName = productName;
    if (databaseProduct.dbVersion == null && version != null) {
      databaseProduct.dbVersion = version;
    }
    if (databaseProduct.versionNums == null && versionNums != null) {
      databaseProduct.versionNums = versionNums;
    }
    return databaseProduct;
  }

  public static DatabaseProduct determineDatabaseProduct(DataSource connPool) {
    try (Connection conn = connPool.getConnection()) {
      String s = conn.getMetaData().getDatabaseProductName();
      String version = conn.getMetaData().getDatabaseProductVersion();
      int majorVersion = conn.getMetaData().getDatabaseMajorVersion();
      int minorVersion = conn.getMetaData().getDatabaseMinorVersion();
      return determineDatabaseProduct(s, version, Pair.of(majorVersion, minorVersion));
    } catch (SQLException e) {
      // Legacy code, should we throw the IllegalStateException instead?
      LOG.warn("Cannot determine database product; assuming OTHER", e);
      return DatabaseProduct.OTHER;
    }
  }

  public static boolean isDeadlock(DatabaseProduct dbProduct, SQLException e) {
    return e instanceof SQLTransactionRollbackException
        || ((dbProduct == MYSQL || dbProduct == POSTGRES || dbProduct == SQLSERVER)
            && "40001".equals(e.getSQLState()))
        || (dbProduct == POSTGRES && "40P01".equals(e.getSQLState()))
        || (dbProduct == ORACLE && (e.getMessage() != null && (e.getMessage().contains("deadlock detected")
            || e.getMessage().contains("can't serialize access for this transaction"))));
  }
  /**
   * Is the given exception a table not found exception
   * @param t Exception
   * @return
   */
  public static boolean isTableNotExistsError(DatabaseProduct dbProduct, Throwable t) {
      SQLException e = TxnUtils.getSqlException(t);
      return (dbProduct == POSTGRES && "42P01".equalsIgnoreCase(e.getSQLState()))
        || (dbProduct == MYSQL && "42S02".equalsIgnoreCase(e.getSQLState()))
        || (dbProduct == ORACLE && "42000".equalsIgnoreCase(e.getSQLState()) && e.getMessage().contains("ORA-00942"))
        || (dbProduct == SQLSERVER && "S0002".equalsIgnoreCase(e.getSQLState()) && e.getMessage().contains("Invalid object"))
        || (dbProduct == DERBY && "42X05".equalsIgnoreCase(e.getSQLState()));
  }

  public static String toVarChar(DatabaseProduct dbType, String column) {
    switch (dbType) {
    case DERBY:
      return String.format("CAST(%s AS VARCHAR(4000))", column);
    case ORACLE:
      return String.format("to_char(%s)", column);
    default:
      return column;
    }
  }

  /**
   * Whether the RDBMS has restrictions on IN list size (explicit, or poor perf-based).
   */
  public static boolean needsInBatching(DatabaseProduct dbType) {
    return dbType == ORACLE || dbType == SQLSERVER;
  }

  /**
   * Whether the RDBMS has a bug in join and filter operation order described in DERBY-6358.
   */
  public static boolean hasJoinOperationOrderBug(DatabaseProduct dbType) {
    return dbType == DERBY || dbType == ORACLE || dbType == POSTGRES;
  }

  public static boolean isDERBY(DatabaseProduct dbType) {
    return dbType == DERBY;
  }

  public static boolean isMYSQL(DatabaseProduct dbType) {
    return dbType == MYSQL;
  }

  public final boolean isSQLSERVER(DatabaseProduct dbType) {
    return dbType == SQLSERVER;
  }

  public static boolean isDuplicateKeyError(DatabaseProduct dbType, Throwable th) {
    SQLException ex = TxnUtils.getSqlException(th);
    switch (dbType) {
      case DERBY:
        if("23505".equals(ex.getSQLState())) {
          return true;
        }
        break;
      case MYSQL:
        //https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html
        if((ex.getErrorCode() == 1022 || ex.getErrorCode() == 1062 || ex.getErrorCode() == 1586)
                && "23000".equals(ex.getSQLState())) {
          return true;
        }
        break;
      case SQLSERVER:
        //2627 is unique constaint violation incl PK, 2601 - unique key
        if ((ex.getErrorCode() == 2627 || ex.getErrorCode() == 2601) && "23000".equals(ex.getSQLState())) {
          return true;
        }
        break;
      case ORACLE:
        if(ex.getErrorCode() == 1 && "23000".equals(ex.getSQLState())) {
          return true;
        }
        break;
      case POSTGRES:
        //http://www.postgresql.org/docs/8.1/static/errcodes-appendix.html
        if("23505".equals(ex.getSQLState())) {
          return true;
        }
        break;
      default:
        throw new IllegalArgumentException("Unexpected DB type: " + dbType + "; " +
                ex.getMessage() + " (SQLState=" + ex.getSQLState() + ", ErrorCode=" + ex.getErrorCode() + ")");
    }
    return false;
  }

  protected static String toDate(String tableValue, DatabaseProduct dbType) {
    if (ORACLE == dbType) {
      return "TO_DATE(" + tableValue + ", 'YYYY-MM-DD')";
    } else {
      return "cast(" + tableValue + " as date)";
    }
  }

  protected static String toTimestamp(String tableValue, DatabaseProduct dbType) {
    if (ORACLE == dbType) {
      return "TO_TIMESTAMP(" + tableValue + ", 'YYYY-MM-DD HH24:mi:ss')";
    } else if (SQLSERVER == dbType) {
      return "CONVERT(DATETIME, " + tableValue + ")";
    } else {
      return "cast(" + tableValue + " as TIMESTAMP)";
    }
  }


  public static String getHiveSchemaPostfix(DatabaseProduct dbType) {
    switch (dbType) {
    case SQLSERVER:
      return "mssql";
    case DERBY:
    case MYSQL:
    case POSTGRES:
    case ORACLE:
      return dbType.name().toLowerCase();
    case OTHER:
    default:
      return null;
    }
  }

  /**
   * Gets the boolean value specific to database for the given input
   * @param val boolean value
   * @return database specific value
   */
  public Object getBoolean(DatabaseProduct dbType, boolean val) {
    if (isDERBY(dbType)) {
      return val ? "Y" : "N";
    }
    return val;
  }

  /**
   * Get the max rows in a query with paramSize.
   * @param batch the configured batch size
   * @param paramSize the parameter size in a query statement
   * @return the max allowed rows in a query
   */
  public int getMaxRows(int batch, int paramSize, DatabaseProduct dbType) {
    if (isSQLSERVER(dbType)) {
      // SQL Server supports a maximum of 2100 parameters in a request. Adjust the maxRowsInBatch accordingly
      int maxAllowedRows = (2100 - paramSize) / paramSize;
      return Math.min(batch, maxAllowedRows);
    }
    return batch;
  }

  public boolean canMySQLSupportNoWait() {
    if (versionNums == null) {
      // Cannot determine the real version of back db
      return false;
    }
    // Prior to MySQL 8.0.1, the NOWAIT clause for row locking was not supported directly in the s4u syntax.
    // Use the MAX_EXECUTION_TIME to ensure the s4u does not run indefinitely.
    String dbName = productName.replaceAll("\\s+", "").toLowerCase();
    boolean isMariaDB = dbName.contains(MARIADB_NAME) ||
        (dbVersion != null && dbVersion.toLowerCase().contains(MARIADB_NAME));
    if (isMariaDB) {
      // https://mariadb.com/docs/release-notes/community-server/old-releases/release-notes-mariadb-10-3-series/mariadb-1030-release-notes
      return (versionNums.getLeft() >= 10 && versionNums.getRight() > 2);
    } else {
      // https://dev.mysql.com/blog-archive/mysql-8-0-1-using-skip-locked-and-nowait-to-handle-hot-rows/
      return versionNums.getLeft() > 8 ||
          (versionNums.getLeft() == 8 && dbVersion != null && dbVersion.compareToIgnoreCase("8.0.1") >= 0);
    }

  }

}
