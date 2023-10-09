package org.apache.hadoop.hive.metastore.dataconnector.jdbc;

import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class PostgreSQLConnectorProvider extends AbstractJDBCConnectorProvider {
  private static Logger LOG = LoggerFactory.getLogger(MySQLConnectorProvider.class);
  private static final String DRIVER_CLASS = "org.postgresql.Driver".intern();

  public PostgreSQLConnectorProvider(String dbName, DataConnector dataConn) {
    super(dbName, dataConn, DRIVER_CLASS);
  }

  @Override protected String getCatalogName() {
    return scoped_db;
  }

  @Override protected String getDatabaseName() {
    return null;
  }

  protected String getDataType(String dbDataType, int size) {
    String mappedType = super.getDataType(dbDataType, size);
    if (!mappedType.equalsIgnoreCase(ColumnType.VOID_TYPE_NAME)) {
      return mappedType;
    }

    // map any db specific types here.
    //TODO: Geomentric, network, bit, array data types of postgresql needs to be supported.
    switch (dbDataType.toLowerCase())
    {
    case "bpchar":
    case "character":
      mappedType = ColumnType.CHAR_TYPE_NAME + wrapSize(size);
      break;
    case "int2":
      mappedType = ColumnType.SMALLINT_TYPE_NAME;
      break;
    case "int4":
      mappedType = ColumnType.INT_TYPE_NAME;
      break;
    case "int8":
      mappedType = ColumnType.BIGINT_TYPE_NAME;
      break;
    case "float4":
      mappedType = ColumnType.FLOAT_TYPE_NAME;
      break;
    case "float8":
      mappedType = ColumnType.DOUBLE_TYPE_NAME;
      break;
    default:
      mappedType = ColumnType.VOID_TYPE_NAME;
      break;
    }
    return mappedType;
  }
}
