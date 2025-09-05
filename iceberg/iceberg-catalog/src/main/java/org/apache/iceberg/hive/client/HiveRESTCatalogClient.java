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

package org.apache.iceberg.hive.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.BaseMetaStoreClient;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.CatalogUtils;
import org.apache.iceberg.hive.HMSTablePropertyHelper;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.hive.RuntimeMetaException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.thrift.TException;

public class HiveRESTCatalogClient extends BaseMetaStoreClient {

  public static final String NAMESPACE_SEPARATOR = ".";
  public static final String DB_OWNER = "owner";
  public static final String DB_OWNER_TYPE = "ownerType";

  private RESTCatalog restCatalog;

  public HiveRESTCatalogClient(Configuration conf, boolean allowEmbedded) {
    this(conf);
  }

  public HiveRESTCatalogClient(Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) {
    this(conf);
  }

  public HiveRESTCatalogClient(Configuration conf) {
    super(conf);
    try {
      reconnect();
    } catch (MetaException e) {
      throw new RuntimeMetaException(e, "Failed to initialize REST catalog");
    }
  }

  @Override
  public void reconnect() throws MetaException {
    close();
    String catName = MetaStoreUtils.getDefaultCatalog(conf);
    Map<String, String> properties = CatalogUtils.getCatalogProperties(conf, CatalogUtils.getCatalogName(conf));
    restCatalog = (RESTCatalog) CatalogUtil.buildIcebergCatalog(catName, properties, null);
  }

  @Override
  public GetAllFunctionsResponse getAllFunctions() {
    GetAllFunctionsResponse response = new GetAllFunctionsResponse();
    response.setFunctions(Collections.emptyList());
    return response;
  }

  @Override
  public void close() {
    try {
      if (restCatalog != null) {
        restCatalog.close();
      }
    } catch (IOException e) {
      throw new RuntimeMetaException(e.getCause(), "Failed to close existing REST catalog");
    }
  }

  @Override
  public List<String> getDatabases(String catName, String dbPattern) {
    validateCurrentCatalog(catName);
    // Convert the Hive glob pattern (e.g., "db*") to a valid Java regex ("db.*").
    String regex = dbPattern.replace("*", ".*");
    Pattern pattern = Pattern.compile(regex);

    return restCatalog.listNamespaces(Namespace.empty()).stream()
        .map(Namespace::toString)
        .filter(pattern.asPredicate())
        .collect(Collectors.toCollection(ArrayList::new));
  }

  @Override
  public List<String> getAllDatabases(String catName) {
    return getDatabases(catName, "*");
  }

  @Override
  public List<String> getAllDatabases() {
    return getAllDatabases(MetaStoreUtils.getDefaultCatalog(conf));
  }

  @Override
  public List<String> getDatabases(String databasePattern) {
    return getDatabases(MetaStoreUtils.getDefaultCatalog(conf), databasePattern);
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern) {
    validateCurrentCatalog(catName);

    // Convert the Hive glob pattern to a Java regex.
    String regex = tablePattern.replace("*", ".*");
    Pattern pattern = Pattern.compile(regex);

    // List tables from the specific database (namespace) and filter them.
    return restCatalog.listTables(Namespace.of(dbName)).stream()
        .map(TableIdentifier::name)
        .filter(pattern.asPredicate())
        .collect(Collectors.toCollection(ArrayList::new));
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) {
    return getTables(catName, dbName, "*");
  }

  @Override
  public List<String> getTables(String dbName, String tablePattern) {
    return getTables(MetaStoreUtils.getDefaultCatalog(conf), dbName, tablePattern);
  }

  @Override
  public List<String> getTables(String dbName, String tablePattern, TableType tableType) {
    return getTables(MetaStoreUtils.getDefaultCatalog(conf), dbName, tablePattern, tableType);
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern, TableType tableType) {
    List<String> tables = getTables(catName, dbName, tablePattern);
    if (tableType == null || tableType == TableType.EXTERNAL_TABLE) {
      return tables;
    }
    return Lists.newArrayList();
  }

  @Override
  public void dropTable(Table table, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws TException {
    restCatalog.dropTable(TableIdentifier.of(table.getDbName(), table.getTableName()));
  }

  @Override
  public void dropTable(String catName, String dbName, String tableName,
      boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws TException {
    validateCurrentCatalog(catName);
    try {
      restCatalog.dropTable(TableIdentifier.of(dbName, tableName));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      if (!ignoreUnknownTab) {
        throw new NoSuchObjectException(e.getMessage());
      }
    }
  }

  private void validateCurrentCatalog(String catName) {
    if (!restCatalog.name().equals(catName)) {
      throw new IllegalArgumentException(
          String.format("Catalog name '%s' does not match the current catalog '%s'", catName, restCatalog.name()));
    }
  }

  /**
   * Resolves REST namespace metadata to Hive's {@link PrincipalType} without throwing.
   * Missing, blank, or unknown values yield null (owner type left unset).
   */
  private static PrincipalType parsePrincipalType(String ownerTypeName) {
    if (!StringUtils.isNotBlank(ownerTypeName)) {
      return null;
    }
    String trimmed = ownerTypeName.trim();
    for (PrincipalType t : PrincipalType.values()) {
      if (t.name().equals(trimmed)) {
        return t;
      }
    }
    return null;
  }

  @Override
  public boolean tableExists(String catName, String dbName, String tableName) {
    validateCurrentCatalog(catName);
    return restCatalog.tableExists(TableIdentifier.of(dbName, tableName));
  }

  @Override
  public Database getDatabase(String dbName) throws TException {
    String catName = MetaStoreUtils.getDefaultCatalog(conf);
    return getDatabase(catName, dbName);
  }

  @Override
  public Database getDatabase(String catName, String dbName) throws NoSuchObjectException {
    validateCurrentCatalog(catName);

    return restCatalog.listNamespaces(Namespace.empty()).stream()
        .filter(namespace -> namespace.levels()[0].equals(dbName))
        .map(namespace -> {
          Database database = new Database();
          database.setName(String.join(NAMESPACE_SEPARATOR, namespace.levels()));
          Map<String, String> namespaceMetadata = restCatalog.loadNamespaceMetadata(Namespace.of(dbName));
          database.setLocationUri(namespaceMetadata.get(CatalogUtils.LOCATION));
          database.setCatalogName(restCatalog.name());
          database.setOwnerName(namespaceMetadata.get(DB_OWNER));
          PrincipalType ownerType = parsePrincipalType(namespaceMetadata.get(DB_OWNER_TYPE));
          if (ownerType != null) {
            database.setOwnerType(ownerType);
          }
          return database;
        }).findFirst().orElseThrow(() ->
            new NoSuchObjectException("Database " + dbName + " not found"));
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName) throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    return getTable(req).getTable();
  }

  @Override
  public Table getTable(String dbName, String tableName) throws TException {
    return getTable(MetaStoreUtils.getDefaultCatalog(conf), dbName, tableName);
  }

  @Override
  public Table getTable(String dbName, String tableName, boolean getColumnStats, String engine) throws TException {
    String catName = MetaStoreUtils.getDefaultCatalog(conf);
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    req.setGetColumnStats(getColumnStats);
    req.setEngine(engine);
    return getTable(req).getTable();
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName, String validWriteIdList,
      boolean getColumnStats, String engine, boolean getFileMetadata) throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    if (validWriteIdList != null) {
      req.setValidWriteIdList(validWriteIdList);
    }
    req.setGetColumnStats(getColumnStats);
    req.setEngine(engine);
    req.setGetFileMetadata(getFileMetadata);
    return getTable(req).getTable();
  }

  @Override
  public GetTableResult getTable(GetTableRequest tableRequest) throws TException {
    validateCurrentCatalog(tableRequest.getCatName());
    org.apache.iceberg.Table icebergTable;
    try {
      icebergTable = restCatalog.loadTable(TableIdentifier.of(tableRequest.getDbName(),
          tableRequest.getTblName()));
    } catch (NoSuchTableException exception) {
      throw new NoSuchObjectException();
    }
    Table hiveTable = MetastoreUtil.toHiveTable(icebergTable, conf);
    GetTableResult result = new GetTableResult();
    result.setTable(hiveTable);
    return result;
  }

  @Override
  public void createTable(Table table) throws TException {
    createTable(new CreateTableRequest(table));
  }

  @Override
  public void createTable(CreateTableRequest request) throws TException {
    Table table = request.getTable();
    List<FieldSchema> cols = Lists.newArrayList(table.getSd().getCols());
    if (table.isSetPartitionKeys() && !table.getPartitionKeys().isEmpty()) {
      cols.addAll(table.getPartitionKeys());
    }
    Properties tableProperties = CatalogUtils.getCatalogProperties(table);
    Schema schema = HiveSchemaUtil.convert(cols, Collections.emptyMap(), true);
    Map<String, String> envCtxProps = Optional.ofNullable(request.getEnvContext())
        .map(EnvironmentContext::getProperties)
        .orElse(Collections.emptyMap());
    org.apache.iceberg.PartitionSpec partitionSpec =
        HMSTablePropertyHelper.getPartitionSpec(envCtxProps, schema);
    SortOrder sortOrder = HMSTablePropertyHelper.getSortOrder(tableProperties, schema);

    restCatalog.buildTable(TableIdentifier.of(table.getDbName(), table.getTableName()), schema)
        .withPartitionSpec(partitionSpec)
        .withLocation(tableProperties.getProperty(CatalogUtils.LOCATION))
        .withSortOrder(sortOrder)
        .withProperties(Maps.fromProperties(tableProperties))
        .create();
  }

  @Override
  public void createDatabase(Database db) {
    validateCurrentCatalog(db.getCatalogName());
    Map<String, String> props = ImmutableMap.of(
        CatalogUtils.LOCATION, db.getLocationUri(),
        DB_OWNER, db.getOwnerName(),
        DB_OWNER_TYPE, db.getOwnerType().toString()
    );
    restCatalog.createNamespace(Namespace.of(db.getName()), props);
  }


  @Override
  public void dropDatabase(String name) {
    DropDatabaseRequest req = new DropDatabaseRequest();
    req.setName(name);
    req.setCatalogName(restCatalog.name());
    req.setDeleteData(true);
    req.setIgnoreUnknownDb(false);
    req.setCascade(false);
    dropDatabase(req);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb) {
    DropDatabaseRequest req = new DropDatabaseRequest();
    req.setName(name);
    req.setCatalogName(restCatalog.name());
    req.setDeleteData(deleteData);
    req.setIgnoreUnknownDb(ignoreUnknownDb);
    req.setCascade(false);
    dropDatabase(req);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) {
    DropDatabaseRequest req = new DropDatabaseRequest();
    req.setName(name);
    req.setCatalogName(restCatalog.name());
    req.setDeleteData(deleteData);
    req.setIgnoreUnknownDb(ignoreUnknownDb);
    req.setCascade(cascade);
    dropDatabase(req);
  }

  @Override
  public void dropDatabase(DropDatabaseRequest req) {
    validateCurrentCatalog(req.getCatalogName());
    Namespace ns = Namespace.of(req.getName());
    purgeChildNamespacesUnder(ns);
    restCatalog.dropNamespace(ns);
  }

  /**
   * Removes nested namespaces under {@code parent} (orphan directories in a HadoopCatalog
   * warehouse), dropping any tables inside them.
   */
  private void purgeChildNamespacesUnder(Namespace parent) {
    if (!(restCatalog instanceof SupportsNamespaces)) {
      return;
    }
    for (Namespace child : restCatalog.listNamespaces(parent)) {
      dropNamespaceCascade(child);
      restCatalog.dropNamespace(child);
    }
  }

  /** Drops all tables in {@code ns} and recursively removes nested namespaces and their tables. */
  private void dropNamespaceCascade(Namespace ns) {
    purgeChildNamespacesUnder(ns);
    for (TableIdentifier tid : restCatalog.listTables(ns)) {
      restCatalog.dropTable(tid, true);
    }
  }
}
