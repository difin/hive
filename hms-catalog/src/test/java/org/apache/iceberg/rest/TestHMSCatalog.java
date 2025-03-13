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

package org.apache.iceberg.rest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;

public class TestHMSCatalog extends TestHiveCatalog {
  public TestHMSCatalog() {
    super();
  }
  protected void setCatalogClass(Configuration conf) {
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_CATALOG_ACTOR_CLASS, "org.apache.iceberg.rest.HMSCatalogActor");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_AUTH, "jwt");
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testRegisterTableHttp() throws Exception {
    HadoopTables hadoopTables = new HadoopTables(this.conf);
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    File path = temp.newFolder("tbl");
    Table table = hadoopTables.buildTable(path.toString(), schema)
        .withPartitionSpec(spec)
        .withProperty("key1", "value1")
        .withProperty("key2", "value2")
        .create();
    Assert.assertFalse(catalog.tableExists(tableIdent));
    String location = java.nio.file.Paths.get(path.toString(), "metadata", "v1.metadata.json").toString();

    try {
      Table registered = catalog.registerTable(tableIdent, location);
      Assert.assertEquals(table.location(), registered.location());
      Assert.assertEquals("value1", table.properties().get("key1"));
      Assert.assertEquals("value2", table.properties().get("key2"));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateTableWithCaching() throws Exception {
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.newFolder("tbl").toString();
    ImmutableMap<String, String> properties = ImmutableMap.of("key1", "value1", "key2", "value2");
    Catalog cachingCatalog = CachingCatalog.wrap(catalog);

    try {
      Table table = cachingCatalog.createTable(tableIdent, schema, spec, location, properties);

      Assert.assertEquals(location, table.location());
      Assert.assertEquals(2, table.schema().columns().size());
      Assert.assertEquals(1, table.spec().fields().size());
      Assert.assertEquals("value1", table.properties().get("key1"));
      Assert.assertEquals("value2", table.properties().get("key2"));
    } finally {
      cachingCatalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testInitialize() {
    Assertions.assertDoesNotThrow(() -> {
      HMSCatalog catalog = new HMSCatalog(conf);
      catalog.initialize("hive", Maps.newHashMap());
    });
  }

  @Test
  public void testToStringWithoutSetConf() {
    Assertions.assertDoesNotThrow(() -> {
      HMSCatalog catalog = new HMSCatalog(conf);
      catalog.toString();
    });
  }

  @Test
  public void testInitializeCatalogWithProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("uri", "thrift://examplehost:9083");
    properties.put("warehouse", "/user/hive/testwarehouse");
    HiveCatalog catalog = new HiveCatalog();
    catalog.initialize("hive", properties);

    Assert.assertEquals(catalog.getConf().get("hive.metastore.uris"), "thrift://examplehost:9083");
    Assert.assertEquals(catalog.getConf().get("hive.metastore.warehouse.dir"), "/user/hive/testwarehouse");
  }

  @Test
  public void testCreateNamespaceHttp() throws Exception {
    super.testCreateNamespaceHttp();
  }

  @Test
  public void testUrlResolve() {
    String brokerUrlStr = "https://localhost:8444/gateway/";
    URI brokerUri = URI.create(brokerUrlStr);
    URI fetchToken = brokerUri
        .resolve("aws-cab/cab/api/v1/credentials")
        .resolve("role/datalake-admin-role?path="+ DataSharing.urlEncodeUTF8("s3a://bucket/partition/table0"));
    String fetchStr = fetchToken.toString();
    Assert.assertNotNull(fetchStr);
    }
   }

  public void testSetSnapshotSummary() throws Exception {
    Configuration conf = new Configuration();
    conf.addResource(this.conf);
    conf.set("iceberg.hive.table-property-max-size", "4000");
    RawStore store = HMSCatalog.getHandler(conf).getMS();
    HMSTableOperations ops = new HMSTableOperations(conf, store, null, catalog.name(), DB_NAME, "tbl");
    Snapshot snapshot = mock(Snapshot.class);
    Map<String, String> summary = Maps.newHashMap();
    when(snapshot.summary()).thenReturn(summary);

    // create a snapshot summary whose json string size is less than the limit
    for (int i = 0; i < 100; i++) {
      summary.put(String.valueOf(i), "value");
    }
    Assert.assertTrue(JsonUtil.mapper().writeValueAsString(summary).length() < 4000);
    Map<String, String> parameters = Maps.newHashMap();
    ops.setSnapshotSummary(parameters, snapshot);
    Assert.assertEquals("The snapshot summary must be in parameters", 1, parameters.size());

    // create a snapshot summary whose json string size exceeds the limit
    for (int i = 0; i < 1000; i++) {
      summary.put(String.valueOf(i), "value");
    }
    long summarySize = JsonUtil.mapper().writeValueAsString(summary).length();
    // the limit has been updated to 4000 instead of the default value(32672)
    Assert.assertTrue(summarySize > 4000 && summarySize < 32672);
    parameters.remove(CURRENT_SNAPSHOT_SUMMARY);
    ops.setSnapshotSummary(parameters, snapshot);
    Assert.assertEquals("The snapshot summary must not be in parameters due to the size limit", 0, parameters.size());
  }

  @Test
  public void testNotExposeTableProperties() throws Exception {
    Configuration conf = new Configuration();
    conf.addResource(this.conf);
    conf.set("iceberg.hive.table-property-max-size", "0");
    RawStore store = HMSCatalog.getHandler(conf).getMS();
    HMSTableOperations ops = new HMSTableOperations(conf, store, null, catalog.name(), DB_NAME, "tbl");
    TableMetadata metadata = mock(TableMetadata.class);
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(CURRENT_SNAPSHOT_SUMMARY, "summary");
    parameters.put(CURRENT_SNAPSHOT_ID, "snapshotId");
    parameters.put(CURRENT_SNAPSHOT_TIMESTAMP, "timestamp");
    parameters.put(CURRENT_SCHEMA, "schema");
    parameters.put(DEFAULT_PARTITION_SPEC, "partitionSpec");
    parameters.put(DEFAULT_SORT_ORDER, "sortOrder");

    ops.setSnapshotStats(metadata, parameters);
    Assert.assertNull(parameters.get(CURRENT_SNAPSHOT_SUMMARY));
    Assert.assertNull(parameters.get(CURRENT_SNAPSHOT_ID));
    Assert.assertNull(parameters.get(CURRENT_SNAPSHOT_TIMESTAMP));

    ops.setSchema(metadata, parameters);
    Assert.assertNull(parameters.get(CURRENT_SCHEMA));

    ops.setPartitionSpec(metadata, parameters);
    Assert.assertNull(parameters.get(DEFAULT_PARTITION_SPEC));

    ops.setSortOrder(metadata, parameters);
    Assert.assertNull(parameters.get(DEFAULT_SORT_ORDER));
  }

  @Test
  public void testSetDefaultPartitionSpec() throws Exception {
    Schema schema = getTestSchema();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    try {
      Table table = catalog.buildTable(tableIdent, schema).create();
      Assert.assertFalse("Must not have default partition spec",
              hmsTableParameters().containsKey(TableProperties.DEFAULT_PARTITION_SPEC));

      table.updateSpec().addField(bucket("data", 16)).commit();
      Assert.assertEquals(PartitionSpecParser.toJson(table.spec()),
              hmsTableParameters().get(TableProperties.DEFAULT_PARTITION_SPEC));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testSetCurrentSchema() throws Exception {
    Schema schema = getTestSchema();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    try {
      Table table = catalog.buildTable(tableIdent, schema).create();

      Assert.assertEquals(SchemaParser.toJson(table.schema()), hmsTableParameters().get(CURRENT_SCHEMA));

      // add many new fields to make the schema json string exceed the limit
      UpdateSchema updateSchema = table.updateSchema();
      for (int i = 0; i < 600; i++) {
        updateSchema.addColumn("new_col_" + i, Types.StringType.get());
      }
      updateSchema.commit();

      Assert.assertTrue(SchemaParser.toJson(table.schema()).length() > 32672);
      Assert.assertNull(hmsTableParameters().get(CURRENT_SCHEMA));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  private Map<String, String> hmsTableParameters() throws TException {
    org.apache.hadoop.hive.metastore.api.Table hmsTable = metastoreClient.getTable(DB_NAME, "tbl");
    return hmsTable.getParameters();
  }

  @Test
  public void testConstructorWarehousePathWithEndSlash() {
    HiveCatalog catalogWithSlash = new HiveCatalog();
    String wareHousePath = "s3://bucket/db/tbl";

    catalogWithSlash.initialize(
        "hive_catalog", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, wareHousePath + "/"));
    Assert.assertEquals(
        "Should have trailing slash stripped",
        wareHousePath,
        catalogWithSlash.getConf().get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));
  }

  HMSCatalog newHMSCatalog(ImmutableMap<String, String> catalogProps) {
    HMSCatalog catalog = new HMSCatalog(conf);
    CatalogUtil.configureHadoopConf(catalog, conf);
    catalog.initialize(CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE, catalogProps);
    return catalog;
  }

  @Test
  public void testTablePropsDefinedAtCatalogLevel() {
    Schema schema = getTestSchema();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    ImmutableMap<String, String> catalogProps =
        ImmutableMap.of(
            "table-default.key1", "catalog-default-key1",
            "table-default.key2", "catalog-default-key2",
            "table-default.key3", "catalog-default-key3",
            "table-override.key3", "catalog-override-key3",
            "table-override.key4", "catalog-override-key4");
    Catalog hiveCatalog = newHMSCatalog(catalogProps);

    try {
      Table table =
          hiveCatalog
              .buildTable(tableIdent, schema)
              .withProperty("key2", "table-key2")
              .withProperty("key3", "table-key3")
              .withProperty("key5", "table-key5")
              .create();

      Assert.assertEquals(
          "Table defaults set for the catalog must be added to the table properties.",
          "catalog-default-key1",
          table.properties().get("key1"));
      Assert.assertEquals(
          "Table property must override table default properties set at catalog level.",
          "table-key2",
          table.properties().get("key2"));
      Assert.assertEquals(
          "Table property override set at catalog level must override table default" +
              " properties set at catalog level and table property specified.",
          "catalog-override-key3",
          table.properties().get("key3"));
      Assert.assertEquals(
          "Table override not in table props or defaults should be added to table properties",
          "catalog-override-key4",
          table.properties().get("key4"));
      Assert.assertEquals(
          "Table properties without any catalog level default or override should be added to table" +
                  " properties.",
          "table-key5",
          table.properties().get("key5"));
    } finally {
      hiveCatalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testDatabaseLocationWithSlashInWarehouseDir() {
    Configuration conf = new Configuration();
    // With a trailing slash
    conf.set("hive.metastore.warehouse.dir", "s3://bucket/");
    conf.set("hive.metastore.warehouse.external.dir", "s3://bucket/");

    HMSCatalog catalog = new HMSCatalog(conf);
    catalog.setConf(conf);

    Database database = catalog.convertToDatabase(Namespace.of("database"), ImmutableMap.of());

    Assert.assertEquals("s3://bucket/database.db", database.getLocationUri());
  }

  @Test
  public void testRegisterTable() {
    TableIdentifier identifier = TableIdentifier.of(DB_NAME, "t1");
    catalog.createTable(identifier, getTestSchema());
    Table registeringTable = catalog.loadTable(identifier);
    catalog.dropTable(identifier, false);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((BaseMetastoreTableOperations) ops).currentMetadataLocation();
    Table registeredTable = catalog.registerTable(identifier, metadataLocation);
    assertThat(registeredTable).isNotNull();
    //TestHelpers.assertSerializedAndLoadedMetadata(registeringTable, registeredTable);
    String expectedMetadataLocation =
        ((HasTableOperations) registeredTable).operations().current().metadataFileLocation();
    assertThat(metadataLocation).isEqualTo(expectedMetadataLocation);
    assertThat(catalog.loadTable(identifier)).isNotNull();
    assertThat(catalog.dropTable(identifier)).isTrue();
  }

  @Test
  public void testRegisterExistingTable() {
    TableIdentifier identifier = TableIdentifier.of(DB_NAME, "t1");
    catalog.createTable(identifier, getTestSchema());
    Table registeringTable = catalog.loadTable(identifier);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((BaseMetastoreTableOperations) ops).currentMetadataLocation();
    assertThatThrownBy(() -> catalog.registerTable(identifier, metadataLocation))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Table already exists: hivedb.t1");
    assertThat(catalog.dropTable(identifier, true)).isTrue();
  }
}
