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

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Tests that verify {@link HMSCatalogServer} (standalone mode) produces the same REST API
 * behavior as the embedded {@link HMSCatalogFactory} mode.
 *
 * <p>This test starts:
 * <ol>
 *   <li>An embedded HMS (via MetaStoreTestUtils) which includes the embedded REST catalog
 *       served by {@link HMSCatalogFactory}</li>
 *   <li>A standalone {@link HMSCatalogServer} that connects to the same embedded HMS via Thrift</li>
 * </ol>
 * </p>
 */
@SuppressWarnings("unchecked")
public class TestHMSCatalogServer extends HMSTestBase {

  private HMSCatalogServer standaloneServer;
  private int standalonePort;

  @Override
  protected void setCatalogClass(Configuration conf) {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_AUTH, "jwt");
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Configuration standaloneConf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(standaloneConf, MetastoreConf.ConfVars.THRIFT_URIS,
        "thrift://localhost:" + port);

    MetastoreConf.setLongVar(standaloneConf, MetastoreConf.ConfVars.CATALOG_SERVLET_PORT, 0);
    MetastoreConf.setVar(standaloneConf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_PATH,
        catalogPath);

    MetastoreConf.setVar(standaloneConf, MetastoreConf.ConfVars.CATALOG_SERVLET_AUTH, "jwt");
    MetastoreConf.setVar(standaloneConf,
        MetastoreConf.ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
        "http://localhost:" + MOCK_JWKS_SERVER_PORT + "/jwks");

    MetastoreConf.setVar(standaloneConf, MetastoreConf.ConfVars.WAREHOUSE,
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.WAREHOUSE));
    MetastoreConf.setVar(standaloneConf, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL,
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL));

    MetastoreConf.setBoolVar(standaloneConf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setBoolVar(standaloneConf, MetastoreConf.ConfVars.HIVE_IN_TEST_ICEBERG, true);

    standaloneServer = new HMSCatalogServer(standaloneConf);
    standaloneServer.start();
    standalonePort = standaloneServer.getServer().getURI().getPort();
    System.out.println("Standalone HMSCatalogServer started on port " + standalonePort);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (standaloneServer != null) {
        standaloneServer.stop();
        standaloneServer = null;
      }
    } finally {
      super.tearDown();
    }
  }

  private Schema getTestSchema() {
    return new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get()));
  }

  private URL embeddedUrl(String apiPath) throws Exception {
    return new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath + apiPath);
  }

  private URL standaloneUrl(String apiPath) throws Exception {
    return new URL("http://hive@localhost:" + standalonePort + "/" + catalogPath + apiPath);
  }

  @Test
  public void testListNamespacesEquivalence() throws Exception {
    String jwt = generateJWT();

    Object embeddedResponse = clientCall(jwt, embeddedUrl("/v1/namespaces"), "GET", null);
    Object standaloneResponse = clientCall(jwt, standaloneUrl("/v1/namespaces"), "GET", null);

    Assert.assertTrue("Embedded response should be a Map", embeddedResponse instanceof Map);
    Assert.assertTrue("Standalone response should be a Map", standaloneResponse instanceof Map);

    Map<String, Object> embeddedMap = (Map<String, Object>) embeddedResponse;
    Map<String, Object> standaloneMap = (Map<String, Object>) standaloneResponse;

    // Both should return HTTP 200
    Assert.assertEquals("Status codes should match",
        embeddedMap.get("status"), standaloneMap.get("status"));

    // Both should list the same namespaces
    List<Object> embeddedNs = (List<Object>) embeddedMap.get("namespaces");
    List<Object> standaloneNs = (List<Object>) standaloneMap.get("namespaces");
    Assert.assertNotNull("Embedded namespaces should not be null", embeddedNs);
    Assert.assertNotNull("Standalone namespaces should not be null", standaloneNs);
    Assert.assertEquals("Standalone should return same number of namespaces as embedded",
        embeddedNs.size(), standaloneNs.size());
    Assert.assertTrue("Standalone should contain all embedded namespaces",
        standaloneNs.containsAll(embeddedNs));
  }

  @Test
  public void testListTablesEquivalence() throws Exception {
    Schema schema = getTestSchema();
    String tblName = "equiv_tbl_" + Integer.toHexString(RND.nextInt(65536));
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, tblName);

    try {
      catalog.buildTable(tableIdent, schema).create();

      String jwt = generateJWT();
      String apiPath = "/v1/namespaces/" + DB_NAME + "/tables";

      Object embeddedResponse = clientCall(jwt, embeddedUrl(apiPath), "GET", null);
      Object standaloneResponse = clientCall(jwt, standaloneUrl(apiPath), "GET", null);

      Assert.assertTrue("Embedded response should be a Map", embeddedResponse instanceof Map);
      Assert.assertTrue("Standalone response should be a Map", standaloneResponse instanceof Map);

      Map<String, Object> embeddedMap = (Map<String, Object>) embeddedResponse;
      Map<String, Object> standaloneMap = (Map<String, Object>) standaloneResponse;

      Assert.assertEquals("Status codes should match",
          embeddedMap.get("status"), standaloneMap.get("status"));

      List<Object> embeddedTables = (List<Object>) embeddedMap.get("identifiers");
      List<Object> standaloneTables = (List<Object>) standaloneMap.get("identifiers");
      Assert.assertNotNull("Embedded tables should not be null", embeddedTables);
      Assert.assertNotNull("Standalone tables should not be null", standaloneTables);
      Assert.assertEquals("Standalone should return same number of tables as embedded",
          embeddedTables.size(), standaloneTables.size());
      Assert.assertTrue("Standalone should contain all embedded tables",
          standaloneTables.containsAll(embeddedTables));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  /**
   * Verifies that load-table returns the same response from both servers.
   * Without Ranger, both return 404 "Table is not accessible"
   */
  @Test
  public void testLoadTableEquivalence() throws Exception {
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    String tblName = "equiv_load_" + Integer.toHexString(RND.nextInt(65536));
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, tblName);

    try {
      catalog.buildTable(tableIdent, schema)
          .withPartitionSpec(spec)
          .withProperty("key1", "value1")
          .create();

      String jwt = generateJWT();
      String apiPath = "/v1/namespaces/" + DB_NAME + "/tables/" + tblName;

      Object embeddedResponse = clientCall(jwt, embeddedUrl(apiPath), "GET", null);
      Object standaloneResponse = clientCall(jwt, standaloneUrl(apiPath), "GET", null);

      Assert.assertTrue("Embedded response should be a Map", embeddedResponse instanceof Map);
      Assert.assertTrue("Standalone response should be a Map", standaloneResponse instanceof Map);

      Map<String, Object> embeddedMap = (Map<String, Object>) embeddedResponse;
      Map<String, Object> standaloneMap = (Map<String, Object>) standaloneResponse;

      // Both servers should return the same status code.
      // Without Ranger, accessTable() returns 404 for both
      Assert.assertEquals("Status codes should match (both 404 without Ranger)",
          embeddedMap.get("status"), standaloneMap.get("status"));

      // Both should have the same error structure
      Assert.assertEquals("Error responses should match",
          embeddedMap.containsKey("error"), standaloneMap.containsKey("error"));

      // If both return 200 (Ranger available), compare metadata
      if (Integer.valueOf(200).equals(embeddedMap.get("status"))) {
        Assert.assertEquals("metadata-location should match",
            embeddedMap.get("metadata-location"), standaloneMap.get("metadata-location"));
      }
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testStandaloneListNamespaces() throws Exception {
    String jwt = generateJWT();

    Object response = clientCall(jwt, standaloneUrl("/v1/namespaces"), "GET", null);
    Assert.assertTrue("Response should be a Map", response instanceof Map);

    Map<String, Object> map = (Map<String, Object>) response;
    Assert.assertEquals("Should return 200", 200, map.get("status"));

    List<Object> namespaces = (List<Object>) map.get("namespaces");
    Assert.assertNotNull("Namespaces should not be null", namespaces);
    // Should contain default and hivedb (created by HMSTestBase.setUp)
    Assert.assertTrue("Should contain 'default' namespace",
        namespaces.contains(Arrays.asList("default")));
    Assert.assertTrue("Should contain '" + DB_NAME + "' namespace",
        namespaces.contains(Arrays.asList(DB_NAME)));
  }

  @Test
  public void testStandaloneListTables() throws Exception {
    Schema schema = getTestSchema();
    String tblName = "standalone_tbl_" + Integer.toHexString(RND.nextInt(65536));
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, tblName);

    try {
      // Create table via the direct catalog (backed by same HMS)
      catalog.buildTable(tableIdent, schema)
          .withProperty("key1", "value1")
          .create();

      String jwt = generateJWT();

      // List tables via standalone server - should find the table
      Object response = clientCall(jwt,
          standaloneUrl("/v1/namespaces/" + DB_NAME + "/tables"), "GET", null);
      Assert.assertTrue("Response should be a Map", response instanceof Map);

      Map<String, Object> map = (Map<String, Object>) response;
      Assert.assertEquals("Should return 200", 200, map.get("status"));

      List<?> identifiers = (List<?>) map.get("identifiers");
      Assert.assertNotNull("Identifiers should not be null", identifiers);
      Assert.assertFalse("Table list should not be empty", identifiers.isEmpty());
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testStandaloneCreateNamespace() throws Exception {
    String ns = "standalone_ns_" + Integer.toHexString(RND.nextInt(65536));

    try {
      Map<String, String> props = new HashMap<>();
      props.put("owner", "test_owner");
      props.put("group", "test_group");
      nsCatalog.createNamespace(Namespace.of(ns), props);

      String jwt = generateJWT();

      Object standaloneResponse = clientCall(jwt, standaloneUrl("/v1/namespaces"), "GET", null);
      Assert.assertTrue("Response should be a Map", standaloneResponse instanceof Map);
      Map<String, Object> standaloneMap = (Map<String, Object>) standaloneResponse;
      Assert.assertEquals("Should return 200", 200, standaloneMap.get("status"));
      List<Object> standaloneNs = (List<Object>) standaloneMap.get("namespaces");
      Assert.assertTrue("Namespace should be visible via standalone",
          standaloneNs.contains(Arrays.asList(ns)));

      Object embeddedResponse = clientCall(jwt, embeddedUrl("/v1/namespaces"), "GET", null);
      Assert.assertTrue("Embedded response should be a Map", embeddedResponse instanceof Map);
      Map<String, Object> embeddedMap = (Map<String, Object>) embeddedResponse;
      List<Object> embeddedNs = (List<Object>) embeddedMap.get("namespaces");
      Assert.assertTrue("Namespace should be visible via embedded",
          embeddedNs.contains(Arrays.asList(ns)));
    } finally {
      try {
        nsCatalog.dropNamespace(Namespace.of(ns));
      } catch (Exception e) {
        LOG.warn("Failed to cleanup namespace {}: {}", ns, e.getMessage());
      }
    }
  }

  /**
   * Verifies the standalone server handles drop table the same as embedded.
   */
  @Test
  public void testStandaloneDropTable() throws Exception {
    Schema schema = getTestSchema();
    String tblName = "standalone_drop_" + Integer.toHexString(RND.nextInt(65536));
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, tblName);

    // Create table via direct catalog
    catalog.buildTable(tableIdent, schema).create();

    String jwt = generateJWT();
    String listApiPath = "/v1/namespaces/" + DB_NAME + "/tables";

    Object response = clientCall(jwt, standaloneUrl(listApiPath), "GET", null);
    Assert.assertTrue("Response should be a Map", response instanceof Map);
    Map<String, Object> map = (Map<String, Object>) response;
    Assert.assertEquals("Should return 200", 200, map.get("status"));

    response = clientCall(jwt,
        standaloneUrl("/v1/namespaces/" + DB_NAME + "/tables/" + tblName), "DELETE", null);
    Assert.assertNotNull("Delete response should not be null", response);
    if (response instanceof Map) {
      Object deleteStatus = ((Map<?, ?>) response).get("status");
      if (deleteStatus instanceof Number) {
        int statusCode = ((Number) deleteStatus).intValue();
        Assert.assertNotEquals("Delete should not be forbidden", 403, statusCode);
      }
    }

    response = clientCall(jwt, embeddedUrl(listApiPath), "GET", null);
    Assert.assertTrue("Response should be a Map", response instanceof Map);
    map = (Map<String, Object>) response;
    List<?> identifiers = (List<?>) map.get("identifiers");
    if (identifiers != null) {
      for (Object id : identifiers) {
        if (id instanceof Map) {
          Map<?, ?> idMap = (Map<?, ?>) id;
          Assert.assertNotEquals("Dropped table should not appear in list",
              tblName, idMap.get("name"));
        }
      }
    }
  }

  @Test
  public void testMultipleRequestsOnStandalone() throws Exception {
    String jwt = generateJWT();
    URL url = standaloneUrl("/v1/namespaces");

    // Make multiple requests to verify server stability
    for (int i = 0; i < 10; i++) {
      Object response = clientCall(jwt, url, "GET", null);
      Assert.assertNotNull("Request " + i + " should succeed", response);
      Assert.assertTrue("Response " + i + " should be a Map", response instanceof Map);
      Assert.assertEquals("Request " + i + " should return 200",
          200, ((Map<?, ?>) response).get("status"));
    }
  }

  /**
   * Verifies that creating a table via the direct catalog API makes it visible
   * through both the embedded and standalone REST endpoints via list-tables.
   */
  @Test
  public void testTableVisibleOnBothServers() throws Exception {
    Schema schema = getTestSchema();
    String tblName = "visible_tbl_" + Integer.toHexString(RND.nextInt(65536));
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, tblName);

    try {
      catalog.buildTable(tableIdent, schema).create();

      String jwt = generateJWT();
      String apiPath = "/v1/namespaces/" + DB_NAME + "/tables";

      Object embeddedResponse = clientCall(jwt, embeddedUrl(apiPath), "GET", null);
      Assert.assertTrue("Embedded response should be a Map, got: " + embeddedResponse.getClass().getName(),
          embeddedResponse instanceof Map);

      Object standaloneResponse = clientCall(jwt, standaloneUrl(apiPath), "GET", null);
      Assert.assertTrue("Standalone response should be a Map, got: " + standaloneResponse.getClass().getName(),
          standaloneResponse instanceof Map);

      Map<String, Object> embeddedMap = (Map<String, Object>) embeddedResponse;
      Map<String, Object> standaloneMap = (Map<String, Object>) standaloneResponse;

      // Both should return 200
      Assert.assertEquals("Embedded should return 200", 200, embeddedMap.get("status"));
      Assert.assertEquals("Standalone should return 200", 200, standaloneMap.get("status"));

      // Both should list the table
      List<Object> embeddedTables = (List<Object>) embeddedMap.get("identifiers");
      List<Object> standaloneTables = (List<Object>) standaloneMap.get("identifiers");

      Assert.assertNotNull("Embedded tables should not be null", embeddedTables);
      Assert.assertNotNull("Standalone tables should not be null", standaloneTables);
      Assert.assertFalse("Embedded should have tables", embeddedTables.isEmpty());
      Assert.assertFalse("Standalone should have tables", standaloneTables.isEmpty());

      // The table lists should be identical
      Assert.assertEquals("Table counts should match",
          embeddedTables.size(), standaloneTables.size());
      Assert.assertTrue("Table lists should match",
          standaloneTables.containsAll(embeddedTables));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }
}
