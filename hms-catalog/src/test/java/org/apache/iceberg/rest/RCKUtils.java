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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.extension.RESTCatalogServer;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Utility class for REST Catalog testing.
 * This is a local implementation to avoid dependency on iceberg-open-api test-fixtures
 * which is not available in the CDH version.
 */
public final class RCKUtils {
  private static final String CATALOG_ENV_PREFIX = "CATALOG_";
  private static final List<String> TEST_NAMESPACES = Arrays.asList("ns", "newdb");

  private RCKUtils() {
    // Utility class, no instantiation
  }

  /**
   * Read catalog configuration from environment variables.
   * Variables prefixed with CATALOG_ are read, with naming conventions transformed:
   * - Double underscores become dashes
   * - Single underscores become dots
   */
  public static Map<String, String> environmentCatalogConfig() {
    return System.getenv().entrySet().stream()
        .filter(e -> e.getKey().startsWith(CATALOG_ENV_PREFIX))
        .collect(
            Collectors.toMap(
                e -> e.getKey()
                    .substring(CATALOG_ENV_PREFIX.length())
                    .replace("__", "-")
                    .replace("_", ".")
                    .toLowerCase(),
                Map.Entry::getValue,
                (v1, v2) -> v1,
                HashMap::new));
  }

  /**
   * Initialize a REST catalog client with the given configuration.
   */
  public static RESTCatalog initCatalogClient(Map<String, String> configuration) {
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.putAll(environmentCatalogConfig());
    for (String key : System.getProperties().stringPropertyNames()) {
      catalogConfig.put(key, System.getProperty(key));
    }
    catalogConfig.putAll(configuration);

    String port = catalogConfig.getOrDefault("rest.port", String.valueOf(RESTCatalogServer.REST_PORT));
    catalogConfig.putIfAbsent(CatalogProperties.URI, String.format("http://localhost:%s/", port));
    catalogConfig.putIfAbsent(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse/wh");
    catalogConfig.putIfAbsent(CatalogProperties.TABLE_DEFAULT_PREFIX + "write.format.default", "parquet");
    // Note: Views are not supported in this catalog version

    Map<String, String> additionalProperties =
      ImmutableMap.of(
        CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1",
        "catalog-default-key1",
        CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2",
        "catalog-default-key2",
        CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3",
        "catalog-default-key3",
        CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3",
        "catalog-override-key3",
        CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4",
        "catalog-override-key4");

    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize("rest_catalog",
      ImmutableMap.<String, String>builder()
        .putAll(catalogConfig)
        .putAll(additionalProperties)
        .build());
    return catalog;
  }

  /**
   * Purge test entries from the catalog.
   */
  public static void purgeCatalogTestEntries(Catalog catalog) {
    String purgeEnabled = System.getProperty("rck.purge-test-namespaces", "true");
    if (!Boolean.parseBoolean(purgeEnabled)) {
      return;
    }

    for (String ns : TEST_NAMESPACES) {
      Namespace namespace = Namespace.of(ns);
      try {
        // Drop all tables in the namespace
        for (TableIdentifier table : catalog.listTables(namespace)) {
          catalog.dropTable(table, true);
        }

        // Note: Views are not supported in this catalog version

        // Drop the namespace if catalog supports namespaces
        if (catalog instanceof SupportsNamespaces) {
          ((SupportsNamespaces) catalog).dropNamespace(namespace);
        }
      } catch (Exception e) {
        // Ignore exceptions when cleaning up test namespaces
      }
    }
  }

  /**
   * Find an available port for testing.
   */
  public static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }
}
