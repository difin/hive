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
import org.apache.hadoop.hive.metastore.HMSServletSecurity;
import org.apache.hadoop.hive.metastore.ServletServerBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.TreeMap;
/**
 * Catalog &amp; servlet factory.
 */
public class HMSCatalogFactory {
  private static final String CACHE_AUTHORIZATION = "hive.metastore.catalog.cache.authorization";
  private static final String CATALOG_SERVLET_PATH_KEY = "hive.metastore.catalog.servlet.path";
  private static final String CATALOG_CACHE_EXPIRY_KEY = "hive.metastore.catalog.cache.expiry";

  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalogFactory.class);
  /**
   * Convenience soft reference to last catalog.
   */
  private static final String SERVLET_ID_KEY = "metastore.in.test.iceberg.catalog.servlet.id";
  private static volatile DataSharing dataSharing;
  private static volatile SoftReference<Catalog> restCatalog;

  private final Configuration configuration;
  private final int port;
  private final String path;

  /**
   * Factory constructor.
   * <p>Called by the static method {@link HMSCatalogFactory#createServlet(Configuration)} that is
   * declared in configuration and found through introspection.</p>
   * @param conf the configuration
   */
  private HMSCatalogFactory(Configuration conf) {
    port = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_PORT);
    // Use raw key for servlet path to avoid dependency on newer ConfVars
    String configuredPath = conf.get(CATALOG_SERVLET_PATH_KEY);
    if (configuredPath == null || configuredPath.isEmpty()) {
      // Fallback to the generic CATALOG_SERVLET_PATH ConfVar if present
      configuredPath = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_PATH);
    }
    path = configuredPath;
    this.configuration = conf;
  }

  public int getPort() {
    return port;
  }

  public String getPath() {
    return path;
  }

  private static boolean isDataSharingEnabled(Configuration conf) {
    String idBrokerUrl = conf.get(DataSharing.CONFVAR_IDBROKER_URL, "");
    return !idBrokerUrl.trim().isEmpty();
  }

  static DataSharing getDataSharing() {
    return dataSharing;
  }

  static void setDataSharing(DataSharing dataSharing) {
    HMSCatalogFactory.dataSharing = dataSharing;
  }

  private static synchronized void initializeDataSharing(Configuration conf) {
    if (dataSharing == null && isDataSharingEnabled(conf)) {
      try {
        dataSharing = DataSharing.create(conf);
        LOG.info("Data sharing initialized successfully");
      } catch (Exception e) {
        LOG.warn("Failed to initialize data sharing, continuing without it: {}", e.getMessage());
        // leave dataSharing null to indicate unavailable
      }
    }
  }

  /**
   * Creates the catalog instance.
   * @return the catalog
   */
  private Catalog createCatalog() {
    Map<String, String> properties = new TreeMap<>();
    MetastoreConf.setVar(configuration, MetastoreConf.ConfVars.THRIFT_URIS, "");
    final String configUri = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.THRIFT_URIS);
    if (configUri != null) {
      properties.put("uri", configUri);
    }
    final String configWarehouse = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.WAREHOUSE);
    if (configWarehouse != null) {
      properties.put("warehouse", configWarehouse);
    }
    final String configExtWarehouse = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL);
    if (configExtWarehouse != null) {
      properties.put("external-warehouse", configExtWarehouse);
    }
    if (configuration.get(SERVLET_ID_KEY) != null) {
      // For the testing purpose. HiveCatalog caches a metastore client in a static field. As our tests can spin up
      // multiple HMS instances, we need a unique cache key.
      properties.put(CatalogProperties.CLIENT_POOL_CACHE_KEYS, String.format("conf:%s", SERVLET_ID_KEY));
    }
    final HiveCatalog hiveCatalog = new org.apache.iceberg.hive.HiveCatalog();
    hiveCatalog.setConf(configuration);
    final String catalogName = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.CATALOG_DEFAULT);
    hiveCatalog.initialize(catalogName, properties);
    long expiry = configuration.getLong(CATALOG_CACHE_EXPIRY_KEY, 60_000L);
    boolean cacheAuthorization = configuration.getBoolean(CACHE_AUTHORIZATION, true);
    if (expiry <= 0) {
      return hiveCatalog; // no catalog cache, no auth cache
    }
    boolean caseSensitive = true;
    return new HMSCachingCatalog(hiveCatalog, expiry, cacheAuthorization, caseSensitive, configuration);
  }

  /**
   * Creates the REST catalog servlet instance.
   * @param catalog the Iceberg catalog
   * @return the servlet
   */
  private HttpServlet createServlet(Catalog catalog) {
    String authType = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.CATALOG_SERVLET_AUTH);
    boolean jwt = "jwt".equalsIgnoreCase(authType);

    try {
      // Construct the adapter with configuration (backport detail)
      HMSCatalogAdapter adapter = new HMSCatalogAdapter(catalog, configuration);
      HMSCatalogServlet servlet = new HMSCatalogServlet(adapter);

      // HMSServletSecurity extends ServletSecurity and provides proxy() to wrap the servlet
      // so that all requests go through auth/UGI handling.
      HMSServletSecurity security = new HMSServletSecurity(configuration, jwt);
      HttpServlet proxied = security.proxy(servlet);
      return proxied != null ? proxied : servlet;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create HMSCatalogServlet", e);
    }
  }

  /**
   * Creates the REST catalog servlet instance.
   * @return the servlet
   */
  private HttpServlet createServlet() {
    if (port >= 0 && path != null && !path.isEmpty()) {
      Catalog catalog = createCatalog();
      restCatalog = new SoftReference<>(catalog);
      return createServlet(catalog);
    }
    return null;
  }

  /**
   * Gets the REST catalog instance.
   * This is primarily used for testing purposes.
   * @return the catalog or null if not initialized
   */
  public static Catalog getRESTCatalog() {
    return restCatalog.get();
  }


  /**
   * Factory method to describe Iceberg servlet.
   * <p>This method name is found through configuration as {@link MetastoreConf.ConfVars#CATALOG_SERVLET_FACTORY}
   * and looked up through reflection to start from HMS.</p>
   *
   * @param configuration the configuration
   * @return the servlet descriptor instance
   */
  @SuppressWarnings("unused")
  public static ServletServerBuilder.Descriptor createServlet(Configuration configuration) {
    initializeDataSharing(configuration);
    HMSCatalogFactory hms = new HMSCatalogFactory(configuration);
    HttpServlet servlet = hms.createServlet();
    if (servlet != null) {
      return new ServletServerBuilder.Descriptor(hms.getPort(), hms.getPath(), servlet);
    }
    return null;
  }
}