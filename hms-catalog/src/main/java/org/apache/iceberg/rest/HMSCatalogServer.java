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

import java.util.Map;
import java.util.TreeMap;
import javax.servlet.http.HttpServlet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSServletSecurity;
import org.apache.hadoop.hive.metastore.ServletServerBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.metering.MeteringHealthWatcher;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone REST Catalog server that connects to a remote Hive Metastore via Thrift.
 */
public class HMSCatalogServer {
  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalogServer.class);
  private static final String CACHE_AUTHORIZATION = "hive.metastore.catalog.cache.authorization";

  private final Configuration configuration;
  private final int port;
  private final String path;
  private Server server;
  private static volatile DataSharing dataSharing;
  private MeteringHealthWatcher meteringWatcher;

  /**
   * Constructs the server, reading port and path from configuration.
   *
   * @param conf the Hadoop/Metastore configuration
   */
  public HMSCatalogServer(Configuration conf) {
    this.configuration = conf;
    this.port = MetastoreConf.getIntVar(conf, ConfVars.CATALOG_SERVLET_PORT);
    this.path = MetastoreConf.getVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH);
  }

  /**
   * Creates the Iceberg catalog.
   * @return the catalog instance, optionally wrapped in {@link HMSCachingCatalog}
   */
  private Catalog createCatalog() {
    Map<String, String> properties = new TreeMap<>();
    // NOTE: Unlike HMSCatalogFactory, we do NOT clear THRIFT_URIS.
    final String configUri = MetastoreConf.getVar(configuration, ConfVars.THRIFT_URIS);
    if (configUri != null) {
      properties.put("uri", configUri);
    }
    final String configWarehouse = MetastoreConf.getVar(configuration, ConfVars.WAREHOUSE);
    if (configWarehouse != null) {
      properties.put("warehouse", configWarehouse);
    }
    final String configExtWarehouse = MetastoreConf.getVar(configuration, ConfVars.WAREHOUSE_EXTERNAL);
    if (configExtWarehouse != null) {
      properties.put("external-warehouse", configExtWarehouse);
    }
    final HiveCatalog hiveCatalog = new HiveCatalog();
    hiveCatalog.setConf(configuration);
    final String catalogName = MetastoreConf.getVar(configuration, ConfVars.CATALOG_DEFAULT);
    hiveCatalog.initialize(catalogName, properties);
    long expiry = MetastoreConf.getLongVar(configuration, ConfVars.ICEBERG_CATALOG_CACHE_EXPIRY);
    boolean cacheAuthorization = configuration.getBoolean(CACHE_AUTHORIZATION, true);
    if (expiry <= 0) {
      return hiveCatalog;
    }
    boolean caseSensitive = true;
    return new HMSCachingCatalog(hiveCatalog, expiry, cacheAuthorization, caseSensitive, configuration);
  }

  /**
   * Creates the REST catalog servlet with security wrapper.
   *
   * @param catalog the Iceberg catalog
   * @return the secured servlet
   */
  private HttpServlet createServlet(Catalog catalog) {
    return wrapWithSecurity(new HMSCatalogServlet(new HMSCatalogAdapter(catalog, configuration), meteringWatcher));
  }

  private HttpServlet wrapWithSecurity(HttpServlet inner) {
    String authType = MetastoreConf.getVar(configuration, ConfVars.CATALOG_SERVLET_AUTH);
    boolean jwt = "jwt".equalsIgnoreCase(authType);
    HMSServletSecurity security = new HMSServletSecurity(configuration, jwt);
    HttpServlet proxied = security.proxy(inner);
    return proxied != null ? proxied : inner;
  }

  /**
   * Starts the Jetty server with the REST catalog servlet.
   *
   * @throws Exception if the server fails to start
   */
  public void start() throws Exception {
    meteringWatcher = new MeteringHealthWatcher(configuration);
    meteringWatcher.start();
    Catalog catalog = createCatalog();
    HttpServlet servlet = createServlet(catalog);
    ServletServerBuilder builder = new ServletServerBuilder(configuration);
    builder.addServlet(port, path, servlet);
    this.server = builder.start(LOG);
    if (this.server == null) {
      throw new IllegalStateException("Failed to start HMSCatalogServer");
    }
    LOG.info("HMSCatalogServer started on port {} with path {}", port, path);
  }

  /**
   * Stops the Jetty server.
   *
   * @throws Exception if the server fails to stop
   */
  public void stop() throws Exception {
    if (meteringWatcher != null) {
      meteringWatcher.stop();
    }
    if (server != null) {
      server.stop();
      LOG.info("HMSCatalogServer stopped");
    }
  }

  /**
   * Returns the underlying Jetty server (for join/testing).
   *
   * @return the server, or null if not started
   */
  public Server getServer() {
    return server;
  }

  static DataSharing getDataSharing() {
    return dataSharing;
  }

  private static boolean isDataSharingEnabled(Configuration conf) {
    String idBrokerUrl = conf.get(DataSharing.CONFVAR_IDBROKER_URL, "");
    return !idBrokerUrl.trim().isEmpty();
  }

  /**
   * Initializes DataSharing if IDBroker is configured.
   * @param conf the configuration
   */
  private static synchronized DataSharing initializeDataSharing(Configuration conf) {
    if (dataSharing == null && isDataSharingEnabled(conf)) {
      try {
        dataSharing = DataSharing.create(conf);
        LOG.info("Data sharing initialized successfully");
      } catch (Exception e) {
        LOG.warn("Failed to initialize data sharing, continuing without it: {}", e.getMessage());
      }
    }
    return dataSharing;
  }

  /**
   * Main entry point for the standalone HMSCatalogServer.
   * <p>Kerberos login uses the same config properties as embedded HMS:
   * {@code hive.metastore.kerberos.principal} and {@code hive.metastore.kerberos.keytab.file}.
   * When security is not enabled, defaults the login user to "hive".</p>
   *
   * @param args command-line arguments: [--port port]
   * @throws Exception if startup fails
   */
  public static void main(String[] args) throws Exception {
    // 1. Load configuration (same sources as embedded HMS)
    Configuration conf = MetastoreConf.newMetastoreConf();
    conf.addResource("hms-catalog-site.xml");

    // 2. Parse CLI arguments
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--port":
          if (i + 1 < args.length) {
            int cliPort = Integer.parseInt(args[++i]);
            MetastoreConf.setLongVar(conf, ConfVars.CATALOG_SERVLET_PORT, cliPort);
          }
          break;
        default:
          LOG.warn("Unknown argument: {}", args[i]);
          break;
      }
    }

    // 3. Kerberos login - same logic as HiveMetaStore (HiveMetaStore.java:11692-11701)
    if (UserGroupInformation.isSecurityEnabled()) {
      String kerberosName = SecurityUtil.getServerPrincipal(
          MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL), "0.0.0.0");
      String keyTabFile = MetastoreConf.getVar(conf, ConfVars.KERBEROS_KEYTAB_FILE);
      LOG.info("Logging in via keytab as {}", kerberosName);
      UserGroupInformation.loginUserFromKeytab(kerberosName, keyTabFile);
    } else {
      LOG.info("Security is not enabled, defaulting login user to 'hive'");
      UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hive"));
    }

    // 4. Initialize DataSharing (Ozone lazily initializes on first table load via OzoneLinkBucketManager) and inject into HMSCatalogFactory
    initializeDataSharing(conf);

    // 5. Create and start server
    HMSCatalogServer catalogServer = new HMSCatalogServer(conf);

    // 6. Add shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        LOG.info("Shutdown hook triggered, stopping HMSCatalogServer...");
        catalogServer.stop();
      } catch (Exception e) {
        LOG.error("Error stopping HMSCatalogServer", e);
      }
    }));

    catalogServer.start();

    // 7. Block main thread until server is stopped
    catalogServer.getServer().join();
  }
}
