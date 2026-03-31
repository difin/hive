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

package org.apache.hive.iceberg.it;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import javax.servlet.http.HttpServlet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.JavaxIcebergRESTCatalogServlet;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Iceberg REST catalog test server: Jetty + {@link RESTCatalogAdapter} + {@link HadoopCatalog}. */
public class NativeIcebergRESTCatalogServer {

  /**
   * {@link HadoopCatalog} rejects namespace metadata, but {@link org.apache.iceberg.hive.client.HiveRESTCatalogClient}
   * sends LOCATION / owner fields when creating a database. Strip metadata so namespaces still create.
   */
  private static final class HadoopCatalogIgnoringNamespaceMetadata extends HadoopCatalog {
    HadoopCatalogIgnoringNamespaceMetadata(Configuration conf, String warehouseLocation) {
      super(conf, warehouseLocation);
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> metadata) {
      super.createNamespace(namespace, Collections.emptyMap());
    }
  }
  private static final Logger LOG = LoggerFactory.getLogger(NativeIcebergRESTCatalogServer.class);

  private Path warehouseDir;
  private int restPort = -1;
  private Server httpServer;
  private RESTCatalogAdapter catalogAdapter;
  private HadoopCatalog hadoopCatalog;

  /** No-op: kept for callers that passed a metastore schema class for the old HMS fixture. */
  @SuppressWarnings("unused")
  public void setSchemaInfoClass(Class<? extends MetaStoreSchemaInfo> schemaInfoClass) {
    // no-op
  }

  public void start(Configuration conf) throws Exception {
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    conf.set("metastore.in.test.iceberg.catalog.servlet.id", UUID.randomUUID().toString());

    String uniqueTestKey = String.format("NativeIcebergRESTCatalogServer_%s", UUID.randomUUID());
    warehouseDir = Paths.get(MetaStoreTestUtils.getTestWarehouseDir(uniqueTestKey));
    Files.createDirectories(warehouseDir);

    String managedPath = warehouseDir.resolve("managed").toAbsolutePath().toString();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE, managedPath);
    String externalPath = warehouseDir.resolve("external").toAbsolutePath().toString();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL, externalPath);
    conf.set(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL.varname, externalPath);

    String icebergWarehouse = warehouseDir.resolve("iceberg-rest").toAbsolutePath().toString();
    Files.createDirectories(Paths.get(icebergWarehouse));

    Configuration hadoopConf = new Configuration(conf);
    hadoopCatalog = new HadoopCatalogIgnoringNamespaceMetadata(hadoopConf, icebergWarehouse);
    catalogAdapter = new RESTCatalogAdapter(hadoopCatalog);
    HttpServlet servlet = new JavaxIcebergRESTCatalogServlet(catalogAdapter);

    restPort = MetaStoreTestUtils.findFreePort();
    httpServer = createHttpServer(restPort);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    ServletHolder servletHolder = new ServletHolder(servlet);
    servletHolder.setInitParameter("javax.ws.rs.Application", "ServiceListPublic");
    context.addServlet(servletHolder, "/*");
    context.setVirtualHosts(null);
    context.setGzipHandler(new GzipHandler());
    httpServer.setHandler(context);
    httpServer.start();

    LOG.info(
        "Started native Iceberg REST catalog on port {} (warehouse {})",
        restPort,
        icebergWarehouse);
  }

  private static Server createHttpServer(int port) throws IOException {
    QueuedThreadPool threadPool = new QueuedThreadPool(256, 8, 60_000);
    Server httpServer = new Server(threadPool);
    ServerConnector connector = new ServerConnector(httpServer);
    connector.setPort(port);
    httpServer.setConnectors(new Connector[] {connector});
    for (ConnectionFactory factory : connector.getConnectionFactories()) {
      if (factory instanceof HttpConnectionFactory) {
        HttpConnectionFactory httpFactory = (HttpConnectionFactory) factory;
        HttpConfiguration httpConf = httpFactory.getHttpConfiguration();
        httpConf.setSendServerVersion(false);
        httpConf.setSendXPoweredBy(false);
      }
    }
    return httpServer;
  }

  public void stop() {
    if (httpServer != null) {
      try {
        httpServer.stop();
        httpServer.join();
      } catch (Exception e) {
        LOG.warn("Error stopping REST catalog Jetty server", e);
      }
      httpServer = null;
    }
    catalogAdapter = null;
    if (hadoopCatalog != null) {
      try {
        hadoopCatalog.close();
      } catch (Exception e) {
        LOG.warn("Error closing HadoopCatalog", e);
      }
      hadoopCatalog = null;
    }
  }

  public Path getWarehouseDir() {
    return warehouseDir;
  }

  public String getRestEndpoint() {
    return String.format("http://127.0.0.1:%d", restPort);
  }
}
