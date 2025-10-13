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

package org.apache.iceberg.rest.extension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hive.iceberg.it.NativeIcebergRESTCatalogServer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit 5 extension: {@link NativeIcebergRESTCatalogServer} (HadoopCatalog REST) with optional OAuth2
 * (Keycloak via {@link OAuth2AuthorizationServer}). Lives under {@code org.apache.iceberg.rest.extension}
 * for parity with upstream Iceberg/HMS REST ITs when {@code metastore-rest-catalog} is not a module.
 *
 * <p>OAuth2 is exercised on the <em>client</em> ({@code HiveRESTCatalogClient} + Iceberg catalog props).
 * {@link NativeIcebergRESTCatalogServer} does not use HMS catalog servlet {@code ConfVars}; this branch
 * also lacks upstream {@code CATALOG_SERVLET_AUTH_OAUTH2_*} keys, so they are not set here.
 */
public class HiveRESTCatalogServerExtension implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback {

  public enum AuthType {
    NONE,
    OAUTH2
  }

  private static final Logger LOG = LoggerFactory.getLogger(HiveRESTCatalogServerExtension.class);

  private final Configuration conf;
  private final OAuth2AuthorizationServer authorizationServer;
  private final NativeIcebergRESTCatalogServer restCatalogServer;

  private HiveRESTCatalogServerExtension(AuthType authType, Class<? extends MetaStoreSchemaInfo> schemaInfoClass,
      Map<String, String> configurations) {
    this.conf = MetastoreConf.newMetastoreConf();
    if (authType == AuthType.OAUTH2) {
      authorizationServer = new OAuth2AuthorizationServer();
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, "oauth2");
    } else {
      authorizationServer = null;
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, "simple");
    }
    configurations.forEach(conf::set);
    restCatalogServer = new NativeIcebergRESTCatalogServer();
    if (schemaInfoClass != null) {
      restCatalogServer.setSchemaInfoClass(schemaInfoClass);
    }
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    if (authorizationServer != null) {
      authorizationServer.start();
      LOG.info("OAuth2 authorization server issuer {}", authorizationServer.getIssuer());
    }
    restCatalogServer.start(conf);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws IOException {
    try (Stream<Path> children = Files.list(restCatalogServer.getWarehouseDir())) {
      children
          .filter(path -> !path.getFileName().toString().equals("derby.log"))
          .filter(path -> !path.getFileName().toString().equals("metastore_db"))
          .forEach(path -> {
            try {
              if (Files.isDirectory(path)) {
                FileUtils.deleteDirectory(path.toFile());
              } else {
                Files.delete(path);
              }
            } catch (IOException e) {
              LOG.error("Failed to delete path: {}", path, e);
            }
          });
    }
  }

  @Override
  public void afterAll(ExtensionContext context) {
    if (authorizationServer != null) {
      authorizationServer.stop();
    }
    restCatalogServer.stop();
  }

  public String getRestEndpoint() {
    return restCatalogServer.getRestEndpoint();
  }

  public String getOAuth2TokenEndpoint() {
    if (authorizationServer == null) {
      throw new IllegalStateException("OAuth2 is not enabled for this extension");
    }
    return authorizationServer.getTokenEndpoint();
  }

  public String getOAuth2ClientCredential() {
    if (authorizationServer == null) {
      throw new IllegalStateException("OAuth2 is not enabled for this extension");
    }
    return authorizationServer.getClientCredential();
  }

  public static class Builder {
    private final AuthType authType;
    private Class<? extends MetaStoreSchemaInfo> metaStoreSchemaClass;
    private final Map<String, String> configurations = new HashMap<>();

    private Builder(AuthType authType) {
      this.authType = authType;
    }

    public Builder addMetaStoreSchemaClassName(Class<? extends MetaStoreSchemaInfo> metaStoreSchemaClass) {
      this.metaStoreSchemaClass = metaStoreSchemaClass;
      return this;
    }

    public HiveRESTCatalogServerExtension build() {
      return new HiveRESTCatalogServerExtension(authType, metaStoreSchemaClass, configurations);
    }
  }

  public static Builder builder(AuthType authType) {
    return new Builder(authType);
  }
}
