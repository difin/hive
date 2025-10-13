/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hive.iceberg.it.NativeIcebergRESTCatalogServer;
import org.apache.iceberg.rest.extension.OAuth2AuthorizationServer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class HiveRESTCatalogServerExtension extends ExternalResource {

  /** Matches {@code hive.metastore.catalog.servlet.auth}. */
  public enum AuthType {
    NONE,
    JWT,
    OAUTH2
  }

  private final Configuration conf;
  private final OAuth2AuthorizationServer authorizationServer;
  private final NativeIcebergRESTCatalogServer restCatalogServer;

  private static final Logger LOG = LoggerFactory.getLogger(HiveRESTCatalogServerExtension.class);

  private HiveRESTCatalogServerExtension(AuthType authType, Class<? extends MetaStoreSchemaInfo> schemaInfoClass,
      Map<String, String> configurations) {
    this.conf = MetastoreConf.newMetastoreConf();
    if (authType == AuthType.OAUTH2) {
      authorizationServer = new OAuth2AuthorizationServer();
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, "oauth2");
    } else {
      authorizationServer = null;
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH,
          authType == AuthType.NONE ? "simple" : authType.name().toLowerCase(java.util.Locale.ROOT));
    }
    configurations.forEach(conf::set);
    restCatalogServer = new NativeIcebergRESTCatalogServer();
    if (schemaInfoClass != null) {
      restCatalogServer.setSchemaInfoClass(schemaInfoClass);
    }
  }

  @Override
  protected void before() throws Throwable {
    if (authorizationServer != null) {
      authorizationServer.start();
      LOG.info("An authorization server {} started", authorizationServer.getIssuer());
    }
    restCatalogServer.start(conf);
  }

  @Override
  protected void after() {
    if (authorizationServer != null) {
      authorizationServer.stop();
    }
    restCatalogServer.stop();
  }

  public String getRestEndpoint() {
    return restCatalogServer.getRestEndpoint();
  }

  public String getOAuth2TokenEndpoint() {
    return authorizationServer.getTokenEndpoint();
  }

  public String getOAuth2ClientCredential() {
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
  
  public NativeIcebergRESTCatalogServer getRestCatalogServer() {
    return restCatalogServer;
  }
}
