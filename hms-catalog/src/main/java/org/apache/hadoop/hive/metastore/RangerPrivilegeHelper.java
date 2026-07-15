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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactoryImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveResourceACLs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Calls the authorizer's getAllowedPrivileges API to determine the access level
 * a user has on a given table. This replaces the old HiveActor/getTableMeta approach
 * and eliminates the need for an embedded metastore client for authorization.
 */
public class RangerPrivilegeHelper {
  private static final Logger LOG = LoggerFactory.getLogger(RangerPrivilegeHelper.class);

  /**
   * The level of access a user has on a table.
   */
  public enum AccessLevel {
    NONE,
    READ_ONLY,
    READ_WRITE
  }

  private final HiveAuthorizer authorizer;

  public RangerPrivilegeHelper(Configuration conf) {
    HiveAuthorizer auth = null;
    try {
      HiveConf hiveConf = (conf instanceof HiveConf) ? (HiveConf) conf : new HiveConf(conf, HiveConf.class);
      HiveAuthorizerFactory factory = HiveUtils.getAuthorizerFactory(hiveConf,
          HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER);
      if (factory != null) {
        final var authenticator = HiveUtils.getAuthenticator(hiveConf,
            HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER);
        authenticator.setConf(hiveConf);
        final var authzContextBuilder = new HiveAuthzSessionContext.Builder();
        authzContextBuilder.setClientType(HiveAuthzSessionContext.CLIENT_TYPE.REST_CATALOG);
        authzContextBuilder.setSessionString("IcebergRESTCatalog");
        auth = factory.createHiveAuthorizer(new HiveMetastoreClientFactoryImpl(hiveConf), hiveConf, authenticator,
            authzContextBuilder.build());
        LOG.info("RangerPrivilegeHelper initialized with authorizer: {}", auth.getClass().getName());
      } else {
        LOG.warn("RangerPrivilegeHelper: no authorizer factory found");
      }
    } catch (Exception e) {
      LOG.warn("RangerPrivilegeHelper: failed to initialize authorizer", e);
    }
    this.authorizer = auth;
  }

  /**
   * Whether the helper was successfully initialized with an authorizer.
   */
  public boolean isAvailable() {
    return authorizer != null;
  }

  /**
   * Determines the access level a user has on a given table by calling
   * AbstractHiveAuthorizer#getAllowedPrivileges()
   *
   * @param dbName    the database name
   * @param tableName the table name
   * @param userName  the user name
   * @return the access level (NONE, READ_ONLY, or READ_WRITE)
   */
  public AccessLevel getAccessLevel(String dbName, String tableName, String userName) {
    if (authorizer == null) {
      LOG.debug("No authorizer available, defaulting to NONE for {}.{} user={}", dbName, tableName, userName);
      return AccessLevel.NONE;
    }
    try {
      HivePrincipal principal = new HivePrincipal(userName, HivePrincipal.HivePrincipalType.USER);
      HivePrivilegeObject privObj = new HivePrivilegeObject(
          HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tableName);
      Set<HiveResourceACLs.Privilege> privileges = authorizer.getAllowedPrivileges(
          privObj, principal, new HashSet<>(Set.of(HiveResourceACLs.Privilege.READ,
              HiveResourceACLs.Privilege.WRITE)), buildAuthzContext());
      if (privileges == null || privileges.isEmpty()) {
        LOG.debug("No privileges found for user {} on {}.{}", userName, dbName, tableName);
        return AccessLevel.NONE;
      }
      boolean hasRead = false;
      boolean hasWrite = false;
      for (HiveResourceACLs.Privilege priv : privileges) {
        LOG.debug("Privilege: {} for user {} on {}.{}", priv, userName, dbName, tableName);
        switch (priv) {
          case SELECT:
          case READ:
            hasRead = true;
            break;
          case UPDATE:
          case WRITE:
            hasWrite = true;
            hasRead = true;
            break;
          default:
            LOG.debug("Ignoring privilege {} for user {} on {}.{}", priv, userName, dbName, tableName);
            break;
        }
      }
      if (hasWrite) {
        return AccessLevel.READ_WRITE;
      } else if (hasRead) {
        return AccessLevel.READ_ONLY;
      }
      return AccessLevel.NONE;
    } catch (Exception e) {
      LOG.warn("Failed to check privileges for user {} on {}.{}", userName, dbName, tableName, e);
      return AccessLevel.NONE;
    }
  }

  private HiveAuthzContext buildAuthzContext() {
    HiveAuthzContext.Builder builder = new HiveAuthzContext.Builder();
    Map<String, Object> clientConfig = Collections.singletonMap("REST_CATALOG", true);
    builder.setClientConfig(clientConfig);
    builder.setCommandString("GET PRIVILEGES");
    return builder.build();
  }
}
