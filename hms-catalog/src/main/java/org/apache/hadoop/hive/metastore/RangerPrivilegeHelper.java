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
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
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

  private static final Set<HiveResourceACLs.Privilege> READ_WRITE_PRIVILEGES_TO_CHECK =
      Set.of(HiveResourceACLs.Privilege.READ, HiveResourceACLs.Privilege.WRITE);

  private static final Set<HiveResourceACLs.Privilege> WRITE_PRIVILEGES_TO_CHECK =
      Set.of(HiveResourceACLs.Privilege.WRITE, HiveResourceACLs.Privilege.UPDATE);

  private static final Set<HiveResourceACLs.Privilege> READ_PRIVILEGES_TO_CHECK =
      Set.of(HiveResourceACLs.Privilege.READ, HiveResourceACLs.Privilege.SELECT);

  private final HiveAuthorizer authorizer;

  /**
   * The level of access a user has on a table.
   */
  public enum AccessLevel {
    NONE,
    READ_ONLY,
    READ_WRITE
  }

  public RangerPrivilegeHelper(Configuration conf) {
    HiveAuthorizer auth = null;
    HiveConf hiveConf = (conf instanceof HiveConf) ? (HiveConf) conf : new HiveConf(conf, RangerPrivilegeHelper.class);
    try {
      HiveAuthorizerFactory authorizerFactory = HiveUtils.getAuthorizerFactory(hiveConf,
          HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER);
      if (authorizerFactory != null) {
        LOG.debug("Using HiveAuthorizerFactory: {}", authorizerFactory.getClass().getName());

        HiveAuthzSessionContext.Builder ctxBuilder = new HiveAuthzSessionContext.Builder();
        ctxBuilder.setClientType(HiveAuthzSessionContext.CLIENT_TYPE.REST_CATALOG);
        ctxBuilder.setSessionString("IcebergRESTCatalog");
        HiveAuthzSessionContext sessionContext = ctxBuilder.build();

        HiveAuthenticationProvider authenticator = HiveUtils.getAuthenticator(
            hiveConf, HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER);
        if (authenticator != null) {
          authenticator.setConf(hiveConf);
        }

        HiveMetastoreClientFactoryImpl clientFactory = new HiveMetastoreClientFactoryImpl(hiveConf);
        auth = authorizerFactory.createHiveAuthorizer(
            clientFactory, hiveConf, authenticator, sessionContext);
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
   * Check if user has write privilege on a table via getAllowedPrivileges().
   *
   * @param dbName Database name
   * @param tableName Table name
   * @param userName User name to check
   * @return true if user has UPDATE or WRITE privilege
   */
  public boolean hasWritePrivilege(String dbName, String tableName, String userName) {
    if (authorizer == null) {
      LOG.warn("No authorizer available, defaulting to no write privilege");
      return false;
    }

    try {
      LOG.debug("Checking write privilege for user {} on {}.{}", userName, dbName, tableName);
      Set<HiveResourceACLs.Privilege> allowed =
          getAllowedPrivileges(dbName, tableName, userName, WRITE_PRIVILEGES_TO_CHECK);
      boolean canWrite = indicatesWriteAccess(allowed);
      if (canWrite) {
        LOG.info("User {} has write privilege on {}.{}", userName, dbName, tableName);
      } else {
        LOG.debug("User {} does not have write privilege on {}.{}", userName, dbName, tableName);
      }
      return canWrite;
    } catch (Exception e) {
      LOG.error("Error checking write privilege for user {} on {}.{}: {}",
          userName, dbName, tableName, e.getMessage(), e);
      return false;
    }
  }

  /**
   * Check if user has read privilege on a table via getAllowedPrivileges().
   *
   * @param dbName Database name
   * @param tableName Table name
   * @param userName User name to check
   * @return true if user has SELECT or READ privilege
   */
  public boolean hasReadPrivilege(String dbName, String tableName, String userName) {
    if (authorizer == null) {
      LOG.warn("No authorizer available, defaulting to no read privilege");
      return false;
    }

    try {
      Set<HiveResourceACLs.Privilege> allowed =
          getAllowedPrivileges(dbName, tableName, userName, READ_PRIVILEGES_TO_CHECK);
      boolean canRead = indicatesReadAccess(allowed);
      LOG.debug("User {} {} read privilege on {}.{}",
          userName, canRead ? "has" : "does not have", dbName, tableName);
      return canRead;
    } catch (Exception e) {
      LOG.error("Error checking read privilege for user {} on {}.{}: {}",
          userName, dbName, tableName, e.getMessage(), e);
      return false;
    }
  }

  /**
   * Determines the access level a user has on a given table by calling
   * AbstractHiveAuthorizer#getAllowedPrivileges().
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
      Set<HiveResourceACLs.Privilege> privileges =
          getAllowedPrivileges(dbName, tableName, userName, READ_WRITE_PRIVILEGES_TO_CHECK);
      if (privileges == null || privileges.isEmpty()) {
        LOG.debug("No privileges found for user {} on {}.{}", userName, dbName, tableName);
        return AccessLevel.NONE;
      }

      boolean hasRead = indicatesReadAccess(privileges);
      boolean hasWrite = indicatesWriteAccess(privileges);

      for (HiveResourceACLs.Privilege priv : privileges) {
        LOG.debug("Privilege: {} for user {} on {}.{}", priv, userName, dbName, tableName);
      }

      if (hasWrite) {
        LOG.debug("User {} has READ_WRITE access on {}.{}", userName, dbName, tableName);
        return AccessLevel.READ_WRITE;
      }
      if (hasRead) {
        LOG.debug("User {} has READ_ONLY access on {}.{}", userName, dbName, tableName);
        return AccessLevel.READ_ONLY;
      }
      LOG.debug("User {} has no recognised access on {}.{} (privileges={})", userName, dbName, tableName, privileges);
      return AccessLevel.NONE;
    } catch (Exception e) {
      LOG.warn("Failed to check privileges for user {} on {}.{}", userName, dbName, tableName, e);
      return AccessLevel.NONE;
    }
  }

  private Set<HiveResourceACLs.Privilege> getAllowedPrivileges(
      String dbName, String tableName, String userName, Set<HiveResourceACLs.Privilege> privilegesToCheck) {
    HivePrincipal principal = new HivePrincipal(userName, HivePrincipal.HivePrincipalType.USER);
    HivePrivilegeObject privObj = new HivePrivilegeObject(
        HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tableName);
    return authorizer.getAllowedPrivileges(
        privObj, principal, new HashSet<>(privilegesToCheck), buildAuthzContext());
  }

  private static boolean indicatesWriteAccess(Set<HiveResourceACLs.Privilege> privileges) {
    for (HiveResourceACLs.Privilege priv : privileges) {
      switch (priv) {
        case UPDATE:
        case WRITE:
          return true;
        default:
          break;
      }
    }
    return false;
  }

  private static boolean indicatesReadAccess(Set<HiveResourceACLs.Privilege> privileges) {
    for (HiveResourceACLs.Privilege priv : privileges) {
      switch (priv) {
        case SELECT:
        case READ:
          return true;
        default:
          break;
      }
    }
    return false;
  }

  private HiveAuthzContext buildAuthzContext() {
    HiveAuthzContext.Builder builder = new HiveAuthzContext.Builder();
    Map<String, Object> clientConfig = Collections.singletonMap("REST_CATALOG", true);
    builder.setClientConfig(clientConfig);
    builder.setCommandString("GET PRIVILEGES");
    return builder.build();
  }
}
