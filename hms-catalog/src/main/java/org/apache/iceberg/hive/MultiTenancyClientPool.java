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

package org.apache.iceberg.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.util.PropertyUtil;

import java.security.PrivilegedExceptionAction;
import java.util.Map;

/**
 * The Iceberg default CachedClientPool doesn't support multi-tenancy due to:
 *  1. The key to obtain the connection from pool is static;
 *  2. In case of Kerberos environment, the proxy user cannot authenticate with HMS.
 *
 *  To solve the first, the key association with the current user will only be created upon
 *  calling the {@link CachedClientPool#clientPool}, where the caller retrieves the connection
 *  from the pool.
 *  To solve the second, the main point is to get the delegation token for the proxy user,
 *  and assign it to hive.metastore.token.signature, HiveMetaStoreClient will use this token
 *  to authentication with HMS.
 */

public class MultiTenancyClientPool extends CachedClientPool {
  private final Configuration conf;
  private final Map<String, String> properties;
  private final int clientPoolSize;

  public MultiTenancyClientPool(Configuration conf, Map<String, String> properties) {
    super(conf, properties);
    this.conf = conf;
    this.properties = properties;
    this.clientPoolSize = PropertyUtil.propertyAsInt(properties, "clients", 2);
  }

  /**
   * Retrieve the connection pool for the current user, as it will build
   * the cache key dynamically associate with the caller.
   */
  @Override
  HiveClientPool clientPool() {
    return CachedClientPool.clientPoolCache().get(extractKey(properties.get(CatalogProperties.CLIENT_POOL_CACHE_KEYS), conf),
        UserGroupInformation.isSecurityEnabled() ?
            k -> new TokenAuthHiveClientPool(clientPoolSize, conf) :
            k -> new HiveClientPool(clientPoolSize, conf));
  }

  /**
   * A HiveMetaStoreClient pool using Token-based authentication to create the raw client
   * for the proxy user.
   */
  public static class TokenAuthHiveClientPool extends HiveClientPool {
    private final HiveConf hiveConf;
    public TokenAuthHiveClientPool(int poolSize, Configuration conf) {
      super(poolSize, conf);
      this.hiveConf = super.hiveConf();
    }

    /**
     * Get the delegation token for the proxy user, and assign it to hive.metastore.token.signature,
     * This can help the proxy user authenticate with HMS on creating a HiveMetaStoreClient.
     */
    private void setDelegationToken() {
      IMetaStoreClient client = null;
      try {
        UserGroupInformation currentUgi = SecurityUtils.getUGI();
        if (currentUgi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.PROXY) {
          hiveConf.unset(MetastoreConf.ConfVars.TOKEN_SIGNATURE.getVarname());
          client = currentUgi.getRealUser().doAs(
              (PrivilegedExceptionAction<IMetaStoreClient>) () -> new HiveMetaStoreClient(hiveConf));
          String proxyUser = currentUgi.getUserName();
          String delegationTokenPropString = "DelegationTokenForHiveMetaStoreServer";
          String delegationTokenStr = client.getDelegationToken(proxyUser, proxyUser);
          SecurityUtils.setTokenStr(currentUgi, delegationTokenStr, delegationTokenPropString);
          MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.TOKEN_SIGNATURE, delegationTokenPropString);
        }
      } catch (Exception e) {
        throw new RuntimeMetaException(e, "Failed to get the delegation token", new Object[0]);
      } finally {
        if (client != null) {
          client.close();
        }
      }
    }

    @Override
    protected IMetaStoreClient newClient() {
      setDelegationToken();
      IMetaStoreClient newClient = super.newClient();
      return newClient;
    }

    @Override
    protected IMetaStoreClient reconnect(IMetaStoreClient client) {
      // Fetch a new token in case of the current token is expired.
      setDelegationToken();
      return super.reconnect(client);
    }
  }
}

