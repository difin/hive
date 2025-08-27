/* * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;

import java.time.Duration;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Secures servlet processing for HMS-catalog.
 */
public class HMSServletSecurity extends ServletSecurity {
  private static final Logger LOG = LoggerFactory.getLogger(HMSServletSecurity.class);
  private static final String KNOX = "knox";
  private static final String X_USER_GROUPS = "X-Knox-Actor-Groups-1";
  private static final String CLIENT_TYPE_KEY = "client_type";
  private static final String REST_CATALOG_VALUE = "rest_catalog";

  private final Cache<UgiKey, UserGroupInformation> ugiCache;

  public HMSServletSecurity(Configuration conf, boolean jwt) {
    super(conf, jwt);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    long ugiCacheExpiryMs = getCacheExpiryMs(conf);
    int ugiCacheMaxSize = getCacheMaxSize(conf);

    this.ugiCache = createCacheWithConfig(ugiCacheExpiryMs, ugiCacheMaxSize);
  }

  /**
   * Gets the cache expiry time
   */
  private long getCacheExpiryMs(Configuration conf) {
    try {
      return MetastoreConf.getLongVar(conf, MetastoreConf.ConfVars.AGGREGATE_STATS_CACHE_TTL) * 1000L;
    } catch (AssertionError | Exception e) {
      String configKey = MetastoreConf.ConfVars.AGGREGATE_STATS_CACHE_TTL.getVarname();
      long ttlSeconds = conf.getLong(configKey, 600L);
      return ttlSeconds * 1000L;
    }
  }

  /**
   * Gets the cache max size
   */
  private int getCacheMaxSize(Configuration conf) {
    try {
      return MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.SERVER_MAX_THREADS);
    } catch (AssertionError | Exception e) {
      String configKey = MetastoreConf.ConfVars.SERVER_MAX_THREADS.getVarname();
      return conf.getInt(configKey, 1000);
    }
  }

  /**
   * Extracts the username from a request.
   *
   * @param request a request?doAs=username
   * @return the username
   */
  private static String getDoAsQueryParam(HttpServletRequest request) {
    Map<String, String[]> params = request.getParameterMap();
    Set<String> keySet = params.keySet();
    for (String key : keySet) {
      if (key.equalsIgnoreCase("doAs")) {
        return params.get(key)[0];
      }
    }
    return null;
  }

  /**
   * Check that the proxy user comes from a trusted IP and is authorized do-as for a given real user.
   *
   * @param realUser  the real user
   * @param proxyUser the proxy user
   * @param ipAddress the proxy host ip address
   * @throws IOException
   */
  private static void verifyProxyAccess(UserGroupInformation realUser, String proxyUser, String ipAddress) throws IOException {
    try {
      if (!proxyUser.equalsIgnoreCase(realUser.getUserName())) {
        ProxyUsers.authorize(UserGroupInformation.createProxyUser(proxyUser, realUser), ipAddress);
      }
    } catch (IOException e) {
      LOG.warn("Failed to validate proxy privilege of {} for {}", realUser, proxyUser, e);
      throw e;
    }
  }

  @Override
  protected String extractUserName(HttpServletRequest request, HttpServletResponse response)
      throws HttpAuthenticationException {
    if (UserGroupInformation.isSecurityEnabled()) {
      String proxyHeader = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METASTORE_TRUSTED_PROXY_TRUSTHEADER);
      // Trusted header is present, which means the user is already authenticated.
      LOG.info("TrustedProxy configuration value {}", proxyHeader);
      if (proxyHeader != null && !proxyHeader.trim().isEmpty()) {
        try {
          final UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
          final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
          LOG.info("TrustedProxy configuration enabled, authenticating login {}, current {}", loginUser, currentUser);
          final String userFromHeader = getDoAsQueryParam(request);
          if (userFromHeader != null && !userFromHeader.isEmpty()) {
            try {
              LOG.info("doAs user from header is {}", userFromHeader);
              // verifyProxyAccess(UserGroupInformation.getLoginUser().getUserName(), request.getUserPrincipal().getName(), request.getRemoteAddr());
              verifyProxyAccess(loginUser, KNOX, request.getRemoteAddr());
            } catch (IOException e) {
              LOG.error("Failed to verify proxy access for {}, should fall back to JWT but NOT falling back (HACK)", userFromHeader);
            }
            return userFromHeader;
          }
        } catch (IOException e) {
          LOG.info("UGI exception caught", e);
        }
      }
    }
    return super.extractUserName(request, response);
  }

  Cache<UgiKey, UserGroupInformation> getUgiCache() {
    return ugiCache;
  }

  static class UgiKey {
    private final String realUser;
    private final String loginUser;

    public UgiKey(String realUser, String loginUser) {
      this.realUser = realUser;
      this.loginUser = loginUser;
    }

    @Override
    public int hashCode() {
      return realUser.hashCode() + (37 * loginUser.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof UgiKey) {
        UgiKey other = (UgiKey) obj;
        return realUser.equals(other.realUser) && loginUser.equals(other.loginUser);
      }
      return false;
    }

    String getRealUser() {
      return realUser;
    }

    String getLoginUser() {
      return loginUser;
    }
  }

  /**
   * Creates a UGI cache with the specified expiration time and maximum size.
   *
   * @param expirationMs Time in milliseconds after which entries expire due to inactivity
   * @param maxSize      Maximum number of entries the cache can hold
   * @return A configured Caffeine cache for UGI objects
   */
  private Cache<UgiKey, UserGroupInformation> createCacheWithConfig(long expirationMs, int maxSize) {
    RemovalListener<UgiKey, UserGroupInformation> cleanupListener =
        (key, ugi, cause) -> {
          if (ugi != null) {
            try {
              FileSystem.closeAllForUGI(ugi);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Cleaned up FileSystem handles for evicted UGI: {} (cause: {})",
                    ugi.getUserName(), cause);
              }
            } catch (IOException cleanupException) {
              LOG.error("Failed to clean up FileSystem handles for evicted UGI: {} (cause: {})",
                  ugi, cause, cleanupException);
            }
          }
        };

    Caffeine<UgiKey, UserGroupInformation> builder = Caffeine.<UgiKey, UserGroupInformation>newBuilder()
        .maximumSize(maxSize)
        .executor(Runnable::run)
        .removalListener(cleanupListener);

    if (expirationMs > 0) {
      builder.expireAfterAccess(Duration.ofMillis(expirationMs))
          .ticker(Ticker.systemTicker());
    }

    return builder.build();
  }

  UserGroupInformation getUgi(String realUser, UserGroupInformation loginUser) {
    UgiKey key = new UgiKey(realUser, loginUser.getUserName());
    return ugiCache.get(key, v -> {
      return UserGroupInformation.createProxyUser(realUser, loginUser);
    });
  }

  /**
   * Creates a cache for testing purposes with both custom expiration and size.
   */
  Cache<UgiKey, UserGroupInformation> createTableCacheForTesting(long expirationMs, int maxSize) {
    return createCacheWithConfig(expirationMs, maxSize);
  }

  @Override
  public void execute(HttpServletRequest request, HttpServletResponse response, MethodExecutor executor)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Logging headers in {} request", request.getMethod());
      Enumeration<String> headerNames = request.getHeaderNames();
      while (headerNames.hasMoreElements()) {
        String headerName = headerNames.nextElement();
        LOG.debug("Header: [{}], Value: [{}]", headerName,
            request.getHeader(headerName));
      }
    }
    try {
      String userFromHeader = extractUserName(request, response);
      UserGroupInformation clientUgi;
      LOG.info("Creating proxy user for: {}", userFromHeader);
      clientUgi = getUgi(userFromHeader, UserGroupInformation.getLoginUser());
      String groupsHeader = request.getHeader(X_USER_GROUPS);
      if (groupsHeader != null && !groupsHeader.isEmpty() && !"$primary_group".equals(groupsHeader)) {
        HMSGroup.set(userFromHeader, groupsHeader);
        LOG.info("Groups are set for user {} in the request header:{}", userFromHeader, groupsHeader);
      } else if (null != getDoAsQueryParam(request)) {
        // FIXME: remove after Knox has support for Knox-Groups
        HMSGroup.set(userFromHeader, userFromHeader);
        LOG.info("Patching groups for user in the request header:{}", userFromHeader);
      }
      setAuthClientConfig(Collections.singletonMap(CLIENT_TYPE_KEY, REST_CATALOG_VALUE));
      PrivilegedExceptionAction<Void> action = () -> {
        executor.execute(request, response);
        return null;
      };
      try {
        clientUgi.doAs(action);
      } catch (InterruptedException e) {
        LOG.error("Exception when executing http request as user {}: ", clientUgi.getUserName(), e);
        Thread.currentThread().interrupt();
      } catch (RuntimeException e) {
        LOG.error("Exception when executing http request as user {}: ", clientUgi.getUserName(), e);
        throw new IOException(e);
      } finally {
        HMSGroup.setGroups(null);
        setAuthClientConfig(null);
      }
    } catch (AuthorizationException | HttpAuthenticationException e) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      response.getWriter().println("Authentication error: " + e.getMessage());
      // Also log the error message on server side
      LOG.error("Authentication error: ", e);
    }
  }

  public static boolean isTableAccessible(IMetaStoreClient client, String databaseName, String tableName) {
    try {
      List<TableMeta> list = client.getTableMeta(databaseName, tableName, null);
      return list != null && !list.isEmpty();
    } catch (TException e) {
      // any error, especially security means no-access
      LOG.info("checking table access failed, database {}, table {}", databaseName, tableName, e);
      return false;
    }
  }

  public static boolean isTableAccessible(IHMSHandler handler, String databaseName, String tableName) {
    try {
      List<TableMeta> list = handler.get_table_meta(databaseName, tableName, null);
      return list != null && !list.isEmpty();
    } catch (TException e) {
      LOG.info("checking table access failed, database {}, table {}", databaseName, tableName, e);
      // any error, especially security means no-access
      return false;
    }
  }

  private static final String AUTH_CLAZZ = "org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizer";
  private static final Method SET_AUTHZ_CLIENT_CONFIG = getAuthzClientConfig();

  public static Method getAuthzClientConfig() {
    try {
      Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(AUTH_CLAZZ);
      return clazz.getMethod("setClientConfig", Map.class);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      LOG.error("unable to initialize HiveMetaStoreAuthorizer.setClientConfig", e);
    }
    return null;
  }

  private static void setAuthClientConfig(Map<String, Object> map) {
    if (SET_AUTHZ_CLIENT_CONFIG != null) {
      try {
        SET_AUTHZ_CLIENT_CONFIG.invoke(null, map);
      } catch (IllegalAccessException | InvocationTargetException e) {
        LOG.error("unable to call HiveMetaStoreAuthorizer.setClientConfig", e);
      }
    }
  }

}
