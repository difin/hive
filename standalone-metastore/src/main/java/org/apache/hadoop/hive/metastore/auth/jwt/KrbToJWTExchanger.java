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
package org.apache.hadoop.hive.metastore.auth.jwt;

import com.cloudera.client.api.TokenProvider;
import com.cloudera.client.api.TokenProviderFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Krb to JWT exchange(Knox)
 */
public class KrbToJWTExchanger {
  private static final Logger LOGGER = LoggerFactory.getLogger(KrbToJWTExchanger.class);
  private static final String RETRIES_LIMIT = "knox.jwt.client.failure.retries";
  private static KrbToJWTExchanger krbJwtExchanger;

  private final int retryLimit;
  private final LoadingCache<String, TokenProvider> tokenProviders;

  private KrbToJWTExchanger(Configuration conf) {
    requireNonNull(conf, "conf is null");
    this.retryLimit = conf.getInt(RETRIES_LIMIT, 5);
    this.tokenProviders =
        CacheBuilder.newBuilder().maximumSize(1000)
            .softValues()
            .expireAfterWrite(300000L, TimeUnit.MILLISECONDS)
            .removalListener((RemovalListener<String, TokenProvider>) removalNotification -> {
              LOGGER.info("Removed the TokenProvider for user: {}", removalNotification.getKey());
              try {
                removalNotification.getValue().close();
              } catch (Exception e) {
                LOGGER.warn("Failed to close the TokenProvider", e);
              }
            })
            .build(new CacheLoader<String, TokenProvider>() {
              @Override
              public TokenProvider load(final String user) throws Exception {
                LOGGER.info("Creating TokenProvider for user: {}", user);
                return TokenProviderFactory.getTokenProvider(conf);
              }
            });
  }

  public synchronized static KrbToJWTExchanger getTokenProvider(Configuration conf) {
    if (krbJwtExchanger == null) {
      krbJwtExchanger = new KrbToJWTExchanger(conf);
    }
    return krbJwtExchanger;
  }

  public static boolean isKrbToJWTEnabled(Configuration conf) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return false;
    }
    String authType =
        MetastoreConf.getAsString(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_AUTH_MODE);
    if (!"jwt".equalsIgnoreCase(authType)) {
      return false;
    }
    if (StringUtils.isEmpty(MetastoreConf.get(conf, "knox.jwt.client.gateway.address"))) {
      LOGGER.warn("Kerberos ticket for JWT is disabled as knox.jwt.client.gateway.address is not configured");
      return false;
    }
    return true;
  }

  public String getJwtToken() throws Exception {
    String userName = SecurityUtils.getUser();
    final TokenProvider tokenProvider = tokenProviders.get(userName);
    Retry<String> retry = new Retry<String>(retryLimit, 1000) {
      @Override
      public String execute() throws Exception {
        LOGGER.debug("==> TokenProvider.getBearerToken(), tokenProvider: {}", tokenProvider);
        String jwtToken = tokenProvider.getBearerToken();
        if (LOGGER.isDebugEnabled()) {
          int length = jwtToken.length();
          LOGGER.debug("==> TokenProvider.getBearerToken(), token: {}",
              StringUtils.repeat("*", length / 2) + jwtToken.substring(length / 2));
        }
        return jwtToken;
      }
    };
    return retry.runWithDelay();
  }

  private abstract class Retry<T> {
    private final int retryLimit;
    private final int retryInterval;
    private int retries;

    Retry(int retryLimit, int retryInterval) {
      this.retryLimit = retryLimit;
      this.retryInterval = retryInterval;
    }
    abstract T execute() throws Exception;
    T runWithDelay() throws Exception {
      try {
        return execute();
      } catch(Exception e) {
        retries++;
        if (retryLimit <= retries) {
          throw e;
        } else {
          Thread.sleep(retryInterval);
          return runWithDelay();
        }
      }
    }
  }

}
