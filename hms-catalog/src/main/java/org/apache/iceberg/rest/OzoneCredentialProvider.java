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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Ozone credential provider for data sharing.
 *
 * <p>Fetches temporary S3-compatible credentials from the IdBroker (Knox) for accessing
 * Ozone data via the Ozone S3 Gateway. The credentials are vended as standard S3
 * access key / secret key / session token so that external clients (e.g. Spark, Trino)
 * can read Iceberg tables stored on Ozone through the S3A connector pointed at the
 * Ozone S3 Gateway endpoint.
 */
public class OzoneCredentialProvider extends DataSharing {

  private static final Logger LOG = LoggerFactory.getLogger(OzoneCredentialProvider.class);

  // Cloud provider name used in hive.metastore.catalog.cloud.provider
  public static final String CLOUD_PROVIDER_OZONE = "ozone";

  // IdBroker response keys - Ozone S3 Gateway returns STS-style credentials
  private static final String CREDENTIALS = "Credentials";
  private static final String SESSION_TOKEN = "SessionToken";
  private static final String ACCESS_KEY_ID = "AccessKeyId";
  private static final String SECRET_ACCESS_KEY = "SecretAccessKey";

  // Iceberg S3 FileIO config keys returned to clients
  public static final String S3_SESSION_TOKEN = "s3.session-token";
  public static final String S3_ACCESS_KEY_ID = "s3.access-key-id";
  public static final String S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  public static final String S3_ENDPOINT = "s3.endpoint";
  public static final String S3_REMOTE_SIGNING_ENABLED = "s3.remote-signing-enabled";
  public static final String S3_PATH_STYLE_ACCESS = "s3.path-style-access";

  // Config key for the Ozone S3 Gateway endpoint
  public static final String CONFVAR_OZONE_S3_GATEWAY = "hive.metastore.catalog.ozone.s3.gateway";
  public static final String CONFVAR_OZONE_REMOTE_SIGNING = "hive.metastore.catalog.ozone.s3.remote.signing";

  private final String accessTokenPath;
  private final String s3GatewayEndpoint;
  private final String remoteSigningEnabled;

  public OzoneCredentialProvider(Configuration conf) throws IOException {
    super(conf);
    String apath = conf.get(CONFVAR_IDBROKER_ACCESS_PATH, "aws-cab/cab/api/v1/credentials");
    accessTokenPath = composable(apath);
    s3GatewayEndpoint = conf.get(CONFVAR_OZONE_S3_GATEWAY, "");
    remoteSigningEnabled = conf.get(CONFVAR_OZONE_REMOTE_SIGNING, "false");
    LOG.info("OzoneCredentialProvider initialized. Access path: {}, S3 gateway: {}",
        accessTokenPath, s3GatewayEndpoint);
  }

  @Override
  protected Map<String, String> fetchCredentials(IdBrokerDelegationToken idt, String table, String policy)
      throws IOException {
    LOG.info("==> OzoneCredentialProvider.fetchCredentials(), table={}, policy={}", table, policy);
    Map<String, String> tokenRes = new HashMap<>();
    try {
      URL url = accessTokenUrlWithPolicy(table, policy);
      Object result = clientCall(idt.getDelegationToken(), url, HTTP_GET, null);

      if (result instanceof Map) {
        Map<?, ?> resultMap = (Map<?, ?>) result;

        // Log non-sensitive metadata if present
        Object assumedRoleUserObj = resultMap.get("AssumedRoleUser");
        if (assumedRoleUserObj instanceof Map) {
          Map<?, ?> assumedRoleUser = (Map<?, ?>) assumedRoleUserObj;
          LOG.info("==> OzoneCredentialProvider.fetchCredentials(): AssumedRole={}, Arn={}",
              assumedRoleUser.get("AssumedRole"), assumedRoleUser.get("Arn"));
        }

        // Extract STS-style credentials (same structure as AWS STS AssumeRole response)
        Object credentialsObj = resultMap.get(CREDENTIALS);
        if (credentialsObj instanceof Map) {
          Map<?, ?> credMap = (Map<?, ?>) credentialsObj;
          putIfNotNull(tokenRes, S3_ACCESS_KEY_ID, credMap.get(ACCESS_KEY_ID));
          putIfNotNull(tokenRes, S3_SECRET_ACCESS_KEY, credMap.get(SECRET_ACCESS_KEY));
          putIfNotNull(tokenRes, S3_SESSION_TOKEN, credMap.get(SESSION_TOKEN));
        }

        if (!tokenRes.isEmpty()) {
          // Add Ozone S3 Gateway endpoint so client knows where to connect
          if (s3GatewayEndpoint != null && !s3GatewayEndpoint.isEmpty()) {
            tokenRes.put(S3_ENDPOINT, s3GatewayEndpoint);
            // Ozone S3 Gateway requires path-style access (bucket in path, not subdomain)
            tokenRes.put(S3_PATH_STYLE_ACCESS, "true");
          }
          tokenRes.put(S3_REMOTE_SIGNING_ENABLED, remoteSigningEnabled);
        }
      }
    } catch (IOException e) {
      LOG.error("Exception fetching Ozone access token with policy", e);
      throw e;
    }

    LOG.info("<== OzoneCredentialProvider.fetchCredentials(): {}",
        tokenRes.isEmpty() ? "failed (no credentials returned)" : "successful");
    return tokenRes;
  }

  /**
   * Compose the IdBroker access-token URL with the Ozone policy path and policy parameter.
   *
   * <p>For Ozone, the policy path is derived from the {@code ofs://} table location by
   * stripping the scheme and OM service ID, yielding {@code volume/bucket[/key-prefix]}.
   * For example:
   * <pre>
   *   ofs://ozone1/hive/warehouse/db/table  ->  ?path=hive/warehouse/db/table&policy=read-only
   * </pre>
   */
  private URL accessTokenUrlWithPolicy(String table, String policy) throws IOException {
    String policyPath = normalizePolicyPath(table);
    int idx = lastKnownGood.get();
    URL url = resolve(idx, accessTokenPath + "?path=" + urlEncodeUTF8(policyPath) + "&policy=" + policy).toURL();
    LOG.debug("==> OzoneCredentialProvider.accessTokenUrlWithPolicy() returning: {}", url);
    return url;
  }

  /**
   * Normalize an {@code ofs://} table location to an IdBroker policy path.
   *
   * <p>The IdBroker policy path for Ozone follows the format {@code volume/bucket[/prefix]},
   * which is the path after the OM service ID in an {@code ofs://} URI.
   *
   * <p>Examples:
   * <pre>
   *   ofs://ozone1/hive/warehouse/db/table  ->  hive/warehouse/db/table
   *   ofs://ozone1:9862/vol/bkt/key         ->  vol/bkt/key
   *   s3a://bucket/prefix                   ->  bucket/prefix  (fallback: strip scheme)
   * </pre>
   */
  static String normalizePolicyPath(String location) {
    if (location == null) {
      return "";
    }
    // Handle ofs:// — strip scheme + authority (OM service ID / host:port)
    if (OzonePathUtils.isOfsScheme(location)) {
      try {
        OzonePathUtils.OzonePath ozPath = OzonePathUtils.parseOfsPath(location);
        // Policy path = volume/bucket[/key]
        String path = ozPath.volume + "/" + ozPath.bucket;
        if (ozPath.key != null && !ozPath.key.isEmpty()) {
          path = path + "/" + ozPath.key;
        }
        return path;
      } catch (Exception e) {
        LOG.warn("Could not parse ofs:// path '{}', falling back to stripScheme: {}", location, e.getMessage());
      }
    }
    // Fallback for s3a://, s3:// or unparseable ofs:// — strip scheme and use as-is
    return tableRoot(stripScheme(location));
  }

  private static void putIfNotNull(Map<String, String> map, String key, Object value) {
    if (value != null) {
      map.put(key, value.toString());
    } else {
      LOG.info("<== OzoneCredentialProvider: {} value is null", key);
    }
  }
}
