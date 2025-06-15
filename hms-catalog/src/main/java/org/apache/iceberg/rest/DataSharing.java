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

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.SecurityFriend;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.servlet.http.HttpServletResponse;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utilities for data sharing (Iceberg tables on s3).
 */
public class DataSharing {
  private static final Logger LOG = LoggerFactory.getLogger(DataSharing.class);
  public static final String ACCEPT = "Accept";
  public static final String ACCESS_TOKEN = "access_token";
  public static final String APPLICATION_JSON = "application/json";
  public static final String APPLICATION_WWW_FORM = "application/x-www-form-urlencoded";
  public static final String CONTENT_TYPE = "Content-Type";
  public static final String CREDENTIALS = "Credentials";
  public static final String CONFVAR_IDBROKER_URL = "hive.metastore.catalog.idbroker.url";
  public static final String CONFVAR_IDBROKER_DELEGATION_PATH = "hive.metastore.catalog.idbroker.delegation_path";
  public static final String CONFVAR_IDBROKER_ACCESS_PATH = "hive.metastore.catalog.idbroker.access_path";
  public static final String EXPIRES_IN = "expires_in";
  public static final String HTTP_GET = "GET";
  public static final String SESSION_TOKEN = "SessionToken";
  public static final String ACCESS_KEY_ID = "AccessKeyId";
  public static final String SECRET_ACCESS_KEY = "SecretAccessKey";
  public static final String S3_SESSION_TOKEN = "s3.session-token";
  public static final String S3_ACCESS_KEY_ID = "s3.access-key-id";
  public static final String S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  public static final String S3_REMOTE_SIGNING_ENABLED = "s3.remote-signing-enabled";
  public static final String CLIENT_REGION = "client.region";

  private final String delegationTokenPath;
  private final String accessTokenPath;
  private final URI[] idBrokerUris;
  private final SslContextFactory sslFactory;
  private volatile IdBrokerDelegationToken delegationToken = null;
  private final String s3ClientRegion;
  private final String s3RemoteSigningEnabled;
  private final AtomicInteger lastKnownGood = new AtomicInteger(0);

  /**
   * Remove leading and trailing slashes from URL segments so we can compose them.
   * @param path the path segment
   * @return the cleaned up segment
   */
  private static String composable(String path) {
    while(path.startsWith("/")) {
      path = path.substring(1);
    }
    while(path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

  public DataSharing(Configuration conf) throws IOException {
    String dpath = conf.get(CONFVAR_IDBROKER_DELEGATION_PATH, "dt/knoxtoken/api/v1/token");
    delegationTokenPath = composable(dpath);
    String apath = conf.get(CONFVAR_IDBROKER_ACCESS_PATH, "aws-cab/cab/api/v1/credentials");
    accessTokenPath = composable(apath);
    s3ClientRegion = conf.get(CLIENT_REGION, "us-west-2");
    s3RemoteSigningEnabled = conf.get(S3_REMOTE_SIGNING_ENABLED, "false");
    String urls = conf.get(CONFVAR_IDBROKER_URL, "");
    if (urls == null || urls.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Data sharing requires configuration property '" + CONFVAR_IDBROKER_URL + "' " +
              "to be set with one or more comma-separated IdBroker URLs. " +
              "Example: https://broker1:8444/gateway,https://broker2:8444/gateway"
      );
    }
    // split the URLs by comma, trim whitespace, and ensure they are valid URIs
    try {
      idBrokerUris = Arrays.stream(urls.split(",")).map(String::trim).filter(s -> !s.isEmpty()).map(u -> {
        try {
          return URI.create(u.endsWith("/") ? u : u + "/");
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Invalid IdBroker URL: " + u, e);
        }
      }).toArray(URI[]::new);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse IdBroker URLs from property '" + CONFVAR_IDBROKER_URL + "' with value: " + urls, e);
    }
    // ensure we have at least one valid URI
    if (idBrokerUris.length == 0) {
      throw new IllegalArgumentException("No valid IdBroker URLs found in property '" + CONFVAR_IDBROKER_URL + "' with value: " + urls);
    }

    this.sslFactory = "https".equalsIgnoreCase(idBrokerUris[0].getScheme()) ? SecurityFriend.createSslContextFactory(conf) : null;
    LOG.info("DataSharing agent initialized with IdBroker endpoints: {}", Arrays.toString(idBrokerUris));
  }

  /**
   * For testing purposes only - clears the delegation token to force a refresh
   */
  void clearDelegationTokenCache() {
    this.delegationToken = null;
  }

  /**
   * Gets a data sharing access token from the idbroker.
   *
   * @param table the table's manifest location
   * @return an access token or null if no token could be obtained
   * @throws NotAuthorizedException if the access token could not be fetched due to authorization issues
   */

  public Map<String, String> getAccessToken(String table) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==> DataSharing.getAccessToken()");
    }
    Map<String, String> token = null;
    try {
      IdBrokerDelegationToken idt = delegationToken;
      if (idt == null || idt.isRenewalRequired()) {
        synchronized (this) {
          idt = delegationToken;
          if (idt == null || idt.isRenewalRequired()) {
            delegationToken = idt = fetchDelegationToken();
          }
        }
      }
      if (idt == null) {
        LOG.error("Failed to get IdBroker delegationToken");
        throw new NotAuthorizedException(
          "Unable to obtain IdBroker delegation token for current user");
      } else {
        token = fetchAccessToken(idt, table);
        if (token.isEmpty()) {
          throw new NotAuthorizedException(
              "Unable to obtain S3 access token for table " + table);
        }
      }
    } catch (NotAuthorizedException e) {
      // Rethrow NotAuthorizedException to indicate authorization failure
      LOG.error("<== DataSharing.getAccessToken() : Authorization failure", e);
      throw e;
    } catch (Exception e) {
      LOG.error("<== DataSharing.getAccessToken() : Failed to fetch access token", e);
      throw new NotAuthorizedException("Failed to fetch access token: " + e.getMessage());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("<== DataSharing.getAccessToken() : {}", "successful");
    }
    return token;
  }

  // RR helper
  private int nextIndex(int attempt) {
    return (lastKnownGood.get() + attempt) % idBrokerUris.length;
  }

  private URI resolve(int idx, String relPath) {
    return idBrokerUris[idx].resolve(relPath);
  }

  /**
   * Compose the Data Sharing delegation-token URL.
   * @return the URL
   * @throws IOException if url composition fails
   */
  URL delegationTokenUrl(int idx) throws IOException {
    return resolve(idx, delegationTokenPath).toURL();
  }

  /**
   * Fetches the Data Sharing delegation-token.
   * @return the delegation token
   * @throws IOException if url composition fails
   */
  private IdBrokerDelegationToken fetchDelegationToken() throws IOException {
    LOG.debug("==> DataSharing.fetchDelegationToken()");
    IdBrokerDelegationToken token = null;
    for (int attempt = 0; attempt < idBrokerUris.length && token == null; ++attempt) {
      int idx = nextIndex(attempt);
      try {
        URL brokerUrl = delegationTokenUrl(idx);
        LOG.info("==> DataSharing.fetchDelegationToken(), trying broker url: {} (attempt {}/{})",
            brokerUrl, attempt, idBrokerUris.length);

        Object result = clientCall(null, brokerUrl, HTTP_GET, null);
        LOG.info("==> DataSharing.fetchDelegationToken(), broker response: {} ", result);
        if (result instanceof Map) {
          Object sessionToken = ((Map<?, ?>) result).get(ACCESS_TOKEN);
          LOG.debug("==> DataSharing.fetchDelegationToken(), access_token from response: {} ", sessionToken);
          if (sessionToken != null) {
            String expirationDate = (((Map<?, ?>) result).get(EXPIRES_IN)).toString();
            LOG.debug("==> DataSharing.fetchDelegationToken(), access_token is not null: expiration is {} ",
                expirationDate);
            token = new IdBrokerDelegationToken(sessionToken.toString(), Double.valueOf(expirationDate).longValue());
            lastKnownGood.set(idx);
          } else {
            LOG.info("<== DataSharing.fetchDelegationToken(): access_token value is null");
          }
        }
      } catch (Exception e) {
        LOG.warn("Delegation-token fetch failed from {} ({})", idBrokerUris[idx], e.toString());
      }
    }
    LOG.info("<== DataSharing.fetchDelegationToken(): {}", (token == null ? " failed" : "successful"));
    return token;
  }

  /**
   * Compose the Data Sharing access-token URL.
   *
   * @param table the table's manifest location
   * @return the URL
   * @throws IOException if url composition fails
   */
  URL accessTokenUrl(final String table) throws IOException {
    String tableRoot = table;
    if (table.endsWith(".json")) {
      // derive the root location for the table, hack until we find a better way
      tableRoot = table.substring(0, table.lastIndexOf("/metadata"));
    }
    int idx = lastKnownGood.get();
    URL accessTokenUrl = resolve(idx, accessTokenPath + "?path=" + urlEncodeUTF8(tableRoot) + "&permissions=read-only").toURL();
    LOG.debug("==> DataSharing.accessTokenUrl() returning : {} ", accessTokenUrl);
    return accessTokenUrl;
  }

  /**
   * Fetches the Data Sharing access-token.
   * @param idt the delegation token
   * @param table the table's manifest location
   * @return the map containing information including the access token
   * @throws IOException if url composition fails
   */
  private Map<String, String> fetchAccessToken(IdBrokerDelegationToken idt, String table) throws IOException {
    LOG.info("==> DataSharing.fetchAccessToken(), using bearer token {}", idt.getDelegationToken());
    Map<String, String> tokenRes = new HashMap<>();
    try {
      Object result = clientCall(idt.getDelegationToken(), accessTokenUrl(table), HTTP_GET, null);
      if (result instanceof Map) {
        Map<?, ?> resultMap = (Map<?, ?>) result;
        Map<?, ?> assumedRoleUser = (Map<?, ?>) resultMap.get("AssumedRoleUser");
        String assumedRole = assumedRoleUser != null ? (String) assumedRoleUser.get("AssumedRole") : "N/A";
        String arn = assumedRoleUser != null ? (String) assumedRoleUser.get("Arn") : "N/A";
        LOG.info("==> DataSharing.fetchAccessToken(), broker response (non-sensitive): AssumedRole={}, Arn={}", assumedRole, arn);
        Object sessionToken = null;
        Object accessKeyId = null;
        Object secretKey = null;
        Object credentials = ((Map<?, ?>) result).get(CREDENTIALS);
        if (credentials instanceof Map<?, ?>) {
          sessionToken = ((Map<?, ?>) credentials).get(SESSION_TOKEN);
          accessKeyId = ((Map<?, ?>) credentials).get(ACCESS_KEY_ID);
          secretKey = ((Map<?, ?>) credentials).get(SECRET_ACCESS_KEY);
        }
        if (sessionToken != null) {
          tokenRes.put(S3_SESSION_TOKEN, sessionToken.toString());
        } else {
          LOG.info("<== DataSharing.fetchAccessToken(): returned null token value");
        }
        if (accessKeyId != null) {
          tokenRes.put(S3_ACCESS_KEY_ID, accessKeyId.toString());
        } else {
          LOG.info("<== DataSharing.fetchAccessToken(): returned null accessKey value");
        }
        if (secretKey != null) {
          tokenRes.put(S3_SECRET_ACCESS_KEY, secretKey.toString());
        } else {
          LOG.info("<== DataSharing.fetchAccessToken(): returned null secret value");
        }
        // add those pseudo-constants if we are returning something
        if (!tokenRes.isEmpty()) {
          tokenRes.put(S3_REMOTE_SIGNING_ENABLED, s3RemoteSigningEnabled);
          tokenRes.put(CLIENT_REGION, s3ClientRegion);
        }
      }
    } catch (IOException e) {
      LOG.error("Exception fetching the access token", e);
    }
    LOG.info("<== DataSharing.fetchAccessToken(): {}", (tokenRes == null ? " failed" : "successful"));
    return tokenRes;
  }

  static String urlEncodeUTF8(String str) {
    try {
      return URLEncoder.encode(str, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return str;
    }
  }

  static class IdBrokerDelegationToken {
    private final String delegationToken;
    private final long expiryDate;

    IdBrokerDelegationToken(String delegationToken, long expiryDate) {
      this.delegationToken = delegationToken;
      this.expiryDate      = expiryDate;
    }

    String getDelegationToken() {
      return this.delegationToken;
    }

    boolean isRenewalRequired() {
      return System.currentTimeMillis() >= expiryDate;
    }
  }

  /**
   * Performs a Json client call.
   * @param jwt the jwt token
   * @param url the url
   * @param method the http method
   * @param arg the argument that will be transported as JSon
   * @return the result the was returned through Json
   * @throws IOException if marshalling the request/response fail
   */
  public Object clientCall(String jwt, URL url, String method, Object arg) throws IOException {
    return clientCall(jwt, url, method, true, arg);
  }

  /**
   * A generic server response (when content is not JSON or error).
   */
  public static class ServerResponse {
    private final int code;
    private final String content;

    public ServerResponse(int code, String content) {
      this.code = code;
      this.content = content;
    }

    public int getCode() {
      return code;
    }

    public String getContent() {
      return content;
    }
  }

  public Object clientCall(String jwt, URL url, String method, boolean json, Object arg) throws IOException {
    HttpURLConnection con;
    if (sslFactory != null) {
      HttpsURLConnection sslcon = (HttpsURLConnection) url.openConnection();
      sslcon.setSSLSocketFactory(sslFactory.getSslContext().getSocketFactory());
      con = sslcon;
    } else {
      con = (HttpURLConnection) url.openConnection();
    }
    con.setRequestMethod(method);
    con.setRequestProperty(MetaStoreUtils.USER_NAME_HTTP_HEADER, url.getUserInfo());
    if (json) {
      con.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON);
      con.setRequestProperty(ACCEPT, APPLICATION_JSON);
    } else {
      con.setRequestProperty(CONTENT_TYPE, APPLICATION_WWW_FORM);
    }
    con.setDoInput(true);
    // perform http method
    int responseCode = con.getResponseCode();
    InputStream responseStream = con.getErrorStream();
    if (responseStream == null) {
      responseStream = con.getInputStream();
    }
    if (responseStream != null) {
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(responseStream, StandardCharsets.UTF_8))) {
        // if not strictly ok, check we are still receiving a JSON
        LOG.info("<== DataSharing.clientCall(): responseCode={}", responseCode);
        if (responseCode != HttpServletResponse.SC_OK) {
          String contentType = con.getContentType();
          LOG.info("<== DataSharing.clientCall(): contentType={}", contentType);
          if (contentType == null || contentType.indexOf(APPLICATION_JSON) == -1) {
            String line = null;
            StringBuilder response = new StringBuilder("error " + responseCode + ":");
            while ((line = reader.readLine()) != null) {
              response.append(line);
            }
            LOG.info("<== DataSharing.clientCall(): response={}", response);
            ServerResponse sr = new ServerResponse(responseCode, response.toString());
            return sr;
          }
        }
        return new Gson().fromJson(reader, Object.class);
      }
    }
    // no response stream,
    return responseCode;
  }
}
