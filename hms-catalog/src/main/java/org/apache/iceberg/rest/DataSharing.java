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
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base for data sharing credential providers.
 * Contains common IdBroker communication logic (delegation tokens, HTTP client, HA failover).
 * Subclasses implement cloud-specific credential fetching and response parsing.
 *
 * @see AwsCredentialProvider
 * @see AzureCredentialProvider
 */
public abstract class DataSharing {
  private static final Logger LOG = LoggerFactory.getLogger(DataSharing.class);
  public static final String ACCEPT = "Accept";
  public static final String ACCESS_TOKEN = "access_token";
  public static final String APPLICATION_JSON = "application/json";
  public static final String APPLICATION_WWW_FORM = "application/x-www-form-urlencoded";
  public static final String CONTENT_TYPE = "Content-Type";
  public static final String HTTP_GET = "GET";
  public static final String EXPIRES_IN = "expires_in";

  // Common IdBroker configuration
  public static final String CONFVAR_IDBROKER_URL = "hive.metastore.catalog.idbroker.url";
  public static final String CONFVAR_IDBROKER_DELEGATION_PATH = "hive.metastore.catalog.idbroker.delegation_path";
  public static final String CONFVAR_IDBROKER_ACCESS_PATH = "hive.metastore.catalog.idbroker.access_path";

  // Cloud provider selection
  public static final String CONFVAR_CLOUD_PROVIDER = "rest.catalog.env.cloud.provider";
  public static final String CLOUD_PROVIDER_AWS = "aws";
  public static final String CLOUD_PROVIDER_AZURE = "azure";

  private final String delegationTokenPath;
  private final URI[] idBrokerUris;
  private final SSLContext clientSslContext;
  private volatile IdBrokerDelegationToken delegationToken = null;
  protected final AtomicInteger lastKnownGood = new AtomicInteger(0);

  /**
   * Creates the appropriate DataSharing provider based on configuration.
   *
   * @param conf the configuration
   * @return an AwsCredentialProvider or AzureCredentialProvider
   * @throws IOException if initialization fails
   */
  public static DataSharing create(Configuration conf) throws IOException {
    String provider = conf.get(CONFVAR_CLOUD_PROVIDER, CLOUD_PROVIDER_AWS);
    if (CLOUD_PROVIDER_AZURE.equalsIgnoreCase(provider)) {
      return new AzureCredentialProvider(conf);
    }
    if (OzoneCredentialProvider.CLOUD_PROVIDER_OZONE.equalsIgnoreCase(provider)) {
      return new OzoneCredentialProvider(conf);
    }
    return new AwsCredentialProvider(conf);
  }

  protected DataSharing(Configuration conf) throws IOException {
    String dpath = conf.get(CONFVAR_IDBROKER_DELEGATION_PATH, "dt/knoxtoken/api/v1/token");
    delegationTokenPath = composable(dpath);
    String urls = conf.get(CONFVAR_IDBROKER_URL, "");
    if (urls == null || urls.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Data sharing requires configuration property '" + CONFVAR_IDBROKER_URL + "' " +
              "to be set with one or more comma-separated IdBroker URLs. " +
              "Example: https://broker1:8444/gateway,https://broker2:8444/gateway"
      );
    }
    try {
      idBrokerUris = Arrays.stream(urls.split(",")).map(String::trim).filter(s -> !s.isEmpty()).map(u -> {
        try {
          return URI.create(u.endsWith("/") ? u : u + "/");
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Invalid IdBroker URL: " + u, e);
        }
      }).toArray(URI[]::new);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse IdBroker URLs from property '"
          + CONFVAR_IDBROKER_URL + "' with value: " + urls, e);
    }
    if (idBrokerUris.length == 0) {
      throw new IllegalArgumentException("No valid IdBroker URLs found in property '"
          + CONFVAR_IDBROKER_URL + "' with value: " + urls);
    }

    this.clientSslContext = "https".equalsIgnoreCase(idBrokerUris[0].getScheme())
        ? buildClientSslContext(conf) : null;
    LOG.info("DataSharing agent initialized with IdBroker endpoints: {}", Arrays.toString(idBrokerUris));
  }

  /**
   * Builds the metastore outbound client {@link SSLContext} from hive-site SSL settings
   * ({@code hive.metastore.use.SSL}, {@code hive.metastore.truststore.path}, etc.).
   *
   * @param conf metastore configuration
   * @return client SSL context, or {@code null} when SSL is disabled in config
   * @throws IOException if truststore/keystore initialization fails
   */
  public static SSLContext buildClientSslContext(Configuration conf) throws IOException {
    return SecurityFriend.createClientSslContext(conf);
  }

  /**
   * Applies a client {@link SSLContext} to an {@link HttpsURLConnection} when present.
   */
  public static void applyClientSslContext(HttpURLConnection connection, SSLContext sslContext) {
    if (sslContext != null && connection instanceof HttpsURLConnection) {
      ((HttpsURLConnection) connection).setSSLSocketFactory(sslContext.getSocketFactory());
    }
  }

  /**
   * For testing purposes only - clears the delegation token to force a refresh.
   */
  void clearDelegationTokenCache() {
    this.delegationToken = null;
  }

  /**
   * Gets credentials from the IdBroker with the specified policy.
   *
   * @param table the table's location
   * @param policy the access policy ("read-only" or "read-write")
   * @return a map of Iceberg config properties containing the credentials
   * @throws NotAuthorizedException if credentials could not be obtained
   */
  public Map<String, String> getAccessTokenWithPolicy(String table, String policy) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==> DataSharing.getAccessTokenWithPolicy(table={}, policy={})", table, policy);
    }
    Map<String, String> token;
    try {
      IdBrokerDelegationToken idt = ensureDelegationToken();
      token = fetchCredentials(idt, table, policy);
      if (token.isEmpty()) {
        throw new NotAuthorizedException("Unable to obtain access token for table " + table);
      }
    } catch (NotAuthorizedException e) {
      LOG.error("<== DataSharing.getAccessTokenWithPolicy() : Authorization failure", e);
      throw e;
    } catch (Exception e) {
      LOG.error("<== DataSharing.getAccessTokenWithPolicy() : Failed to fetch access token", e);
      throw new NotAuthorizedException("Failed to fetch access token: " + e.getMessage());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("<== DataSharing.getAccessTokenWithPolicy() : successful");
    }
    return token;
  }

  /**
   * Gets credentials from the IdBroker with default read-only policy.
   *
   * @param table the table's location
   * @return a map of Iceberg config properties containing the credentials
   * @throws NotAuthorizedException if credentials could not be obtained
   */
  public Map<String, String> getAccessToken(String table) {
    return getAccessTokenWithPolicy(table, "read-only");
  }

  /**
   * Fetches cloud-specific credentials using the delegation token.
   * Subclasses implement this to handle provider-specific URL construction and response parsing.
   *
   * @param idt the IdBroker delegation token
   * @param table the table's location
   * @param policy the access policy ("read-only" or "read-write")
   * @return a map of Iceberg config properties containing the credentials
   * @throws IOException if the credential fetch fails
   */
  protected abstract Map<String, String> fetchCredentials(IdBrokerDelegationToken idt, String table, String policy)
      throws IOException;

  // --- Common IdBroker utilities ---

  private IdBrokerDelegationToken ensureDelegationToken() throws IOException {
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
      throw new NotAuthorizedException("Unable to obtain IdBroker delegation token for current user");
    }
    return idt;
  }

  private IdBrokerDelegationToken fetchDelegationToken() throws IOException {
    LOG.debug("==> DataSharing.fetchDelegationToken()");
    IdBrokerDelegationToken token = null;
    for (int attempt = 0; attempt < idBrokerUris.length && token == null; ++attempt) {
      int idx = nextIndex(attempt);
      try {
        URL brokerUrl = delegationTokenUrl(idx);
        LOG.info("==> DataSharing.fetchDelegationToken(), trying broker url: {} (attempt {}/{})",
            brokerUrl, attempt, idBrokerUris.length);

        Object result = fetchDelegationTokenFromBroker(brokerUrl);
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

  // --- URL composition helpers ---

  protected int nextIndex(int attempt) {
    return (lastKnownGood.get() + attempt) % idBrokerUris.length;
  }

  protected URI resolve(int idx, String relPath) {
    return idBrokerUris[idx].resolve(relPath);
  }

  protected int getIdBrokerCount() {
    return idBrokerUris.length;
  }

  URL delegationTokenUrl(int idx) throws IOException {
    return resolve(idx, delegationTokenPath).toURL();
  }

  // --- Static utilities ---

  /**
   * Remove leading and trailing slashes from URL segments so we can compose them.
   */
  protected static String composable(String path) {
    while (path.startsWith("/")) {
      path = path.substring(1);
    }
    while (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

  /**
   * Strip the URI scheme from a path, returning the path after "://".
   */
  protected static String stripScheme(String path) {
    if (path == null) {
      return null;
    }
    int schemeEnd = path.indexOf("://");
    if (schemeEnd > 0) {
      return path.substring(schemeEnd + 3);
    }
    return path;
  }

  /**
   * Derive the root location for the table from a metadata file path.
   */
  protected static String tableRoot(String table) {
    if (table != null && table.endsWith(".json")) {
      return table.substring(0, table.lastIndexOf("/metadata"));
    }
    return table;
  }

  static String urlEncodeUTF8(String str) {
    try {
      return URLEncoder.encode(str, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return str;
    }
  }

  // --- HTTP client ---

  /**
   * Fetches a Knox delegation JWT from {@code /dt/knoxtoken/api/v1/token}.
   * Uses SPNEGO as the HMS login user when Kerberos is enabled; plain HTTP otherwise (tests).
   */
  private Object fetchDelegationTokenFromBroker(URL url) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        return UserGroupInformation.getLoginUser().doAs(
            (PrivilegedExceptionAction<Object>) () -> executeJsonGet(url, true));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while fetching IdBroker delegation token", e);
      }
    }
    return executeJsonGet(url, false);
  }

  private HttpURLConnection openSpnegoConnection(URL url) throws IOException {
    AuthenticatedURL.Token spnegoToken = new AuthenticatedURL.Token();
    HttpURLConnection con;
    try {
      con = new AuthenticatedURL().openConnection(url, spnegoToken);
    } catch (AuthenticationException e) {
      throw new IOException("SPNEGO authentication failed for " + url, e);
    }
    applyClientSslContext(con, clientSslContext);
    return con;
  }

  private HttpURLConnection openPlainConnection(URL url) throws IOException {
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    applyClientSslContext(con, clientSslContext);
    return con;
  }

  private Object executeJsonGet(URL url, boolean useSpnego) throws IOException {
    HttpURLConnection con = useSpnego ? openSpnegoConnection(url) : openPlainConnection(url);
    try {
      con.setRequestMethod(HTTP_GET);
      con.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON);
      con.setRequestProperty(ACCEPT, APPLICATION_JSON);
      con.setDoInput(true);
      return readHttpResponse(con);
    } finally {
      con.disconnect();
    }
  }

  /**
   * Performs a JSON client call to the given URL with a JWT bearer token.
   *
   * @param jwt the JWT bearer token from {@link #fetchDelegationToken()}
   * @param url the url
   * @param method the HTTP method
   * @param arg the argument that will be transported as JSON
   * @return the parsed JSON result, a ServerResponse on error, or the HTTP response code
   * @throws IOException if the request fails
   * @throws IllegalArgumentException if {@code jwt} is null or empty
   */
  public Object clientCall(String jwt, URL url, String method, Object arg) throws IOException {
    return clientCall(jwt, url, method, true, arg);
  }

  public Object clientCall(String jwt, URL url, String method, boolean json, Object arg) throws IOException {
    if (jwt == null || jwt.isEmpty()) {
      throw new IllegalArgumentException(
          "IdBroker client call requires a JWT bearer token; unauthenticated requests are not supported");
    }
    HttpURLConnection con = openPlainConnection(url);
    try {
      con.setRequestMethod(method);
      con.setRequestProperty(MetaStoreUtils.USER_NAME_HTTP_HEADER, url.getUserInfo());
      con.setRequestProperty("Authorization", "Bearer " + jwt);
      if (json) {
        con.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON);
        con.setRequestProperty(ACCEPT, APPLICATION_JSON);
      } else {
        con.setRequestProperty(CONTENT_TYPE, APPLICATION_WWW_FORM);
      }
      con.setDoInput(true);
      return readHttpResponse(con);
    } finally {
      con.disconnect();
    }
  }

  private Object readHttpResponse(HttpURLConnection con) throws IOException {
    int responseCode = con.getResponseCode();
    InputStream responseStream = con.getErrorStream();
    if (responseStream == null) {
      responseStream = con.getInputStream();
    }
    if (responseStream != null) {
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(responseStream, StandardCharsets.UTF_8))) {
        LOG.info("<== DataSharing.clientCall(): responseCode={}", responseCode);
        if (responseCode != HttpServletResponse.SC_OK) {
          String contentType = con.getContentType();
          LOG.info("<== DataSharing.clientCall(): contentType={}", contentType);
          if (contentType == null || contentType.indexOf(APPLICATION_JSON) == -1) {
            String line;
            StringBuilder response = new StringBuilder("error " + responseCode + ":");
            while ((line = reader.readLine()) != null) {
              response.append(line);
            }
            LOG.info("<== DataSharing.clientCall(): response={}", response);
            return new ServerResponse(responseCode, response.toString());
          }
        }
        return new Gson().fromJson(reader, Object.class);
      }
    }
    return responseCode;
  }

  // --- Inner classes ---

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

  static class IdBrokerDelegationToken {
    private final String delegationToken;
    private final long expiryDate;

    IdBrokerDelegationToken(String delegationToken, long expiryDate) {
      this.delegationToken = delegationToken;
      this.expiryDate = expiryDate;
    }

    String getDelegationToken() {
      return this.delegationToken;
    }

    boolean isRenewalRequired() {
      return System.currentTimeMillis() >= expiryDate;
    }
  }
}
