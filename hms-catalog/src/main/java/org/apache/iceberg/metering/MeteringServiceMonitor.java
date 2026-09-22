package org.apache.iceberg.metering;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;

/**
 * Utility class to perform a single health check against the co-located metering service at
 * <scheme>://localhost:<port>/health. Threading and retry logic live in
 * MeteringHealthWatcher; this class is purely responsible for the HTTP call itself.
 * If TLS is enabled for the metering service, co-located services often present a certificate
 * whose SAN matches a cluster FQDN rather than localhost. For loopback-only connections we relax
 * hostname verification (certificate chain still uses the JVM trust store).
 * The host is intentionally fixed to localhost to prevent nefarious users from overriding
 * the metering service location.
 */
public class MeteringServiceMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(MeteringServiceMonitor.class);

  static final String METERING_SERVICE_HOST = "localhost";
  static final int METERING_SERVICE_NON_SSL_PORT = 50051;
  static final int METERING_SERVICE_SSL_PORT = 50052;
  private static final String METERING_SERVICE_PORT_SYS_PROP = "metering.service.port";
  private static final String HEALTH_ENDPOINT = "/health";

  /**
   * Private constructor to prevent instantiation of utility class.
   */
  private MeteringServiceMonitor() {
    throw new UnsupportedOperationException("Utility class");
  }

  static Integer resolveMeteringServicePort() {
    String configuredPort = System.getProperty(METERING_SERVICE_PORT_SYS_PROP);
    if (configuredPort == null || configuredPort.trim().isEmpty()) {
      return null;
    }

    String trimmedPort = configuredPort.trim();
    try {
      int parsedPort = Integer.parseInt(trimmedPort);
      if (parsedPort <= 0 || parsedPort > 65535) {
        throw new IllegalArgumentException("port out of range");
      }
      return parsedPort;
    } catch (Exception e) {
      LOG.warn("Invalid JVM property {}='{}'; ignoring and using default metering port order",
          METERING_SERVICE_PORT_SYS_PROP, configuredPort, e);
      return null;
    }
  }

  static String resolveMeteringServiceScheme(Configuration conf) {
    boolean sslEnabled = conf != null && MetastoreConf.getBoolVar(conf, ConfVars.USE_SSL);
    return sslEnabled ? "https" : "http";
  }

  static List<URI> resolveMeteringServiceUris(Configuration conf) {
    List<MeteringEndpoint> endpoints = new ArrayList<>();

    Integer configuredPort = resolveMeteringServicePort();
    if (configuredPort != null) {
      endpoints.add(new MeteringEndpoint(resolveMeteringServiceScheme(conf), configuredPort,
          "jvm-property"));
    }

    endpoints.add(new MeteringEndpoint("http", METERING_SERVICE_NON_SSL_PORT, "default-non-ssl"));
    endpoints.add(new MeteringEndpoint("https", METERING_SERVICE_SSL_PORT, "default-ssl"));

    List<URI> uris = new ArrayList<>(endpoints.size());
    for (MeteringEndpoint endpoint : endpoints) {
      URI uri = endpoint.uri();
      if (!uris.contains(uri)) {
        uris.add(uri);
      }
    }
    return uris;
  }

  /**
   * Check if the metering service is available and healthy.
   *
   * @param conf metastore configuration; SSL mode is derived from ConfVars.USE_SSL only for the
   *             JVM-configured metering port.
   * @return true if the metering service health endpoint returns a 200 and a "Healthy" message
   */
  public static boolean isMeteringServiceHealthy(Configuration conf) {
    for (URI meteringUri : resolveMeteringServiceUris(conf)) {
      if (isMeteringServiceHealthy(meteringUri)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isMeteringServiceHealthy(URI meteringUri) {
    HttpURLConnection conn = null;
    try {
      URL url = meteringUri.toURL();
      conn = (HttpURLConnection) url.openConnection();
      if (conn instanceof HttpsURLConnection) {
        ((HttpsURLConnection) conn)
            .setHostnameVerifier((hostname, session) -> isLoopbackHostname(hostname));
      }
      conn.setRequestMethod("GET");

      int responseCode = conn.getResponseCode();

      if (responseCode == HttpURLConnection.HTTP_OK) {
        try (BufferedReader in = new BufferedReader(
            new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
          JsonObject response = JsonParser.parseReader(in).getAsJsonObject();
          String message = response.has("message") ? response.get("message").getAsString() : "";
          if ("Healthy".equalsIgnoreCase(message)) {
            LOG.info("Metering service is healthy at {}", meteringUri);
            return true;
          }
        }
      }

      LOG.warn("Metering service health check failed: {} response code at {}",
          responseCode, meteringUri);
    } catch (Exception e) {
      LOG.warn("Could not connect to metering service at {} => {}",
          meteringUri, e.getMessage(), e);
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }

    return false;
  }

  private static boolean isLoopbackHostname(String hostname) {
    if (hostname == null) {
      return false;
    }
    return "localhost".equalsIgnoreCase(hostname)
        || "127.0.0.1".equals(hostname)
        || "[::1]".equalsIgnoreCase(hostname)
        || "::1".equalsIgnoreCase(hostname);
  }

  private static class MeteringEndpoint {
    private final String scheme;
    private final int port;
    @SuppressWarnings("unused")
    private final String source;

    MeteringEndpoint(String scheme, int port, String source) {
      this.scheme = scheme;
      this.port = port;
      this.source = source;
    }

    URI uri() {
      return URI.create(String.format("%s://%s:%d%s",
          scheme, METERING_SERVICE_HOST, port, HEALTH_ENDPOINT));
    }
  }
}
