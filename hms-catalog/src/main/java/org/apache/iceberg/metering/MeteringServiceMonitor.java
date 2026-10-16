package org.apache.iceberg.metering;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.rest.DataSharing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;

/**
 * Utility class to perform a single health check against the co-located metering service at
 * <scheme>://&lt;local-hostname&gt;:&lt;port&gt;/health. Threading and retry logic live in
 * MeteringHealthWatcher; this class is purely responsible for the HTTP call itself.
 * The local FQDN is resolved once via {@link InetAddress#getCanonicalHostName()} so TLS
 * certificates whose SAN matches the node FQDN are validated normally. HTTPS probes reuse the metastore client
 * {@link SSLContext} helpers in {@link DataSharing}. The host is not user-configurable to prevent
 * nefarious users from overriding the co-located metering service location.
 */
public class MeteringServiceMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(MeteringServiceMonitor.class);

  private static final String FALLBACK_METERING_SERVICE_HOST = "localhost";
  private static volatile String meteringServiceHost;
  private static volatile SSLContext clientSslContext;
  private static volatile Configuration clientSslContextConf;
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

  static String resolveMeteringServiceHost() {
    if (meteringServiceHost == null) {
      synchronized (MeteringServiceMonitor.class) {
        if (meteringServiceHost == null) {
          meteringServiceHost = lookupLocalHostname();
          LOG.info("Using host '{}' for metering service health checks", meteringServiceHost);
        }
      }
    }
    return meteringServiceHost;
  }

  private static String lookupLocalHostname() {
    try {
      InetAddress localHost = InetAddress.getLocalHost();
      return selectHostname(localHost.getCanonicalHostName(), localHost.getHostName(),
          localHost.getHostAddress());
    } catch (UnknownHostException e) {
      LOG.warn("Unable to resolve local hostname for metering health check; falling back to {}",
          FALLBACK_METERING_SERVICE_HOST, e);
      return FALLBACK_METERING_SERVICE_HOST;
    }
  }

  /**
   * Prefers the FQDN because metering TLS certificates carry only the FQDN as SAN, while the OS
   * hostname may be the short name on cloud hosts. {@link InetAddress#getCanonicalHostName()}
   * returns the IP literal when reverse lookup fails, in which case the OS hostname is used.
   */
  static String selectHostname(String canonicalHostName, String hostName, String hostAddress) {
    if (canonicalHostName != null && !canonicalHostName.isEmpty()
        && !canonicalHostName.equals(hostAddress)) {
      return canonicalHostName;
    }
    LOG.info("Reverse DNS lookup did not return an FQDN (got '{}'); using local hostname '{}'",
        canonicalHostName, hostName);
    return hostName;
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

  static SSLContext resolveClientSslContext(Configuration conf) throws IOException {
    if (conf == null) {
      return null;
    }
    if (clientSslContext == null || clientSslContextConf != conf) {
      synchronized (MeteringServiceMonitor.class) {
        if (clientSslContext == null || clientSslContextConf != conf) {
          clientSslContext = DataSharing.buildClientSslContext(conf);
          clientSslContextConf = conf;
        }
      }
    }
    return clientSslContext;
  }

  static List<URI> resolveMeteringServiceUris(Configuration conf) {
    List<MeteringEndpoint> endpoints = new ArrayList<>();
    String host = resolveMeteringServiceHost();

    Integer configuredPort = resolveMeteringServicePort();
    if (configuredPort != null) {
      endpoints.add(new MeteringEndpoint(host, resolveMeteringServiceScheme(conf), configuredPort,
          "jvm-property"));
    }

    endpoints.add(new MeteringEndpoint(host, "http", METERING_SERVICE_NON_SSL_PORT, "default-non-ssl"));
    endpoints.add(new MeteringEndpoint(host, "https", METERING_SERVICE_SSL_PORT, "default-ssl"));

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
   *             JVM-configured metering port. HTTPS health checks use the metastore truststore when
   *             hive.metastore.use.SSL is enabled.
   * @return true if the metering service health endpoint returns a 200 and a "Healthy" message
   */
  public static boolean isMeteringServiceHealthy(Configuration conf) {
    SSLContext sslContext = null;
    try {
      sslContext = resolveClientSslContext(conf);
    } catch (IOException e) {
      LOG.warn("Unable to initialize metastore SSL context for metering health check", e);
    }
    for (URI meteringUri : resolveMeteringServiceUris(conf)) {
      if (isMeteringServiceHealthy(meteringUri, sslContext)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isMeteringServiceHealthy(URI meteringUri, SSLContext sslContext) {
    HttpURLConnection conn = null;
    try {
      URL url = meteringUri.toURL();
      conn = (HttpURLConnection) url.openConnection();
      DataSharing.applyClientSslContext(conn, sslContext);
      if ("https".equalsIgnoreCase(meteringUri.getScheme()) && sslContext == null) {
        LOG.debug("Metering HTTPS health check at {} using JVM default truststore because "
            + "metastore client SSL context is unavailable", meteringUri);
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

  private static class MeteringEndpoint {
    private final String host;
    private final String scheme;
    private final int port;
    @SuppressWarnings("unused")
    private final String source;

    MeteringEndpoint(String host, String scheme, int port, String source) {
      this.host = host;
      this.scheme = scheme;
      this.port = port;
      this.source = source;
    }

    URI uri() {
      return URI.create(String.format("%s://%s:%d%s",
          scheme, host, port, HEALTH_ENDPOINT));
    }
  }
}
