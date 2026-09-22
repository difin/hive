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

import javax.net.ssl.HttpsURLConnection;

/**
 * Utility class to perform a single health check against the co-located metering service at
 * <scheme>://localhost:<port>/health (defaults: scheme=https, port=50052). Threading and retry
 * logic live in MeteringHealthWatcher; this class is purely responsible for the HTTP call itself.
 * If TLS is enabled for the metering service, co-located services often present a certificate
 * whose SAN matches a cluster FQDN rather than localhost. For loopback-only connections we relax
 * hostname verification (certificate chain still uses the JVM trust store).
 * The host is intentionally fixed to localhost to prevent nefarious users from overriding
 * the metering service location. The port can be overridden via JVM property
 * `metering.service.port`;
 */
public class MeteringServiceMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(MeteringServiceMonitor.class);

    static final String METERING_SERVICE_HOST = "localhost";
    static final int DEFAULT_METERING_SERVICE_PORT = 50052;
    private static final String METERING_SERVICE_PORT_SYS_PROP = "metering.service.port";
    static final int METERING_SERVICE_PORT = resolveMeteringServicePort();
    private static final String HEALTH_ENDPOINT = "/health";

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private MeteringServiceMonitor() {
        throw new UnsupportedOperationException("Utility class");
    }

    static int resolveMeteringServicePort() {
        String configuredPort = System.getProperty(METERING_SERVICE_PORT_SYS_PROP);
        if (configuredPort == null || configuredPort.trim().isEmpty()) {
            return DEFAULT_METERING_SERVICE_PORT;
        }

        String trimmedPort = configuredPort.trim();
        try {
            int parsedPort = Integer.parseInt(trimmedPort);
            if (parsedPort <= 0 || parsedPort > 65535) {
                throw new IllegalArgumentException("port out of range");
            }
            return parsedPort;
        } catch (Exception e) {
            LOG.warn("Invalid JVM property {}='{}'; using default metering port {}",
                METERING_SERVICE_PORT_SYS_PROP, configuredPort, DEFAULT_METERING_SERVICE_PORT, e);
            return DEFAULT_METERING_SERVICE_PORT;
        }
    }

    static String resolveMeteringServiceScheme(Configuration conf) {
        boolean sslEnabled = conf != null && MetastoreConf.getBoolVar(conf, ConfVars.USE_SSL);
        return sslEnabled ? "https" : "http";
    }

    /**
     * Check if the metering service is available and healthy.
     *
     * @param conf metastore configuration; SSL mode is derived from ConfVars.USE_SSL
     * @return true if the metering service health endpoint returns a 200 and a "Healthy" message
     */
    public static boolean isMeteringServiceHealthy(Configuration conf) {
        HttpURLConnection conn = null;
        try {
            String scheme = resolveMeteringServiceScheme(conf);
            URI meteringUri = URI.create(String.format("%s://%s:%d%s", scheme,
                METERING_SERVICE_HOST, METERING_SERVICE_PORT, HEALTH_ENDPOINT));
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
                        LOG.info("Metering service is healthy at {}://{}:{}",
                            scheme, METERING_SERVICE_HOST, METERING_SERVICE_PORT);
                        return true;
                    }
                }
            }

            LOG.warn("Metering service health check failed: {} response code at {}://{}:{}",
                responseCode, scheme, METERING_SERVICE_HOST, METERING_SERVICE_PORT);
        } catch (Exception e) {
            String scheme = resolveMeteringServiceScheme(conf);
            LOG.warn("Could not connect to metering service at {}://{}:{} => {}",
                scheme, METERING_SERVICE_HOST, METERING_SERVICE_PORT,
                e.getMessage(), e);
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
}
