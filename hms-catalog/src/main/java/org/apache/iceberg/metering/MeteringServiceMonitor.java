package org.apache.iceberg.metering;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import javax.net.ssl.HttpsURLConnection;

/**
 * Utility class to perform a single health check against the co-located metering service at
 * https://localhost:50052/health. Threading and retry logic live in MeteringHealthWatcher;
 * this class is purely responsible for the HTTP call itself.
 * The health endpoint uses TLS; co-located services often present a certificate whose SAN
 * matches a cluster FQDN rather than localhost. For loopback-only connections we
 * relax hostname verification (certificate chain still uses the JVM trust store).
 * Note: This is intentionally not configurable to prevent nefarious users from overriding
 * the metering service location.
 */
public class MeteringServiceMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(MeteringServiceMonitor.class);

    static final String METERING_SERVICE_HOST = "localhost";
    static final int METERING_SERVICE_PORT = 50052;
    private static final String HEALTH_ENDPOINT = "/health";
    private static final URI METERING_URI = URI.create(
        String.format("https://%s:%d%s", METERING_SERVICE_HOST, METERING_SERVICE_PORT, HEALTH_ENDPOINT));

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private MeteringServiceMonitor() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Check if the metering service is available and healthy.
     *
     * @return true if the metering service health endpoint returns a 200 and a "Healthy" message
     */
    public static boolean isMeteringServiceHealthy() {
        HttpsURLConnection conn = null;
        try {
            URL url = METERING_URI.toURL();
            conn = (HttpsURLConnection) url.openConnection();
            conn.setHostnameVerifier((hostname, session) -> isLoopbackHostname(hostname));
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();

            if (responseCode == HttpsURLConnection.HTTP_OK) {
                try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    JsonObject response = JsonParser.parseReader(in).getAsJsonObject();
                    String message = response.has("message") ? response.get("message").getAsString() : "";
                    if ("Healthy".equalsIgnoreCase(message)) {
                        LOG.info("Metering service is healthy at {}:{}", METERING_SERVICE_HOST, METERING_SERVICE_PORT);
                        return true;
                    }
                }
            }

            LOG.warn("Metering service health check failed: {} response code at {}:{}",
                responseCode, METERING_SERVICE_HOST, METERING_SERVICE_PORT);
        } catch (Exception e) {
            LOG.warn("Could not connect to metering service at {}:{} => {}",
                METERING_SERVICE_HOST, METERING_SERVICE_PORT, e.getMessage(), e);
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
