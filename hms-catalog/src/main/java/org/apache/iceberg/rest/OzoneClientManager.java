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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Manages Ozone client lifecycle and provides access to ObjectStore.
 * <p>
 * This class handles lazy initialization of OzoneClient and provides
 * thread-safe access to Ozone ObjectStore for link bucket operations.
 */
public class OzoneClientManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(OzoneClientManager.class);

    public static final String CONF_OZONE_ENABLED = "hive.metastore.catalog.ozone.enabled";
    public static final String CONF_OZONE_OM_ADDRESS = "hive.metastore.catalog.ozone.om.address";

    private final Configuration conf;
    private final String omAddress;
    private final boolean enabled;
    private volatile OzoneClient ozoneClient;
    private volatile ObjectStore objectStore;
    private final Object initLock = new Object();

    public OzoneClientManager(Configuration conf) {
        this.conf = conf;
        this.enabled = conf.getBoolean(CONF_OZONE_ENABLED, false);
        this.omAddress = conf.get(CONF_OZONE_OM_ADDRESS);

        if (enabled) {
            if (omAddress == null || omAddress.trim().isEmpty()) {
                LOG.warn("Ozone is enabled but '{}' is not configured. " +
                         "Ozone features will be disabled.", CONF_OZONE_OM_ADDRESS);
            } else {
                LOG.info("Ozone client manager initialized. OM address: {}", omAddress);
            }
        } else {
            LOG.debug("Ozone integration is disabled");
        }
    }

    /**
     * Check if Ozone integration is enabled and properly configured
     *
     * @return true if Ozone is enabled and OM address is configured
     */
    public boolean isEnabled() {
        return enabled && omAddress != null && !omAddress.trim().isEmpty();
    }

    /**
     * Get or create OzoneClient (lazy initialization with double-checked locking)
     *
     * @return OzoneClient instance
     * @throws IOException if client creation fails
     */
    public OzoneClient getOzoneClient() throws IOException {
        if (!isEnabled()) {
            throw new IllegalStateException(
                "Ozone is not enabled or not properly configured. " +
                "Set '" + CONF_OZONE_ENABLED + "=true' and '" +
                CONF_OZONE_OM_ADDRESS + "=<om-address>'");
        }

        if (ozoneClient == null) {
            synchronized (initLock) {
                if (ozoneClient == null) {
                    LOG.info("Initializing Ozone client for OM: {}", omAddress);
                    ozoneClient = createOzoneClientWithRetry();
                    LOG.info("Ozone client initialized successfully");
                }
            }
        }

        return ozoneClient;
    }

    /**
     * Get ObjectStore (lazy initialization)
     *
     * @return ObjectStore instance
     * @throws IOException if ObjectStore cannot be obtained
     */
    public ObjectStore getObjectStore() throws IOException {
        if (objectStore == null) {
            synchronized (initLock) {
                if (objectStore == null) {
                    objectStore = getOzoneClient().getObjectStore();
                    LOG.debug("ObjectStore obtained from OzoneClient");
                }
            }
        }
        return objectStore;
    }

    /**
     * Create OzoneClient with retry logic
     *
     * @return OzoneClient instance
     * @throws IOException if all retry attempts fail
     */
    private OzoneClient createOzoneClientWithRetry() throws IOException {
        int maxRetries = 3;
        long retryDelayMs = 1000;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                OzoneConfiguration ozoneConf = new OzoneConfiguration(conf);
                ozoneConf.set("ozone.om.address", omAddress);

                // Use RPC client for administrative operations
                OzoneClient client = OzoneClientFactory.getRpcClient(omAddress, ozoneConf);

                LOG.info("Successfully created Ozone RPC client on attempt {}/{}", attempt, maxRetries);
                return client;

            } catch (IOException e) {
                if (attempt == maxRetries) {
                    LOG.error("Failed to create Ozone client after {} attempts", maxRetries, e);
                    throw new IOException(
                        "Cannot connect to Ozone Manager at " + omAddress +
                        " after " + maxRetries + " attempts", e);
                }

                LOG.warn("Ozone client creation failed on attempt {}/{}: {}. Retrying in {}ms...",
                         attempt, maxRetries, e.getMessage(), retryDelayMs);

                try {
                    Thread.sleep(retryDelayMs * attempt); // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while retrying Ozone client creation", ie);
                }
            }
        }

        // Should never reach here
        throw new IOException("Failed to create Ozone client");
    }

    /**
     * Get Ozone Manager address
     *
     * @return OM address or null if not configured
     */
    public String getOmAddress() {
        return omAddress;
    }

    @Override
    public void close() throws IOException {
        synchronized (initLock) {
            if (ozoneClient != null) {
                LOG.info("Closing Ozone client");
                try {
                    ozoneClient.close();
                } finally {
                    ozoneClient = null;
                    objectStore = null;
                }
            }
        }
    }
}
