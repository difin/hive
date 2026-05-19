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
 * Azure credential provider for data sharing.
 */
public class AzureCredentialProvider extends DataSharing {
    private static final Logger LOG = LoggerFactory.getLogger(AzureCredentialProvider.class);

    // Azure-specific configuration
    public static final String CONFVAR_AZURE_ACCESS_PATH = "hive.metastore.catalog.idbroker.azure_access_path";
    public static final String CONFVAR_AZURE_STORAGE_ACCOUNT = "azure.storage.account";

    // Azure IdBroker response keys
    public static final String TOKEN_TYPE = "token_type";
    public static final String EXPIRES_ON = "expires_on";
    public static final String USER_DELEGATION_SAS = "user_delegation_SAS";

    // Iceberg ADLS FileIO: keys match ADLSLocation.host() (e.g. account.dfs.core.windows.net)
    public static final String ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";
    /** Same suffix pattern as {@code org.apache.iceberg.azure.AzureProperties#ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX}. */
    public static final String ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX = "adls.sas-token-expires-at-ms.";

    // Default host suffix when {@link #CONFVAR_AZURE_STORAGE_ACCOUNT} is used without {@code @} in the URI
    private static final String DFS_CORE_WINDOWS_NET = ".dfs.core.windows.net";

    private final String accessTokenPath;
    private final String configuredStorageAccount;

    public AzureCredentialProvider(Configuration conf) throws IOException {
        super(conf);
        // Default path follows Knox IDBroker pattern: {topology}/cab/api/v1/credentials
        // Knox will resolve the role from the authenticated user's mapping in the topology.
        // Using /credentials endpoint (not /credentials/role/{roleId}) so Knox uses user identity.
        String apath = conf.get(CONFVAR_AZURE_ACCESS_PATH,
                conf.get(CONFVAR_IDBROKER_ACCESS_PATH, "azure-cab/cab/api/v1/credentials"));
        accessTokenPath = composable(apath);
        configuredStorageAccount = conf.get(CONFVAR_AZURE_STORAGE_ACCOUNT);
        LOG.info("AzureCredentialProvider initialized with access path: {}, storage account: {}",
                accessTokenPath, configuredStorageAccount);
    }

    @Override
    protected Map<String, String> fetchCredentials(IdBrokerDelegationToken idt, String table, String policy)
            throws IOException {
        LOG.info("==> AzureCredentialProvider.fetchCredentials(), policy={}", policy);
        Map<String, String> tokenRes = new HashMap<>();
        try {
            AzureLocation location = parseAzureLocation(table);
            URL url = accessTokenUrlWithPolicy(location.resourcePath, policy);
            Object result = clientCall(idt.getDelegationToken(), url, HTTP_GET, null);

            if (result instanceof Map) {
                Map<?, ?> resultMap = (Map<?, ?>) result;
                String tokenType = resultMap.get(TOKEN_TYPE) != null ? resultMap.get(TOKEN_TYPE).toString() : null;
                String sasToken = resultMap.get(ACCESS_TOKEN) != null ? resultMap.get(ACCESS_TOKEN).toString() : null;
                String expiresOn = resultMap.get(EXPIRES_ON) != null ? resultMap.get(EXPIRES_ON).toString() : null;

                LOG.info("==> AzureCredentialProvider.fetchCredentials(), token_type={}, expires_on={}",
                        tokenType, expiresOn);

                if (sasToken != null) {
                    if (tokenType != null && !USER_DELEGATION_SAS.equals(tokenType)) {
                        LOG.warn("Unexpected token_type {} from IdBroker (expected {})", tokenType, USER_DELEGATION_SAS);
                    }
                    // Iceberg AzureProperties looks up SAS by ADLS host, same as ADLSFileIO / ADLSLocation#host()
                    String sasConfigKey = ADLS_SAS_TOKEN_PREFIX + location.host;
                    tokenRes.put(sasConfigKey, sasToken);
                    if (expiresOn != null && !expiresOn.isEmpty()) {
                        Long expiresAtMs = parseExpiresOnEpochMillis(expiresOn);
                        if (expiresAtMs != null) {
                            tokenRes.put(ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + location.host,
                                    Long.toString(expiresAtMs));
                        }
                    }
                    LOG.debug("Set {} for host {}", sasConfigKey, location.host);
                } else {
                    LOG.info("<== AzureCredentialProvider.fetchCredentials(): SAS token is null in response");
                }
            }
        } catch (IOException e) {
            LOG.error("Exception fetching Azure SAS token with policy", e);
        }
        LOG.info("<== AzureCredentialProvider.fetchCredentials(): {}", tokenRes.isEmpty() ? "failed" : "successful");
        return tokenRes;
    }

    /**
     * Compose the access-token URL with resource path and policy parameters.
     * The resource path is in the format "container/blobpath".
     *
     * <p>Knox IDBroker endpoint pattern:
     * {@code {topology}/cab/api/v1/credentials?path={container/path}&policy={policy}}
     *
     * <p>Knox will resolve the Azure role from the authenticated user's identity
     * using the user-to-role mapping configured in the Knox topology.
     */
    private URL accessTokenUrlWithPolicy(String resourcePath, String policy) throws IOException {
        int idx = lastKnownGood.get();
        URL url = resolve(idx, accessTokenPath + "?path=" + urlEncodeUTF8(resourcePath)
                + "&policy=" + policy).toURL();
        LOG.debug("==> AzureCredentialProvider.accessTokenUrlWithPolicy() returning: {}", url);
        return url;
    }

    /**
     * Parses IdBroker {@code expires_on} (typically Unix seconds) into epoch milliseconds for Iceberg.
     */
    static Long parseExpiresOnEpochMillis(String expiresOn) {
        try {
            String trimmed = expiresOn.trim();
            long v = Long.parseLong(trimmed);
            // Heuristic: sub-second timestamps are implausible; treat 10-digit values as seconds.
            return v < 10_000_000_000L ? v * 1000L : v;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Parses an Azure table location into its components.
     *
     * <p>Expected formats:
     * <ul>
     *   <li>{@code abfss://container@account.dfs.core.windows.net/path}</li>
     *   <li>{@code abfs://container@account.dfs.core.windows.net/path}</li>
     *   <li>{@code wasbs://container@account.blob.core.windows.net/path}</li>
     * </ul>
     *
     * @param tableLocation the full table location URI
     * @return the parsed Azure location
     */
    AzureLocation parseAzureLocation(String tableLocation) {
        String location = tableRoot(tableLocation);

        // Strip scheme: "container@account.dfs.core.windows.net/path"
        String withoutScheme = stripScheme(location);
        if (withoutScheme == null) {
            throw new IllegalArgumentException("Cannot parse Azure location: " + tableLocation);
        }

        // Parse: container@account.dfs.core.windows.net/path
        int atIdx = withoutScheme.indexOf('@');
        if (atIdx <= 0) {
            // No @ sign - try using configured storage account with container/path format
            if (configuredStorageAccount != null) {
                int slashIdx = withoutScheme.indexOf('/');
                String container = slashIdx > 0 ? withoutScheme.substring(0, slashIdx) : withoutScheme;
                String host = configuredStorageAccount + DFS_CORE_WINDOWS_NET;
                return new AzureLocation(configuredStorageAccount, container, withoutScheme, host);
            }
            throw new IllegalArgumentException(
                    "Cannot parse Azure location (expected abfss://container@account...): " + tableLocation);
        }

        String container = withoutScheme.substring(0, atIdx);
        String remainder = withoutScheme.substring(atIdx + 1); // "account.dfs.core.windows.net/path"

        int slashIdx = remainder.indexOf('/');
        String host = slashIdx > 0 ? remainder.substring(0, slashIdx) : remainder;
        String blobPath = slashIdx > 0 ? remainder.substring(slashIdx + 1) : "";

        // Extract storage account name from host (remove .dfs.core.windows.net or .blob.core.windows.net)
        String storageAccount;
        int dotIdx = host.indexOf('.');
        if (dotIdx > 0) {
            storageAccount = host.substring(0, dotIdx);
        } else {
            storageAccount = host;
        }

        // Resource path for IdBroker: container/blobpath
        String resourcePath = blobPath.isEmpty() ? container : container + "/" + blobPath;
        return new AzureLocation(storageAccount, container, resourcePath, host);
    }

    /**
     * Parsed components of an Azure storage location.
     */
    static class AzureLocation {
        final String storageAccount;
        final String container;
        final String resourcePath; // container/path for IdBroker
        /** Storage service hostname (Iceberg {@code ADLSLocation#host()}, e.g. {@code acct.dfs.core.windows.net}). */
        final String host;

        AzureLocation(String storageAccount, String container, String resourcePath, String host) {
            this.storageAccount = storageAccount;
            this.container = container;
            this.resourcePath = resourcePath;
            this.host = host;
        }
    }
}