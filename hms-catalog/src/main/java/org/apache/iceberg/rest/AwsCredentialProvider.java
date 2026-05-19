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
 * AWS credential provider for data sharing.
 * Fetches temporary S3 credentials (STS AssumeRole) from IdBroker
 * and maps them to Iceberg S3 FileIO config properties.
 */
public class AwsCredentialProvider extends DataSharing {
    private static final Logger LOG = LoggerFactory.getLogger(AwsCredentialProvider.class);

    // AWS-specific IdBroker response keys
    public static final String CREDENTIALS = "Credentials";
    public static final String SESSION_TOKEN = "SessionToken";
    public static final String ACCESS_KEY_ID = "AccessKeyId";
    public static final String SECRET_ACCESS_KEY = "SecretAccessKey";

    // Iceberg S3 FileIO config keys
    public static final String S3_SESSION_TOKEN = "s3.session-token";
    public static final String S3_ACCESS_KEY_ID = "s3.access-key-id";
    public static final String S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
    public static final String S3_REMOTE_SIGNING_ENABLED = "s3.remote-signing-enabled";
    public static final String CLIENT_REGION = "client.region";

    private final String accessTokenPath;
    private final String s3ClientRegion;
    private final String s3RemoteSigningEnabled;

    public AwsCredentialProvider(Configuration conf) throws IOException {
        super(conf);
        String apath = conf.get(CONFVAR_IDBROKER_ACCESS_PATH, "aws-cab/cab/api/v1/credentials");
        accessTokenPath = composable(apath);
        s3ClientRegion = conf.get(CLIENT_REGION, "us-west-2");
        s3RemoteSigningEnabled = conf.get(S3_REMOTE_SIGNING_ENABLED, "false");
        LOG.info("AwsCredentialProvider initialized with access path: {}", accessTokenPath);
    }

    @Override
    protected Map<String, String> fetchCredentials(IdBrokerDelegationToken idt, String table, String policy)
            throws IOException {
        LOG.info("==> AwsCredentialProvider.fetchCredentials(), policy={}", policy);
        Map<String, String> tokenRes = new HashMap<>();
        try {
            URL url = accessTokenUrlWithPolicy(table, policy);
            Object result = clientCall(idt.getDelegationToken(), url, HTTP_GET, null);
            if (result instanceof Map) {
                Map<?, ?> resultMap = (Map<?, ?>) result;

                // Log non-sensitive metadata
                Map<?, ?> assumedRoleUser = (Map<?, ?>) resultMap.get("AssumedRoleUser");
                String assumedRole = assumedRoleUser != null ? (String) assumedRoleUser.get("AssumedRole") : "N/A";
                String arn = assumedRoleUser != null ? (String) assumedRoleUser.get("Arn") : "N/A";
                LOG.info("==> AwsCredentialProvider.fetchCredentials(), broker response: AssumedRole={}, Arn={}",
                        assumedRole, arn);

                // Extract STS credentials
                Object credentials = resultMap.get(CREDENTIALS);
                if (credentials instanceof Map<?, ?>) {
                    Map<?, ?> credMap = (Map<?, ?>) credentials;
                    putIfNotNull(tokenRes, S3_SESSION_TOKEN, credMap.get(SESSION_TOKEN));
                    putIfNotNull(tokenRes, S3_ACCESS_KEY_ID, credMap.get(ACCESS_KEY_ID));
                    putIfNotNull(tokenRes, S3_SECRET_ACCESS_KEY, credMap.get(SECRET_ACCESS_KEY));
                }

                if (!tokenRes.isEmpty()) {
                    tokenRes.put(S3_REMOTE_SIGNING_ENABLED, s3RemoteSigningEnabled);
                    tokenRes.put(CLIENT_REGION, s3ClientRegion);
                }
            }
        } catch (IOException e) {
            LOG.error("Exception fetching AWS access token with policy", e);
        }
        LOG.info("<== AwsCredentialProvider.fetchCredentials(): {}", tokenRes.isEmpty() ? "failed" : "successful");
        return tokenRes;
    }

    /**
     * Compose the access-token URL with path and policy parameters.
     */
    private URL accessTokenUrlWithPolicy(String table, String policy) throws IOException {
        String tableRoot = tableRoot(table);
        // Strip the URI scheme (s3a://, s3://, ofs://, etc.) -> [bucket]/[prefix]
        tableRoot = stripScheme(tableRoot);
        int idx = lastKnownGood.get();
        URL url = resolve(idx, accessTokenPath + "?path=" + urlEncodeUTF8(tableRoot) + "&policy=" + policy).toURL();
        LOG.debug("==> AwsCredentialProvider.accessTokenUrlWithPolicy() returning: {}", url);
        return url;
    }

    private static void putIfNotNull(Map<String, String> map, String key, Object value) {
        if (value != null) {
            map.put(key, value.toString());
        } else {
            LOG.info("<== AwsCredentialProvider: {} value is null", key);
        }
    }
}