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
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link OzoneCredentialProvider#normalizePolicyPath}.
 *
 * <p>Regression guard for the "double s3v" bug: the IdBroker policy path for an ofs:// table must
 * be the S3 Gateway <em>link bucket</em> path ({@code volume-bucket/key}), NOT the native
 * {@code volume/bucket}. Sending the native path scopes the vended STS token to
 * {@code bucket=<volume>}, which Ozone maps under the s3v volume as {@code /s3v/s3v...} — a path the
 * client never touches, so every request is AccessDenied. The policy path must match the vended
 * credential {@code prefix} (which uses the same {@code generateS3BucketName}).
 */
public class TestOzoneCredentialProvider {

  private static final String IDBROKER_URL = "http://localhost:18080/gateway";
  private static final String S3_GATEWAY = "https://s3g.example.com";

  @Test
  public void pathStyleAccessDefaultsToTrue() throws Exception {
    assertEquals(
        "true",
        fetchCredentials(new Configuration(false))
            .get(OzoneCredentialProvider.S3_PATH_STYLE_ACCESS));
  }

  @Test
  public void pathStyleAccessCanBeDisabled() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(OzoneCredentialProvider.CONFVAR_OZONE_S3_PATH_STYLE_ACCESS, false);

    assertEquals(
        "false",
        fetchCredentials(conf).get(OzoneCredentialProvider.S3_PATH_STYLE_ACCESS));
  }

  @Test
  public void ofsLocationMapsToLinkBucketPath() {
    assertEquals(
        "s3v-iceberg/warehouse/default/emp_data.db/departments",
        OzoneCredentialProvider.normalizePolicyPath(
            "ofs://ozone1787597340/s3v/iceberg/warehouse/default/emp_data.db/departments"));
  }

  @Test
  public void ofsLocationWithOmPortMapsToLinkBucketPath() {
    assertEquals(
        "hive-warehouse/db/table",
        OzoneCredentialProvider.normalizePolicyPath("ofs://ozone1:9862/hive/warehouse/db/table"));
  }

  @Test
  public void ofsVolumeBucketRootMapsToLinkBucket() {
    assertEquals(
        "vol-bkt",
        OzoneCredentialProvider.normalizePolicyPath("ofs://om/vol/bkt"));
  }

  @Test
  public void ofsPolicyPathNeverContainsNativeVolumeSlashBucket() {
    // Explicitly assert the buggy "s3v/iceberg" form is NOT produced.
    String path = OzoneCredentialProvider.normalizePolicyPath(
        "ofs://ozone1/s3v/iceberg/warehouse/tbl");
    assertEquals("s3v-iceberg/warehouse/tbl", path);
  }

  @Test
  public void s3aLocationFallsBackToSchemeStrip() {
    assertEquals(
        "s3v-iceberg/warehouse/tbl",
        OzoneCredentialProvider.normalizePolicyPath("s3a://s3v-iceberg/warehouse/tbl"));
  }

  @Test
  public void nullLocationYieldsEmpty() {
    assertEquals("", OzoneCredentialProvider.normalizePolicyPath(null));
  }

  private static Map<String, String> fetchCredentials(Configuration conf) throws Exception {
    conf.set(DataSharing.CONFVAR_IDBROKER_URL, IDBROKER_URL);
    conf.set(OzoneCredentialProvider.CONFVAR_OZONE_S3_GATEWAY, S3_GATEWAY);
    Map<String, Object> credentials = new HashMap<>();
    credentials.put("AccessKeyId", "access-key");
    credentials.put("SecretAccessKey", "secret-key");
    credentials.put("SessionToken", "session-token");
    Map<String, Object> response = new HashMap<>();
    response.put("Credentials", credentials);
    OzoneCredentialProvider provider = new StubOzoneCredentialProvider(conf, response);
    return provider.fetchCredentials(
        new DataSharing.IdBrokerDelegationToken("delegation-token", Long.MAX_VALUE),
        "s3a://bucket/table",
        "read-only");
  }

  private static final class StubOzoneCredentialProvider extends OzoneCredentialProvider {
    private final Object response;

    private StubOzoneCredentialProvider(Configuration conf, Object response) throws IOException {
      super(conf);
      this.response = response;
    }

    @Override
    public Object clientCall(String jwt, URL url, String method, Object arg) {
      return response;
    }
  }
}
