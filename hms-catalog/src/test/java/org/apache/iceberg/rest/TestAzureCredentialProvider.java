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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestAzureCredentialProvider {

  private static AzureCredentialProvider newProvider() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DataSharing.CONFVAR_IDBROKER_URL, "https://localhost:8444/gateway/");
    return new AzureCredentialProvider(conf);
  }

  @Test
  public void parseAbfssLocationYieldsDfsHostForIcebergLookup() throws Exception {
    AzureCredentialProvider p = newProvider();
    AzureCredentialProvider.AzureLocation loc = p.parseAzureLocation(
        "abfss://c1@myacct.dfs.core.windows.net/data/db/tbl");
    assertEquals("myacct", loc.storageAccount);
    assertEquals("c1", loc.container);
    assertEquals("c1/data/db/tbl", loc.resourcePath);
    assertEquals("myacct.dfs.core.windows.net", loc.host);
  }

  @Test
  public void parseWasbsLocationYieldsBlobHost() throws Exception {
    AzureCredentialProvider p = newProvider();
    AzureCredentialProvider.AzureLocation loc = p.parseAzureLocation(
        "wasbs://c1@myacct.blob.core.windows.net/data");
    assertEquals("myacct.blob.core.windows.net", loc.host);
  }

  @Test
  public void parseWithConfiguredStorageAccountUsesDfsHostSuffix() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DataSharing.CONFVAR_IDBROKER_URL, "https://localhost:8444/gateway/");
    conf.set(AzureCredentialProvider.CONFVAR_AZURE_STORAGE_ACCOUNT, "cfgacct");
    AzureCredentialProvider p = new AzureCredentialProvider(conf);
    AzureCredentialProvider.AzureLocation loc = p.parseAzureLocation("mycontainer/path/to/table");
    assertEquals("cfgacct", loc.storageAccount);
    assertEquals("mycontainer", loc.container);
    assertEquals("cfgacct.dfs.core.windows.net", loc.host);
  }

  @Test
  public void expiresOnSecondsConvertedToMillis() {
    assertEquals(Long.valueOf(1_700_000_000_000L),
        AzureCredentialProvider.parseExpiresOnEpochMillis("1700000000"));
  }

  @Test
  public void expiresOnMillisPassedThrough() {
    assertEquals(Long.valueOf(1_700_000_000_000L),
        AzureCredentialProvider.parseExpiresOnEpochMillis("1700000000000"));
  }

  @Test
  public void expiresOnInvalidReturnsNull() {
    assertNull(AzureCredentialProvider.parseExpiresOnEpochMillis("not-a-number"));
  }
}
