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
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;

/**
 * Tests for DataSharing utility functions that don't require HMS setup.
 */
public class TestDataSharingUtil {

  /**
   * Test URI resolution with a single broker URL.
   */
  @Test
  public void testUrlResolve() {
    String brokerUrlStr = "https://localhost:8444/gateway/";
    URI brokerUri = URI.create(brokerUrlStr);
    URI fetchToken = brokerUri
        .resolve("aws-cab/cab/api/v1/credentials")
        .resolve("role/datalake-admin-role?path=" + DataSharing.urlEncodeUTF8("s3a://bucket/partition/table0"));
    String fetchStr = fetchToken.toString();
    Assert.assertNotNull(fetchStr);
  }

  /**
   * Test that DataSharing can be initialized with multiple broker URLs.
   */
  @Test
  public void testMultipleBrokerUrlInit() {
    String brokerUrlsStr = "https://broker1:8444/gateway/,https://broker2:8444/gateway/";
    Configuration conf = new Configuration();
    conf.set("hive.metastore.catalog.idbroker.url", brokerUrlsStr);

    try {
      DataSharing ds = new DataSharing(conf);
      Assert.assertNotNull(ds);
    } catch (Exception e) {
      Assert.fail("DataSharing should initialize successfully with multiple broker URLs: " + e.getMessage());
    }
  }

  /**
   * Test that NotAuthorizedException is properly propagated from runAsHive.
   */
  @Test
  public void testRunAsHiveExceptionPropagation() {
    try {
      HMSCatalogAdapter.runAsHive(() -> {
        throw new NotAuthorizedException("Test exception");
      });
      Assert.fail("Should have thrown NotAuthorizedException");
    } catch (NotAuthorizedException e) {
      Assert.assertEquals("Test exception", e.getMessage());
    }
  }
}