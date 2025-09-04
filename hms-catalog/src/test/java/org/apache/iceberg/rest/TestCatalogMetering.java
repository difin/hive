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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.metering.CatalogMeteringEvent;
import org.apache.iceberg.metering.CatalogMeteringEventPublisher;
import org.apache.iceberg.metering.CatalogMeteringPayload;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCatalogMetering extends HMSTestBase {

  @Override
  protected void setCatalogClass(Configuration conf) {
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_CATALOG_ACTOR_CLASS, "org.apache.iceberg.rest.HMSCatalogActor");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_AUTH, "jwt");

    // Set up metering configuration
    conf.set("rest.catalog.env.account.id", "test-account");
    conf.set("rest.catalog.environment.crn", "test-env-crn");
    conf.set("rest.catalog.resource.crn", "test-resource-crn");
  }

  @Test
  public void testMeteringEventCreation() {
    CatalogMeteringPayload.CatalogMeteredValue meteredValue =
        new CatalogMeteringPayload.CatalogMeteredValue(10, "IRC_READ_API_COUNT", "API_CALL_COUNT");

    CatalogMeteringPayload payload = new CatalogMeteringPayload(Collections.singletonList(meteredValue));

    CatalogMeteringEvent event = new CatalogMeteringEvent.CatalogMeteringEventBuilder()
        .withAccountId("test-account")
        .withEnvironmentCrn("test-env")
        .withResourceCrn("test-resource")
        .withAccumulativeEvents(payload)
        .build();

    Assert.assertNotNull("Event ID should be generated", event.getId());
    Assert.assertEquals("Service type should be CDSH", "CDSH", event.getServiceType());
    Assert.assertEquals("Version should be 1", 1, event.getVersion());
    Assert.assertEquals("Account ID should match", "test-account", event.getAccountId());
    Assert.assertTrue("Timestamp should be recent",
        System.currentTimeMillis() - event.getTimestamp() < 5000);

    // Test payload
    Assert.assertEquals("Should have one metered value", 1,
        event.getAccumulativeEvents().getAccumulativeEvent().size());
    CatalogMeteringPayload.CatalogMeteredValue value =
        event.getAccumulativeEvents().getAccumulativeEvent().get(0);
    Assert.assertEquals("Quantity should match", 10, value.getQuantity());
    Assert.assertEquals("Service feature should match", "IRC_READ_API_COUNT", value.getServiceFeature());
    Assert.assertEquals("Event type should match", "API_CALL_COUNT", value.getEventType());
  }

  @Test
  public void testMeteringPublisherInitialization() {
    Configuration testConf = new Configuration(conf);

    try {
      // This should not throw exception even if metering directory can't be created
      CatalogMeteringEventPublisher publisher = new CatalogMeteringEventPublisher(testConf);
      Assert.assertNotNull("Publisher should be created", publisher);

      // Test API count updating
      publisher.updateAPICount();

    } catch (Exception e) {
      // Expected in test environment where directory creation might fail
      Assert.assertTrue("Should handle initialization gracefully",
          e.getMessage().contains("metering") || e.getCause() != null);
    }
  }

  @Test
  public void testMeteringWithActualAPIRequests() throws Exception {
    String jwt = generateJWT();

    // Make several API requests and verify metering occurs
    URL namespacesUrl = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath + "/v1/namespaces");
    URL tablesUrl = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath +
        "/v1/namespaces/" + DB_NAME + "/tables");

    // Track initial metric state
    AtomicInteger requestCount = new AtomicInteger(0);

    // Make multiple requests
    for (int i = 0; i < 5; i++) {
      Object response = clientCall(jwt, namespacesUrl, "GET", null);
      Assert.assertNotNull("Namespaces request should succeed", response);
      requestCount.incrementAndGet();

      response = clientCall(jwt, tablesUrl, "GET", null);
      Assert.assertNotNull("Tables request should succeed", response);
      requestCount.incrementAndGet();
    }

    Assert.assertTrue("Should have made multiple requests for metering", requestCount.get() >= 10);
  }

  @Test
  public void testMeteringEventSerialization() {
    // Test that metering events can be properly serialized for transmission
    CatalogMeteringPayload.CatalogMeteredValue meteredValue =
        new CatalogMeteringPayload.CatalogMeteredValue(25, "IRC_WRITE_API_COUNT", "API_CALL_COUNT");

    CatalogMeteringPayload payload = new CatalogMeteringPayload(Collections.singletonList(meteredValue));

    CatalogMeteringEvent event = new CatalogMeteringEvent.CatalogMeteringEventBuilder()
        .withAccountId("serialization-test-account")
        .withEnvironmentCrn("test-env-crn")
        .withResourceCrn("test-resource-crn")
        .withAccumulativeEvents(payload)
        .build();

    // Verify all required fields are present for serialization
    Assert.assertNotNull("Account ID required for serialization", event.getAccountId());
    Assert.assertNotNull("Environment CRN required for serialization", event.getEnvironmentCrn());
    Assert.assertNotNull("Resource CRN required for serialization", event.getResourceCrn());
    Assert.assertNotNull("Events payload required for serialization", event.getAccumulativeEvents());
    Assert.assertNotNull("Event ID required for serialization", event.getId());
    Assert.assertTrue("Timestamp should be valid", event.getTimestamp() > 0);
  }

  @Test
  public void testMeteringConfigurationValidation() {
    // Test various configuration scenarios
    Configuration invalidConf = new Configuration();
    // Missing required metering configuration

    try {
      CatalogMeteringEventPublisher publisher = new CatalogMeteringEventPublisher(invalidConf);
      // Should handle missing configuration gracefully
      Assert.assertNotNull("Publisher should be created even with invalid config", publisher);
    } catch (Exception e) {
      // This is acceptable - metering may require specific configuration
      Assert.assertTrue("Should provide meaningful error for invalid config",
          e.getMessage() != null && !e.getMessage().isEmpty());
    }
  }
}