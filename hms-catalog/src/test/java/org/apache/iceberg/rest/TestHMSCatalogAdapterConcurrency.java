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
import org.apache.iceberg.catalog.Catalog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Tests for concurrent behavior and request ID generation in HMSCatalogAdapter.
 */
public class TestHMSCatalogAdapterConcurrency {

  @Mock
  private Catalog mockCatalog;

  @Mock
  private Configuration mockConfiguration;

  private HMSCatalogAdapter adapter;
  private Method generateRequestIdMethod;

  // Pattern to validate request ID format: timestamp-4digitsequence
  private static final Pattern REQUEST_ID_PATTERN = Pattern.compile("\\d{13}-\\d{4}");

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    adapter = new HMSCatalogAdapter(mockCatalog, mockConfiguration);
    generateRequestIdMethod = HMSCatalogAdapter.class.getDeclaredMethod("generateRequestId");
    generateRequestIdMethod.setAccessible(true);
  }

  @Test
  public void testRequestIdFormat() throws Exception {
    String requestId = (String) generateRequestIdMethod.invoke(adapter);
    Assert.assertTrue("Request ID should match pattern timestamp-4digits: " + requestId,
        REQUEST_ID_PATTERN.matcher(requestId).matches());
    String[] parts = requestId.split("-");
    Assert.assertEquals("Should have exactly 2 parts separated by hyphen", 2, parts.length);

    long timestamp = Long.parseLong(parts[0]);
    int sequence = Integer.parseInt(parts[1]);
    long now = System.currentTimeMillis();
    Assert.assertTrue("Timestamp should be recent", Math.abs(now - timestamp) < 5000);
    Assert.assertTrue("Sequence should be between 0 and 9999", sequence >= 0 && sequence <= 9999);
    Assert.assertEquals("Sequence part should be exactly 4 characters", 4, parts[1].length());
  }

  @Test
  public void testConcurrentRequestIdGeneration() throws Exception {
    int numThreads = 10;
    int requestsPerThread = 100;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completeLatch = new CountDownLatch(numThreads);
    Set<String> allRequestIds = ConcurrentHashMap.newKeySet();
    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < requestsPerThread; j++) {
            String requestId = (String) generateRequestIdMethod.invoke(adapter);
            allRequestIds.add(requestId);
          }
        } catch (Exception e) {
          Assert.fail("Exception in concurrent request ID generation: " + e.getMessage());
        } finally {
          completeLatch.countDown();
        }
      });
    }
    startLatch.countDown();
    Assert.assertTrue("All threads should complete within reasonable time",
        completeLatch.await(10, TimeUnit.SECONDS));
    executor.shutdown();

    int expectedTotal = numThreads * requestsPerThread;
    Assert.assertEquals("All generated request IDs should be unique",
        expectedTotal, allRequestIds.size());

    for (String requestId : allRequestIds) {
      Assert.assertTrue("All request IDs should match expected pattern: " + requestId,
          REQUEST_ID_PATTERN.matcher(requestId).matches());
    }
  }

  @Test
  public void testRequestIdSequenceWrapping() throws Exception {
    String[] requestIds = new String[15];
    for (int i = 0; i < requestIds.length; i++) {
      requestIds[i] = (String) generateRequestIdMethod.invoke(adapter);
      Thread.sleep(1);
    }

    int[] sequences = new int[requestIds.length];
    for (int i = 0; i < requestIds.length; i++) {
      String[] parts = requestIds[i].split("-");
      sequences[i] = Integer.parseInt(parts[1]);
    }

    boolean foundIncrement = false;
    for (int i = 1; i < sequences.length; i++) {
      if (sequences[i] > sequences[i - 1]) {
        foundIncrement = true;
      }
      Assert.assertTrue("Sequence should be in range 0-9999: " + sequences[i],
          sequences[i] >= 0 && sequences[i] <= 9999);
    }

    Assert.assertTrue("Should see at least some sequence increments", foundIncrement);
  }

  @Test
  public void testRequestIdUniquenessAcrossTime() throws Exception {
    Set<String> requestIds = ConcurrentHashMap.newKeySet();

    for (int i = 0; i < 5; i++) {
      String requestId = (String) generateRequestIdMethod.invoke(adapter);
      Assert.assertTrue("Request ID should be unique: " + requestId,
          requestIds.add(requestId));

      Thread.sleep(10);
    }

    Assert.assertEquals("All request IDs should be unique", 5, requestIds.size());
  }
}
