/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.benmanes.caffeine.cache.Cache;

public class TestHMSServletSecurityUGICache {

  private HMSServletSecurity security;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    String cacheKeyTTL = MetastoreConf.ConfVars.AGGREGATE_STATS_CACHE_TTL.getVarname();
    String cacheKeySize = MetastoreConf.ConfVars.SERVER_MAX_THREADS.getVarname();
    String trustHeaderKey = MetastoreConf.ConfVars.METASTORE_TRUSTED_PROXY_TRUSTHEADER.getVarname();

    conf.setLong(cacheKeyTTL, 600L);
    conf.setInt(cacheKeySize, 1000);
    conf.set(trustHeaderKey, "X-Knox-Actor");
    security = new HMSServletSecurity(conf, true);
  }

  @After
  public void tearDown() throws Exception {
    HMSGroup.setGroups(null);
  }

  @Test
  public void testUgiKeyEquality() {
    HMSServletSecurity.UgiKey key1 = new HMSServletSecurity.UgiKey("user1", "login1");
    HMSServletSecurity.UgiKey key2 = new HMSServletSecurity.UgiKey("user1", "login1");
    HMSServletSecurity.UgiKey key3 = new HMSServletSecurity.UgiKey("user1", "login2");

    Assert.assertEquals("Same keys should be equal", key1, key2);
    Assert.assertNotEquals("Different keys should not be equal", key1, key3);
    Assert.assertEquals("Same keys should have same hashCode", key1.hashCode(), key2.hashCode());

    Assert.assertEquals("Real user should match", "user1", key1.getRealUser());
    Assert.assertEquals("Login user should match", "login1", key1.getLoginUser());
  }

  @Test
  public void testCacheInitialization() {
    Cache<HMSServletSecurity.UgiKey, UserGroupInformation> cache = security.getUgiCache();
    Assert.assertNotNull("UGI cache should be initialized", cache);
  }

  @Test
  public void testUgiCachingAndReuse() throws Exception {
    UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();
    String testUser = "test_user";

    UserGroupInformation ugi1 = security.getUgi(testUser, loginUser);
    UserGroupInformation ugi2 = security.getUgi(testUser, loginUser);

    Assert.assertNotNull("UGI should not be null", ugi1);
    Assert.assertSame("Same user should return cached UGI", ugi1, ugi2);
    Assert.assertEquals("UGI should have correct username", testUser, ugi1.getUserName());
  }

  @Test
  public void testDifferentUsersGetDifferentUgis() throws Exception {
    UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();

    UserGroupInformation ugi1 = security.getUgi("user1", loginUser);
    UserGroupInformation ugi2 = security.getUgi("user2", loginUser);

    Assert.assertNotSame("Different users should get different UGIs", ugi1, ugi2);
    Assert.assertEquals("UGI1 should have correct username", "user1", ugi1.getUserName());
    Assert.assertEquals("UGI2 should have correct username", "user2", ugi2.getUserName());
  }

  @Test
  public void testTimeBasedEviction() throws Exception {
    Cache<HMSServletSecurity.UgiKey, UserGroupInformation> shortCache = security.createTableCacheForTesting(50L, 10);

    UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();
    HMSServletSecurity.UgiKey key = new HMSServletSecurity.UgiKey("expiry_user", loginUser.getUserName());

    UserGroupInformation ugi1 = shortCache.get(key, v -> UserGroupInformation.createProxyUser("expiry_user", loginUser));
    UserGroupInformation ugi2 = shortCache.get(key, v -> UserGroupInformation.createProxyUser("expiry_user", loginUser));
    Assert.assertSame("Should return cached UGI before expiry", ugi1, ugi2);

    Thread.sleep(100);

    UserGroupInformation ugi3 = shortCache.get(key, v -> UserGroupInformation.createProxyUser("expiry_user", loginUser));
    Assert.assertNotSame("Should create new UGI after expiry", ugi1, ugi3);
  }

  @Test
  public void testSizeBasedEviction() throws Exception {
    UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();
    int testCacheSize = 3;
    Cache<HMSServletSecurity.UgiKey, UserGroupInformation> testCache = security.createTableCacheForTesting(60000L, testCacheSize);

    for (int i = 0; i < testCacheSize; i++) {
      final String userName = "user_" + i;
      HMSServletSecurity.UgiKey key = new HMSServletSecurity.UgiKey(userName, loginUser.getUserName());
      testCache.get(key, v -> UserGroupInformation.createProxyUser(userName, loginUser));
    }

    Assert.assertEquals("Cache should be at maximum size", testCacheSize, testCache.estimatedSize());

    HMSServletSecurity.UgiKey overflowKey = new HMSServletSecurity.UgiKey("overflow_user", loginUser.getUserName());
    testCache.get(overflowKey, v -> UserGroupInformation.createProxyUser("overflow_user", loginUser));

    Assert.assertTrue("Cache size should not exceed limit", testCache.estimatedSize() <= testCacheSize);
  }

  @Test
  public void testManualCacheInvalidation() throws Exception {
    UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();
    String testUser = "invalidation_user";

    UserGroupInformation ugi1 = security.getUgi(testUser, loginUser);
    UserGroupInformation ugi2 = security.getUgi(testUser, loginUser);
    Assert.assertSame("Should return cached UGI", ugi1, ugi2);

    security.getUgiCache().invalidateAll();

    UserGroupInformation ugi3 = security.getUgi(testUser, loginUser);
    Assert.assertNotSame("Should create new UGI after invalidation", ugi1, ugi3);
  }

  @Test
  public void testConcurrentAccess() throws Exception {
    UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();
    int threadCount = 5;
    int callsPerThread = 20;
    AtomicInteger successCount = new AtomicInteger(0);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      final String userId = "concurrent_user_" + (i % 2);
      executor.submit(() -> {
        try {
          for (int j = 0; j < callsPerThread; j++) {
            UserGroupInformation ugi = security.getUgi(userId, loginUser);
            Assert.assertNotNull("UGI should not be null", ugi);
            Assert.assertEquals("UGI should have correct username", userId, ugi.getUserName());
            successCount.incrementAndGet();
          }
        } catch (Exception e) {
          Assert.fail("Concurrent access failed: " + e.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    Assert.assertTrue("All threads should complete", latch.await(10, TimeUnit.SECONDS));
    Assert.assertEquals("All calls should succeed", threadCount * callsPerThread, successCount.get());
    executor.shutdown();
  }

  @Test
  public void testCacheConfiguration() {
    Cache<HMSServletSecurity.UgiKey, UserGroupInformation> configuredCache = security.createTableCacheForTesting(5000L, 100);
    Cache<HMSServletSecurity.UgiKey, UserGroupInformation> noExpiryCache = security.createTableCacheForTesting(0L, 50);

    Assert.assertNotNull("Configured cache should be created", configuredCache);
    Assert.assertNotNull("No-expiry cache should be created", noExpiryCache);
  }
}
