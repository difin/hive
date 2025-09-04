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

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHMSGroup {
  private static final List<String> G09 = Collections.singletonList("0123456789");

  @After
  public void cleanup() {
    // Clean up thread local state after each test to prevent interference
    HMSGroup.setGroups(null);
  }

  @Test
  public void testSetGet() {
    HMSGroup.set("henrib", "0123456789");
    Assert.assertEquals(G09, HMSGroup.get("henrib"));
    String json = HMSGroup.getGroups();
    Assert.assertNotNull(json);
    json = json.replace("henrib", "naveen");
    HMSGroup.setGroups(json);
    Assert.assertEquals(G09, HMSGroup.get("naveen"));
  }

  @Test
  public void testMultipleGroups() {
    // Test setting and getting multiple groups for a user
    HMSGroup.set("testuser", "group1", "group2", "group3");
    List<String> groups = HMSGroup.get("testuser");
    Assert.assertNotNull("Groups should not be null", groups);
    Assert.assertEquals("Should have 3 groups", 3, groups.size());
    Assert.assertTrue("Should contain group1", groups.contains("group1"));
    Assert.assertTrue("Should contain group2", groups.contains("group2"));
    Assert.assertTrue("Should contain group3", groups.contains("group3"));
  }

  @Test
  public void testEmptyGroups() {
    // Test setting empty groups
    HMSGroup.set("emptyuser");
    List<String> groups = HMSGroup.get("emptyuser");
    Assert.assertNotNull("Groups should not be null", groups);
    Assert.assertTrue("Groups should be empty", groups.isEmpty());
  }

  @Test
  public void testNullAndEmptyInputs() {
    // Test behavior with null user
    List<String> nullUserGroups = HMSGroup.get(null);
    Assert.assertNull("Should return null for null user when no groups set", nullUserGroups);

    // Test behavior with non-existent user
    List<String> nonExistentGroups = HMSGroup.get("nonexistent");
    Assert.assertNull("Should return null for non-existent user", nonExistentGroups);

    // Test setting groups to null
    HMSGroup.setGroups(null);
    Assert.assertNull("Should return null after clearing groups", HMSGroup.getGroups());
  }

  @Test
  public void testInvalidJsonHandling() {
    // Test deserialization with invalid JSON
    Map<String, List<String>> result = HMSGroup.deserializeGroups("invalid-json");
    Assert.assertNull("Should return null for invalid JSON", result);

    // Test deserialization with empty string
    result = HMSGroup.deserializeGroups("");
    Assert.assertNull("Should return null for empty string", result);

    // Test deserialization with null
    result = HMSGroup.deserializeGroups(null);
    Assert.assertNull("Should return null for null input", result);

    // Test setting invalid JSON
    HMSGroup.setGroups("invalid-json-string");
    Assert.assertNull("Groups should be null after setting invalid JSON", HMSGroup.getGroups());
  }

  @Test
  public void testSerializationDeserialization() {
    // Test round-trip serialization and deserialization
    Map<String, List<String>> originalGroups = new HashMap<>();
    originalGroups.put("user1", Arrays.asList("group1", "group2"));
    originalGroups.put("user2", Arrays.asList("group3"));
    originalGroups.put("user3", Collections.emptyList());

    // Serialize
    String serialized = HMSGroup.serializeGroups(originalGroups);
    Assert.assertNotNull("Serialized groups should not be null", serialized);
    Assert.assertTrue("Serialized string should contain JSON", serialized.contains("{"));

    // Deserialize
    Map<String, List<String>> deserialized = HMSGroup.deserializeGroups(serialized);
    Assert.assertNotNull("Deserialized groups should not be null", deserialized);
    Assert.assertEquals("Should have same number of users", originalGroups.size(), deserialized.size());

    // Verify each user's groups
    for (String user : originalGroups.keySet()) {
      Assert.assertEquals("Groups should match for user " + user, originalGroups.get(user), deserialized.get(user));
    }
  }

  @Test
  public void testSerializeNullAndEmpty() {
    // Test serializing null groups
    String nullSerialized = HMSGroup.serializeGroups(null);
    Assert.assertNull("Should return null for null groups", nullSerialized);

    // Test serializing empty map
    String emptySerialized = HMSGroup.serializeGroups(Collections.emptyMap());
    Assert.assertNull("Should return null for empty groups map", emptySerialized);
  }

  @Test
  public void testThreadLocalIsolation() {
    // Test that thread local storage properly isolates groups between threads
    final String[] results = new String[2];
    final Exception[] exceptions = new Exception[2];

    Thread thread1 = new Thread(() -> {
      try {
        HMSGroup.set("thread1user", "thread1group");
        Thread.sleep(100); // Give other thread time to set its groups
        List<String> groups = HMSGroup.get("thread1user");
        results[0] = groups != null ? groups.get(0) : "null";
      } catch (Exception e) {
        exceptions[0] = e;
      }
    });

    Thread thread2 = new Thread(() -> {
      try {
        HMSGroup.set("thread2user", "thread2group");
        Thread.sleep(100); // Give other thread time to set its groups
        List<String> groups = HMSGroup.get("thread2user");
        results[1] = groups != null ? groups.get(0) : "null";
      } catch (Exception e) {
        exceptions[1] = e;
      }
    });

    thread1.start();
    thread2.start();

    try {
      thread1.join();
      thread2.join();
    } catch (InterruptedException e) {
      Assert.fail("Thread interrupted: " + e.getMessage());
    }

    // Check for exceptions
    Assert.assertNull("Thread 1 should not have thrown exception", exceptions[0]);
    Assert.assertNull("Thread 2 should not have thrown exception", exceptions[1]);

    // Verify thread isolation
    Assert.assertEquals("Thread 1 should have its own groups", "thread1group", results[0]);
    Assert.assertEquals("Thread 2 should have its own groups", "thread2group", results[1]);
  }

  @Test
  public void testGroupsJsonFormat() {
    // Test that the JSON format is as expected
    HMSGroup.set("jsontest", "group1", "group2");
    String json = HMSGroup.getGroups();

    Assert.assertNotNull("JSON should not be null", json);
    Assert.assertTrue("JSON should contain user key", json.contains("jsontest"));
    Assert.assertTrue("JSON should contain group1", json.contains("group1"));
    Assert.assertTrue("JSON should contain group2", json.contains("group2"));
    Assert.assertTrue("JSON should be valid JSON format", json.startsWith("{") && json.endsWith("}"));
  }

  @Test
  public void testConcurrentAccess() {
    // Test concurrent access to the same thread's groups
    final String testUser = "concurrentuser";
    final String[] groups = {"group1", "group2", "group3"};

    HMSGroup.set(testUser, groups);

    // Multiple rapid accesses should all return the same result
    for (int i = 0; i < 100; i++) {
      List<String> retrieved = HMSGroup.get(testUser);
      Assert.assertNotNull("Groups should not be null on iteration " + i, retrieved);
      Assert.assertEquals("Should have correct number of groups on iteration " + i, 3, retrieved.size());
    }
  }
}
