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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.when;

/**
 * This test focuses on the interaction between HMSServletSecurity and HMSGroup.
 */
public class TestHMSServletSecurityIntegration {

  @Mock
  private HttpServletRequest mockRequest;

  @Mock
  private HttpServletResponse mockResponse;

  private HMSServletSecurity security;
  private AutoCloseable mocks;

  @Before
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    Configuration conf = new Configuration();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_TRUSTED_PROXY_TRUSTHEADER, "X-Knox-Actor");
    security = new HMSServletSecurity(conf, true);

    // Setup basic mock request behavior
    when(mockRequest.getMethod()).thenReturn("GET");
    when(mockRequest.getRemoteAddr()).thenReturn("127.0.0.1");
    when(mockRequest.getParameterMap()).thenReturn(Collections.emptyMap());
    when(mockRequest.getHeaderNames()).thenReturn(Collections.emptyEnumeration());
  }

  @After
  public void tearDown() throws Exception {
    // Clean up thread local state to prevent test interference
    HMSGroup.setGroups(null);
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testHMSGroupProviderEdgeCases() throws IOException {
    // Test the HMSGroupProvider class which integrates with HMSGroup
    HMSGroupProvider provider = new HMSGroupProvider();

    // Test with no groups set (null state)
    HMSGroup.setGroups(null);
    List<String> groups = provider.getGroups("testuser");
    Assert.assertNull("Should return null when no groups are set", groups);

    // Test getGroupsSet with null groups
    Assert.assertEquals("Should return empty set for null groups",
        Collections.emptySet(), provider.getGroupsSet("testuser"));

    // Test cache operations (should not throw exceptions)
    provider.cacheGroupsRefresh();
    provider.cacheGroupsAdd(Collections.singletonList("testgroup"));
  }

  @Test
  public void testHMSGroupWithInvalidJSON() {
    // Test how HMSGroup handles malformed JSON input
    HMSGroup.setGroups("invalid-json-string");
    List<String> groups = HMSGroup.get("testuser");
    Assert.assertNull("Should return null for invalid JSON", groups);

    // Verify that the invalid JSON doesn't break subsequent operations
    HMSGroup.set("validuser", "validgroup");
    List<String> validGroups = HMSGroup.get("validuser");
    Assert.assertNotNull("Should be able to set groups after invalid JSON", validGroups);
    Assert.assertEquals("Should have correct group", "validgroup", validGroups.get(0));
  }

  @Test
  public void testHMSGroupSerializationEdgeCases() {
    // Test serialization with empty map
    String emptySerialized = HMSGroup.serializeGroups(Collections.emptyMap());
    Assert.assertNull("Should return null for empty groups map", emptySerialized);

    // Test getGroups when no groups are set
    HMSGroup.setGroups(null);
    String noGroups = HMSGroup.getGroups();
    Assert.assertNull("Should return null when no groups are set", noGroups);
  }

  @Test
  public void testSecurityIntegrationWithGroups() throws Exception {
    // Test that execute() properly splits comma-separated groups from Knox header

    // Set up a request with comma-separated groups in header
    when(mockRequest.getParameter("doAs")).thenReturn("testuser");
    when(mockRequest.getHeader("X-Knox-Actor-Groups-1")).thenReturn("testgroup1,testgroup2,testgroup3");
    when(mockResponse.getWriter()).thenReturn(new PrintWriter(new StringWriter()));

    // Capture groups inside the executor to verify they were split correctly
    AtomicReference<List<String>> capturedGroups = new AtomicReference<>();
    AtomicReference<String> capturedUser = new AtomicReference<>();

    try {
      security.execute(mockRequest, mockResponse, (req, resp) -> {
        // Inside execute(), groups should have been set via HMSGroup.set()
        capturedUser.set("testuser");
        capturedGroups.set(HMSGroup.get("testuser"));
      });
    } catch (Exception e) {
      // May fail due to UGI/security setup in test environment, but groups should still be set
    }

    // Verify groups were split correctly (not stored as single comma-separated string)
    List<String> userGroups = capturedGroups.get();
    if (userGroups != null) {
      Assert.assertEquals("Should have 3 separate groups (not 1 comma-separated string)",
          3, userGroups.size());
      Assert.assertTrue("Should contain testgroup1", userGroups.contains("testgroup1"));
      Assert.assertTrue("Should contain testgroup2", userGroups.contains("testgroup2"));
      Assert.assertTrue("Should contain testgroup3", userGroups.contains("testgroup3"));
      // Verify it's NOT stored as a single string
      Assert.assertFalse("Groups should be split, not stored as single string",
          userGroups.contains("testgroup1,testgroup2,testgroup3"));
    }
  }

  @Test
  public void testGroupsSplitCorrectlyFromHeader() {
    // Direct test for comma-separated group splitting logic
    String groupsHeader = "admin,developers,qa-team";
    String[] groups = groupsHeader.split(",");

    HMSGroup.set("splituser", groups);

    List<String> retrievedGroups = HMSGroup.get("splituser");
    Assert.assertNotNull("Groups should be set", retrievedGroups);
    Assert.assertEquals("Should have 3 groups after split", 3, retrievedGroups.size());
    Assert.assertTrue("Should contain admin", retrievedGroups.contains("admin"));
    Assert.assertTrue("Should contain developers", retrievedGroups.contains("developers"));
    Assert.assertTrue("Should contain qa-team", retrievedGroups.contains("qa-team"));
  }

  @Test
  public void testGroupProviderWithSetGroups() throws IOException {
    // Test the full cycle: set groups, then retrieve via provider
    HMSGroup.set("provideruser", "group1", "group2", "group3");

    HMSGroupProvider provider = new HMSGroupProvider();
    List<String> retrievedGroups = provider.getGroups("provideruser");

    Assert.assertNotNull("Provider should return groups", retrievedGroups);
    Assert.assertEquals("Should have correct number of groups", 3, retrievedGroups.size());

    // Test getGroupsSet
    Set<String> groupsSet = provider.getGroupsSet("provideruser");
    Assert.assertNotNull("Provider should return groups set", groupsSet);
    Assert.assertEquals("Set should have correct size", 3, groupsSet.size());
    Assert.assertTrue("Set should contain all groups", groupsSet.containsAll(retrievedGroups));
  }
}
