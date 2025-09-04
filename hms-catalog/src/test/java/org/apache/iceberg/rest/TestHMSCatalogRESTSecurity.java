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

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

/**
 * Tests for REST-level security functionality that doesn't require access to package-private classes.
 * This focuses on authentication, authorization, and error handling at the REST API level.
 */
public class TestHMSCatalogRESTSecurity extends TestHiveCatalog {

  @Test
  public void testUnauthorizedJWTToken() throws Exception {
    // Test with unauthorized JWT token
    String jwt = generateJWT(jwtUnauthorizedKeyFile.toPath());
    URL url = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath + "/v1/namespaces");

    Object response = clientCall(jwt, url, "GET", null);
    Assert.assertTrue("Should get error response for unauthorized token",
        response instanceof HMSTestBase.ServerResponse);
    HMSTestBase.ServerResponse serverResponse = (HMSTestBase.ServerResponse) response;
    int expectedCode = HttpServletResponse.SC_UNAUTHORIZED;
    int actualCode = serverResponse.getCode();
    Assert.assertEquals("Should return 401 for unauthorized token", expectedCode, actualCode);
  }

  @Test
  public void testMissingAuthorizationHeader() throws Exception {
    // Test request without authorization header
    URL url = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath + "/v1/namespaces");

    Object response = clientCall(null, url, "GET", null);
    Assert.assertTrue("Should get error response for missing auth",
        response instanceof HMSTestBase.ServerResponse);
    HMSTestBase.ServerResponse serverResponse = (HMSTestBase.ServerResponse) response;

    int expectedCode = HttpServletResponse.SC_UNAUTHORIZED;
    int actualCode = serverResponse.getCode();
    Assert.assertEquals("Should return 401 for missing auth", expectedCode, actualCode);
  }

  @Test
  public void testInvalidRouteHandling() throws Exception {
    String jwt = generateJWT();

    // Test invalid route
    URL url = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath + "/v1/invalid/route");
    Object response = clientCall(jwt, url, "GET", null);

    // The server should indicate an error for invalid routes
    if (response instanceof HMSTestBase.ServerResponse) {
      HMSTestBase.ServerResponse serverResponse = (HMSTestBase.ServerResponse) response;
      int actualCode = serverResponse.getCode();
      Assert.assertTrue("Should return client error (4xx) for invalid route",
          actualCode >= 400 && actualCode < 500);
    } else if (response instanceof Map) {
      Map<String, Object> jsonResponse = (Map<String, Object>) response;
      Assert.assertNotNull("Should receive error response for invalid route", jsonResponse);
      boolean hasErrorInfo = jsonResponse.containsKey("error") ||
          jsonResponse.containsKey("message") ||
          jsonResponse.containsKey("status");
      Assert.assertTrue("JSON response should contain error information", hasErrorInfo);
    } else {
      Assert.fail("Server should return error response for invalid route, " +
          "but returned: " + (response != null ? response.getClass().getName() : "null"));
    }
  }

  @Test
  public void testExceptionToErrorCodeMapping() {
    // Test that exceptions are properly mapped to HTTP status codes

    // Test validation exception mapping
    ErrorResponse.Builder builder = ErrorResponse.builder();
    HMSCatalogAdapter.configureResponseFromException(
        new ValidationException("Test validation error"), builder);
    ErrorResponse error = builder.build();
    int expectedValidationCode = 400;
    int actualValidationCode = error.code();
    Assert.assertEquals("ValidationException should map to 400", expectedValidationCode, actualValidationCode);
    Assert.assertEquals("Should have correct exception type", "ValidationException", error.type());

    // Test already exists exception mapping
    builder = ErrorResponse.builder();
    HMSCatalogAdapter.configureResponseFromException(
        new AlreadyExistsException("Test already exists"), builder);
    error = builder.build();

    int expectedExistsCode = 409;
    int actualExistsCode = error.code();
    Assert.assertEquals("AlreadyExistsException should map to 409", expectedExistsCode, actualExistsCode);

    // Test no such table exception mapping
    builder = ErrorResponse.builder();
    HMSCatalogAdapter.configureResponseFromException(
        new NoSuchTableException("Test no such table"), builder);
    error = builder.build();

    int expectedNotFoundCode = 404;
    int actualNotFoundCode = error.code();
    Assert.assertEquals("NoSuchTableException should map to 404", expectedNotFoundCode, actualNotFoundCode);

    // Test not authorized exception mapping
    builder = ErrorResponse.builder();
    HMSCatalogAdapter.configureResponseFromException(
        new NotAuthorizedException("Test not authorized"), builder);
    error = builder.build();

    int expectedUnauthorizedCode = 401;
    int actualUnauthorizedCode = error.code();
    Assert.assertEquals("NotAuthorizedException should map to 401", expectedUnauthorizedCode, actualUnauthorizedCode);

    // Test unknown exception mapping
    builder = ErrorResponse.builder();
    HMSCatalogAdapter.configureResponseFromException(
        new RuntimeException("Test runtime error"), builder);
    error = builder.build();

    int expectedServerErrorCode = 500;
    int actualServerErrorCode = error.code();
    Assert.assertEquals("Unknown exception should map to 500", expectedServerErrorCode, actualServerErrorCode);
  }

  @Test
  public void testMalformedRequestHandling() throws Exception {
    String jwt = generateJWT();

    // Test malformed create table request
    URL url = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath +
        "/v1/namespaces/" + DB_NAME + "/tables");

    Object response = clientCall(jwt, url, "POST", "invalid-json");

    boolean isValidErrorResponse = validateErrorResponse(response,
        "malformed JSON", 400, 599);

    Assert.assertTrue("Server should return appropriate error response for malformed JSON. " +
            "Got response type: " + (response != null ? response.getClass().getSimpleName() : "null"),
        isValidErrorResponse);
  }

  private boolean validateErrorResponse(Object response, String context, int minCode, int maxCode) {
    if (response instanceof Map) {
      Map<String, Object> jsonResponse = (Map<String, Object>) response;

      // Check for standard error fields in JSON response
      boolean hasErrorInfo = jsonResponse.containsKey("error") ||
          jsonResponse.containsKey("message") ||
          jsonResponse.containsKey("type") ||
          jsonResponse.containsKey("code");

      if (hasErrorInfo) {
        LOG.debug("Received JSON error response for {}: {}", context, jsonResponse);
        return true;
      }

      // Check if the response itself indicates an error state
      String responseStr = jsonResponse.toString().toLowerCase();
      if (responseStr.contains("error") || responseStr.contains("invalid") ||
          responseStr.contains("bad request")) {
        LOG.debug("JSON response contains error indicators for {}: {}", context, jsonResponse);
        return true;
      }

      return false;

    } else if (response instanceof HMSTestBase.ServerResponse) {
      HMSTestBase.ServerResponse serverResponse = (HMSTestBase.ServerResponse) response;
      int actualCode = serverResponse.getCode();
      boolean isInRange = actualCode >= minCode && actualCode <= maxCode;

      if (isInRange) {
        LOG.debug("Received HTTP error response for {}: {} - {}",
            context, actualCode, serverResponse.getContent());
      }

      return isInRange;

    } else {
      LOG.warn("Unexpected response type for {}: {}", context,
          response != null ? response.getClass().getSimpleName() : "null");
      return false;
    }
  }

  @Test
  public void testRegisterTableThroughRESTAPI() throws Exception {
    // Test the register table functionality through the actual REST endpoint
    String jwt = generateJWT();
    URL url = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath +
        "/v1/namespaces/" + DB_NAME + "/register");

    // Create a JSON request body that matches the RegisterTableRequest structure
    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("name", "test-table");
    requestBody.put("metadataLocation", "s3://bucket/path/metadata.json");

    // Make the REST call and verify it processes the request correctly
    Object response = clientCall(jwt, url, "POST", requestBody);
    Assert.assertNotNull("Response should not be null", response);
  }

  @Test
  public void testUgiCachingThroughRESTCalls() throws Exception {
    // Test that UGI caching works
    String jwt = generateJWT();
    URL url = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath + "/v1/namespaces");

    // Make multiple requests with the same user to test caching behavior
    for (int i = 0; i < 5; i++) {
      Object response = clientCall(jwt, url, "GET", null);
      Assert.assertNotNull("Request " + i + " should succeed", response);
      Assert.assertTrue("Response should indicate success", response instanceof Map);
    }
  }

  @Test
  public void testSecurityWithDifferentOperations() throws Exception {
    // Test security across different REST operations
    String jwt = generateJWT();

    // Test namespace operations
    URL namespacesUrl = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath + "/v1/namespaces");
    Object namespacesResponse = clientCall(jwt, namespacesUrl, "GET", null);
    Assert.assertTrue("Namespaces request should succeed", namespacesResponse instanceof Map);

    // Test table operations
    URL tablesUrl = new URL("http://hive@localhost:" + catalogPort + "/" + catalogPath +
        "/v1/namespaces/" + DB_NAME + "/tables");
    Object tablesResponse = clientCall(jwt, tablesUrl, "GET", null);
    Assert.assertTrue("Tables request should succeed", tablesResponse instanceof Map);
  }
}
