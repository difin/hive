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

import org.apache.hive.iceberg.com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.iceberg.metering.MeteringHealthWatcher;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.rest.HMSCatalogAdapter.HTTPMethod;
import org.apache.iceberg.rest.HMSCatalogAdapter.Route;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Original @ <a href="https://github.com/apache/iceberg/blob/1.6.x/core/src/test/java/org/apache/iceberg/rest/RESTCatalogServlet.java">...</a>
 * The RESTCatalogServlet provides a servlet implementation used in combination with a
 * RESTCatalogAdaptor to proxy the REST Spec to any Catalog implementation.
 */
public class HMSCatalogServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalogServlet.class);
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String APPLICATION_JSON = "application/json";

  private final HMSCatalogAdapter restCatalogAdapter;
  private final MeteringHealthWatcher meteringWatcher;
  private final Map<String, String> responseHeaders =
      ImmutableMap.of(CONTENT_TYPE, APPLICATION_JSON);

  /**
   * Creates a servlet without metering health gating (used by existing embedded tests).
   */
  public HMSCatalogServlet(HMSCatalogAdapter restCatalogAdapter) {
    this(restCatalogAdapter, null);
  }

  /**
   * Creates a servlet that gates requests on the given {@link MeteringHealthWatcher}.
   * When {@code meteringWatcher} is {@code null} all requests are served unconditionally.
   */
  public HMSCatalogServlet(HMSCatalogAdapter restCatalogAdapter, MeteringHealthWatcher meteringWatcher) {
    this.restCatalogAdapter = restCatalogAdapter;
    this.meteringWatcher = meteringWatcher;
  }

  @Override
  public String getServletName() {
    return "Iceberg REST Catalog";
  }

  // TODO: check upstream - Handle OPTIONS for CORS preflight requests
  @Override
  protected void doOptions(HttpServletRequest request, HttpServletResponse response) {
    response.setStatus(HttpServletResponse.SC_OK);
    responseHeaders.forEach(response::setHeader);
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) {
    // TODO: check upstream - Handle OPTIONS for CORS preflight requests
    String method = request.getMethod();
    if ("OPTIONS".equalsIgnoreCase(method)) {
      response.setStatus(HttpServletResponse.SC_OK);
      responseHeaders.forEach(response::setHeader);
      return;
    }
    // TODO: check upstream - Block security-sensitive HTTP methods
    if ("TRACE".equalsIgnoreCase(method) || "CONNECT".equalsIgnoreCase(method)) {
      response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
      return;
    }
    if (meteringWatcher != null && !meteringWatcher.isHealthy()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      responseHeaders.forEach(response::setHeader);
      try {
        ErrorResponse err = ErrorResponse.builder()
            .responseCode(HttpServletResponse.SC_SERVICE_UNAVAILABLE)
            .withType("ServiceUnavailableException")
            .withMessage("Metering service is not healthy; REST catalog is temporarily unavailable.")
            .build();
        RESTObjectMapper.mapper().writeValue(response.getWriter(), err);
      } catch (IOException writeError) {
        LOG.error("Failed to write 503 error response", writeError);
      }
      return;
    }
    try {
      ServletRequestContext context = ServletRequestContext.from(request);
      response.setStatus(HttpServletResponse.SC_OK);
      responseHeaders.forEach(response::setHeader);

      final Optional<ErrorResponse> error = context.error();
      if (error.isPresent()) {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        RESTObjectMapper.mapper().writeValue(response.getWriter(), error.get());
        return;
      }
      Object responseBody =
          restCatalogAdapter.execute(
              context.method(),
              context.path(),
              context.queryParams(),
              context.body(),
              context.route().responseClass(),
              context.headers(),
              handle(response));

      if (responseBody != null) {
        RESTObjectMapper.mapper().writeValue(response.getWriter(), responseBody);
      }
    } catch (JsonProcessingException e) {
      // TODO: check upstream - Malformed JSON should return 400 Bad Request
      LOG.warn("Malformed JSON in request", e);
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      try {
        ErrorResponse errorResponse = ErrorResponse.builder()
            .responseCode(400)
            .withType("BadRequestException")
            .withMessage("Malformed JSON: " + e.getOriginalMessage())
            .build();
        RESTObjectMapper.mapper().writeValue(response.getWriter(), errorResponse);
      } catch (IOException writeError) {
        LOG.error("Failed to write error response", writeError);
      }
    } catch (RuntimeException | IOException e) {
      // should be a RESTException but not able to see them through dependencies
      LOG.error("Error processing REST request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  protected Consumer<ErrorResponse> handle(HttpServletResponse response) {
    return errorResponse -> {
      response.setStatus(errorResponse.code());
      try {
        RESTObjectMapper.mapper().writeValue(response.getWriter(), errorResponse);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  public static class ServletRequestContext {
    private HTTPMethod method;
    private Route route;
    private String path;
    private Map<String, String> headers;
    private Map<String, String> queryParams;
    private Object body;

    private ErrorResponse errorResponse;

    private ServletRequestContext(ErrorResponse errorResponse) {
      this.errorResponse = errorResponse;
    }

    private ServletRequestContext(
        HTTPMethod method,
        Route route,
        String path,
        Map<String, String> headers,
        Map<String, String> queryParams,
        Object body) {
      this.method = method;
      this.route = route;
      this.path = path;
      this.headers = headers;
      this.queryParams = queryParams;
      this.body = body;
    }

    static ServletRequestContext from(HttpServletRequest request) throws IOException {
      HTTPMethod method = HTTPMethod.valueOf(request.getMethod());
      // path = uri - context-path + servlet-path + /
      String path = request.getPathInfo();
      if (path == null) {
        path = request.getRequestURI().substring(
            request.getContextPath().length() + request.getServletPath().length());
      }
      // remove leading /
      path = path.substring(1);
      Pair<Route, Map<String, String>> routeContext = Route.from(method, path);

      if (routeContext == null) {
        return new ServletRequestContext(
            ErrorResponse.builder()
                .responseCode(400)
                .withType("BadRequestException")
                .withMessage(String.format("No route for request: %s %s", method, path))
                .build());
      }

      Route route = routeContext.first();
      Object requestBody = null;
      try {
        if (route.requestClass() != null) {
          requestBody = RESTObjectMapper.mapper().readValue(request.getInputStream(), route.requestClass());
        } else if (route == Route.TOKENS) {
          try (Reader reader = new InputStreamReader(request.getInputStream())) {
            requestBody = RESTUtil.decodeFormData(CharStreams.toString(reader));
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed to read request body", e);
        return new ServletRequestContext(
            ErrorResponse.builder()
                .responseCode(500)
                .withType("BadRequestException")
                .withMessage(String.format("Unreadable request body request: %s %s", method, path))
                .build());
      }

      Map<String, String> queryParams =
          request.getParameterMap().entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()[0]));
      Map<String, String> headers =
          Collections.list(request.getHeaderNames()).stream()
              .collect(Collectors.toMap(Function.identity(), request::getHeader));

      return new ServletRequestContext(method, route, path, headers, queryParams, requestBody);
    }

    HTTPMethod method() {
      return method;
    }

    Route route() {
      return route;
    }

    public String path() {
      return path;
    }

    public Map<String, String> headers() {
      return headers;
    }

    public Map<String, String> queryParams() {
      return queryParams;
    }

    public Object body() {
      return body;
    }

    public Optional<ErrorResponse> error() {
      return Optional.ofNullable(errorResponse);
    }
  }
}
