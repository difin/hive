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

import org.apache.hive.iceberg.com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.hive.iceberg.com.fasterxml.jackson.annotation.PropertyAccessor;
import org.apache.hive.iceberg.com.fasterxml.jackson.core.JsonFactory;
import org.apache.hive.iceberg.com.fasterxml.jackson.core.JsonFactoryBuilder;
import org.apache.hive.iceberg.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.hive.iceberg.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hive.iceberg.com.fasterxml.jackson.databind.PropertyNamingStrategies;

/**
 * JSON mapper for {@link JavaxIcebergRESTCatalogServlet}. Avoids {@code RESTObjectMapper.mapper()}
 * (classpath / API skew with embedded Iceberg in {@code hive-iceberg-handler}).
 *
 * <p>Uses Jackson types relocated like {@code hive-iceberg-shading} ({@code org.apache.hive.iceberg.com.fasterxml.*})
 * so {@link RESTSerializers#registerAll} accepts the same {@link ObjectMapper} type Iceberg expects.
 */
public final class JavaxRESTObjectMapper {
  private static final JsonFactory FACTORY =
      new JsonFactoryBuilder()
          .configure(JsonFactory.Feature.INTERN_FIELD_NAMES, false)
          .configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false)
          .build();
  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);
  private static volatile boolean initialized = false;

  private JavaxRESTObjectMapper() {}

  public static ObjectMapper mapper() {
    if (!initialized) {
      synchronized (JavaxRESTObjectMapper.class) {
        if (!initialized) {
          MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
          MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
          MAPPER.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
          RESTSerializers.registerAll(MAPPER);
          initialized = true;
        }
      }
    }
    return MAPPER;
  }
}
