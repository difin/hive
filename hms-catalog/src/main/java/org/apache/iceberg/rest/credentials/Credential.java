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

package org.apache.iceberg.rest.credentials;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A credential representing a storage location prefix and its associated config.
 * Manual builder pattern since Iceberg 1.5.x does not have Immutables.
 */
public class Credential {
  private final String prefix;
  private final Map<String, String> config;

  private Credential(String prefix, Map<String, String> config) {
    this.prefix = prefix;
    this.config = Collections.unmodifiableMap(new HashMap<>(config));
  }

  public String prefix() {
    return prefix;
  }

  public Map<String, String> config() {
    return config;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String prefix;
    private final Map<String, String> config = new HashMap<>();

    public Builder prefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder config(Map<String, String> config) {
      this.config.putAll(config);
      return this;
    }

    public Credential build() {
      return new Credential(prefix, config);
    }
  }
}
