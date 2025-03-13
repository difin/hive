/* * Licensed to the Apache Software Foundation (ASF) under one
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A TLS (singleton) map of list of groups keyed by username.
 */
class HMSGroup {
  private static final Logger LOG = LoggerFactory.getLogger(HMSGroup.class);
  private static final ThreadLocal<Map<String, List<String>>> GROUPS = new ThreadLocal<>();

  private HMSGroup() {
  }

  /**
   * Gets the HMS group for a user.
   *
   * @param user the user
   * @return the groups
   */
  static List<String> get(String user) {
    Map<String, List<String>> group = GROUPS.get();
    return group == null ? null : group.get(user);
  }

  /**
   * Sets the HMS group singleton.
   *
   * @param user   the user
   * @param groups its groups
   */
  static void set(String user, String... groups) {
    Map<String, List<String>> group = Collections.singletonMap(user, Arrays.asList(groups));
    if (LOG.isInfoEnabled()) {
      LOG.info("Setting HMSGroup {}", serializeGroups(group));
    }
    GROUPS.set(group);
  }

  /**
   * @return the HMS groups as JSON, singleton map of list of groups keyed by users (1)
   */
  static String getGroups() {
    return serializeGroups(GROUPS.get());
  }

  /**
   * Sets the HMS group, the singleton.
   *
   * @param json the json
   */
  static void setGroups(String json) {
    Map<String, List<String>> group = deserializeGroups(json);
    if (group == null) {
      LOG.info("Removing HMSGroups");
      GROUPS.remove();
    } else {
      LOG.info("Setting HMSGroups {}", json);
      GROUPS.set(group);
    }
  }

  /**
   * Deserializes a json representation of groups.
   *
   * @param json the json serialized
   * @return a map of list of groups keyed by user
   */
  static Map<String, List<String>> deserializeGroups(String json) {
    if (json != null && !json.isEmpty()) {
      ObjectMapper mapper = new ObjectMapper();
      try {
       return mapper.readValue(json,
            new TypeReference<Map<String, List<String>>>() {});
      } catch (JsonProcessingException e) {
        LOG.error("unable to deserialize HMS groups", e);
      }
    }
    return null;
  }

  /**
   * Serialize a map of user to list of groups into json.
   *
   * @param groups the map
   * @return the json
   */
  static String serializeGroups(Map<String, List<String>> groups) {
    if (groups != null && !groups.isEmpty()) {
      ObjectMapper mapper = new ObjectMapper();
      try {
        return mapper.writeValueAsString(groups);
      } catch (JsonProcessingException e) {
        LOG.error("unable to serialize HMS groups", e);
      }
    }
    return null;
  }
}
