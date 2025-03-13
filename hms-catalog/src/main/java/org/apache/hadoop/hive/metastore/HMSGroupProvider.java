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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.GroupMappingServiceProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The companion class to HMSGroup used to resolve doAs() groups.
 * <property>
 *   <name>hadoop.security.group.mapping.provider.knoxactor</name>
 *   <value>org.apache.hadoop.hive.metastore.HMSGroupProvider</value>
 *   <description>
 *     Class for group mapping provider named by 'knoxactor'. The name can then be referenced
 *     by hadoop.security.group.mapping.providers property.
 *   </description>
 * </property>
 */
public class HMSGroupProvider implements GroupMappingServiceProvider {

  @Override
  public List<String> getGroups(String s) throws IOException {
    return HMSGroup.get(s);
  }

  @Override
  public void cacheGroupsRefresh() throws IOException {
    // ignore
  }

  @Override
  public void cacheGroupsAdd(List<String> list) throws IOException {
    // ignore
  }

  @Override
  public Set<String> getGroupsSet(String s) throws IOException {
    List<String> list = getGroups(s);
    return list == null || list.isEmpty()
            ? Collections.emptySet()
            : new HashSet<>(list);
  }
}
