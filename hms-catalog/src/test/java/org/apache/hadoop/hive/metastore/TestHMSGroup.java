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
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TestHMSGroup {
  private static final List<String> G09 = Collections.singletonList("0123456789");

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

}

