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
package org.apache.hadoop.hive.metastore.auth.jwt;

import com.cloudera.client.api.TokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

public class TestKrbToJWTExchanger {
  private static final String FAILURE_TIMES = "test.jwt.failure.times";
  public static class JwtTokenProvider implements TokenProvider {
    private Configuration conf;
    private Integer failureTimesLimit;
    private int failureTimes;
    Map<String, String> tokenCache = new HashMap<>();
    public JwtTokenProvider(Configuration conf) {
      this.conf = conf;
      tokenCache.put("user1", "user1-123456");
      tokenCache.put("user2", "user2-123456");
      tokenCache.put("user3", "user3-123456");
    }
    @Override
    public String getBearerToken() {
      if (this.failureTimesLimit == null) {
        this.failureTimesLimit = conf.getInt(FAILURE_TIMES, 0);
      }
      if (failureTimes < failureTimesLimit) {
        failureTimes++;
        throw new RuntimeException("Failed to get JWT");
      }
      try {
        String userName = SecurityUtils.getUser();
        return tokenCache.get(userName);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() throws Exception {
    }
  }

  @Test
  public void testGetBearerToken() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    conf.set("cloudera.jwt.client.classname", JwtTokenProvider.class.getName());
    UserGroupInformation user1 = UserGroupInformation.createRemoteUser("user1");
    KrbToJWTExchanger exchanger = KrbToJWTExchanger.getTokenProvider(conf);
    String jwtToken = user1.doAs((PrivilegedExceptionAction<String>) () -> exchanger.getJwtToken());
    Assert.assertEquals("user1-123456", jwtToken);

    UserGroupInformation user2 = UserGroupInformation.createRemoteUser("user2");
    long startTime = System.currentTimeMillis();
    conf.set(FAILURE_TIMES, "3");
    jwtToken = user2.doAs((PrivilegedExceptionAction<String>) () -> exchanger.getJwtToken());
    Assert.assertEquals("user2-123456", jwtToken);
    long spent = (System.currentTimeMillis() - startTime) / 1000;
    Assert.assertTrue(spent > 2);

    jwtToken = user1.doAs((PrivilegedExceptionAction<String>) () -> exchanger.getJwtToken());
    Assert.assertEquals("user1-123456", jwtToken);

    UserGroupInformation user3 = UserGroupInformation.createRemoteUser("user3");
    conf.set(FAILURE_TIMES, "6");
    try {
      jwtToken = user3.doAs((PrivilegedExceptionAction<String>) () -> exchanger.getJwtToken());
      Assert.fail("An exception is expected");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Failed to get JWT"));
    }
  }
}
