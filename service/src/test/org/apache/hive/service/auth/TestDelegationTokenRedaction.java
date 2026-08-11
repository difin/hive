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

package org.apache.hive.service.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.junit.Test;

/**
 * Regression tests ensuring that raw Hive delegation tokens (bearer credentials) are never echoed
 * into log messages, HiveSQLException text, or client-visible TStatus error messages on the
 * cancel/renew/verify failure paths.
 */
public class TestDelegationTokenRedaction {

  private static final String SECRET_TOKEN =
      "AAAABANoaXZlBGhpdmUAigF-super-secret-live-delegation-token-value-do-not-leak";

  /**
   * A token manager whose token operations always fail, so we can exercise the failure/error
   * message construction paths in {@link HiveAuthFactory} without a live Kerberos setup.
   */
  private static final class FailingTokenManager extends MetastoreDelegationTokenManager {
    @Override
    public void cancelDelegationToken(String tokenStrForm) throws IOException {
      throw new IOException("simulated cancel failure");
    }

    @Override
    public long renewDelegationToken(String tokenStrForm) throws IOException {
      throw new IOException("simulated renew failure");
    }

    @Override
    public String verifyDelegationToken(String tokenStrForm) throws IOException {
      throw new IOException("simulated verify failure");
    }

    @Override
    public String getUserFromToken(String tokenStr) throws IOException {
      throw new IOException("simulated getUserFromToken failure");
    }
  }

  private HiveAuthFactory newFactoryWithFailingManager() throws Exception {
    // A default (non-Kerberos) HiveConf leaves the real delegationTokenManager null; we then
    // inject a failing one to drive the error-message construction paths under test.
    HiveAuthFactory factory = new HiveAuthFactory(new HiveConf(), false);
    Field field = HiveAuthFactory.class.getDeclaredField("delegationTokenManager");
    field.setAccessible(true);
    field.set(factory, new FailingTokenManager());
    return factory;
  }

  @Test
  public void testCancelFailureDoesNotLeakToken() throws Exception {
    HiveAuthFactory factory = newFactoryWithFailingManager();
    try {
      factory.cancelDelegationToken(SECRET_TOKEN);
      fail("Expected HiveSQLException");
    } catch (HiveSQLException e) {
      assertMessageAndTStatusRedacted(e);
    }
  }

  @Test
  public void testRenewFailureDoesNotLeakToken() throws Exception {
    HiveAuthFactory factory = newFactoryWithFailingManager();
    try {
      factory.renewDelegationToken(SECRET_TOKEN);
      fail("Expected HiveSQLException");
    } catch (HiveSQLException e) {
      assertMessageAndTStatusRedacted(e);
    }
  }

  @Test
  public void testVerifyFailureDoesNotLeakToken() throws Exception {
    HiveAuthFactory factory = newFactoryWithFailingManager();
    try {
      factory.verifyDelegationToken(SECRET_TOKEN);
      fail("Expected HiveSQLException");
    } catch (HiveSQLException e) {
      assertMessageAndTStatusRedacted(e);
    }
  }

  @Test
  public void testGetUserFromTokenFailureDoesNotLeakToken() throws Exception {
    HiveAuthFactory factory = newFactoryWithFailingManager();
    try {
      factory.getUserFromToken(SECRET_TOKEN);
      fail("Expected HiveSQLException");
    } catch (HiveSQLException e) {
      assertMessageAndTStatusRedacted(e);
    }
  }

  private void assertMessageAndTStatusRedacted(HiveSQLException e) {
    // Neither the exception message nor the client-visible TStatus may contain the raw token.
    assertFalse("Exception message leaked the raw delegation token: " + e.getMessage(),
        e.getMessage().contains(SECRET_TOKEN));

    TStatus status = e.toTStatus();
    assertFalse("TStatus.errorMessage leaked the raw delegation token",
        status.getErrorMessage().contains(SECRET_TOKEN));
    if (status.getInfoMessages() != null) {
      for (String info : status.getInfoMessages()) {
        assertFalse("TStatus.infoMessages leaked the raw delegation token",
            info.contains(SECRET_TOKEN));
      }
    }
    // A stable, non-reversible fingerprint should be present for operator correlation instead.
    assertTrue("Expected a redacted token fingerprint in the message: " + e.getMessage(),
        e.getMessage().contains(HiveAuthFactory.tokenFingerprint(SECRET_TOKEN)));
  }

  @Test
  public void testTokenFingerprintProperties() {
    String fp = HiveAuthFactory.tokenFingerprint(SECRET_TOKEN);
    assertNotNull(fp);
    assertFalse(fp.contains(SECRET_TOKEN));
    assertTrue(fp.startsWith("token-sha256:"));

    // Stable for the same input, different for a different input.
    assertEquals(fp, HiveAuthFactory.tokenFingerprint(SECRET_TOKEN));
    assertFalse(fp.equals(HiveAuthFactory.tokenFingerprint(SECRET_TOKEN + "x")));

    // Null / empty are handled without leaking or throwing.
    assertEquals("null", HiveAuthFactory.tokenFingerprint(null));
    assertEquals("empty", HiveAuthFactory.tokenFingerprint(""));
  }
}
