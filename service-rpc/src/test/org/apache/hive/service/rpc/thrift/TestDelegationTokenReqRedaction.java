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

package org.apache.hive.service.rpc.thrift;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Test;

/**
 * Regression tests ensuring the delegation-token request DTOs never emit the raw token (a bearer
 * credential) in the toString() output.
 */
public class TestDelegationTokenReqRedaction {

  private static final String SECRET_TOKEN =
      "AAAABANoaXZlBGhpdmUAigF-super-secret-live-delegation-token-value-do-not-leak";

  @Test
  public void testCancelReqToStringRedactsToken() {
    String rendered = new TCancelDelegationTokenReq(newSessionHandle(), SECRET_TOKEN).toString();
    assertFalse("TCancelDelegationTokenReq.toString() leaked the raw token: " + rendered,
        rendered.contains(SECRET_TOKEN));
    assertTrue(rendered.contains("REDACTED"));
  }

  @Test
  public void testRenewReqToStringRedactsToken() {
    String rendered = new TRenewDelegationTokenReq(newSessionHandle(), SECRET_TOKEN).toString();
    assertFalse("TRenewDelegationTokenReq.toString() leaked the raw token: " + rendered,
        rendered.contains(SECRET_TOKEN));
    assertTrue(rendered.contains("REDACTED"));
  }

  private static TSessionHandle newSessionHandle() {
    THandleIdentifier id =
        new THandleIdentifier(ByteBuffer.wrap(new byte[16]), ByteBuffer.wrap(new byte[16]));
    return new TSessionHandle(id);
  }
}
