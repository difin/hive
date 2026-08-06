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
package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Reproduces CDPD-84058 regression: AbortTxnHandler NPE when legacy ABORT_TXN
 * notification messages do not contain dbsUpdated and hive.repl.filter.transactions=true.
 */
public class TestAbortTxnHandlerNPE {

  private static final String LEGACY_JSON =
      "{\"txnid\":12787,\"timestamp\":1654116516,\"server\":\"\",\"servicePrincipal\":\"\"}";

  private static EventHandler.Context buildContext(HiveConf conf, String dbName) {
    Path dumpRoot = new Path("file:///tmp/hive-repl-test/dump");
    Path eventRoot = new Path(dumpRoot, "54833886");
    Path cmRoot = new Path("file:///tmp/hive-repl-test/cmroot");

    ReplScope replScope = new ReplScope();
    replScope.setDbName(dbName);

    return new EventHandler.Context(
        eventRoot,
        dumpRoot,
        cmRoot,
        null,
        conf,
        new ReplicationSpec(),
        replScope,
        null,
        Collections.emptySet());
  }

  private static NotificationEvent buildJsonEvent(String message) {
    NotificationEvent event = new NotificationEvent(
        54833886L,
        (int) (System.currentTimeMillis() / 1000),
        MessageBuilder.ABORT_TXN_EVENT,
        message);
    event.setMessageFormat(JSONMessageEncoder.FORMAT);
    return event;
  }

  private static NotificationEvent buildGzipJsonEvent(String legacyJson) throws Exception {
    AbortTxnMessage abortTxnMessage =
        new JSONMessageDeserializer().getAbortTxnMessage(legacyJson);
    String encodedMessage = GzipJSONMessageEncoder.getInstance()
        .getSerializer()
        .serialize(abortTxnMessage);

    NotificationEvent event = new NotificationEvent(
        54833886L,
        (int) (System.currentTimeMillis() / 1000),
        MessageBuilder.ABORT_TXN_EVENT,
        encodedMessage);
    event.setMessageFormat(GzipJSONMessageEncoder.FORMAT);
    return event;
  }

  @Test
  public void testLegacyAbortTxnWithoutDbsUpdatedDoesNotThrowNPE() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.REPL_FILTER_TRANSACTIONS, true);

    JSONMessageDeserializer deserializer = new JSONMessageDeserializer();
    Assert.assertNull(deserializer.getAbortTxnMessage(LEGACY_JSON).getDbsUpdated());

    EventHandler.Context context = buildContext(conf, "prd_zone");
    AbortTxnHandler handler = new AbortTxnHandler(buildJsonEvent(LEGACY_JSON));

    handler.handle(context);

    // Legacy event should be filtered out, not dumped.
    Assert.assertFalse(context.isDmdCreated());
  }

  @Test
  public void testGzipLegacyAbortTxnWithoutDbsUpdatedDoesNotThrowNPE() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.REPL_FILTER_TRANSACTIONS, true);

    AbortTxnMessage abortTxnMessage =
        new JSONMessageDeserializer().getAbortTxnMessage(LEGACY_JSON);
    Assert.assertNull(abortTxnMessage.getDbsUpdated());

    EventHandler.Context context = buildContext(conf, "prd_zone");
    AbortTxnHandler handler = new AbortTxnHandler(buildGzipJsonEvent(LEGACY_JSON));

    handler.handle(context);

    Assert.assertFalse(context.isDmdCreated());
  }

  @Test
  public void testEmptyDbsUpdatedIsFilteredOut() throws Exception {
    String json =
        "{\"txnid\":12787,\"timestamp\":1654116516,\"server\":\"\",\"servicePrincipal\":\"\","
            + "\"dbsUpdated\":[],\"writeIds\":[]}";

    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.REPL_FILTER_TRANSACTIONS, true);

    EventHandler.Context context = buildContext(conf, "prd_zone");
    AbortTxnHandler handler = new AbortTxnHandler(buildJsonEvent(json));

    handler.handle(context);

    // Empty dbsUpdated/writeIds -> filtered out.
    Assert.assertFalse(context.isDmdCreated());
  }

  @Test
  public void testAbortTxnForReplDbIsDumped() throws Exception {
    // Same shape as production event 54833950 (prd_zone).
    String json =
        "{\"txnid\":12787,\"timestamp\":1654116516,\"server\":\"\",\"servicePrincipal\":\"\","
            + "\"dbsUpdated\":[\"prd_zone\"],\"writeIds\":[22593]}";

    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.REPL_FILTER_TRANSACTIONS, true);

    EventHandler.Context context = buildContext(conf, "prd_zone");
    AbortTxnHandler handler = new AbortTxnHandler(buildJsonEvent(json));

    handler.handle(context);

    // Matching db + writeIds -> event should be dumped.
    Assert.assertTrue(context.isDmdCreated());
  }
}
