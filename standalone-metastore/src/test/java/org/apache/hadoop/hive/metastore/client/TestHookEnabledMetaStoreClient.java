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

package org.apache.hadoop.hive.metastore.client;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class TestHookEnabledMetaStoreClient {

  private static Table viewShell() {
    Table viewShell = new Table();
    viewShell.setDbName("db");
    viewShell.setTableName("v");
    viewShell.setTableType(TableType.VIRTUAL_VIEW.name());
    return viewShell;
  }

  private static HiveMetaHook countingHook(AtomicInteger hookCalls, String expandedText) {
    return new HiveMetaHook() {
      @Override
      public void preCreateTable(org.apache.hadoop.hive.metastore.api.Table table) {
      }

      @Override
      public void rollbackCreateTable(org.apache.hadoop.hive.metastore.api.Table table) {
      }

      @Override
      public void commitCreateTable(org.apache.hadoop.hive.metastore.api.Table table) {
      }

      @Override
      public void preDropTable(org.apache.hadoop.hive.metastore.api.Table table) {
      }

      @Override
      public void rollbackDropTable(org.apache.hadoop.hive.metastore.api.Table table) {
      }

      @Override
      public void commitDropTable(org.apache.hadoop.hive.metastore.api.Table table, boolean deleteData) {
      }

      @Override
      public void postGetTable(org.apache.hadoop.hive.metastore.api.Table table) {
        hookCalls.incrementAndGet();
        if (expandedText != null) {
          table.setViewExpandedText(expandedText);
        }
      }
    };
  }

  @Test
  public void postGetTableRunsForGetTableRequest() throws Exception {
    Table viewShell = viewShell();
    IMetaStoreClient delegate = Mockito.mock(IMetaStoreClient.class);
    GetTableResult result = new GetTableResult();
    result.setTable(viewShell);
    when(delegate.getTable(any(GetTableRequest.class))).thenReturn(result);

    AtomicInteger hookCalls = new AtomicInteger();
    HiveMetaHookLoader hookLoader = tbl -> countingHook(hookCalls, "select 1");

    Configuration conf = new Configuration();
    IMetaStoreClient client = HookEnabledMetaStoreClient.newClient(conf, hookLoader, delegate);

    Table table = client.getTable(new GetTableRequest("db", "v")).getTable();
    Assert.assertEquals(1, hookCalls.get());
    Assert.assertEquals("select 1", table.getViewExpandedText());
  }

  @Test
  public void postGetTableRunsForGetTableOverload() throws Exception {
    Table viewShell = viewShell();
    IMetaStoreClient delegate = Mockito.mock(IMetaStoreClient.class);
    when(delegate.getTable("hive", "db", "v")).thenReturn(viewShell);

    AtomicInteger hookCalls = new AtomicInteger();
    HiveMetaHookLoader hookLoader = tbl -> countingHook(hookCalls, "select 2");

    Configuration conf = new Configuration();
    IMetaStoreClient client = HookEnabledMetaStoreClient.newClient(conf, hookLoader, delegate);

    Table table = client.getTable("hive", "db", "v");
    Assert.assertEquals(1, hookCalls.get());
    Assert.assertEquals("select 2", table.getViewExpandedText());
  }

  @Test
  public void postGetTableRunsForGetTableObjectsByName() throws Exception {
    Table viewShell = viewShell();
    IMetaStoreClient delegate = Mockito.mock(IMetaStoreClient.class);
    when(delegate.getTableObjectsByName("hive", "db", Collections.singletonList("v")))
        .thenReturn(Collections.singletonList(viewShell));

    AtomicInteger hookCalls = new AtomicInteger();
    HiveMetaHookLoader hookLoader = tbl -> countingHook(hookCalls, null);

    Configuration conf = new Configuration();
    IMetaStoreClient client = HookEnabledMetaStoreClient.newClient(conf, hookLoader, delegate);
    client.getTableObjectsByName("hive", "db", Collections.singletonList("v"));
    Assert.assertEquals(1, hookCalls.get());
  }
}
