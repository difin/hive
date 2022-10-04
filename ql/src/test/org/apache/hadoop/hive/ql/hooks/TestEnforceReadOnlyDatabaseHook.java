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
package org.apache.hadoop.hive.ql.hooks;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLTask;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.database.alter.poperties.AlterDatabaseSetPropertiesDesc;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
public class TestEnforceReadOnlyDatabaseHook {
  EnforceReadOnlyDatabaseHook hook = new EnforceReadOnlyDatabaseHook();

  <T> Stream<T> concat(Stream<T>... streams) {
    return Arrays.stream(streams).reduce(Stream::concat).orElse(Stream.empty());
  }

  @Test
  public void testAllowReadOperations() {
    concat(
        ImmutableSet.of(
            HiveOperation.EXPLAIN,
            HiveOperation.SWITCHDATABASE,
            HiveOperation.REPLDUMP,
            HiveOperation.REPLSTATUS,
            HiveOperation.EXPORT,
            HiveOperation.KILL_QUERY)
            .stream(),
        Arrays
            .stream(HiveOperation.values())
            .filter(op -> op.getOperationName().startsWith("SHOW")),
        Arrays
            .stream(HiveOperation.values())
            .filter(op -> op.getOperationName().startsWith("DESC"))
    ).forEach(op -> testOperationSucceeds(op, queryPlanWithQuery(null)));
  }

  @Test
  public void testAllowSelectQuery() {
    testOperationSucceeds(HiveOperation.QUERY, queryPlanWithQuery("SELECT * FROM table"));
  }

  @Test
  public void testDisallowNonSelectQuery() {
    testOperationFails(HiveOperation.QUERY, queryPlanWithQuery("INSERT INTO table VALUES (1)"));
    testOperationFails(HiveOperation.QUERY, queryPlanWithQuery("DELETE FROM table"));
    testOperationFails(HiveOperation.QUERY, queryPlanWithQuery("UPDATE table SET a = 1"));
  }

  @Test
  public void testAllowReplLoad() {
    testOperationSucceeds(HiveOperation.REPLLOAD, queryPlanWithQuery(null));
  }

  @Test
  public void testAllowAlterTable() {
    QueryPlan queryPlan = queryPlanWithQuery(null);
    Map<String, String> properties = ImmutableMap.of(EnforceReadOnlyDatabaseHook.READONLY, "false");
    AlterDatabaseSetPropertiesDesc desc = new AlterDatabaseSetPropertiesDesc(null, properties, null);
    DDLWork work = new DDLWork(null, null, desc);
    DDLTask task = new DDLTask();
    task.setWork(work);
    queryPlan.setRootTasks(Collections.singletonList(task));
    queryPlan.setOutputs(
        Sets.newHashSet(
            new WriteEntity(
                new Table("database1", "table1"),
                WriteEntity.WriteType.INSERT)));
    testOperationSucceeds(HiveOperation.ALTERDATABASE, queryPlan);
  }

  @Test
  public void testDisallowWriteOperations() {
    concat(
        Arrays
            .stream(HiveOperation.values())
            .filter(op -> op.getOperationName().startsWith("CREATE")),
        Arrays
            .stream(HiveOperation.values())
            .filter(op -> op.getOperationName().startsWith("ALTER")),
        Arrays
            .stream(HiveOperation.values())
            .filter(op -> op.getOperationName().startsWith("DROP"))
    ).forEach(op -> testOperationFails(op, queryPlanWithQuery(null)));
  }

  private void testOperationSucceeds(HiveOperation operation, QueryPlan queryPlan) {
    try {
      testOperation(operation, queryPlan);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void testOperationFails(HiveOperation operation, QueryPlan queryPlan) {
    try {
      testOperation(operation, queryPlan);
      throw new RuntimeException("Expected exception");
    } catch (Exception e) {
      // Ignore expected exception
    }
  }

  private void testOperation(HiveOperation operation, QueryPlan queryPlan) throws Exception {
    QueryState queryState = new QueryState.Builder().build();
    queryState.setCommandType(operation);
    HookContext hookContext = new HookContext(queryPlan, queryState, null, null, null, null, null, null, null, true,
        null, null);
    hookContext.setHookType(HookContext.HookType.PRE_EXEC_HOOK);
    hook.run(hookContext);
  }

  private QueryPlan queryPlanWithQuery(String query) {
    QueryPlan queryPlan = new QueryPlan();
    queryPlan.setOutputs(Sets.newHashSet());
    queryPlan.setRootTasks(Collections.emptyList());
    queryPlan.setQueryString(query);
    return queryPlan;
  }
}