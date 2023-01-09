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
package org.apache.hadoop.hive.impala.parse;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Map;
import org.antlr.runtime.Token;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.AnalyzeCommandUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ImmutableCommonToken;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.impala.work.ImpalaWork;
import org.apache.impala.analysis.DropStatsStmt;

/**
 * Analyzer for stats dropping commands.
 */
public class DropStatsSemanticAnalyzer extends SemanticAnalyzer {
  public DropStatsSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    LOG.debug("Drop statistics semantic analyzer");
    Preconditions.checkState(root.getType() == ImpalaToken.TOK_DROP_STATS);
    ASTNode tableNode = (ASTNode) root.getChild(0);
    // PlanUtils.addInput adds the ReadEntity object into "inputs" which is
    // used for authorization.
    Table table = getTable(tableNode);

    if (table.isView()) {
      throw new SemanticException("DROP STATISTICS cannot be executed for views");
    }

    Map<String, String> partitionSpec = getPartSpec((ASTNode) tableNode.getChild(1));
    boolean incrementalStats = AnalyzeCommandUtils.isIncrementalStats(root);
    if (incrementalStats && partitionSpec == null) {
      throw new SemanticException("Partition needs to be statically specified in DROP INCREMENTAL STATISTICS");
    }

    if (partitionSpec != null) {
      if (!incrementalStats) {
        throw new SemanticException(
            "Partitions cannot be statically specified in DROP STATISTICS in Impala without INCREMENTAL");
      }
      // if partitionSpec is specified than validate that all partitions are specified
      validatePartSpec(table, partitionSpec, tableNode, conf, true);
    }
    LOG.debug("Drop statistics analysis completed");
    queryState.setCommandType(HiveOperation.DROP_STATS);
    // The query result will contain a single row with a 'summary' string column.
    this.resultSchema = new ArrayList<>();
    this.resultSchema.add(new FieldSchema("summary", "string", ""));
            // We need to create a fetch operator since only Impala returns
            // results for these statement
    FetchWork fetch = new ImpalaFetchWork();
    fetchTask = (FetchTask) TaskFactory.get(fetch);
    String statsStatement = ImpalaSemanticAnalyzerUtils.getQueryWithDatabase(
        (ASTNode) tableNode.getChild(0),
        queryState.getQueryString(),
        SessionState.get().getCurrentDatabase());
    String invalidateStmt =
        generateInvalidateMetadataStmt(SessionState.get().getCurrentDatabase(), table);
    ImpalaWork invalidateWork = ImpalaWork.createPlannedWork(invalidateStmt);
    Task<ImpalaWork> invalidateTask = TaskFactory.get(invalidateWork);
    this.rootTasks.add(TaskFactory.get(invalidateWork));
    ImpalaWork work = ImpalaWork.createPlannedWork(statsStatement, fetchTask, 1);
    Task<ImpalaWork> dropStatsTask = TaskFactory.get(work);
    this.rootTasks.add(dropStatsTask);
    invalidateTask.addDependentTask(dropStatsTask);
  }

  private String generateInvalidateMetadataStmt(String db, Table table) {
    return "invalidate metadata `" + db + "`" + ".`" + table.getTableName() + "`";
  }

  private Table getTable(ASTNode tableNode) throws SemanticException {
    String tableNameString = getUnescapedName((ASTNode) tableNode.getChild(0));
    Table table = getTable(tableNameString, true);
    TableSpec ts = new TableSpec(db, conf, tableNode, false, false);
    table.setTableSpec(ts);
    return table;
  }

  public static ASTNode getASTNode(DropStatsStmt stmt, String command, Context ctx)
        throws ParseException {
    Token dropStatsToken = new ImmutableCommonToken(
        ImpalaToken.TOK_DROP_STATS, ImpalaToken.DROP_STATS_STRING);
    ImpalaASTNode dropStatsRoot = new ImpalaASTNode(dropStatsToken);
    int indexOfTableString = StringUtils.indexOfIgnoreCase(command, "stats") + "stats ".length();
    String tableAndPartitionString = StringUtils.substring(command, indexOfTableString);

    ASTNode tableASTNode =
        HiveSnippetParser.parse(tableAndPartitionString, HiveParser.TOK_TABLE_PARTITION);
    dropStatsRoot.addChild(tableASTNode);
    int incrementalIndex = StringUtils.indexOfIgnoreCase(command, "incremental");
    if (incrementalIndex >= 0 && incrementalIndex < indexOfTableString) {
      Token incrementalToken = new ImmutableCommonToken(HiveParser.KW_INCREMENTAL, "incremental");
      dropStatsRoot.addChild(new ASTNode(incrementalToken));
    }
    return dropStatsRoot;
  }
}
