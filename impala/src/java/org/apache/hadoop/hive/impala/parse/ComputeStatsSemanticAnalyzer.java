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
import java.util.List;
import java.util.Map;
import org.antlr.runtime.Token;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
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
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.impala.work.ImpalaWork;
import org.apache.impala.analysis.ComputeStatsStmt;
import org.apache.impala.analysis.Parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzer for stats computing command.
 */
public class ComputeStatsSemanticAnalyzer extends SemanticAnalyzer {

  private static final Logger LOG = LoggerFactory.getLogger(ComputeStatsSemanticAnalyzer.class);

  public ComputeStatsSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    LOG.debug("Compute statistics semantic analyzer");
    Preconditions.checkState(root.getType() == ImpalaToken.TOK_COMPUTE_STATS);
    ASTNode tableNode = (ASTNode) root.getChild(0);
    Map<String, String> partitionSpec = getPartSpec((ASTNode) tableNode.getChild(1));
    // PlanUtils.addInput adds the ReadEntity object into "inputs" which is
    // used for authorization.
    Table table;
    try {
      table = getTable(tableNode, partitionSpec);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }

    if (table.isView()) {
      throw new SemanticException("COMPUTE STATISTICS cannot be executed for views");
    }

    PlanUtils.addInput(inputs, new ReadEntity(table, null, true));


    if (partitionSpec != null) {
      if (!AnalyzeCommandUtils.isIncrementalStats(root)) {
        throw new SemanticException(
            "Partitions cannot be statically specified in COMPUTE STATS in Impala without " +
            "INCREMENTAL");
      }
      // if partitionSpec is specified than validate that all partitions are specified
      validatePartSpec(table, partitionSpec, tableNode, conf, true);
      List<Partition> partitions = table.getTableSpec().partitions;
      Preconditions.checkState(partitions != null);
      Preconditions.checkState(partitions.size() == 1);
      outputs.add(new WriteEntity(partitions.get(0), WriteEntity.WriteType.DDL_NO_LOCK));
    }


    LOG.debug("Compute statistics analysis completed");
    queryState.setCommandType(HiveOperation.ANALYZE_TABLE);
    // The query result will contain a single row with a 'summary' string column.
    this.resultSchema = new ArrayList<>();
    this.resultSchema.add(new FieldSchema("summary", "string", ""));

    queryProperties.setAnalyzeRewrite(true);

    // We need to create a fetch operator since only Impala returns
    // results for these statement
    FetchWork fetch = new ImpalaFetchWork();
    fetchTask = (FetchTask) TaskFactory.get(fetch);
    String statsStatement = ImpalaSemanticAnalyzerUtils.getQueryWithDatabase(
        (ASTNode) tableNode.getChild(0),
        regenerateQueryFromAST(root),
        SessionState.get().getCurrentDatabase());
    String invalidateStmt =
        generateInvalidateMetadataStmt((ASTNode) tableNode.getChild(0), table);
    // second parameter forces invalidate metadata to run synchronously since we
    // don't want to kick off the compute stats statement until after the invalidate
    // metadata has been completed.
    ImpalaWork invalidateWork = ImpalaWork.createPlannedWork(invalidateStmt, false);
    Task<ImpalaWork> invalidateTask = TaskFactory.get(invalidateWork);
    this.rootTasks.add(TaskFactory.get(invalidateWork));
    ImpalaWork work = ImpalaWork.createPlannedWork(statsStatement, fetchTask, 1);
    Task<ImpalaWork> computeStatsTask = TaskFactory.get(work);
    this.rootTasks.add(computeStatsTask);
    invalidateTask.addDependentTask(computeStatsTask);
  }

  private String generateInvalidateMetadataStmt(ASTNode tableTokenNode, Table table) {
    // if the token has 2 children, it is of the form db.tbl and we grab the db name
    // from the token. Otherwise, we have to use the current db in the session.
    String db = tableTokenNode.getChildren().size() == 2
        ? ((ASTNode)tableTokenNode.getChild(0)).getText()
        : SessionState.get().getCurrentDatabase();
    return "invalidate metadata `" + db + "`.`" + table.getTableName() + "`";
  }

  private Table getTable(ASTNode tableNode, Map<String, String> partitionSpec)
      throws SemanticException, HiveException {
    String tableNameString = getUnescapedName((ASTNode) tableNode.getChild(0));
    Table table = getTable(tableNameString, true);
    TableSpec ts = new TableSpec(db, tableNameString, partitionSpec, true);
    table.setTableSpec(ts);
    return table;
  }

  private static String regenerateQueryFromAST(ASTNode hiveASTNode) throws SemanticException {
    ASTNode tableNodeParent = (ASTNode) hiveASTNode.getChild(0);
    String tableNameString = getTableString((ASTNode) tableNodeParent.getChild(0));
    String partitionString =
        getPartitionString(getPartSpec((ASTNode) tableNodeParent.getChild(1)));
    String incrementalString =
        AnalyzeCommandUtils.isIncrementalStats(hiveASTNode) ? "INCREMENTAL" : "";
    return "COMPUTE " + incrementalString + " STATS " + tableNameString + " " + partitionString;
  }

  private static String getTableString(ASTNode tableNode) {
    Preconditions.checkState(tableNode.getChildCount() <= 2);
    String tableString = "`" + tableNode.getChild(0).getText() + "`";
    if (tableNode.getChildCount() == 2) {
      tableString += "." + "`" + tableNode.getChild(1).getText() + "`";
    }
    return tableString;
  }

  private static String getPartitionString(Map<String, String> partitions) {
    if (partitions == null || partitions.size() == 0) {
      return "";
    }
    String partitionString = "PARTITION (";
    boolean addComma = false;
    for (String key : partitions.keySet()) {
      if (addComma) {
        partitionString += ", ";
      } else {
        addComma = true;
      }
      partitionString += key + "=" + partitions.get(key);
    }
    partitionString += ")";
    return partitionString;
  }

  // Generate the AST with Impala syntax
  public static ASTNode getASTNode(ComputeStatsStmt stmt, String command, Context ctx)
        throws ParseException {
    Token computeStatsToken = new ImmutableCommonToken(
        ImpalaToken.TOK_COMPUTE_STATS, ImpalaToken.COMPUTE_STATS_STRING);
    ImpalaASTNode computeStatsRoot = new ImpalaASTNode(computeStatsToken);
    String tableAndPartitionString = stmt.getTableName().toString();
    if (stmt.getPartitionSet() != null) {
      tableAndPartitionString += " " + stmt.getPartitionSet().toSql();
    }
    ASTNode tableASTNode =
        HiveSnippetParser.parse(tableAndPartitionString, HiveParser.TOK_TABLE_PARTITION);
    computeStatsRoot.addChild(tableASTNode);
    String incrString = stmt.isIncremental() ? "INCREMENTAL " : "";
    if (stmt.isIncremental()) {
      Token incrementalToken = new ImmutableCommonToken(HiveParser.KW_INCREMENTAL, "incremental");
      computeStatsRoot.addChild(new ASTNode(incrementalToken));
    }
    return computeStatsRoot;
  }

  // Given an AST in Hive format, regenerate it in Impala format. The Hive AST
  // was derived from the statement "ANALYZE TABLE <tbl> COMPUTE STATISTICS"
  public static ASTNode rewriteTree(ASTNode root, Context ctx) {
    String impalaComputeStatsStmt = "";
    try {
      impalaComputeStatsStmt = regenerateQueryFromAST(root);
      ComputeStatsStmt impalaStmt = (ComputeStatsStmt) Parser.parse(impalaComputeStatsStmt);
      return getASTNode(impalaStmt, impalaComputeStatsStmt, ctx);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
