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
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.function.AbstractFunctionAnalyzer;
import org.apache.hadoop.hive.ql.ddl.function.reload.ReloadFunctionsDesc;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
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
import org.apache.hadoop.hive.impala.exec.ImpalaTask;
import org.apache.hadoop.hive.impala.work.ImpalaWork;
import org.apache.impala.analysis.CreateFunctionStmtBase;
import org.apache.impala.analysis.CreateFunctionStmtBase.OptArg;
import org.apache.impala.analysis.CreateUdfStmt;
import org.apache.impala.analysis.Parser;
import org.apache.impala.common.AnalysisException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzer for the create function command.
 */
public class CreateFuncSemanticAnalyzer extends AbstractFunctionAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(CreateFuncSemanticAnalyzer.class);

  private List<FieldSchema> resultSchema;

  public CreateFuncSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    LOG.debug("Create function semantic analyzer");
    assert root.getType() == ImpalaToken.TOK_CREATE_FUNCTION;
    String functionName = root.getChild(0).getText().toLowerCase();
    boolean isTemporary = false;
    String className = unescapeSQLString(root.getChild(1).getText());

    String location = PlanUtils.stripQuotes(unescapeSQLString(root.getChild(2).getText()));
    List<ResourceUri> resources = new ArrayList<ResourceUri>();
    resources.add(new ResourceUri(ResourceType.JAR, location));

    FetchWork fetch = new ImpalaFetchWork();
    fetchTask = (FetchTask) TaskFactory.get(fetch);

    addEntities(functionName, className, isTemporary, resources);

    String createFuncStmt = queryState.getQueryString();
    if (!functionName.contains(".")) {
      createFuncStmt = ImpalaSemanticAnalyzerUtils.getQueryWithDatabase(
          functionName, createFuncStmt, SessionState.get().getCurrentDatabase());
    }
    ImpalaWork work = ImpalaWork.createPlannedWork(createFuncStmt, fetchTask, 1);
    Task<ImpalaWork> impalaTask = TaskFactory.get(work);
    this.rootTasks.add(impalaTask);
    queryState.setCommandType(HiveOperation.CREATEFUNCTION);

    Task<DDLWork> reloadTask =
        TaskFactory.get(new DDLWork(getInputs(), getOutputs(), new ReloadFunctionsDesc()));
    rootTasks.add(reloadTask);
    impalaTask.addDependentTask(reloadTask);
    // The query result will contain a single row with a 'summary' string column.
    this.resultSchema = new ArrayList<>();
    this.resultSchema.add(new FieldSchema("summary", "string", ""));

  }

  public static ASTNode getASTNode(CreateFunctionStmtBase stmt, String command, Context ctx)
        throws AnalysisException, ParseException {
    Token createFuncToken = new ImmutableCommonToken(
        ImpalaToken.TOK_CREATE_FUNCTION, ImpalaToken.CREATE_FUNCTION_STRING);
    ImpalaASTNode createFuncRoot = new ImpalaASTNode(createFuncToken);

    Token functionNameToken = new ImmutableCommonToken(
        HiveParser.StringLiteral, stmt.getFunctionName().toString());
    ASTNode funcNameASTNode = new ASTNode(functionNameToken);
    createFuncRoot.addChild(funcNameASTNode);

    OptArg arg = (stmt instanceof CreateUdfStmt) ? OptArg.SYMBOL : OptArg.UPDATE_FN;
    Token classToken = new ImmutableCommonToken(
        HiveParser.StringLiteral, stmt.checkAndGetOptArg(arg).toString());
    ASTNode funcClassASTNode = new ASTNode(classToken);
    createFuncRoot.addChild(funcClassASTNode);

    Token locationToken = new ImmutableCommonToken(
        HiveParser.StringLiteral, "'" + stmt.getLocation().toString() + "'");
    ASTNode locationASTNode = new ASTNode(locationToken);
    createFuncRoot.addChild(locationASTNode);

    return createFuncRoot;
  }

  @Override
  public List<FieldSchema> getResultSchema() {
    return resultSchema;
  }
}
