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

package org.apache.hadoop.hive.impala;

import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HMSConverter;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.engine.EngineCompileHelper;
import org.apache.hadoop.hive.ql.engine.EngineEventSequence;
import org.apache.hadoop.hive.ql.engine.EngineQueryHelper;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ExplainSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.impala.calcite.ImpalaTypeSystemImpl;
import org.apache.hadoop.hive.impala.calcite.rules.TezEngineScalarFixerRule;
import org.apache.hadoop.hive.impala.parse.ImpalaParseException;
import org.apache.hadoop.hive.impala.parse.ImpalaToken;
import org.apache.hadoop.hive.impala.parse.ResetMetadataSemanticAnalyzer;
import org.apache.hadoop.hive.impala.plan.ImpalaHMSConverter;
import org.apache.hadoop.hive.impala.plan.ImpalaQueryHelperImpl;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.ResetMetadataStmt;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TPrimitiveType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ImpalaCompileHelper implements EngineCompileHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaCompileHelper.class);

  public HMSConverter getHMSConverter() {
    return new ImpalaHMSConverter();
  }

  public EngineEventSequence getEventSequence(String event) {
    return new ImpalaEventSequence(event);
  }

  public EngineQueryHelper getQueryHelper(HiveConf conf, String dbname, String username,
                                             HiveTxnManager txnMgr, Context ctx,
                                             QueryState queryState) throws SemanticException {
    return new ImpalaQueryHelperImpl(conf, dbname, username, txnMgr, ctx, queryState);
  }

  public EngineQueryHelper resetQueryHelper(
      EngineQueryHelper queryHelper) throws SemanticException {
    return new ImpalaQueryHelperImpl((ImpalaQueryHelperImpl) queryHelper);
  }

  public RelDataTypeSystem getRelDataTypeSystem() {
    return new ImpalaTypeSystemImpl();
  }

  @Override
  public HepProgram adjustPlanForEngine() {
    HepProgramBuilder programBuilder = new HepProgramBuilder();
    programBuilder.addRuleInstance(TezEngineScalarFixerRule.INSTANCE);
    return programBuilder.build();
  }

  @Override
  public ASTNode parse(String command, Context ctx) throws ParseException {
    ParseException caughtException = null;
    ASTNode tree = null;
    // First we try parsing through the regular Hive parser. If it doesn't parse
    // correctly, hold onto the exception while we try other means.
    try {
      tree = ParseUtils.parse(command, ctx);
    } catch(ParseException e) {
      caughtException = e;
    }
    if (caughtException == null) {
      return tree;
    }

    // Now try through the Impala parser. Note: If the Impala parser works, it will
    // return an Impala StatementBase object, and this needs to be converted into an
    // ASTNode tree that can be handled by the ql module.
    try {
      return getASTForImpala(command, ctx);
    } catch (AnalysisException e) {
      LOG.info("Impala parsing exception: " + e);
      // Need to determine which exception message to throw back.
      throw isImpalaCommand(command) ? new ImpalaParseException(e) : caughtException;
    }
  }

  @Override
  public SemanticAnalyzer getSemanticAnalyzer(QueryState queryState, ASTNode tree)
      throws SemanticException {
    if (tree.getToken() == null) {
      throw new RuntimeException("Empty Syntax Tree");
    }

    switch (tree.getType()) {
      case ImpalaToken.TOK_REFRESH_TABLE:
        return new ResetMetadataSemanticAnalyzer(queryState);
      default:
        throw new SemanticException("Unknown token found: " + tree.getType());
    }
  }

  @Override
  public HiveOperation getCommandType(ASTNode root) throws SemanticException {
    if (root.getToken() == null) {
      return null;
    }
    switch(root.getToken().getType()) {
      case ImpalaToken.TOK_REFRESH_TABLE:
        return HiveOperation.REFRESH_TABLE;
      default:
        return null;
    }
  }

  private ASTNode getASTForImpala(String command, Context ctx)
      throws AnalysisException, ParseException {
    StatementBase impalaStmt = Parser.parse(command);
    if (impalaStmt instanceof ResetMetadataStmt) {
      return ResetMetadataSemanticAnalyzer.getASTNode((ResetMetadataStmt) impalaStmt,
          command, ctx);
    }
    throw new AnalysisException("This is a valid Impala statement but it is not supported " +
        "yet.");
  }

  private boolean isImpalaCommand(String command) {
    String[] tokens = command.trim().split("\\s");
    if (tokens[0].toLowerCase().equals("refresh")) {
      return true;
    }
    return false;
  }
}
