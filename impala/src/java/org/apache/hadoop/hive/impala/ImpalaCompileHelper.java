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

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HMSConverter;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.engine.EngineCompileHelper;
import org.apache.hadoop.hive.ql.engine.EngineEventSequence;
import org.apache.hadoop.hive.ql.engine.EngineQueryHelper;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.impala.calcite.ImpalaTypeSystemImpl;
import org.apache.hadoop.hive.impala.calcite.rules.TezEngineScalarFixerRule;
import org.apache.hadoop.hive.impala.parse.ComputeStatsWithHiveSyntaxSemanticAnalyzer;
import org.apache.hadoop.hive.impala.parse.CreateFuncSemanticAnalyzer;
import org.apache.hadoop.hive.impala.parse.ImpalaParseException;
import org.apache.hadoop.hive.impala.parse.ShowColumnStatsSemanticAnalyzer;
import org.apache.hadoop.hive.impala.parse.StatementType;
import org.apache.hadoop.hive.impala.parse.StmtTypeConstants;
import org.apache.hadoop.hive.impala.parse.ImpalaSyntaxSemanticAnalyzer;
import org.apache.hadoop.hive.impala.plan.ImpalaHMSConverter;
import org.apache.hadoop.hive.impala.plan.ImpalaQueryHelperImpl;
import org.apache.impala.analysis.AlterTableDropColStmt;
import org.apache.impala.analysis.ComputeStatsStmt;
import org.apache.impala.analysis.DropStatsStmt;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.ResetMetadataStmt;
import org.apache.impala.analysis.ShowFilesStmt;
import org.apache.impala.analysis.ShowStatsStmt;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.common.AnalysisException;


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
      if (tree.getToken().getType() != HiveParser.TOK_SHOWPARTITIONS) {
        return convertTreeIfNecessary(tree);
      }
    }

    // Now try through the Impala parser.
    try {
      return StatementType.getASTNode(command);
    } catch (AnalysisException e) {
      LOG.info("Impala parsing exception: " + e);
      switch (StatementType.getParsingErrorType(command)) {
        case IMPALA:
          throw new ImpalaParseException(e.getMessage());
        case HIVE:
          throw caughtException;
        case AMBIGUOUS:
          boolean useImpalaExceptionMsg = SessionState.get().getConf().getBoolVar(
              ConfVars.HIVE_IMPALA_SHOW_IMPALA_PARSING_ERROR);
          Exception exceptionToUse = useImpalaExceptionMsg ? e : caughtException;
          throw getExtendedParsingExceptionMessage(exceptionToUse, useImpalaExceptionMsg);
        default:
          Preconditions.checkState(false);
          return null;
      }
    }
  }

  /**
   * Get the appropriate SemanticAnalyzer object. Anything that has a ASTNode where
   * the top token is of type ImpalaToken will get the SemanticAnalyzer from this
   * method.
   */
  @Override
  public BaseSemanticAnalyzer getSemanticAnalyzer(QueryState queryState, ASTNode tree)
      throws SemanticException {
    if (tree.getToken() == null) {
      throw new RuntimeException("Empty Syntax Tree");
    }

    // Most SemanticAnalyzers for Impala commands will just use the generic
    // ImpalaSyntaxSemanticAnalyzer.
    StatementType stmtType = StatementType.getStatementType(tree.getType());
    if (stmtType != null) {
      if (stmtType == StatementType.SHOW_COLUMN_STATS) {
        return new ShowColumnStatsSemanticAnalyzer(queryState, SessionState.get().getConf());
      }
      return new ImpalaSyntaxSemanticAnalyzer(queryState, stmtType);
    }

    // Two exceptions for special SemanticAnalyzers
    // 1) Compute statistics with Hive Syntax.  The Impala syntax for computing stats
    //    is 'compute stats ...' whereas the hive syntax is 'analyze table <tbl>
    //    compute statistics'.  The command still runs on Impala but has to be
    //    regenerated with Impala syntax.
    // 2) Create Function.  The parsing for create function is too different
    //    from other parsers (most of which have a <tbl>) to use the generic
    //    semantic analyzer.
    switch (tree.getType()) {
      case StmtTypeConstants.TOK_COMPUTE_STATS_WITH_HIVE_SYNTAX:
        return new ComputeStatsWithHiveSyntaxSemanticAnalyzer(queryState);
      case StmtTypeConstants.TOK_CREATE_FUNCTION:
        return new CreateFuncSemanticAnalyzer(queryState);
      default:
        throw new SemanticException("Unknown token found: " + tree.getType());
    }
  }

  @Override
  public HiveOperation getCommandType(ASTNode root) throws SemanticException {
    if (root.getToken() == null) {
      return null;
    }

    HiveOperation operation = StatementType.getOperation(root.getToken().getType());
    if (operation != null) {
      return operation;
    }

    switch(root.getToken().getType()) {
      case StmtTypeConstants.TOK_COMPUTE_STATS_WITH_HIVE_SYNTAX:
        return HiveOperation.ANALYZE_TABLE;
      case StmtTypeConstants.TOK_CREATE_FUNCTION:
        return HiveOperation.CREATEFUNCTION;
      default:
        return null;
    }
  }

  /**
   * Convert the ASTNode if necessary.
   * Only analyze (compute statistics) needs to change the tree.  The reason compute statistics
   * needs a different tree is because the tree has been created via the Hive parser,
   * but we need Impala to run the compute statistics command. We convert the tree to the
   * similar tree created by the Impala 'compute statistics' command.
   */
  private ASTNode convertTreeIfNecessary(ASTNode astNode) {
    if (astNode.getType() != HiveParser.TOK_ANALYZE) {
      return astNode;
    }
    return ComputeStatsWithHiveSyntaxSemanticAnalyzer.rewriteTree(astNode);
  }

  /**
   * Helper method to place the right engine in the parsing error message.
   */
  private ParseException getExtendedParsingExceptionMessage(Exception e, boolean impalaEngine) {
    String parsingMessage = e.getMessage() + "\n";
    String parsingMessageEngine = impalaEngine ? "Impala" : "Hive";
    String nonParsingMessageEngine = impalaEngine ? "Hive" : "Impala";
    return new ImpalaParseException(parsingMessage + "Note: this parsing error was produced by " +
        parsingMessageEngine + "." + " To see the parsing error message produced by " +
        nonParsingMessageEngine + ", set \"" + ConfVars.HIVE_IMPALA_SHOW_IMPALA_PARSING_ERROR +
        "\" to " + !impalaEngine);
  }
}
