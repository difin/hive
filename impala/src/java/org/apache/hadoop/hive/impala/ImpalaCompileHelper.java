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
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ExplainSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.impala.calcite.ImpalaTypeSystemImpl;
import org.apache.hadoop.hive.impala.calcite.rules.TezEngineScalarFixerRule;
import org.apache.hadoop.hive.impala.parse.ComputeStatsSemanticAnalyzer;
import org.apache.hadoop.hive.impala.parse.CreateFuncSemanticAnalyzer;
import org.apache.hadoop.hive.impala.parse.DropStatsSemanticAnalyzer;
import org.apache.hadoop.hive.impala.parse.ImpalaParseException;
import org.apache.hadoop.hive.impala.parse.ImpalaToken;
import org.apache.hadoop.hive.impala.parse.ResetMetadataSemanticAnalyzer;
import org.apache.hadoop.hive.impala.plan.ImpalaHMSConverter;
import org.apache.hadoop.hive.impala.plan.ImpalaQueryHelperImpl;
import org.apache.hadoop.hive.impala.calcite.ImpalaTypeSystemImpl;
import org.apache.impala.analysis.ComputeStatsStmt;
import org.apache.impala.analysis.CreateFunctionStmtBase;
import org.apache.impala.analysis.DropStatsStmt;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.ResetMetadataStmt;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TPrimitiveType;

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ImpalaCompileHelper implements EngineCompileHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaCompileHelper.class);

  private static Pattern REFRESH_STMT =
      Pattern.compile("\\s*refresh\\s+.*", Pattern.CASE_INSENSITIVE);
  private static Pattern COMPUTE_STATS_STMT =
      Pattern.compile("\\s*compute\\s+.*", Pattern.CASE_INSENSITIVE);
  private static Pattern DROP_STATS_STMT =
      Pattern.compile("\\s*drop\\s+stats\\s+.*", Pattern.CASE_INSENSITIVE);
  private static Pattern DROP_INCR_STATS_STMT =
      Pattern.compile("\\s*drop\\s+incremental\\s+stats\\s+.*", Pattern.CASE_INSENSITIVE);
  private static Pattern CREATE_AGG_FUNC_STMT =
      Pattern.compile("\\s*create\\s*aggregate\\s+function\\s+.*", Pattern.CASE_INSENSITIVE);
  private static Pattern CREATE_FUNC_STMT =
      Pattern.compile("\\s*create\\s+function\\s+.*", Pattern.CASE_INSENSITIVE);

  private enum ParsingErrorEngine {
    IMPALA,
    HIVE,
    AMBIGUOUS
  }

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
      return convertTreeIfNecessary(tree, ctx);
    }

    // Now try through the Impala parser. Note: If the Impala parser works, it will
    // return an Impala StatementBase object, and this needs to be converted into an
    // ASTNode tree that can be handled by the ql module.
    try {
      return getASTForImpala(command, ctx);
    } catch (AnalysisException e) {
      LOG.info("Impala parsing exception: " + e);
      switch (getParsingErrorType(command)) {
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

  @Override
  public BaseSemanticAnalyzer getSemanticAnalyzer(QueryState queryState, ASTNode tree)
      throws SemanticException {
    if (tree.getToken() == null) {
      throw new RuntimeException("Empty Syntax Tree");
    }

    switch (tree.getType()) {
      case ImpalaToken.TOK_REFRESH_TABLE:
        return new ResetMetadataSemanticAnalyzer(queryState);
      case ImpalaToken.TOK_DROP_STATS:
        return new DropStatsSemanticAnalyzer(queryState);
      case ImpalaToken.TOK_COMPUTE_STATS_WITH_HIVE_SYNTAX:
      case ImpalaToken.TOK_COMPUTE_STATS_WITH_IMPALA_SYNTAX:
        return new ComputeStatsSemanticAnalyzer(queryState);
      case ImpalaToken.TOK_CREATE_FUNCTION:
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
    switch(root.getToken().getType()) {
      case ImpalaToken.TOK_REFRESH_TABLE:
        return HiveOperation.REFRESH_TABLE;
      case ImpalaToken.TOK_DROP_STATS:
        return HiveOperation.DROP_STATS;
      case ImpalaToken.TOK_COMPUTE_STATS_WITH_HIVE_SYNTAX:
      case ImpalaToken.TOK_COMPUTE_STATS_WITH_IMPALA_SYNTAX:
        return HiveOperation.ANALYZE_TABLE;
      case ImpalaToken.TOK_CREATE_FUNCTION:
        return HiveOperation.CREATEFUNCTION;
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
    if (impalaStmt instanceof DropStatsStmt) {
      return DropStatsSemanticAnalyzer.getASTNode((DropStatsStmt) impalaStmt, command, ctx);
    }
    if (impalaStmt instanceof ComputeStatsStmt) {
      return ComputeStatsSemanticAnalyzer.getASTNode((ComputeStatsStmt) impalaStmt, command, ctx);
    }
    if (impalaStmt instanceof CreateFunctionStmtBase) {
      return CreateFuncSemanticAnalyzer.getASTNode((CreateFunctionStmtBase) impalaStmt, command, ctx);
    }
    throw new AnalysisException("This is a valid Impala statement but it is not supported " +
        "yet.");
  }

  /**
   * getParsingErrorType takes a command and decides whether the returned error
   * should come from Hive, Impala, or potentially either one (ambiguous)
   */
  private ParsingErrorEngine getParsingErrorType(String command) {
    if (REFRESH_STMT.matcher(command).matches() ||
        COMPUTE_STATS_STMT.matcher(command).matches() ||
        DROP_STATS_STMT.matcher(command).matches() ||
        DROP_INCR_STATS_STMT.matcher(command).matches() ||
        CREATE_AGG_FUNC_STMT.matcher(command).matches()) {
      return ParsingErrorEngine.IMPALA;
    }

    if (CREATE_FUNC_STMT.matcher(command).matches()) {
      return ParsingErrorEngine.AMBIGUOUS;
    }

    return ParsingErrorEngine.HIVE;
  }

  private ASTNode convertTreeIfNecessary(ASTNode astNode, Context ctx) {
    // Only analyze (compute stats) needs to change the tree.  The reason compute stats
    // needs a different tree is because we need Impala to run the compute stats command.
    if (astNode.getType() != HiveParser.TOK_ANALYZE) {
      return astNode;
    }
    return ComputeStatsSemanticAnalyzer.rewriteTree(astNode, ctx);
  }

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
