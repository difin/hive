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
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.antlr.runtime.Token;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.parse.AnalyzeCommandUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ImmutableCommonToken;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.impala.parse.StmtTypeConstants.TableAfterCommand;
import org.apache.impala.analysis.ComputeStatsStmt;
import org.apache.impala.analysis.Parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzer for stats computing command.
 */
public class ComputeStatsWithHiveSyntaxSemanticAnalyzer
    extends ImpalaSyntaxSemanticAnalyzer {

  private static final List<String> ANALYZE_TABLE_KEYWORDS = ImmutableList.of("analyze", "table");
  private static final String ANALYZE_TABLE_STRING = StringUtils.join(ANALYZE_TABLE_KEYWORDS, ' ');

  private static final Logger LOG =
      LoggerFactory.getLogger(ComputeStatsWithHiveSyntaxSemanticAnalyzer.class);

  public ComputeStatsWithHiveSyntaxSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState, StatementType.COMPUTE_STATS);
    LOG.info("Generating Impala syntax for analyze table compute statistics command.");
  }

  @Override
  protected boolean overrideQueryString() {
    return true;
  }

  /**
   * Override the query string in the Semantic Analyzer. The semantic analyzer will contain
   * the 'analyze table', statement created by the user, not the regenerated 'compute stats'
   * statement, so we need to regenerate the Impala statement again.
   */
  @Override
  protected String getOverrideQueryString(ASTNode root) {
    try {
      return regenerateQueryFromAST(root);
    } catch (Exception e) {
      // throw a runtime exception here since the "regenerate" call has already
      // been called before with success and should never fail.
      throw new RuntimeException(e);
    }
  }

  /**
   * Regenerate the Impala 'compute stats' statement given the ASTNode.
   */
  private static String regenerateQueryFromAST(ASTNode hiveASTNode) throws SemanticException {
    // Top level first parameter contains the table name.
    ASTNode tableNodeParent = (ASTNode) hiveASTNode.getChild(0);
    String tableNameString = getTableString((ASTNode) tableNodeParent.getChild(0));
    // Top level second parameter contains the partition.
    String partitionString =
        getPartitionString(getPartSpec((ASTNode) tableNodeParent.getChild(1)));
    // Node will also contain whether the incremental keyword was in the original command.
    String incrementalString =
        AnalyzeCommandUtils.isIncrementalStats(hiveASTNode) ? "INCREMENTAL" : "";
    return "COMPUTE " + incrementalString + " STATS " + tableNameString + " " + partitionString;
  }

  /**
   * Create the table string. If it has two leaves, the first one is the db.
   */
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

  /**
   * Static method used to generate the Impala ASTNode given the ASTNode generated
   * by the Hive parser.
   */
  public static ASTNode rewriteTree(ASTNode root) {
    try {
      String impalaComputeStatsStmt = regenerateQueryFromAST(root);
      ComputeStatsStmt impalaStmtObj = (ComputeStatsStmt) Parser.parse(impalaComputeStatsStmt);
      StatementType stmtType = impalaStmtObj.isIncremental()
          ? StatementType.COMPUTE_INCR_STATS
          : StatementType.COMPUTE_STATS;
      ASTNode rootNode = StatementType.getASTNode(
          StmtTypeConstants.TOK_COMPUTE_STATS_WITH_HIVE_SYNTAX,
          impalaComputeStatsStmt, stmtType.keywords, TableAfterCommand.REQUIRED);
      if (impalaStmtObj.isIncremental()) {
        Token incrementalToken = new ImmutableCommonToken(HiveParser.KW_INCREMENTAL, "incremental");
        rootNode.addChild(new ASTNode(incrementalToken));
      }
      return rootNode;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
