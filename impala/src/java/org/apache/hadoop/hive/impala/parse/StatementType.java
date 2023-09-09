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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.antlr.runtime.Token;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ImmutableCommonToken;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.impala.parse.StmtTypeConstants.ParsingErrorEngine;
import org.apache.hadoop.hive.impala.parse.StmtTypeConstants.TableAfterCommand;
import org.apache.hadoop.hive.impala.parse.StmtTypeConstants.WriteLock;
import org.apache.impala.analysis.AlterTableAddColsStmt;
import org.apache.impala.analysis.AlterTableAddPartitionStmt;
import org.apache.impala.analysis.AlterTableAlterColStmt;
import org.apache.impala.analysis.AlterTableDropColStmt;
import org.apache.impala.analysis.AlterTableAddColsStmt;
import org.apache.impala.analysis.AlterTableAddDropRangePartitionStmt;
import org.apache.impala.analysis.AlterTableAddPartitionStmt;
import org.apache.impala.analysis.AlterTableAlterColStmt;
import org.apache.impala.analysis.AlterTableDropColStmt;
import org.apache.impala.analysis.AlterTableDropPartitionStmt;
import org.apache.impala.analysis.AlterTableExecuteExpireSnapshotsStmt;
import org.apache.impala.analysis.AlterTableExecuteRollbackStmt;
import org.apache.impala.analysis.AlterTableExecuteStmt;
import org.apache.impala.analysis.AlterTableOrViewRenameStmt;
import org.apache.impala.analysis.AlterTableOrViewSetOwnerStmt;
import org.apache.impala.analysis.AlterTableRecoverPartitionsStmt;
import org.apache.impala.analysis.AlterTableReplaceColsStmt;
import org.apache.impala.analysis.AlterTableSetCachedStmt;
import org.apache.impala.analysis.AlterTableSetColumnStats;
import org.apache.impala.analysis.AlterTableSetFileFormatStmt;
import org.apache.impala.analysis.AlterTableSetLocationStmt;
import org.apache.impala.analysis.AlterTableSetOwnerStmt;
import org.apache.impala.analysis.AlterTableSetPartitionSpecStmt;
import org.apache.impala.analysis.AlterTableSetRowFormatStmt;
import org.apache.impala.analysis.AlterTableSetStmt;
import org.apache.impala.analysis.AlterTableSetTblProperties;
import org.apache.impala.analysis.AlterTableSortByStmt;
import org.apache.impala.analysis.AlterTableStmt;
import org.apache.impala.analysis.AlterTableUnSetTblProperties;
import org.apache.impala.analysis.ComputeStatsStmt;
import org.apache.impala.analysis.CreateFunctionStmtBase;
import org.apache.impala.analysis.CreateTableAsSelectStmt;
import org.apache.impala.analysis.CreateTableLikeStmt;
import org.apache.impala.analysis.CreateTableStmt;
import org.apache.impala.analysis.DropStatsStmt;
import org.apache.impala.analysis.ResetMetadataStmt;
import org.apache.impala.analysis.ShowFilesStmt;
import org.apache.impala.analysis.ShowStatsStmt;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.analysis.Parser;
import org.apache.impala.common.AnalysisException;

/**
 * Enum containing all statement types and their attributes.
 * This file also contains some static methods used with these
 * statement types.
 */
public enum StatementType {

  DROP_STATS(StmtTypeConstants.TOK_DROP_STATS,
      ImmutableList.of("drop", "stats"),
      HiveOperation.DROP_STATS,
      StmtTypeConstants.SUMMARY_COLS,
      StmtTypeConstants.SUMMARY_TYPES,
      StmtTypeConstants.NEEDS_INVALIDATE_CMD,
      StmtTypeConstants.NO_PARTITION_COLS,
      StmtTypeConstants.CREATE_FETCH_TASK,
      TableAfterCommand.REQUIRED,
      StmtTypeConstants.WriteLock.NO_DDL_LOCK,
      StmtTypeConstants.ParsingErrorEngine.IMPALA,
      StmtTypeConstants.DROP_STATS_VALIDATOR,
      ImmutableSet.of(DropStatsStmt.class)),

  DROP_INCR_STATS(StmtTypeConstants.TOK_DROP_INCR_STATS,
      ImmutableList.of("drop", "incremental", "stats"),
      HiveOperation.DROP_STATS,
      StmtTypeConstants.SUMMARY_COLS,
      StmtTypeConstants.SUMMARY_TYPES,
      StmtTypeConstants.NEEDS_INVALIDATE_CMD,
      StmtTypeConstants.NO_PARTITION_COLS,
      StmtTypeConstants.CREATE_FETCH_TASK,
      TableAfterCommand.REQUIRED,
      StmtTypeConstants.WriteLock.NO_DDL_LOCK,
      StmtTypeConstants.ParsingErrorEngine.IMPALA,
      StmtTypeConstants.DROP_STATS_VALIDATOR,
      ImmutableSet.of(DropStatsStmt.class)),

  COMPUTE_STATS(StmtTypeConstants.TOK_COMPUTE_STATS,
      ImmutableList.of("compute", "stats"),
      HiveOperation.ANALYZE_TABLE,
      StmtTypeConstants.SUMMARY_COLS,
      StmtTypeConstants.SUMMARY_TYPES,
      StmtTypeConstants.NEEDS_INVALIDATE_CMD,
      StmtTypeConstants.NO_PARTITION_COLS,
      StmtTypeConstants.CREATE_FETCH_TASK,
      TableAfterCommand.REQUIRED,
      StmtTypeConstants.WriteLock.NO_DDL_LOCK,
      StmtTypeConstants.ParsingErrorEngine.IMPALA,
      StmtTypeConstants.NO_VALIDATOR_DEFINED,
      ImmutableSet.of(ComputeStatsStmt.class)),

  COMPUTE_INCR_STATS(StmtTypeConstants.TOK_COMPUTE_INCR_STATS,
      ImmutableList.of("compute", "incremental", "stats"),
      HiveOperation.ANALYZE_TABLE,
      StmtTypeConstants.SUMMARY_COLS,
      StmtTypeConstants.SUMMARY_TYPES,
      StmtTypeConstants.NEEDS_INVALIDATE_CMD,
      StmtTypeConstants.NO_PARTITION_COLS,
      StmtTypeConstants.CREATE_FETCH_TASK,
      TableAfterCommand.REQUIRED,
      StmtTypeConstants.WriteLock.NO_DDL_LOCK,
      StmtTypeConstants.ParsingErrorEngine.IMPALA,
      StmtTypeConstants.NO_VALIDATOR_DEFINED,
      ImmutableSet.of(ComputeStatsStmt.class)),

  ALTER_TABLE_DROP_COL(StmtTypeConstants.TOK_ALTER_TABLE_DROP_COL,
      ImmutableList.of("alter", "table"),
      HiveOperation.ALTERTABLE_REPLACECOLS,
      StmtTypeConstants.SUMMARY_COLS,
      StmtTypeConstants.SUMMARY_TYPES,
      StmtTypeConstants.NEEDS_INVALIDATE_CMD,
      StmtTypeConstants.NO_PARTITION_COLS,
      StmtTypeConstants.CREATE_FETCH_TASK,
      TableAfterCommand.REQUIRED,
      StmtTypeConstants.WriteLock.ACID_DDL_LOCK,
      StmtTypeConstants.ParsingErrorEngine.HIVE,
      StmtTypeConstants.NO_VALIDATOR_DEFINED,
      ImmutableSet.of(AlterTableAddColsStmt.class, AlterTableAddDropRangePartitionStmt.class,
          AlterTableAddPartitionStmt.class, AlterTableAlterColStmt.class,
          AlterTableDropColStmt.class, AlterTableDropPartitionStmt.class,
          AlterTableExecuteExpireSnapshotsStmt.class, AlterTableExecuteRollbackStmt.class,
          AlterTableExecuteStmt.class, AlterTableOrViewRenameStmt.class,
          AlterTableOrViewSetOwnerStmt.class,  AlterTableRecoverPartitionsStmt.class,
          AlterTableReplaceColsStmt.class, AlterTableSetCachedStmt.class,
          AlterTableSetColumnStats.class, AlterTableSetFileFormatStmt.class,
          AlterTableSetLocationStmt.class, AlterTableSetOwnerStmt.class,
          AlterTableSetPartitionSpecStmt.class, AlterTableSetRowFormatStmt.class,
          AlterTableSetStmt.class, AlterTableSetTblProperties.class,
          AlterTableSortByStmt.class, AlterTableStmt.class,
          AlterTableUnSetTblProperties.class)),

  SHOW_TABLE_STATS(StmtTypeConstants.TOK_SHOW_TABLE_STATS,
      ImmutableList.of("show", "table", "stats"),
      HiveOperation.SHOW_TABLE_STATS,
      StmtTypeConstants.SHOW_TABLE_STATS_COLS,
      StmtTypeConstants.SHOW_TABLE_STATS_TYPES,
      StmtTypeConstants.NEEDS_INVALIDATE_CMD,
      StmtTypeConstants.PREPEND_PARTITION_COLS,
      StmtTypeConstants.CREATE_FETCH_TASK,
      TableAfterCommand.REQUIRED,
      StmtTypeConstants.WriteLock.NO_LOCK,
      StmtTypeConstants.ParsingErrorEngine.HIVE,
      StmtTypeConstants.NO_VALIDATOR_DEFINED,
      ImmutableSet.of(ShowStatsStmt.class)),

  SHOW_COLUMN_STATS(StmtTypeConstants.TOK_SHOW_COLUMN_STATS,
      ImmutableList.of("show", "column", "stats"),
      HiveOperation.SHOW_COLUMN_STATS,
      StmtTypeConstants.SHOW_COLUMN_STATS_COLS,
      StmtTypeConstants.SHOW_COLUMN_STATS_TYPES,
      StmtTypeConstants.NEEDS_INVALIDATE_CMD,
      StmtTypeConstants.NO_PARTITION_COLS,
      StmtTypeConstants.CREATE_FETCH_TASK,
      TableAfterCommand.REQUIRED,
      StmtTypeConstants.WriteLock.NO_LOCK,
      StmtTypeConstants.ParsingErrorEngine.HIVE,
      StmtTypeConstants.NO_VALIDATOR_DEFINED,
      ImmutableSet.of(ShowStatsStmt.class)),

  SHOW_PARTITIONS(StmtTypeConstants.TOK_SHOW_PARTITIONS,
      ImmutableList.of("show", "partitions"),
      HiveOperation.SHOWPARTITIONS,
      StmtTypeConstants.SHOW_PARTITION_COLS,
      StmtTypeConstants.SHOW_PARTITION_TYPES,
      StmtTypeConstants.NEEDS_INVALIDATE_CMD,
      StmtTypeConstants.PREPEND_PARTITION_COLS,
      StmtTypeConstants.CREATE_FETCH_TASK,
      TableAfterCommand.REQUIRED,
      StmtTypeConstants.WriteLock.NO_LOCK,
      StmtTypeConstants.ParsingErrorEngine.HIVE,
      StmtTypeConstants.NO_VALIDATOR_DEFINED,
      ImmutableSet.of(ShowStatsStmt.class)),

  SHOW_RANGE_PARTITIONS(StmtTypeConstants.TOK_SHOW_RANGE_PARTITIONS,
      ImmutableList.of("show", "range", "partitions"),
      HiveOperation.SHOWPARTITIONS,
      StmtTypeConstants.SHOW_PARTITION_COLS,
      StmtTypeConstants.SHOW_PARTITION_TYPES,
      StmtTypeConstants.NEEDS_INVALIDATE_CMD,
      StmtTypeConstants.PREPEND_PARTITION_COLS,
      StmtTypeConstants.CREATE_FETCH_TASK,
      TableAfterCommand.REQUIRED,
      StmtTypeConstants.WriteLock.NO_LOCK,
      StmtTypeConstants.ParsingErrorEngine.IMPALA,
      StmtTypeConstants.NO_VALIDATOR_DEFINED,
      ImmutableSet.of(ShowStatsStmt.class)),

  SHOW_FILES_IN(StmtTypeConstants.TOK_SHOW_FILES_IN,
      ImmutableList.of("show", "files", "in"),
      HiveOperation.SHOW_FILES_IN,
      StmtTypeConstants.SHOW_FILES_IN_COLS,
      StmtTypeConstants.SHOW_FILES_IN_TYPES,
      StmtTypeConstants.NEEDS_INVALIDATE_CMD,
      StmtTypeConstants.NO_PARTITION_COLS,
      StmtTypeConstants.CREATE_FETCH_TASK,
      TableAfterCommand.REQUIRED,
      StmtTypeConstants.WriteLock.NO_LOCK,
      StmtTypeConstants.ParsingErrorEngine.IMPALA,
      StmtTypeConstants.NO_VALIDATOR_DEFINED,
      ImmutableSet.of(ShowFilesStmt.class)),

  INVALIDATE_METADATA(StmtTypeConstants.TOK_INVALIDATE_METADATA,
      ImmutableList.of("invalidate", "metadata"),
      HiveOperation.REFRESH_TABLE,
      StmtTypeConstants.NO_COLS,
      StmtTypeConstants.NO_TYPES,
      StmtTypeConstants.NO_INVALIDATE_CMD,
      StmtTypeConstants.NO_PARTITION_COLS,
      StmtTypeConstants.NO_FETCH_TASK,
      TableAfterCommand.OPTIONAL,
      StmtTypeConstants.WriteLock.NO_LOCK,
      StmtTypeConstants.ParsingErrorEngine.IMPALA,
      StmtTypeConstants.REFRESH_VALIDATOR,
      ImmutableSet.of(ResetMetadataStmt.class)),

  REFRESH_AUTH(StmtTypeConstants.TOK_REFRESH_AUTH,
      ImmutableList.of("refresh", "authorization"),
      HiveOperation.REFRESH_TABLE,
      StmtTypeConstants.NO_COLS,
      StmtTypeConstants.NO_TYPES,
      StmtTypeConstants.NO_INVALIDATE_CMD,
      StmtTypeConstants.NO_PARTITION_COLS,
      StmtTypeConstants.NO_FETCH_TASK,
      TableAfterCommand.NOT_REQUIRED,
      StmtTypeConstants.WriteLock.NO_LOCK,
      StmtTypeConstants.ParsingErrorEngine.IMPALA,
      StmtTypeConstants.NO_VALIDATOR_DEFINED,
      ImmutableSet.of(ResetMetadataStmt.class)),

  REFRESH_FUNCTION(StmtTypeConstants.TOK_REFRESH_FUNCTION,
      ImmutableList.of("refresh", "functions"),
      HiveOperation.REFRESH_TABLE,
      StmtTypeConstants.NO_COLS,
      StmtTypeConstants.NO_TYPES,
      StmtTypeConstants.NO_INVALIDATE_CMD,
      StmtTypeConstants.NO_PARTITION_COLS,
      StmtTypeConstants.NO_FETCH_TASK,
      TableAfterCommand.NOT_REQUIRED,
      StmtTypeConstants.WriteLock.NO_LOCK,
      StmtTypeConstants.ParsingErrorEngine.IMPALA,
      StmtTypeConstants.NO_VALIDATOR_DEFINED,
      ImmutableSet.of(ResetMetadataStmt.class)),

  REFRESH_TABLE(StmtTypeConstants.TOK_REFRESH_TABLE,
      ImmutableList.of("refresh"),
      HiveOperation.REFRESH_TABLE,
      StmtTypeConstants.NO_COLS,
      StmtTypeConstants.NO_TYPES,
      StmtTypeConstants.NO_INVALIDATE_CMD,
      StmtTypeConstants.NO_PARTITION_COLS,
      StmtTypeConstants.NO_FETCH_TASK,
      TableAfterCommand.REQUIRED,
      StmtTypeConstants.WriteLock.NO_LOCK,
      StmtTypeConstants.ParsingErrorEngine.IMPALA,
      StmtTypeConstants.REFRESH_VALIDATOR,
      ImmutableSet.of(ResetMetadataStmt.class)),

  CREATE_TABLE(StmtTypeConstants.TOK_CREATE_TABLE,
      ImmutableList.of("create", "table"),
      HiveOperation.CREATETABLE,
      StmtTypeConstants.NO_COLS,
      StmtTypeConstants.NO_TYPES,
      StmtTypeConstants.NO_INVALIDATE_CMD,
      StmtTypeConstants.NO_PARTITION_COLS,
      StmtTypeConstants.NO_FETCH_TASK,
      TableAfterCommand.REQUIRED,
      StmtTypeConstants.WriteLock.NO_LOCK,
      StmtTypeConstants.ParsingErrorEngine.IMPALA,
      StmtTypeConstants.NO_VALIDATOR_DEFINED,
      ImmutableSet.of(CreateTableStmt.class, CreateTableAsSelectStmt.class,
          CreateTableLikeStmt.class));

  // Static map containg token key to its statement type
  private final static Map<Integer, StatementType> STATEMENT_TYPE_MAP;

  // populate static statement map.
  static {
    ImmutableMap.Builder<Integer, StatementType> builder = ImmutableMap.builder();
    EnumSet.allOf(StatementType.class).forEach(
        stmtType -> builder.put(stmtType.token, stmtType));
    STATEMENT_TYPE_MAP = builder.build();
  }

  // A unique token integer for each statement.
  // These integers are started at a high value to avoid overlapping with tokens
  // from the Hive parser.
  public final int token;

  // Keywords at the start of the statement (e.g. "show files in")
  public final List<String> keywords;

  // The HiveOperation for the statement type
  public final HiveOperation operation;

  // The column names for the fetched columns. If there is no fetch needed
  // (e.g. for 'refresh') the list will be empty.
  public final List<String> colNames;

  // The column types for the fetched columns. If there is no fetch needed
  // (e.g. for 'refresh') the list will be empty.
  public final List<String> colTypes;

  // True if an invalidate metadata statement needs to be run before this statement type.
  public final boolean needsInvalidate;

  // True if the fetched columns contain the partition names (if the table is partitioned)
  public final boolean prependPartCols;

  // True if the statement returns output
  public final boolean hasFetchTask;

  // enum describing if there is a table in the statement. The three possible values are
  // 1) a table is required in the statement (e.g. refresh <tbl>)
  // 2) no table is required (e.g. refresh authorization)
  // 3) table is optional (e.g. invalidate metadata <optional tbl>)
  public final TableAfterCommand tableRequired;

  // Type of write lock for the output if needed
  public final WriteLock writeEntityType;

  // Type of error to return to the user if parsing fails. Choices are to return
  // the Hive parser error, the Impala parser error, or an error determined by
  // a config flag.
  public final ParsingErrorEngine parsingErrorType;

  // Validator class if needed. A non-null value provides a mechanism to validate
  // the sql statement for a specific statement type.
  public final Class validatorClass;

  // The Impala StatementBase class associated with the sql statement.
  public final Set<Class> impalaStmtClasses;

  // A pattern matcher for the statement type.
  private final Pattern pattern;

  private StatementType(int token, List<String> keywords,
      HiveOperation operation, List<String> colNames, List<String> colTypes,
      boolean needsInvalidate, boolean prependPartCols, boolean hasFetchTask,
      TableAfterCommand tableRequired, WriteLock writeEntityType,
      ParsingErrorEngine parsingErrorType, Class validatorClass, Set<Class> impalaStmtClasses) {
    this.token = token;
    this.keywords = keywords;
    this.operation = operation;
    this.colNames = colNames;
    this.colTypes = colTypes;
    this.needsInvalidate = needsInvalidate;
    this.prependPartCols = prependPartCols;
    this.hasFetchTask = hasFetchTask;
    this.tableRequired = tableRequired;
    this.writeEntityType = writeEntityType;
    this.parsingErrorType = parsingErrorType;
    this.validatorClass = validatorClass;
    this.impalaStmtClasses = impalaStmtClasses;
    this.pattern = getPattern();
  }

  /**
   * Retrieve the pattern matcher that will match the sql statement.
   * An example here is "\\*scompute\\s+stats\\s+.*" (case insensitive)
   */
  private Pattern getPattern() {
    String patternString = "\\s*";
    boolean firstWord = true;
    for (String keyword : this.keywords) {
      if (firstWord) {
        firstWord = false;
      } else {
        patternString += "\\s+";
      }
      patternString += keyword;
    }
    if (this.tableRequired == TableAfterCommand.REQUIRED) {
      patternString += "\\s+";
    }
    patternString += ".*";
    return Pattern.compile(patternString, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  }

  public String getCmd() {
    return StringUtils.join(keywords, ' ');
  }

  /**
   * get the table string from the command. For all the statement types, the table
   * will be immediate after the keywords.
   */
  private static String getTableString(String command, List<String> keywords) {
    String tmpString = command;
    for (String keywordToDelete : keywords) {
      tmpString = StringUtils.replaceOnceIgnoreCase(tmpString, keywordToDelete, "").trim();
    }

    // Walk the string and grab the db.tablename or tablename. The trick here is that
    // a) The db, tbl, or both could be within backticks (e.g. `my_db`.`my_tbl`)
    // b) Double backticks serve as an escape character within Hive. While this is not
    //    yet supported within Impala, the code here will handle this case when it is.
    // c) embedded spaces are skipped over if they within the backticks.
    // d) If we've reached this point in the code, the sql command was parsed by the Impala
    //    parser and must be well-formed. So we know that a db/tbl combo will match the
    //    criteria listed here.
    boolean insideBacktick = false;
    boolean escapedBacktick = false;
    int i = 0;
    while (i < tmpString.length() &&
        (!Character.isWhitespace(tmpString.charAt(i)) || insideBacktick)) {
      if (escapedBacktick) {
        Preconditions.checkState(tmpString.charAt(i) == StmtTypeConstants.BACKTICK_CHAR);
        escapedBacktick = false;
      } else if (tmpString.charAt(i) == StmtTypeConstants.BACKTICK_CHAR) {
        if ((i != tmpString.length() - 1) &&
            tmpString.charAt(i+1) == StmtTypeConstants.BACKTICK_CHAR) {
          escapedBacktick = true;
        } else {
          insideBacktick = !insideBacktick;
        }
      }
      i++;
    }
    return tmpString.substring(0, i);
  }

  public static StatementType getStatementType(int token) {
    return STATEMENT_TYPE_MAP.get(token);
  }

  public static HiveOperation getOperation(int token) {
    StatementType s = getStatementType(token);
    return s != null ? s.operation : null;
  }

  public static ParsingErrorEngine getParsingErrorType(String command) {
    for (StatementType e : EnumSet.allOf(StatementType.class)) {
      if (e.pattern.matcher(command).matches()) {
        return e.parsingErrorType;
      }
    }

    // Special cases for create function statements that don't fit into the
    // normal statement type logic.
    if (StmtTypeConstants.CREATE_AGG_FUNC_STMT.matcher(command).matches()) {
      return ParsingErrorEngine.IMPALA;
    }

    if (StmtTypeConstants.CREATE_FUNC_STMT.matcher(command).matches()) {
      return ParsingErrorEngine.AMBIGUOUS;
    }

    return ParsingErrorEngine.HIVE;
  }

  public static ASTNode getASTNode(String command)
      throws ParseException, AnalysisException {
    StatementBase stmtObj = Parser.parse(command);

    // Special case for 'create function' statements.
    if (stmtObj instanceof CreateFunctionStmtBase) {
      return CreateFuncSemanticAnalyzer.getASTNode(
          (CreateFunctionStmtBase) stmtObj, command);
    }
    // iteratively check for a match with all of the pattern matches (O(n) is ok
    // here, because there are a small number of statement types and the time added
    // for these types of operations is not a concern.
    for (StatementType e : EnumSet.allOf(StatementType.class)) {
      if (e.pattern.matcher(command).matches() &&
          e.impalaStmtClasses.contains(stmtObj.getClass())) {
        return getASTNode(e.token, command, e.keywords, e.tableRequired);
      }
    }

    throw new AnalysisException("This is a valid Impala statement but it is not supported " +
        "yet in Unified Analytics.");
  }

  // Get the AST node for parameters for a found match.
  public static ASTNode getASTNode(int token, String command,
      List<String> keywords, TableAfterCommand tableRequired) throws ParseException {
    // retrieve the string following the keywords. This should be the table name as
    // long as the statement type says that a table is required or optional.
    String tableString = getTableString(command, keywords);
    Token stmtToken = new ImmutableCommonToken(token, tableString);
    ImpalaASTNode tableRoot = new ImpalaASTNode(stmtToken);
    if (!tableString.isEmpty() && tableRequired != TableAfterCommand.NOT_REQUIRED) {
      ASTNode tableASTNode =
          HiveSnippetParser.parse(tableString, HiveParser.TOK_TABLE_PARTITION);
      tableRoot.addChild(tableASTNode);
    }
    return tableRoot;
  }
}
