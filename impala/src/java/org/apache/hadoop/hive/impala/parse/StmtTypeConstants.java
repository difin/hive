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

import java.util.List;
import java.util.regex.Pattern;
import com.google.common.collect.ImmutableList;

/**
 * Define constants used by statement types.
 */
public class StmtTypeConstants {

  public static final List<String> NO_COLS =
      ImmutableList.of();
  public static final List<String> NO_TYPES =
      ImmutableList.of();
  public static final List<String> SUMMARY_COLS =
      ImmutableList.of("summary");
  public static final List<String> SUMMARY_TYPES =
      ImmutableList.of("string");
  // TODO: CDPD-57435: It would be better if the column names for 'show table stats' and
  // 'show column stats' could be retrieved dynamically from Impala so that the column
  // names will always be in sync.
  public static final List<String> SHOW_TABLE_STATS_COLS =
      ImmutableList.of("#Rows", "#Files", "Size", "Bytes Cached",
          "Cache Replication", "Format", "Incremental stats", "Location", "EC Policy");
  public static final List<String> SHOW_TABLE_STATS_TYPES =
      ImmutableList.of("bigint", "bigint", "string", "string", "string",
          "string", "string", "string", "string");
  public static final List<String> SHOW_COLUMN_STATS_COLS =
      ImmutableList.of("Column", "Type", "#Distinct Values", "#Nulls", "Max Size", "Avg Size",
          "#Trues", "#Falses");
  public static final List<String> SHOW_COLUMN_STATS_TYPES =
      ImmutableList.of("string", "string", "bigint", "bigint", "bigint", "double", "bigint",
          "bigint");
  public static final List<String> SHOW_PARTITION_COLS =
      ImmutableList.of("#Rows", "#Files", "Size", "Bytes Cached",
          "Cache Replication", "Format", "Incremental stats", "Location", "EC Policy");
  public static final List<String> SHOW_PARTITION_TYPES =
      ImmutableList.of("bigint", "bigint", "string", "string", "string",
          "string", "string", "string", "string");
  public static final List<String> SHOW_FILES_IN_COLS =
      ImmutableList.of("Path", "Size", "Partition", "EC Policy");
  public static final List<String> SHOW_FILES_IN_TYPES =
      ImmutableList.of("string", "string", "string", "string");

  public static final char BACKTICK_CHAR = '`';

  public static boolean NEEDS_INVALIDATE_CMD = true;
  public static boolean NO_INVALIDATE_CMD = false;

  // The PREPEND_PARTITION_COLS constant should be used on very special Impala statement cases.
  // For example, the "show table stats" command returns a variable number of columns. If there
  // are <n> partition columns, the first <n> columns returned will be the partition names, and
  // the stats will exist in the columns after the partition columns. For most Impala statements,
  // it is correct to use NO_PARTITION_COLS
  public static boolean PREPEND_PARTITION_COLS = true;
  public static boolean NO_PARTITION_COLS = false;

  public static boolean CREATE_FETCH_TASK = true;
  public static boolean NO_FETCH_TASK = false;
  public static boolean WRITE_ENTITY_NEEDED = true;
  public static boolean NO_WRITE_ENTITY = false;

  public static Pattern CREATE_AGG_FUNC_STMT =
      Pattern.compile("\\s*create\\s*aggregate\\s+function\\s+.*", Pattern.CASE_INSENSITIVE);
  public static Pattern CREATE_FUNC_STMT =
      Pattern.compile("\\s*create\\s+function\\s+.*", Pattern.CASE_INSENSITIVE);

  public enum WriteLock {
    NO_LOCK,
    NO_DDL_LOCK,
    ACID_DDL_LOCK,
  }

  public enum ParsingErrorEngine {
    IMPALA,
    HIVE,
    AMBIGUOUS
  }

  public enum TableAfterCommand {
    REQUIRED,
    OPTIONAL,
    NOT_REQUIRED
  }

  public static final Class NO_VALIDATOR_DEFINED = null;
  public static final Class DROP_STATS_VALIDATOR = DropStatsValidator.class;
  public static final Class REFRESH_VALIDATOR = RefreshValidator.class;

  private static final int BASE_TOKEN_ID = 2000000;
  // 2000001 is an integer value that is out of range from anything that
  // the Antlr parser will produce.
  public static final int TOK_REFRESH_TABLE = BASE_TOKEN_ID + 1;
  public static final int TOK_DROP_STATS = BASE_TOKEN_ID + 2;
  public static final int TOK_COMPUTE_STATS = BASE_TOKEN_ID + 3;
  public static final int TOK_CREATE_FUNCTION = BASE_TOKEN_ID + 4;
  public static final int TOK_COMPUTE_STATS_WITH_HIVE_SYNTAX = BASE_TOKEN_ID + 5;
  public static final int TOK_ALTER_TABLE_DROP_COL = BASE_TOKEN_ID + 6;
  public static final int TOK_SHOW_TABLE_STATS = BASE_TOKEN_ID + 7;
  public static final int TOK_SHOW_COLUMN_STATS = BASE_TOKEN_ID + 8;
  public static final int TOK_SHOW_PARTITIONS = BASE_TOKEN_ID + 9;
  public static final int TOK_SHOW_RANGE_PARTITIONS = BASE_TOKEN_ID + 10;
  public static final int TOK_SHOW_FILES_IN = BASE_TOKEN_ID + 11;
  public static final int TOK_REFRESH_FUNCTION = BASE_TOKEN_ID + 12;
  public static final int TOK_REFRESH_AUTH = BASE_TOKEN_ID + 13;
  public static final int TOK_INVALIDATE_METADATA = BASE_TOKEN_ID + 14;
  public static final int TOK_DROP_INCR_STATS = BASE_TOKEN_ID + 15;
  public static final int TOK_COMPUTE_INCR_STATS = BASE_TOKEN_ID + 16;
  public static final int TOK_CREATE_TABLE = BASE_TOKEN_ID + 17;
  public static final String CREATE_FUNCTION_STRING = "create function";
}
