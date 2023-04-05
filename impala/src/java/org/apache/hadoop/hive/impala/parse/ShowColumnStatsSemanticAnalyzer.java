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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.BooleanUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzer for Show Column Stats command.
 * This is used instead of the default to allow the user to set the query option
 * 'show_column_minmax_stats' which adds on the 'min' and 'max' columns.
 */
public class ShowColumnStatsSemanticAnalyzer extends ImpalaSyntaxSemanticAnalyzer {
  protected static final Logger LOG = LoggerFactory.getLogger(ShowColumnStatsSemanticAnalyzer.class.getName());
  private HiveConf conf;

  public ShowColumnStatsSemanticAnalyzer(QueryState queryState,
      HiveConf conf) throws SemanticException {
    super(queryState, StatementType.SHOW_COLUMN_STATS);
    this.conf = conf;
  }

  @Override
  protected boolean overrideColumns() {
    return BooleanUtils.toBoolean(conf.get("show_column_minmax_stats"));
  }

  @Override
  protected List<String> getOverrideColumnNames() {
    LOG.debug("Override column names for show column stats, adding min/max cols");
    List<String> columns = new ArrayList<>(StmtTypeConstants.SHOW_COLUMN_STATS_COLS);
    columns.add("Min");
    columns.add("Max");
    return columns;
  }

  @Override
  protected List<String> getOverrideColumnTypes() {
    List<String> types = new ArrayList<>(StmtTypeConstants.SHOW_COLUMN_STATS_TYPES);
    types.add("string");
    types.add("string");
    return types;
  }
}
