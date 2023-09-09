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

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.impala.work.ImpalaWork;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SemanticAnalyzer used for the "create table" statement.
 */
public class CreateTableSemanticAnalyzer extends SemanticAnalyzer {

  private static final Logger LOG =
      LoggerFactory.getLogger(CreateTableSemanticAnalyzer.class);

  private final StatementType stmtType;

  public CreateTableSemanticAnalyzer(QueryState queryState, StatementType stmtType)
      throws SemanticException {
    super(queryState);
    this.stmtType = stmtType;
  }

  /**
   * main method in this class called from Driver.
   */
  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    LOG.debug("Create table semantic analyzer");

    super.queryState.setCommandType(stmtType.operation);

    // Need to add a use <current_db> statement so both the created table
    // and any tables within a CTAS use the current database if the table
    // specified does not contain the database name.
    String useDbStmt = "use " + SessionState.get().getCurrentDatabase();
    ImpalaWork work = ImpalaWork.createPlannedWork(useDbStmt, null, -1, false);
    super.rootTasks.add(TaskFactory.get(work));

    work = ImpalaWork.createPlannedWork(queryState.getQueryString(), null, -1, false);
    super.rootTasks.add(TaskFactory.get(work));

    LOG.debug(stmtType.getCmd() + " analysis completed");
  }
}
