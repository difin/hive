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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.impala.work.ImpalaWork;
import org.apache.impala.analysis.ShowStatsStmt;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.analysis.Parser;
import org.apache.impala.common.AnalysisException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic semantic analyzer handling Impala statements that require
 * either a single leaf (e.g. 'REFRESH AUTHORIZATION' which has no
 * variable input following the statement) or a table name (e.g.
 * compute stats <tbl>).  We do a bit of special checking with the
 * table name and optional partition information,  so we need the
 * ability to extract this from the sql command, but anything else
 * following this table (and partition) can be blindly sent to Impala
 * to parse.
 */
public class ImpalaSyntaxSemanticAnalyzer extends SemanticAnalyzer {

  // Statement type (e.g. COMPUTE_STATS, SHOW_FILES_IN, etc...)
  private final StatementType stmtType;

  private static final Logger LOG =
      LoggerFactory.getLogger(ImpalaSyntaxSemanticAnalyzer.class);

  public ImpalaSyntaxSemanticAnalyzer(QueryState queryState, StatementType stmtType)
      throws SemanticException {
    super(queryState);
    this.stmtType = stmtType;
  }

  /**
   * main method in this class called from Driver.
   */
  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    LOG.debug(stmtType.getCmd() + " semantic analyzer");

    // get the table metadata
    Table table = getTable(root);

    // Do semantic validation, throwing a SemanticException if the statement
    // cannot be processed. One failure example is that a refresh command
    // can only be applied to an external table.
    validate(stmtType, table, root);

    // Add read entities which may be used by hooks.
    for (ReadEntity entity : getReadEntities(root, table)) {
      PlanUtils.addInput(super.inputs, entity);
    }

    // Add write entities which may be used by hooks.
    for (WriteEntity entity : getWriteEntities(table)) {
      super.outputs.add(entity);
    }

    super.queryState.setCommandType(stmtType.operation);

    LOG.debug(stmtType.getCmd() + " analysis completed");

    String sqlStmt = getSqlStatement(super.queryState, root, table);

    // Get fetch task if it exists. A 'show' statement will require a
    // fetch task whereas a 'refresh' statement will not.
    super.fetchTask = getImpalaFetchTask();
    if (super.fetchTask != null) {
      super.resultSchema = getFetchSchema(table);
    }

    super.rootTasks.addAll(getTasks(root, table, this.fetchTask, sqlStmt));
  }

  private List<ReadEntity> getReadEntities(ASTNode root, Table table) throws SemanticException {
    List<ReadEntity> readEntities = new ArrayList<>();
    // If there is no table, there are no read entities needed
    if (table != null) {
      try {
        readEntities.add(new ReadEntity(table, null, true));
        ASTNode tableNode = (ASTNode) root.getChild(0);
        Map<String, String> partitionSpec = super.getPartSpec((ASTNode) tableNode.getChild(1));
        if (partitionSpec != null) {
          readEntities.add(new ReadEntity(new Partition(table, partitionSpec, null)));
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
    }
    return readEntities;
  }

  private List<WriteEntity> getWriteEntities(Table table) throws SemanticException {
    List<WriteEntity> writeEntities = new ArrayList<>();
    if (table != null) {
      switch (stmtType.writeEntityType) {
        case ACID_DDL_LOCK:
          WriteEntity.WriteType acidWriteType = AcidUtils.isLocklessReadsEnabled(table, conf)
              ? WriteEntity.WriteType.DDL_EXCL_WRITE : WriteEntity.WriteType.DDL_EXCLUSIVE;
          writeEntities.add(new WriteEntity(table, acidWriteType));
          break;
        case NO_DDL_LOCK:
          writeEntities.add(new WriteEntity(table, WriteEntity.WriteType.DDL_NO_LOCK));
          break;
      }
    }
    return writeEntities;
  }

  private List<FieldSchema> getFetchSchema(Table table) {
    List<String> colNames = overrideColumns() ? getOverrideColumnNames() : stmtType.colNames;
    List<String> colTypes = overrideColumns() ? getOverrideColumnTypes() : stmtType.colTypes;

    Preconditions.checkNotNull(table);
    List<FieldSchema> resultSchema = new ArrayList<>();

    // Some statements require prepending the partition columns in the output. For
    // instance, the "show table stats' statement shows the table stats per partition
    // and adds these columns as the first columns to the output
    if (stmtType.prependPartCols) {
      for (FieldSchema f : table.getPartCols()) {
        resultSchema.add(new FieldSchema(f.getName(), "string", ""));
      }
    }

    for (int i = 0; i < colNames.size(); ++i) {
      resultSchema.add(new FieldSchema(colNames.get(i), colTypes.get(i), ""));
    }
    return resultSchema;
  }

  private Table getTable(ASTNode root) throws SemanticException {
    if (root.getChildren() == null || root.getChildren().size() == 0) {
      if (stmtType.tableRequired == StmtTypeConstants.TableAfterCommand.REQUIRED) {
        throw new SemanticException("Command '" + stmtType.getCmd() + "' requires a table.");
      }
      return null;
    }

    try {
      ASTNode tableNode = (ASTNode) root.getChild(0);

      String tableNameString = super.getUnescapedName((ASTNode) tableNode.getChild(0));
      Table table = super.getTable(tableNameString, true);
      TableSpec ts = new TableSpec(db, conf, tableNode, false, false);
      table.setTableSpec(ts);

      return table;
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  private FetchTask getImpalaFetchTask() {
    if (!stmtType.hasFetchTask) {
      return null;
    }
    FetchWork fetch = new ImpalaFetchWork();
    return (FetchTask) TaskFactory.get(fetch);
  }

  private ImpalaWork getWork(String sqlStmt, FetchTask fetchTask) {
    if (fetchTask != null) {
      return ImpalaWork.createPlannedWork(sqlStmt, fetchTask, 1);
    } else {
      return ImpalaWork.createPlannedWork(sqlStmt, null, -1, false);
    }
  }

  /**
   * Get the sql statement string. If the sql statement has a table, it does not
   * necessarily have the database. We need to add the database to the sql statement
   * because the Impala coordinator does not know what the current database is.
   * This is because we are not passing 'use <db>' commands to Impala.
   */
  private String getSqlStatement(QueryState queryState, ASTNode root, Table table) {
    return (table == null)
        ? getQueryString(root)
        : ImpalaSemanticAnalyzerUtils.getQueryWithDatabase(
            (ASTNode) root.getChild(0).getChild(0),
            getQueryString(root),
            SessionState.get().getCurrentDatabase());
  }

  private void validate(StatementType stmtType, Table table, ASTNode root)
      throws SemanticException {
    if (table == null) {
      return;
    }

    if (table.isView()) {
      throw new SemanticException(stmtType.getCmd() + " cannot be executed for views");
    }

    ASTNode tableNode = (ASTNode) root.getChild(0);

    Map<String, String> partitionSpec = super.getPartSpec((ASTNode) tableNode.getChild(1));
    if (partitionSpec != null) {
      super.validatePartSpec(table, partitionSpec, tableNode, conf, true);
    }

    // Special logic for different statement types. The "validatorClass" will contain
    // the class object for the validator associated with a specific statement type.
    // We instantiate the object here via reflection and call the validate statement.
    try {
      if (stmtType.validatorClass != StmtTypeConstants.NO_VALIDATOR_DEFINED) {
        SemanticAnalyzerValidator instance =
            (SemanticAnalyzerValidator) stmtType.validatorClass.newInstance();
        instance.validate(stmtType, root, table, partitionSpec);
      }
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  // Get the query string. This is overridden for the "analyze table compute stats"
  // statement type which needs to extract the sql statement from the ASTNode. The
  // default here is just to get the query string from the queryState.
  private String getQueryString(ASTNode root) {
    return overrideQueryString() ? getOverrideQueryString(root) : queryState.getQueryString();
  }

  // Allows derived class to have their own implementation
  protected boolean overrideColumns() {
    return false;
  }

  // Allows derived class to have their own implementation
  protected boolean overrideQueryString() {
    return false;
  }

  protected List<String> getOverrideColumnNames() {
    throw new RuntimeException("No Override defined for " + this.stmtType);
  }

  protected List<String> getOverrideColumnTypes() {
    throw new RuntimeException("No Override defined for " + this.stmtType);
  }

  protected String getOverrideQueryString(ASTNode astNode) {
    throw new RuntimeException("No Override defined for " + this.stmtType);
  }

  // Get all the tasks associated with the statement. For some statements, we need
  // to run the 'invalidate metadata' statement before running the sql statement.
  private List<Task<?>> getTasks(ASTNode root, Table table, FetchTask fetchTask,
      String sqlStmt) {
    ImpalaWork work = getWork(sqlStmt, fetchTask);
    List<Task<?>> tasks = new ArrayList<>();
    Task<ImpalaWork> invalidateTask = stmtType.needsInvalidate
        ? createInvalidateMetadataTask((ASTNode) root.getChild(0).getChild(0), table)
        : null;

    if (invalidateTask != null) {
      tasks.add(invalidateTask);
    }

    Task<ImpalaWork> stmtTask = TaskFactory.get(work);

    tasks.add(stmtTask);

    // Force invalidate metadata to run synchronously since we
    // don't want to kick off the alter table statement until after the invalidate
    // metadata has been completed, so we make this a dependent task.
    if (invalidateTask != null) {
      invalidateTask.addDependentTask(stmtTask);
    }

    return tasks;
  }

  public static Task<ImpalaWork> createInvalidateMetadataTask(ASTNode tableNode, Table table) {
    // if the token has 2 children, it is of the form db.tbl and we grab the db name
    // from the token. Otherwise, we have to use the current db in the session.
    String db = tableNode.getChildren().size() == 2
        ? ((ASTNode)tableNode.getChild(0)).getText()
        : SessionState.get().getCurrentDatabase();
    String stmt = "invalidate metadata `" + db + "`.`" + table.getTableName() + "`";
    ImpalaWork invalidateWork = ImpalaWork.createPlannedWork(stmt, false);
    return TaskFactory.get(invalidateWork);
  }
}
