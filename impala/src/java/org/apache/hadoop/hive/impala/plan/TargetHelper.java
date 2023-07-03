/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.impala.plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.hadoop.hive.impala.catalog.ImpalaKuduTable;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * TargetHelper contains variables and methods used specifically when a target table
 * is created from the SQL statement. This includes INSERT, CTAS, and CREATE MV statements.
 */
public class TargetHelper {
  protected static final Logger LOG = LoggerFactory.getLogger(TargetHelper.class);

  // The Impala representation of the target object
  private FeTable impalaTable;

  // The Hive representation of the target object
  private final Table targetTable;

  // The column names of the target table if specified in the insert statement.
  // If this is non-null, the SQL statement will look like this:
  // INSERT INTO TBL (col1, col2, col3) SELECT col1, col2, col3 ...
  // where the names in the list will be the columns explicitly specified in the
  // insert statement. If the columns are not specified, such as in this case:
  // INSERT INTO TBL SELECT col1, col2, cole3...
  // then the value will be null.
  private final List<String> targetedColNames;

  // A map containing the column name as key and the value of the static partition
  // (if the specified column is a static partition). Only static partitions will
  // exist in this map.
  private final Map<String, String> staticPartitions;

  // An integer list of the locations of the dynamic partitions within the
  // select statement. If the targeted columns are not specified (e.g. the insert
  // statement looks like: INSERT INTO TARGET_TBL SELECT C1, C2, P1 FROM TBL), the
  // dynamic columns will always be at the end. If the targeted columns are specified
  // (e.g. INSERT INTO TARGET_TBL (C1, P1, C2) SELECT C1, P1, C2) FROM TBL), the
  // dynamic columns could be anywhere in the list.
  // Note that a "select *" statement is the equivalent of selecting out all columns
  // from the table as defined in the create table statement, so the partitions are at
  // the end.
  private final List<Integer> dynPartitionColPositions;

  private final ImpalaPlannerContext ctx;
  public TargetHelper(ImpalaPlannerContext ctx, QB qb) throws SemanticException {
    this.ctx = ctx;

    // Validate that the select output and target table schema are compatible
    impalaTable = ctx.getTargetTable();
    Preconditions.checkNotNull(impalaTable);

    // dest should have exactly one reference for the target table.
    Preconditions.checkState(qb.getParseInfo().getClauseNames().size() == 1);
    String dest = qb.getParseInfo().getClauseNames().iterator().next();

    // Partition currently only contains a value when all the partitions are
    // static partitions
    Partition partition = qb.getMetaData().getDestPartitionForAlias(dest);

    targetedColNames = qb.getParseInfo().getDestSchemaForClause(dest);

    // CDPD-34767: Currently, Impala does not allow both the target columns clause and
    // the partition clause. Remove this preconditions check if this gets supported and
    // make sure the impala test framework tests are activated.
    // When both partition == null and targetedCols == null, the sql statement is a
    // CTAS or CREATE MV.
    Preconditions.checkState(partition == null || targetedColNames == null);

    // If the partition is not null, we can fetch the table object from the partition.
    // If the partition is null, then we fetch it from the Query Block Metadata. The
    // QB Metadata will have a null value for Table in the cases of CTAS and CREATE MV.
    targetTable = (partition == null)
        ? qb.getMetaData().getDestTableForAlias(dest) : partition.getTable();

    // static partitions are either in the partition (if all partitions are static) or
    // the DPCtx (if either no partitions or static or just some of them are)/
    staticPartitions = createStaticPartitionMap(qb.getMetaData().getDPCtx(dest), partition);

    dynPartitionColPositions = createDynPartitionColPositions(ctx.getResultExprs());
  }

  /**
   * Given a select expression list, return the expressions cast into their proper type.
   * The return Pair contains the expressions broken down into the nonpartitioned columns
   * and the partitioned columns.
   * The return expressions will also contain the mapped columns that do not exist in the
   * select result expressions which can happen if either
   *   a) it's a static partition column, in which case, the expression is derived from the
   *      static value
   *   b) A targeted list is used and it did not contain all the columns, in which case, a
   *      default value will be used.
   */
  public Pair<List<Expr>, List<Expr>> getMappedExprs(List<Expr> selectResultExprs)
      throws SemanticException, AnalysisException {
    // target table is only null in the CREATE table case (both MV and CTAS)
    if (targetTable == null) {
      return getCreateTableExprs(selectResultExprs);
    }
    return targetedColNames != null
        ? getMappedExprsWithTargetedList(selectResultExprs)
        : getMappedExprsWithoutTargetedList(selectResultExprs);
  }

  public int getNumStaticPartitionColumns() {
    // See variable description for details
    return staticPartitions.size();
  }

  public List<Integer> getDynPartitionColPositions() {
    // See variable description for details
    return dynPartitionColPositions;
  }

  /**
   * Given a list of result expressions (from the select statement), return
   * the sublist of dynamic partition expressions. The positions of the dynamic
   * partitions have been precalculated at constructor time. This is used on
   * a passed in list.
   */
  public List<Expr> getDynPartitionExprs(List<Expr> selectResultExprs) {
    List<Expr> partitionExprs = new ArrayList<>();
    for (Integer i : dynPartitionColPositions) {
      partitionExprs.add(selectResultExprs.get(i));
    }
    return partitionExprs;
  }

  /**
   * Calculate map of static partitions
   */
  private Map<String, String> createStaticPartitionMap(DynamicPartitionCtx dpCtx,
      Partition partition) {
    Map<String, String> staticPartitions = new HashMap<>();
    if (partition != null) {
      // Case where partition has a value. The only way this can happen is if all the partitioned
      // columns are static partitions.

      List<String> names = MetaStoreUtils.getColumnNames(partition.getTable().getPartCols());
      List<String> values = partition.getValues();
      Preconditions.checkState(names.size() == values.size());
      for (int i = 0; i < names.size(); ++i) {
        staticPartitions.put(names.get(i), values.get(i));
      }
    } else if (dpCtx != null) {
      // Case where the PARTITION clause is specified in the insert clause, but at least one
      // partitioned column is dynamic.
      Map<String, String> partSpec = dpCtx.getPartSpec();
      if (partSpec != null) {
        for (Map.Entry<String,String> partEntry : partSpec.entrySet()) {
          // Walk through the entries. All partitioned columns have entries, but if they are
          // dynamic, the value is null.  We skip over these columns.
          if (partEntry.getValue() != null) {
            staticPartitions.put(partEntry.getKey(), partEntry.getValue());
          }
        }
      }
    }
    return staticPartitions;
  }

  /**
   * Create the position list of the location of the dynamic partitions in the given
   * result expression list. If a targeted list is not provided, the partitioned expressions
   * will be at the end. If a targeted list is provided, the partitions have to be located within
   * the expression list. But if some partitions are left out of the targeted list, they are
   * assumed to be at the end if there are expressions that are unaccounted for.
   * Examples:
   *   - INSERT INTO TBL SELECT col1, col2, partition1
   *     In this case, the partition must be at the end because no target list was provided. 
   *   - INSERT INTO TBL (C1, P1, C2) SELECT C1, P1, C2, P2 FROM ...
   *     In this case, 3 targeted columns were named.  P1 here is in slot 2. But there are
   *     four columns declared in the SELECT, so P2 is also going to be treated as a
   *     partitioned column.
   */
  private List<Integer> createDynPartitionColPositions(List<Expr> selectResultExprs) {
    Preconditions.checkNotNull(impalaTable);
    List<Integer> dynPartitionColPositions = new ArrayList<>();
    if (targetedColNames == null) {
      // The case where no list is provided. Find the last location of the nonpartitioned
      // column, and all other result expressions are for partitioned columns.
      // For a CTAS statement, 'impalaTable' has to be an instance of HdfsTable or
      // KuduTable because in ImpalaPlanner#initTargetTable(), we use either
      // HdfsTable#createCtasTarget() or KuduTable#createCtasTarget() to create the table
      // object.
      Preconditions.checkState(impalaTable instanceof HdfsTable ||
          impalaTable instanceof KuduTable);
      IntStream.range(impalaTable.getNonClusteringColumns().size(),
          selectResultExprs.size()).forEach(dynPartitionColPositions::add);
    } else {
      Preconditions.checkNotNull(targetTable);
      List<String> partitionedColNames =
          MetaStoreUtils.getColumnNames(targetTable.getPartCols());
      int nCurrentUnnamedColumn = impalaTable.getNonClusteringColumns().size();
      for (String partitionedColName : partitionedColNames) {
        // CDPD-34767: Note that we can go through all the partitions and assume that none
        // of them are static partitions. Hive does not currently allow the PARTITION clause
        // with the targeted list, and the static partitions can only be specified in the
        // PARTITION clause.
        int index = targetedColNames.indexOf(partitionedColName);
        // If the targeted column is not found in the list, it must be at the end. But it's also
        // possible that the partition is totally left off the list. In that case, we don't have
        // a position location for the partition (and will be given a default partition value).
        if (index == -1) {
          if (nCurrentUnnamedColumn == selectResultExprs.size()) {
            continue;
          }
          index = nCurrentUnnamedColumn++;
        }
        dynPartitionColPositions.add(index);
      }
    }
    return dynPartitionColPositions;
  }

  public boolean isPartitioned() {
    return impalaTable instanceof HdfsTable && ((HdfsTable)impalaTable).isPartitioned();
  }

  /**
   * Given a select expression list, return the expressions cast into their proper type.
   * This is only called for create table statements (CTAS or CREATE MV)
   * Partitioned expressions will always be dynamic and always be right after the
   * nonpartitioned expressions.
   */
  private Pair<List<Expr>, List<Expr>> getCreateTableExprs(List<Expr> selectResultExprs)
      throws SemanticException, AnalysisException {
    Preconditions.checkNotNull(impalaTable);
    List<Expr> nonPartitionedExprs = new ArrayList<>();
    List<Expr> partitionedExprs = new ArrayList<>();
    int numNonPartitionedCols = impalaTable.getNonClusteringColumns().size();
    int numPartitionedCols = impalaTable.getClusteringColumns().size();
    int numCols = impalaTable.getColumns().size();

    // slight hack. Impala wants all result expressions present in their result exprs
    // list in the create statement, not just nonpartitioned columns.
    nonPartitionedExprs.addAll(selectResultExprs);
    partitionedExprs.addAll(selectResultExprs.subList(numNonPartitionedCols, numCols));
    Pair<List<Expr>, List<Expr>> result =
        new Pair<>(nonPartitionedExprs, partitionedExprs);
    return result;
  }

  /**
   * Given a select expression list, return the expressions cast into their proper type.
   * This is only called for when the targeted list is provided.
   */
  private Pair<List<Expr>, List<Expr>> getMappedExprsWithTargetedList(
      List<Expr> selectResultExprs) throws SemanticException, AnalysisException {
    Preconditions.checkNotNull(targetTable);
    // Get some lists of column names for easy processing later.
    Set<String> allColNames =
        new HashSet<>(MetaStoreUtils.getColumnNames(targetTable.getAllCols()));
    Set<String> partitionedColNames =
        new LinkedHashSet<>(MetaStoreUtils.getColumnNames(targetTable.getPartCols()));
    Set<String> nonPartitionedColNames =
        new LinkedHashSet<>(MetaStoreUtils.getColumnNames(targetTable.getCols()));

    List<Expr> nonPartitionedExprs = new ArrayList<>();
    List<Expr> partitionedExprs = new ArrayList<>();

    // Create a temporary map containing the column name mapped to its expression
    Map<String, Expr> exprsMap = new HashMap<>();
    exprsMap.putAll(getSelectTargetExprs(targetedColNames, selectResultExprs));

    // We need to take into account all columns, not just the ones in the targeted list. Those
    // columns will use a default value.
    Set<String> targetedColNamesSet = new HashSet<>(targetedColNames);
    Set<String> unprocessedColNames = Sets.difference(allColNames, targetedColNamesSet);

    // The getColNameToDefaultValueMap is a metastore call, so we can avoid that call
    // if we know all nonpartitioned columns are accounted for.
    Map<String, String> colNameToDefaultVal =
        Sets.difference(nonPartitionedColNames, targetedColNamesSet).isEmpty()
        ? null
        : SemanticAnalyzer.getColNameToDefaultValueMap(targetTable);

    for (String colName : unprocessedColNames) {
      // partitioned columns default to null.  nonpartitioned columns default to the value
      // in the map.
      String defaultValue = partitionedColNames.contains(colName)
          ? null
          : colNameToDefaultVal.get(colName);

      exprsMap.put(colName, getDefaultValueExpr(defaultValue, colName));
    }

    // the final list has to be returned in the order that the columns are defined in the
    // target table.
    return getOrderedExprs(exprsMap, nonPartitionedColNames, partitionedColNames);
  }

  /**
   * Given an expression map and the names of all the columns, return the nonpartitioned
   * expressions and partitioned expressions ordered by the way they are defined in the
   * target table. The expression map passed in here should contain all the column names
   * mapped to the associated expression.
   */
  private Pair<List<Expr>, List<Expr>> getOrderedExprs(Map<String, Expr> exprsMap,
      Collection<String> nonPartitionedColNames, Collection<String> partitionedColNames) {
    List<Expr> nonPartitionedExprs = new ArrayList<>();
    List<Expr> partitionedExprs = new ArrayList<>();

    for (String s : nonPartitionedColNames) {
      Expr e = exprsMap.get(s);
      Preconditions.checkNotNull(e);
      nonPartitionedExprs.add(e);
    }

    for (String s : partitionedColNames) {
      Expr e = exprsMap.get(s);
      Preconditions.checkNotNull(e);
      partitionedExprs.add(e);
    }

    Pair<List<Expr>, List<Expr>> result =
        new Pair<>(nonPartitionedExprs, partitionedExprs);
    return result;
  }

  // mapped target version
  private Map<String, Expr> getSelectTargetExprs(
      List<String> selectListColNames,
      List<Expr> selectResultExprs) throws SemanticException, AnalysisException {
    Map<String, Expr> exprsMap = new HashMap<>();
    Preconditions.checkState(selectListColNames.size() == selectResultExprs.size());
    for (int i = 0; i < selectResultExprs.size(); ++i) {
      Expr expr = selectResultExprs.get(i);
      String selectListColName = selectListColNames.get(i);
      // The checkTypeCompatibility call will return the correctly cast type.
      expr = StatementBase.checkTypeCompatibility(targetTable.getCompleteName(),
          impalaTable.getColumn(selectListColName), expr, ctx.getRootAnalyzer(), null);
      exprsMap.put(selectListColName, expr);
    }

    return exprsMap;
  }

  /**
   * Given a select expression list, return the expressions cast into their proper type.
   * This is only called for when there is no targeted list provided
   */
  private Pair<List<Expr>, List<Expr>> getMappedExprsWithoutTargetedList(
      List<Expr> selectResultExprs) throws SemanticException, AnalysisException {
    Preconditions.checkNotNull(targetTable);
    // Get some lists of column names for easy processing later.
    List<String> allColNames = MetaStoreUtils.getColumnNames(targetTable.getAllCols());
    List<String> nonPartitionedColNames = MetaStoreUtils.getColumnNames(targetTable.getCols());
    List<String> partitionedColNames = MetaStoreUtils.getColumnNames(targetTable.getPartCols());
    List<String> selectColNames = new ArrayList<>();

    // The nonpartitioned column names are always found first in the select result
    // expressions list
    selectColNames.addAll(nonPartitionedColNames);

    Map<String, Expr> exprsMap = new HashMap<>();
    // Iterate through partitioned columns. If a static partition is declared, the expression
    // map can be populated with the value. Otherwise, it will be found in the select result
    // expression list. In that case, we add it onto the selectColNames in the order that the
    // partitions are declared in the table.
    for (String partitionedColName : partitionedColNames) {
      String staticValue = staticPartitions.get(partitionedColName);
      if (staticValue != null) {
        exprsMap.put(partitionedColName, getDefaultValueExpr(staticValue, partitionedColName));
      } else {
        selectColNames.add(partitionedColName);
      }
    }

    // Now we have all the selectColNames in a list and all the selectResultExprs in a list
    // They should map the exact same number and we call getSelectTargetExprs that will
    // create the mapping for us.
    Preconditions.checkState(selectColNames.size() == selectResultExprs.size());
    exprsMap.putAll(getSelectTargetExprs(selectColNames, selectResultExprs));

    // the final list has to be returned in the order that the columns are defined in the
    // target table.
    return getOrderedExprs(exprsMap, nonPartitionedColNames, partitionedColNames);
  }

  // Get a default value expression for the value. This needs to be cast correctly to
  // the proper target table type.
  private Expr getDefaultValueExpr(String value, String colName)
      throws SemanticException, AnalysisException {
    Expr expr;
    if (value != null) {
      try {
        expr = LiteralExpr.createFromUnescapedStr(value,
            impalaTable.getColumn(colName).getType());
        LOG.debug("Added default value from metastore: {}", expr);
      } catch (Exception e) {
        // In case the default value isn't mapped correctly, just use null.
        LOG.error("Default value " + value + " could not be used for " + colName +
            ", using null.");
        throw new AnalysisException(e);
      }
    } else {
      expr = NullLiteral.create(impalaTable.getColumn(colName).getType());
    }
    // The checkTypeCompatibility call will return the correctly cast type.
    return StatementBase.checkTypeCompatibility(targetTable.getCompleteName(),
        impalaTable.getColumn(colName), expr, ctx.getRootAnalyzer(), null);
  }
}

