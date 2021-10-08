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

package org.apache.hadoop.hive.impala.plan;

import com.google.common.base.Preconditions;

import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.impala.node.ImpalaPlanRel;
import org.apache.hadoop.hive.impala.node.ImpalaProjectRel;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.parse.QB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Special processing if it's a materialized view. The partitioned columns
// in the mv need to be selected at the end. If they are already in the correct place,
// the query remains as/is. If not, a "select" (HiveProject RelNode) is added on top
// of the top node to reorder the columns.
// CDPD-30321: This is not the most efficient way since we'll be adding
// another node. Preferably we can add the Calcite node upstream and let Calcite
// optimization take care of the inefficiencies.
public class ReorderMVPartitionSelect {

  private static final Logger LOG = LoggerFactory.getLogger(ReorderMVPartitionSelect.class);

  private final boolean needsReordering;

  private final List<Integer> columnOrder;

  private ReorderMVPartitionSelect(
      QB qb, Table table, List<FieldSchema> resultSchema) {
    Preconditions.checkNotNull(table);
    Preconditions.checkNotNull(resultSchema);
    Preconditions.checkState(resultSchema.size() > 0);

    // columOrder only returns a non-null value if it needs reordering.
    this.columnOrder = getReorderedColumns(qb, table, resultSchema);
    this.needsReordering = (this.columnOrder != null);
  }

  private ImpalaPlanRel getTopLevelImpalaRelNode(ImpalaPlanRel impalaRelNode) {
    if (!needsReordering) {
      return impalaRelNode;
    }
    LOG.info("Materialized view columns need to be reordered so that the partitioned columns " +
        " are at the end of the selected columns.");
    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(impalaRelNode.getCluster(), null);
    // Add a Project node on top of the top RelNode to place the partitioned columns at the end.
    HiveProject topLevelProject = (HiveProject) HiveRelOptUtil.createProject(relBuilder, impalaRelNode, columnOrder);
    return new ImpalaProjectRel(topLevelProject);
  }

  /**
   * Return index list of input columns so that the partitioned columns indices are placed
   * at the end of the list. If the column order did not change, return null.
   */
  private List<Integer> getReorderedColumns(QB qb, Table table, List<FieldSchema> resultSchema) {
    Preconditions.checkNotNull(table);
    List<Integer> columnOrderList = new ArrayList<>();
    List<String> partitionColNames = new ArrayList<>();
    // gather the partition column names.
    for (FieldSchema partitionFS : table.getPartCols()) {
      partitionColNames.add(partitionFS.getName());
    }
    LOG.debug("Check reorder columns for ctas or materialized views, " +
        "processing unpartitioned columns.");
    // first pass.  Skip over the partition columsn. Gather all other column names.
    for (int i = 0; i < resultSchema.size(); ++i) {
      if (partitionColNames.contains(resultSchema.get(i).getName())) {
        continue;
      }
      LOG.debug("Check reorder columns for ctas or materialized views, adding index " + i);
      columnOrderList.add(i);
    }
    LOG.debug("Check reorder columns for ctas or materialized views, " +
        "processing partitioned columns.");

    // create a map of result schema names to its position in the existing schema.
    Map<String, Integer> resultSchemaNamePosition = new HashMap<>();
    for (int i = 0; i < resultSchema.size(); ++i) {
      // schema name can be "tblname.col" or "col"
      String[] qualifiedFieldName = resultSchema.get(i).getName().split("\\.");
      String name = qualifiedFieldName[qualifiedFieldName.length-1];
      resultSchemaNamePosition.put(name, i);
    }
    // second pass.  Add the partition columns in the order they are stored.
    for (FieldSchema partitionFS : table.getPartCols()) {
      Integer position = resultSchemaNamePosition.get(partitionFS.getName());
      Preconditions.checkNotNull(position,
          "Partition column not found in ctas or materialized view select list.");
      columnOrderList.add(position);
    }

    // check column order.  If the column order didn't change, return null.
    for (int i = 0; i < columnOrderList.size(); ++i) {
      if (i != columnOrderList.get(i)) {
        return columnOrderList;
      }
    }
    return null;
  }

  /**
   * Returns the top level node. If the columns are ordered correctly so that the partitioned
   * columns are at the end, no new node is created and this just returns the impalaRelNode
   * passed in. Otherwise, it generates a new node on top of the impalaRelNode.
   * This is the only visible method from outside of this class.
   */
  public static ImpalaPlanRel getImpalaRelNodeForMV(ImpalaPlanRel impalaRelNode, QB qb,
      Table table, List<FieldSchema> resultSchema) {
    ReorderMVPartitionSelect reorderMVPartitionSelect =
        new ReorderMVPartitionSelect(qb, table, resultSchema);
    return reorderMVPartitionSelect.getTopLevelImpalaRelNode(impalaRelNode);
  }
}
