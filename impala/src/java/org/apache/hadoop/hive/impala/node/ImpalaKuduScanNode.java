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

package org.apache.hadoop.hive.impala.node;

import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.planner.KuduScanNode;
import org.apache.impala.planner.PlanNodeId;

public class ImpalaKuduScanNode extends KuduScanNode {

  /**
   * ImpalaKuduScanNode constructor
   * @param id       Impala PlanNode id
   * @param aggInfo  AggregateInfo passed to KuduScanNode from Impala when there is a potential for
   *                perf optimization
   * @param nodeInfo Impala node information
   * @param kuduTblRef Reference to the Kudu table
   */
  public ImpalaKuduScanNode(PlanNodeId id, MultiAggregateInfo aggInfo, ImpalaNodeInfo nodeInfo, TableRef kuduTblRef) {
    super(id, nodeInfo.getTupleDesc(), nodeInfo.getAssignedConjuncts(), aggInfo, kuduTblRef);
  }

}
