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

import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.AnalyzeCommandUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Map;

/**
 * Validates the semantics of the drop stats statement. Called reflectively
 * from ImpalaSyntaxSemanticAnalyzer.
 * TODO: CDPD-56868: Place all validate methods across different statements into a single
 * validator class.
 */
public class DropStatsValidator implements SemanticAnalyzerValidator {

  public void validate(StatementType stmtType, ASTNode root, Table table,
      Map<String, String> partitionSpec) throws SemanticException {
    boolean incrementalStats = AnalyzeCommandUtils.isIncrementalStats(root);
    if (incrementalStats && partitionSpec == null) {
      throw new SemanticException("Partition needs to be statically specified in DROP INCREMENTAL STATISTICS");
    }

    if (partitionSpec != null) {
      if (!incrementalStats) {
        throw new SemanticException(
            "Partitions cannot be statically specified in DROP STATISTICS in Impala without INCREMENTAL");
      }
    }
  }
}
