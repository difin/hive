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
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
/**
 * Utility functions
 */
public class ImpalaSemanticAnalyzerUtils {

  /**
   * Fully qualify the tablename within the query string. Impala does not keep track
   * of the current database so any table name passed to Impala must be fully qualified.
   */
  public static String getQueryWithDatabase(ASTNode tableTokenNode, String queryString,
      String currentDatabase) {
    // If there are 2 children under the ASTNode, that means it is already of
    // the form db.tbl, so there is no manipulation that needs to be done.
    if (tableTokenNode.getChildren().size() == 2) {
      return queryString;
    }
    String tableName = ((ASTNode)tableTokenNode.getChild(0)).getText();
    return getQueryWithDatabase(tableName, queryString, currentDatabase);
  }

  public static String getQueryWithDatabase(String name, String queryString,
      String currentDatabase) {
    String nameWithBackTicks  = "`" + name + "`";
    String currentDbWithBackTicks = "`" + currentDatabase + "`";

    // First assume the tablename has backticks and try to add the db.
    String newQueryString = queryString.replaceFirst(nameWithBackTicks,
        currentDbWithBackTicks + "." + nameWithBackTicks);

    // If the strings are different, a replacement was done, so we return the new string.
    if (!newQueryString.equals(queryString)) {
      return newQueryString;
    }
    return queryString.replaceFirst(name, currentDbWithBackTicks + "." + name);
  }
}
