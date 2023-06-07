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

package org.apache.hadoop.hive.ql.metadata.formatting;

/**
 * This class is used by FENG Impala.
 * https://github.infra.cloudera.com/CDH/Impala/blob/0e36ec8ecbce59dde81e11449f3eebd1fc0a3a36/fe/src/compat-hive-3/java/org/apache/impala/compat/MetastoreShim.java#L88
 * https://github.infra.cloudera.com/CDH/Impala/blob/0e36ec8ecbce59dde81e11449f3eebd1fc0a3a36/fe/src/compat-hive-3/java/org/apache/impala/compat/MetastoreShim.java#L934
 *
 * TODO: Remove it when the import in org.apache.impala.compat.MetastoreShim is updated to ShowUtils.TextMetaDataTable
 */

import org.apache.hadoop.hive.ql.ddl.ShowUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Deprecated
public class TextMetaDataTable {
  private List<List<String>> table = new ArrayList<>();

  public void addRow(String ... values) {
    table.add(Arrays.asList(values));
  }

  public String renderTable(boolean isOutputPadded) {
    StringBuilder stringBuilder = new StringBuilder();
    for (List<String> row : table) {
      ShowUtils.formatOutput(row.toArray(new String[0]), stringBuilder, isOutputPadded, isOutputPadded);
    }
    return stringBuilder.toString();
  }

  public void transpose() {
    if (table.size() == 0) {
      return;
    }
    List<List<String>> newTable = new ArrayList<>();
    for (int i = 0; i < table.get(0).size(); i++) {
      newTable.add(new ArrayList<>());
    }
    for (List<String> sourceRow : table) {
      if (newTable.size() != sourceRow.size()) {
        throw new RuntimeException("invalid table size");
      }
      for (int i = 0; i < sourceRow.size(); i++) {
        newTable.get(i).add(sourceRow.get(i));
      }
    }
    table = newTable;
  }
}
