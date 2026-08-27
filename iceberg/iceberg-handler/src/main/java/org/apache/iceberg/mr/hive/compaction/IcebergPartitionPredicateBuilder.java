/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.compaction;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.mr.hive.HiveIcebergFilterFactory;
import org.apache.iceberg.types.Types;

/**
 * Builds SQL predicates for Iceberg partition compaction queries from a compaction partition name.
 */
final class IcebergPartitionPredicateBuilder {

  private IcebergPartitionPredicateBuilder() {
  }

  static String build(CompactionInfo ci, PartitionSpec spec) throws MetaException {
    Map<String, String> partSpecMap = Warehouse.makeSpecFromName(ci.partName);
    Map<String, PartitionField> partitionFieldMap = spec.fields().stream()
        .collect(Collectors.toMap(PartitionField::name, Function.identity()));

    Types.StructType partitionType = spec.partitionType();
    return partitionType.fields().stream().map(field -> {
      String value = partSpecMap.get(field.name());
      String literal = "NULL";

      if (value != null && !value.equals("null")) {
        String type = HiveSchemaUtil.convertToTypeString(field.type());
        PartitionField partitionField = partitionFieldMap.get(field.name());
        TransformSpec transformSpec = TransformSpec.fromString(partitionField.transform().toString(), field.name());
        literal = TypeInfoUtils.convertStringToLiteralForSQL(
            HiveIcebergFilterFactory.convertPartitionLiteral(value, transformSpec).toString(),
            ((PrimitiveTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(type)).getPrimitiveCategory());
      }

      return String.format("`partition`.%s %s %s", HiveUtils.unparseIdentifier(field.name()),
          Objects.equals(literal, "NULL") ? "IS" : "=", literal);
    }).collect(Collectors.joining(" AND "));
  }
}
