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

package org.apache.hadoop.hive.impala.funcmapper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;

import java.util.Map;

/**
 * Class containing static helper structures with information about the
 * mapping of Calcite to Impala functions.
 */
public class FunctionDetailStatics {

  // Map of the Impala function to a list of Calcite functions
  public static Map<String, Set<String>> IMPALA_FUNCTION_MAP =
      ImmutableMap.<String, Set<String>> builder()
      .put("add", ImmutableSet.of("+"))
      .put("casttobigint", ImmutableSet.of("cast"))
      .put("casttoboolean", ImmutableSet.of("cast"))
      .put("casttochar", ImmutableSet.of("cast"))
      .put("casttodate", ImmutableSet.of("cast"))
      .put("casttodecimal", ImmutableSet.of("cast"))
      .put("casttodouble", ImmutableSet.of("cast"))
      .put("casttofloat", ImmutableSet.of("cast"))
      .put("casttoint", ImmutableSet.of("cast"))
      .put("casttosmallint", ImmutableSet.of("cast"))
      .put("casttostring", ImmutableSet.of("cast"))
      .put("casttotimestamp", ImmutableSet.of("cast"))
      .put("casttotinyint", ImmutableSet.of("cast"))
      .put("casttovarchar", ImmutableSet.of("cast"))
      .put("concat", ImmutableSet.of("concat", "||"))
      .put("distinctfrom", ImmutableSet.of("is distinct from"))
      .put("divide", ImmutableSet.of("/"))
      .put("eq", ImmutableSet.of("="))
      .put("ge", ImmutableSet.of(">="))
      .put("gt", ImmutableSet.of(">"))
      .put("in_set_lookup", ImmutableSet.of("in"))
      .put("isfalse", ImmutableSet.of("is false", "isfalse"))
      .put("isnotfalse", ImmutableSet.of("is not false", "isnotfalse"))
      .put("is_not_null_pred", ImmutableSet.of("is not null", "isnotnull"))
      .put("isnottrue", ImmutableSet.of("is not true", "isnottrue"))
      .put("is_null_pred", ImmutableSet.of("is null", "isnull"))
      .put("istrue", ImmutableSet.of("is true", "istrue"))
      .put("le", ImmutableSet.of("<="))
      .put("lt", ImmutableSet.of("<"))
      .put("mod", ImmutableSet.of("%"))
      .put("multiply", ImmutableSet.of("*"))
      .put("ne", ImmutableSet.of("!=", "<>"))
      .put("negative", ImmutableSet.of("-"))
      .put("notdistinct", ImmutableSet.of("is not distinct from"))
      .put("subtract", ImmutableSet.of("-"))
      .build();

  // Set containing a pair of casts that are supported. The first type
  // is the argument and the second type is the return type.
  public static Set<Pair<ScalarType, ScalarType>> SUPPORTED_IMPLICIT_CASTS =
      ImmutableSet.<Pair<ScalarType, ScalarType>> builder()
      .add(Pair.of(Type.BOOLEAN, Type.TINYINT))
      .add(Pair.of(Type.BOOLEAN, Type.SMALLINT))
      .add(Pair.of(Type.BOOLEAN, Type.INT))
      .add(Pair.of(Type.BOOLEAN, Type.BIGINT))
      .add(Pair.of(Type.BOOLEAN, Type.FLOAT))
      .add(Pair.of(Type.BOOLEAN, Type.DOUBLE))
      .add(Pair.of(Type.TINYINT, Type.SMALLINT))
      .add(Pair.of(Type.TINYINT, Type.INT))
      .add(Pair.of(Type.TINYINT, Type.BIGINT))
      .add(Pair.of(Type.TINYINT, Type.FLOAT))
      .add(Pair.of(Type.TINYINT, Type.DOUBLE))
      .add(Pair.of(Type.TINYINT, Type.DECIMAL))
      .add(Pair.of(Type.SMALLINT, Type.INT))
      .add(Pair.of(Type.SMALLINT, Type.BIGINT))
      .add(Pair.of(Type.SMALLINT, Type.FLOAT))
      .add(Pair.of(Type.SMALLINT, Type.DOUBLE))
      .add(Pair.of(Type.SMALLINT, Type.DECIMAL))
      .add(Pair.of(Type.INT, Type.BIGINT))
      .add(Pair.of(Type.INT, Type.FLOAT))
      .add(Pair.of(Type.INT, Type.DOUBLE))
      .add(Pair.of(Type.INT, Type.DECIMAL))
      .add(Pair.of(Type.BIGINT, Type.FLOAT))
      .add(Pair.of(Type.BIGINT, Type.DOUBLE))
      .add(Pair.of(Type.BIGINT, Type.DECIMAL))
      .add(Pair.of(Type.FLOAT, Type.DOUBLE))
      .add(Pair.of(Type.FLOAT, Type.DECIMAL))
      .add(Pair.of(Type.DOUBLE, Type.DECIMAL))
      .add(Pair.of(Type.STRING, Type.TIMESTAMP))
      .add(Pair.of(Type.STRING, Type.DATE))
      .add(Pair.of(Type.VARCHAR, Type.STRING))
      .add(Pair.of(Type.VARCHAR, Type.TIMESTAMP))
      .add(Pair.of(Type.VARCHAR, Type.DATE))
      .add(Pair.of(Type.CHAR, Type.CHAR))
      .add(Pair.of(Type.CHAR, Type.STRING))
      .add(Pair.of(Type.CHAR, Type.TIMESTAMP))
      .add(Pair.of(Type.CHAR, Type.DATE))
      .add(Pair.of(Type.DECIMAL, Type.FLOAT))
      .add(Pair.of(Type.DECIMAL, Type.DOUBLE))
      .add(Pair.of(Type.DATE, Type.TIMESTAMP))
      .build();

  // Set containing functions where the return type should always
  // allow nulls.
  public static Set<String> RET_TYPE_ALWAYS_NULLABLE_FUNCS =
      ImmutableSet.<String> builder()
      .add("cast")
      .build();

  // Set containing functions where the hasVarArgs parameter in the
  // Impala needs to be overridden.
  public static Set<String> OVERRIDE_HAS_VAR_ARGS_FUNCS =
      ImmutableSet.<String> builder()
      .add("or")
      .add("and")
      .build();
}
