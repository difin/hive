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

import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TFunctionBinaryType;

import java.util.List;

/**
 * Wrapper interface around the Impala AggFunction. This interface allows
 * us to supply aggregate functions in the test framework without need for the
 * Impala shared library.
 */
public interface AggFunctionWrapper {
  public String dbName();

  public String functionName();

  public Type getReturnType();

  public List<Type> getArgTypes();

  public boolean isPersistent();

  public TFunctionBinaryType getBinaryType();

  public Type getIntermediateType();

  public int getIntermediateTypeLength();

  public boolean isAnalyticFn();

  public String getUpdateFnSymbol();

  public String getInitFnSymbol();

  public String getMergeFnSymbol();

  public String getValueFnSymbol();

  public String getFinalizeFnSymbol();

  public String getRemoveFnSymbol();

  public String getSerializeFnSymbol();

  public boolean ignoresDistinct();

  public boolean returnsNonNullOnEmpty();

  public boolean isAggregateFn();
}
