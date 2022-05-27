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

package org.apache.hadoop.hive.impala;

import org.apache.hadoop.hive.ql.engine.EngineHelper;
import org.apache.hadoop.hive.ql.engine.internal.NativeEngineRuntimeHelper;
import org.apache.hadoop.hive.ql.engine.internal.NativeEngineSessionHelper;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImpalaTypeUnificationHelper is a top level EngineHelper class that is used for
 * DDL queries that use TEZ as its runtime engine but uses Impala for its function
 * resolving.  The EngineCompileHelper used is Impala which uses the ImpalaFunctionResolver
 * to resolve function parameter and return types.
 */
public class ImpalaTypeUnificationHelper extends EngineHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaTypeUnificationHelper.class);

  public ImpalaTypeUnificationHelper() {
    super(new ImpalaCompileHelper(), new NativeEngineRuntimeHelper(),
        new NativeEngineSessionHelper());
  }
}
