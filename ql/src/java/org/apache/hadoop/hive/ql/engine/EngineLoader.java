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

package org.apache.hadoop.hive.ql.engine;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConf.Engine;
import org.apache.hadoop.hive.ql.engine.internal.NativeEngineHelper;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TaskFactory.TaskTuple;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;


/**
 * EngineLoader.  Tracks all database specific engines. There is a native engine
 * automatically loaded, but this also allows external engines to be loaded as well.
 */
@InterfaceStability.Unstable
public class EngineLoader {
  private static final Logger LOG = LoggerFactory.getLogger(EngineLoader.class);

  private enum Loader {
    HIVE
  }

  private static Map<Loader, Throwable> failedLoaders = new HashMap<>();

  private static Map<Loader, EngineHelper> engineHelpers = new HashMap<>();

  static {
    EngineHelper defaultHelper = new NativeEngineHelper();
    engineHelpers.put(Loader.HIVE, defaultHelper);
  }

  public static EngineHelper getInstance(HiveConf conf) {
    return engineHelpers.get(Loader.HIVE);
  }

  public static List<TaskTuple<? extends Serializable>> getTaskTuples() {
    List<TaskTuple<? extends Serializable>> taskTuples = new ArrayList<>();
    for (EngineHelper helper : engineHelpers.values()) {
      taskTuples.addAll(helper.getRuntimeHelper().getTaskTuples());
    }
    return taskTuples;
  }

  public static IdentityHashMap<Class<? extends OperatorDesc>,
      Class<? extends Operator<? extends OperatorDesc>>>
      getOperatorVecs() {
    IdentityHashMap<Class<? extends OperatorDesc>,
        Class<? extends Operator<? extends OperatorDesc>>> opVecs =
            new IdentityHashMap<>();

    for (EngineHelper helper : engineHelpers.values()) {
      opVecs.putAll(helper.getRuntimeHelper().getOperatorVecs());
    }
    return opVecs;
  }
}
