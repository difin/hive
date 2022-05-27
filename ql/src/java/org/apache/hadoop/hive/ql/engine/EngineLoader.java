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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * EngineLoader.  Tracks all database specific engines. There is a native engine
 * automatically loaded, but this also allows external engines to be loaded as well.
 */
@InterfaceStability.Unstable
public class EngineLoader {
  private static final Logger LOG = LoggerFactory.getLogger(EngineLoader.class);

  private enum Loader {
    IMPALA,
    HIVE_WITH_IMPALA_RESOLVER,
    HIVE
  }

  private static Map<Loader, EngineHelper> engineHelpers = new HashMap<>();

  static {
    // TODO: CDPD-20696 we shouldn't hardcode Impala here
    try {
      EngineHelper impalaHelper = (EngineHelper)
          Class.forName("org.apache.hadoop.hive.impala.ImpalaHelper").newInstance();
      engineHelpers.put(Loader.IMPALA, impalaHelper);
      EngineHelper impalaTypeUnificationHelper = (EngineHelper)
          Class.forName("org.apache.hadoop.hive.impala.ImpalaTypeUnificationHelper").newInstance();
      engineHelpers.put(Loader.HIVE_WITH_IMPALA_RESOLVER, impalaTypeUnificationHelper);
    } catch (Throwable e) {
      LOG.info("Could not load Impala Helper class.");
      LOG.debug("Exception information: ", e);
    }
    EngineHelper defaultHelper = new NativeEngineHelper();
    engineHelpers.put(Loader.HIVE, defaultHelper);
  }

  // TODO: CDPD-20696 we should get rid of this flavor. We don't want to hardcode "impala"
  // anywhere within this module
  @Deprecated
  public static EngineHelper getExternalInstance() {
    return engineHelpers.get(Loader.IMPALA);
  }

  public static EngineHelper getInstance(HiveConf conf) {
    if (conf.getEngine() == Engine.IMPALA) {
      return engineHelpers.get(Loader.IMPALA);
    } else {
      if (conf.getFunctionResolverEngine() == Engine.IMPALA) {
        return engineHelpers.get(Loader.HIVE_WITH_IMPALA_RESOLVER);
      }
    }
    return engineHelpers.get(Loader.HIVE);
  }
}
