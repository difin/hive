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

import org.apache.hadoop.hive.impala.funcmapper.AggFunctionDetails;
import org.apache.hadoop.hive.impala.funcmapper.ScalarFunctionDetails;
import org.apache.hadoop.hive.ql.engine.EngineHelper;
import org.apache.impala.service.FeSupport;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImpalaHelper extends EngineHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaHelper.class);

  static {
    try {
      // Need to setExternalFE before calling into any Impala function.
      FeSupport.setExternalFE();

      // If functions already exist, do not load them.  This is the main place where functions
      // are loaded when running hive server2. Most functions are retrieved from Impala and require
      // the impala ".so" file to be present. However, when running unit tests, the ".so" is not
      // available and the functions get pre-loaded via a file. In that case, the step to load
      // the functions from Impala is skipped.
      // In other cases, we use Conf.HIVE_IN_TEST for code specific to test mode, but the HiveConf
      // object is unavailable here because this is being statically initialized.
      if (ScalarFunctionDetails.getAllFuncDetails().size() == 0) {
        // load all functions into memory.
        List<ScalarFunctionDetails> sfdList =
            ScalarFunctionDetails.createScalarFunctionDetailsFromImpala();
        ScalarFunctionDetails.addFunctionsFromImpala(sfdList);
        List<AggFunctionDetails> afdList =
            AggFunctionDetails.createAggFunctionDetailsFromImpala();
        AggFunctionDetails.addFunctionsFromImpala(afdList);
      }
    } catch (Exception e) {
      LOG.warn("Unable to load Impala functions: ", e);
    }
  }

  public ImpalaHelper() {
    super(new ImpalaCompileHelper(), new ImpalaRuntimeHelper(),
        new ImpalaSessionHelper());
  }
}
