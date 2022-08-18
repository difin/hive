package org.apache.hadoop.hive.cli.control;

import org.apache.hadoop.hive.ql.QTestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoreImpalaNegativeCliDriver extends CoreNegativeCliDriver {
  private static final Logger LOG = LoggerFactory.getLogger(CoreImpalaNegativeCliDriver.class);

  public CoreImpalaNegativeCliDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
  }

  @Override
  public void setUp() {
    super.setUp();
    // Due to RELENG-18563, we are not really able to reference the classes within the
    // artifact of impala-frontend. To overcome this limitation, we initialize Impala's
    // backend configuration in TestNegativeImpalaCliDriver under hive-it-impala, which
    // is less elegant.
    //BackendConfigUtil.initializeBackendConfig(getQt().getConf());
  }

  public QTestUtil getQTestUtil() { return getQt(); }
}
