package org.apache.hadoop.hive.cli.control;

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
    BackendConfigUtil.initializeBackendConfig(getQt().getConf());
  }
}
