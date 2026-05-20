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

package org.apache.iceberg.metering;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Unit tests for MeteringHealthWatcher using a mocked MeteringServiceMonitor. */
class TestMeteringHealthWatcher {

  @Test
  void hiveInTestModeAlwaysReportsHealthy() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, true);
    MeteringHealthWatcher watcher = new MeteringHealthWatcher(conf);
    watcher.start();
    assertThat(watcher.isHealthy()).isTrue();
    watcher.stop();
  }

  @Test
  void becomesUnhealthyAfterThreeConsecutiveFailures() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, false);
    MeteringHealthWatcher watcher = new MeteringHealthWatcher(conf);
    try {
      try (MockedStatic<MeteringServiceMonitor> mock = mockStatic(MeteringServiceMonitor.class)) {
        mock.when(MeteringServiceMonitor::isMeteringServiceHealthy).thenReturn(false);
        assertThat(watcher.isHealthy()).isTrue();
        watcher.runHealthCheckOnceForTests();
        assertThat(watcher.isHealthy()).isTrue();
        watcher.runHealthCheckOnceForTests();
        assertThat(watcher.isHealthy()).isTrue();
        watcher.runHealthCheckOnceForTests();
        assertThat(watcher.isHealthy()).isFalse();
      }
    } finally {
      watcher.stop();
    }
  }

  @Test
  void successfulCheckResetsConsecutiveFailures() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, false);
    MeteringHealthWatcher watcher = new MeteringHealthWatcher(conf);
    try {
      try (MockedStatic<MeteringServiceMonitor> mock = mockStatic(MeteringServiceMonitor.class)) {
        mock.when(MeteringServiceMonitor::isMeteringServiceHealthy)
            .thenReturn(false, false, true, false, false, false);
        watcher.runHealthCheckOnceForTests();
        watcher.runHealthCheckOnceForTests();
        watcher.runHealthCheckOnceForTests();
        assertThat(watcher.isHealthy()).isTrue();
        watcher.runHealthCheckOnceForTests();
        watcher.runHealthCheckOnceForTests();
        assertThat(watcher.isHealthy()).isTrue();
        watcher.runHealthCheckOnceForTests();
        assertThat(watcher.isHealthy()).isFalse();
      }
    } finally {
      watcher.stop();
    }
  }

  @Test
  void recoversWhenHealthCheckSucceedsAfterBeingUnhealthy() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, false);
    MeteringHealthWatcher watcher = new MeteringHealthWatcher(conf);
    try {
      try (MockedStatic<MeteringServiceMonitor> mock = mockStatic(MeteringServiceMonitor.class)) {
        mock.when(MeteringServiceMonitor::isMeteringServiceHealthy).thenReturn(false);
        watcher.runHealthCheckOnceForTests();
        watcher.runHealthCheckOnceForTests();
        watcher.runHealthCheckOnceForTests();
        assertThat(watcher.isHealthy()).isFalse();

        mock.when(MeteringServiceMonitor::isMeteringServiceHealthy).thenReturn(true);
        watcher.runHealthCheckOnceForTests();
        assertThat(watcher.isHealthy()).isTrue();
      }
    } finally {
      watcher.stop();
    }
  }
}
