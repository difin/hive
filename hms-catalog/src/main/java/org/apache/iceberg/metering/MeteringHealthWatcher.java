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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Continuously monitors the metering service health in a background daemon thread, polling
 * <scheme>://localhost:<port>/health on a fixed interval clamped between #MIN_POLL_INTERVAL_MS
 * (30s) and #DEFAULT_POLL_INTERVAL_MS (5m). Configured value may be set via hive-site.xml
 * (#POLL_INTERVAL_HIVE_KEY) or JVM override (#POLL_INTERVAL_SYS_PROP,
 * e.g: -Dhive.metastore.metering.service.health.poll.interval=1m).
 * The watcher starts in the "healthy" state. A poll that fails increments a consecutive-failure
 * counter; once that counter reaches #MAX_CONSECUTIVE_FAILURES the watcher transitions
 * to "unhealthy" and stops serving catalog traffic (callers check #isHealthy()). A
 * successful poll at any time resets the counter and restores the "healthy" state.
 * When ConfVars.HIVE_IN_TEST is true on the supplied configuration, no background
 * thread is started and #isHealthy() always returns true. This prevents tests
 * from blocking or failing due to a missing metering service.
 */
public class MeteringHealthWatcher {
  private static final Logger LOG = LoggerFactory.getLogger(MeteringHealthWatcher.class);

  /** Default and maximum poll interval (ms). */
  static final int DEFAULT_POLL_INTERVAL_MS = 5 * 60 * 1000;
  /** Minimum poll interval (ms); values at or below this are raised to this floor. */
  static final int MIN_POLL_INTERVAL_MS = 30 * 1000;

  /**
   * Private tuning key (not registered in MetastoreConf.ConfVars). May appear in hive-site.xml;
   * JVM #POLL_INTERVAL_SYS_PROP takes precedence when set.
   */
  private static final String POLL_INTERVAL_HIVE_KEY =
      "hive.metastore.metering.service.health.poll.interval";
  private static final String POLL_INTERVAL_SYS_PROP = POLL_INTERVAL_HIVE_KEY;

  static final int MAX_CONSECUTIVE_FAILURES = 3;

  private final Configuration conf;
  private final AtomicBoolean healthy;
  private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
  private final ScheduledExecutorService scheduler;
  private final boolean testMode;

  /**
   * Creates a new watcher.
   *
   * @param conf metastore configuration; if ConfVars.HIVE_IN_TEST is set the watcher
   *             runs in no-op test mode
   */
  public MeteringHealthWatcher(Configuration conf) {
    this.conf = conf;
    testMode = conf != null && MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST);
    // Optimistic start: serve traffic until consecutive failures prove metering is down.
    healthy = new AtomicBoolean(true);
    if (testMode) {
      scheduler = null;
      LOG.info("MeteringHealthWatcher: test mode enabled, skipping metering health checks");
    } else {
      scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "metering-health-watcher");
        t.setDaemon(true);
        return t;
      });
    }
  }

  /**
   * Starts the background polling loop. The first poll fires immediately (delay = 0).
   * Has no effect in test mode.
   */
  public void start() {
    if (testMode) {
      return;
    }
    int periodMs = meteringHealthPollPeriodMs(conf);
    LOG.info("MeteringHealthWatcher: starting — polling {}{} every {} ms",
        MeteringServiceMonitor.METERING_SERVICE_HOST, "/health", periodMs);
    scheduler.scheduleAtFixedRate(this::checkHealth, 0, periodMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Effective scheduler period clamped to [#MIN_POLL_INTERVAL_MS, #DEFAULT_POLL_INTERVAL_MS].
   * Precedence: JVM system property, then hive-site / metastore Configuration, then default.
   */
  static int meteringHealthPollPeriodMs(Configuration conf) {
    long configuredMs = resolveConfiguredPollIntervalMs(conf);
    return (int) Math.min(DEFAULT_POLL_INTERVAL_MS,
        Math.max(MIN_POLL_INTERVAL_MS, configuredMs));
  }

  private static long resolveConfiguredPollIntervalMs(Configuration conf) {
    String sysProp = System.getProperty(POLL_INTERVAL_SYS_PROP);
    if (sysProp != null && !sysProp.isEmpty()) {
      return parsePollIntervalMs(sysProp, "system property " + POLL_INTERVAL_SYS_PROP);
    }
    if (conf != null) {
      String val = conf.get(POLL_INTERVAL_HIVE_KEY);
      if (val != null && !val.isEmpty()) {
        return parsePollIntervalMs(val, POLL_INTERVAL_HIVE_KEY);
      }
    }
    return DEFAULT_POLL_INTERVAL_MS;
  }

  private static long parsePollIntervalMs(String val, String source) {
    try {
      return MetastoreConf.convertTimeStr(val.trim(), TimeUnit.MINUTES,
          TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.warn("Invalid {} value '{}'; using default {} ms", source, val,
          DEFAULT_POLL_INTERVAL_MS, e);
      return DEFAULT_POLL_INTERVAL_MS;
    }
  }

  /**
   * Stops the background polling loop. Should be called when the catalog server shuts down.
   * Has no effect in test mode.
   */
  public void stop() {
    if (scheduler != null) {
      scheduler.shutdownNow();
      LOG.info("MeteringHealthWatcher: stopped");
    }
  }

  /**
   * Returns true if the metering service is currently considered healthy.
   * Always returns true in test mode.
   */
  public boolean isHealthy() {
    if (testMode) {
      return true;
    }
    return healthy.get();
  }

  /**
   * Runs one health check synchronously (for unit tests). Does not start the scheduler.
   */
  void runHealthCheckOnceForTests() {
    checkHealth();
  }

  private void checkHealth() {
    try {
      if (MeteringServiceMonitor.isMeteringServiceHealthy(conf)) {
        int prev = consecutiveFailures.getAndSet(0);
        if (prev > 0) {
          LOG.info("MeteringHealthWatcher: metering service recovered after " +
              "{} consecutive failure(s); resuming catalog traffic", prev);
        } else {
          LOG.debug("MeteringHealthWatcher: metering service is healthy");
        }
        healthy.set(true);
      } else {
        int failures = consecutiveFailures.incrementAndGet();
        if (failures >= MAX_CONSECUTIVE_FAILURES) {
          LOG.error(
              "MeteringHealthWatcher: metering service unhealthy for {} " +
                  "consecutive check(s) — stopping catalog traffic", failures);
          healthy.set(false);
        } else {
          LOG.warn(
              "MeteringHealthWatcher: metering service health check failed ({}/{})",
              failures, MAX_CONSECUTIVE_FAILURES);
        }
      }
    } catch (Exception e) {
      LOG.error("MeteringHealthWatcher: unexpected error during health check", e);
    }
  }
}
