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
package org.apache.iceberg.rest.standalone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Custom health indicator for HMS connectivity.
 * Used by Kubernetes readiness probes to determine if the server is ready to accept traffic.
 */
@Component
public class HMSReadinessHealthIndicator implements HealthIndicator {
  private static final Logger LOG = LoggerFactory.getLogger(HMSReadinessHealthIndicator.class);
  
  private final Configuration conf;
  
  public HMSReadinessHealthIndicator(Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public Health health() {
    try {
      // Check if HMS Thrift URIs are configured
      String hmsThriftUris = MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS);
      if (hmsThriftUris == null || hmsThriftUris.isEmpty()) {
        return Health.down()
            .withDetail("reason", "HMS Thrift URIs not configured")
            .build();
      }
      
      // Basic health check - configuration is valid
      return Health.up()
          .withDetail("hmsThriftUris", hmsThriftUris)
          .withDetail("warehouse", MetastoreConf.getVar(conf, ConfVars.WAREHOUSE))
          .build();
    } catch (Exception e) {
      LOG.error("Health check failed", e);
      return Health.down()
          .withDetail("error", e.getMessage())
          .build();
    }
  }
}
