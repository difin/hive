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

package org.apache.iceberg.rest;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.hive.rest.catalog.RestCatalogScanPlanning;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RestCatalogScanPlanning} configuration helpers. */
public class TestRestCatalogScanPlanning {

  @Test
  void catalogPropertyKeyUsesIcebergPropertyName() {
    assertThat(RestCatalogScanPlanning.catalogPropertyKey("ice01"))
        .isEqualTo(
            IcebergCatalogProperties.catalogPropertyConfigKey(
                "ice01", RESTCatalogProperties.SCAN_PLANNING_MODE));
  }

  @Test
  void requestsServerSidePlanningFromConfiguration() {
    Configuration conf = new Configuration();
    assertThat(RestCatalogScanPlanning.requestsServerSidePlanning("ice01", conf)).isFalse();

    RestCatalogScanPlanning.setScanPlanningMode(conf, "ice01", "server");
    assertThat(RestCatalogScanPlanning.requestsServerSidePlanning("ice01", conf)).isTrue();
    assertThat(RestCatalogScanPlanning.isServerMode(conf, "ice01")).isTrue();
    assertThat(RestCatalogScanPlanning.getScanPlanningMode(conf, "ice01").modeName())
        .isEqualTo("server");
  }

  @Test
  void resolveCatalogNameUsesSessionDefaultWhenTablePropertyMissing() {
    Configuration conf = new Configuration();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, "ice01");
    assertThat(RestCatalogScanPlanning.resolveCatalogName(conf, null)).isEqualTo("ice01");
    assertThat(RestCatalogScanPlanning.resolveCatalogName(conf, "ice02")).isEqualTo("ice02");
  }

  @Test
  void shouldPropagateCatalogPropertiesOnlyForRestCatalogInServerMode() {
    Configuration conf = new Configuration();
    conf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    assertThat(RestCatalogScanPlanning.shouldPropagateCatalogPropertiesToJob("ice01", conf)).isFalse();

    RestCatalogScanPlanning.setScanPlanningMode(conf, "ice01", "server");
    assertThat(RestCatalogScanPlanning.shouldPropagateCatalogPropertiesToJob("ice01", conf)).isTrue();

    Configuration hiveConf = new Configuration();
    hiveConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
    RestCatalogScanPlanning.setScanPlanningMode(hiveConf, "ice01", "server");
    assertThat(RestCatalogScanPlanning.shouldPropagateCatalogPropertiesToJob("ice01", hiveConf))
        .isFalse();
  }

  @Test
  void propagateCatalogPropertiesToJobCopiesRestCatalogSettings() {
    Configuration sessionConf = new Configuration();
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181");
    RestCatalogScanPlanning.setScanPlanningMode(sessionConf, "ice01", "server");
    sessionConf.set("unrelated.key", "skip");

    Map<String, String> jobProperties = Maps.newHashMap();
    RestCatalogScanPlanning.propagateCatalogPropertiesToJob(sessionConf, "ice01", jobProperties);

    assertThat(jobProperties)
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
            CatalogUtil.ICEBERG_CATALOG_TYPE_REST)
        .containsEntry(
            IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181")
        .containsEntry(RestCatalogScanPlanning.catalogPropertyKey("ice01"), "server")
        .doesNotContainKey("unrelated.key");
  }

  @Test
  void propagateCatalogPropertiesToJobConfigurationCopiesRestCatalogSettings() {
    Configuration sessionConf = new Configuration();
    MetastoreConf.setVar(sessionConf, MetastoreConf.ConfVars.CATALOG_DEFAULT, "ice01");
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181");
    RestCatalogScanPlanning.setScanPlanningMode(sessionConf, "ice01", "server");

    Configuration jobConf = new Configuration();
    RestCatalogScanPlanning.propagateCatalogPropertiesToJob(sessionConf, null, jobConf);

    assertThat(jobConf.get(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname())).isEqualTo("ice01");
    assertThat(jobConf.get(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE)))
        .isEqualTo(CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    assertThat(jobConf.get(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri")))
        .isEqualTo("http://localhost:8181");
    assertThat(jobConf.get(RestCatalogScanPlanning.catalogPropertyKey("ice01"))).isEqualTo("server");
  }

  @Test
  void propagateCatalogPropertiesToJobSkipsWhenServerModeDisabled() {
    Configuration sessionConf = new Configuration();
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("ice01", "uri"), "http://localhost:8181");

    Map<String, String> jobProperties = Maps.newHashMap();
    RestCatalogScanPlanning.propagateCatalogPropertiesToJob(sessionConf, "ice01", jobProperties);

    assertThat(jobProperties).isEmpty();
  }
}
