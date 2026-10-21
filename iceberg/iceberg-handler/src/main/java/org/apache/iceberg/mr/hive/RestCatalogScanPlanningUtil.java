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

package org.apache.iceberg.mr.hive;

import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.rest.RESTCatalogProperties;

/**
 * Utilities for Iceberg REST catalog server-side scan planning configuration.
 *
 * <p>When a REST catalog server advertises the scan-planning endpoints and
 * {@link RESTCatalogProperties#SCAN_PLANNING_MODE} is set to
 * {@link RESTCatalogProperties.ScanPlanningMode#SERVER}, Iceberg's {@code RESTSessionCatalog} returns a
 * {@code RESTTable} that delegates {@code planTasks()} to the server. Hive's
 * {@code IcebergInputFormat} calls {@code scan.planTasks()} via
 * {@code IcebergInputFormat#getSplits}, which reloads a live REST catalog table for {@code POST /plan}
 * (pinned to the serialized compile-time snapshot). The catalog decides the scan planning mode;
 * {@link HiveConf.ConfVars#HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED} only controls whether
 * Hive is allowed to follow it.
 *
 * <p>Tests: {@code TestRestCatalogScanPlanningUtil} in {@code iceberg-handler};
 * {@code TestHiveIcebergServerSideScanPlanning} in {@code iceberg-handler}; embedded REST server
 * tests {@code TestRestCatalogScanPlanningServerIT} and {@code TestHiveIcebergServerSideScanPlanningServerIT}
 * in {@code itests/hive-iceberg-rest-server}.
 *
 * <p>{@link #setScanPlanningMode(Configuration, String, String)},
 * {@link #getScanPlanningMode(Configuration, String)} and
 * {@link #propagateInputJobCredentials(TableDesc, Map, Configuration)} are public so tests in other Maven modules
 * (for example {@code itests/hive-iceberg-rest-server}) can configure scan planning; those modules
 * compile against this artifact as a JAR and cannot call package-private members. They are not
 * intended as a general operator or application API.
 *
 * @see <a href="https://iceberg.apache.org/docs/latest/catalog-properties/">REST catalog properties</a>
 */
public final class RestCatalogScanPlanningUtil {

  private RestCatalogScanPlanningUtil() {
  }

  public static void setScanPlanningMode(Configuration conf, String catalogName, String mode) {
    conf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey(
            catalogName, RESTCatalogProperties.SCAN_PLANNING_MODE),
        RESTCatalogProperties.ScanPlanningMode.fromString(mode).modeName());
  }

  public static RESTCatalogProperties.ScanPlanningMode getScanPlanningMode(
      Configuration conf, String catalogName) {
    String mode = conf.get(
        IcebergCatalogProperties.catalogPropertyConfigKey(
            catalogName, RESTCatalogProperties.SCAN_PLANNING_MODE),
        RESTCatalogProperties.SCAN_PLANNING_MODE_DEFAULT.modeName());
    return RESTCatalogProperties.ScanPlanningMode.fromString(mode);
  }

  /**
   * Returns true when Hive is allowed to use server-side scan planning (the Hive-side kill switch).
   */
  static boolean isServerSideScanPlanningEnabled(Configuration conf) {
    return conf != null &&
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED);
  }

  /**
   * Returns true when the catalog's scan planning mode is {@code server} and the Hive kill switch is on.
   */
  static boolean shouldUseServerSideScanPlanning(Configuration conf, String catalogName) {
    if (StringUtils.isEmpty(catalogName) || !isServerSideScanPlanningEnabled(conf)) {
      return false;
    }
    return getScanPlanningMode(conf, catalogName) == RESTCatalogProperties.ScanPlanningMode.SERVER;
  }

  /**
   * Returns true when catalog properties should be copied into the Tez job so split generation can
   * reload a live REST catalog table for server-side scan planning.
   */
  static boolean shouldPropagateCatalogPropertiesToJob(String catalogName, Configuration conf) {
    String resolvedCatalogName = HiveTableUtil.resolveCatalogName(conf, catalogName);
    if (StringUtils.isEmpty(resolvedCatalogName) || conf == null) {
      return false;
    }
    if (!CatalogUtil.ICEBERG_CATALOG_TYPE_REST.equals(
        IcebergCatalogProperties.getCatalogType(conf, resolvedCatalogName))) {
      return false;
    }
    return shouldUseServerSideScanPlanning(conf, resolvedCatalogName);
  }

  /**
   * Writes the {@link InputFormatConfig#serverSideScanPlanningKey(String)} marker and the session default
   * catalog into Tez job properties. The {@code iceberg.catalog.<catalog>.*} entries go through job
   * secrets instead, see {@link #propagateInputJobCredentials(TableDesc, Map, Configuration)}.
   * Session values replace existing entries so a session {@code SET} wins.
   */
  public static void propagateCatalogPropertiesToJob(
      Configuration sessionConf, String catalogName, Map<String, String> jobProperties) {
    if (sessionConf == null || jobProperties == null) {
      return;
    }
    propagateServerSideScanPlanningMarker(sessionConf, catalogName, jobProperties::put);
  }

  /**
   * Same as {@link #propagateCatalogPropertiesToJob(Configuration, String, Map)} for the runtime job
   * {@link Configuration} built in {@code configureJobConf}.
   */
  public static void propagateCatalogPropertiesToJob(
      Configuration sessionConf, String catalogName, Configuration jobConf) {
    if (sessionConf == null || jobConf == null) {
      return;
    }
    propagateServerSideScanPlanningMarker(sessionConf, catalogName, jobConf::set);
  }

  /**
   * Copies all {@code iceberg.catalog.<catalog>.*} entries from the HS2 session into job secrets
   * (HIVE-20651), so catalog credentials never land in plain job properties. Only split generation in
   * the AM needs them to load the REST catalog; {@code HiveInputFormat#addSplitsForGroup} copies job
   * secrets into the job conf before {@code getSplits}.
   */
  public static void propagateInputJobCredentials(
      TableDesc tableDesc, Map<String, String> jobSecrets, Configuration sessionConf) {
    if (tableDesc == null || tableDesc.getProperties() == null || jobSecrets == null || sessionConf == null) {
      return;
    }
    String catalogName = tableDesc.getProperty(InputFormatConfig.CATALOG_NAME);
    if (!shouldPropagateCatalogPropertiesToJob(catalogName, sessionConf)) {
      return;
    }
    String catalogPrefix = IcebergCatalogProperties.CATALOG_CONFIG_PREFIX +
        HiveTableUtil.resolveCatalogName(sessionConf, catalogName) + ".";
    sessionConf.forEach(entry -> {
      if (entry.getKey().startsWith(catalogPrefix)) {
        jobSecrets.put(entry.getKey(), entry.getValue());
      }
    });
  }

  private static void propagateServerSideScanPlanningMarker(
      Configuration sessionConf, String catalogName, BiConsumer<String, String> jobPropertySink) {
    if (!shouldPropagateCatalogPropertiesToJob(catalogName, sessionConf)) {
      return;
    }
    jobPropertySink.accept(
        InputFormatConfig.serverSideScanPlanningKey(HiveTableUtil.resolveCatalogName(sessionConf, catalogName)),
        Boolean.TRUE.toString());

    String sessionDefaultCatalog = MetastoreConf.getVar(sessionConf, MetastoreConf.ConfVars.CATALOG_DEFAULT);
    if (StringUtils.isNotBlank(sessionDefaultCatalog)) {
      jobPropertySink.accept(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname(), sessionDefaultCatalog);
    }
  }
}
