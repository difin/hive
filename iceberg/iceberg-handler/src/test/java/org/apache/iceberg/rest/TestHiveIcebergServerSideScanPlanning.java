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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.hive.rest.catalog.RestCatalogScanPlanning;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.HiveTableUtil;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.apache.iceberg.TestBase.FILE_A;
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.apache.iceberg.TestBase.SPEC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

/**
 * Tests for Hive server-side REST catalog scan planning via
 * {@link HiveTableUtil#resolveTableForScanPlanning}.
 */
class TestHiveIcebergServerSideScanPlanning extends TestBaseWithRESTServer {

  private static final String UNIT_CATALOG_NAME = "ice01";
  private static final String REST_CATALOG_NAME = "hive-iceberg-scan-planning";
  private static final TableIdentifier TABLE_ID = TableIdentifier.of(NS, "file_planning_table");
  private static final Schema UNIT_SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @TempDir
  private Path warehouse;

  @Override
  protected boolean useHttpCompression() {
    return false;
  }

  @Override
  protected RESTCatalogAdapter createAdapterForServer() {
    return Mockito.spy(
        new RESTCatalogAdapter(backendCatalog) {
          @Override
          protected <T extends RESTResponse> T execute(
              HTTPRequest request,
              Class<T> responseType,
              Consumer<ErrorResponse> errorHandler,
              Consumer<Map<String, String>> responseHeaders) {
            Object body = roundTripSerialize(request.body(), "request");
            HTTPRequest req = ImmutableHTTPRequest.builder().from(request).body(body).build();
            T response = super.execute(req, responseType, errorHandler, responseHeaders);
            response = roundTripSerialize(response, "response");

            if (response instanceof LoadTableResponse) {
              return RESTCatalogAdapter.castResponse(
                  responseType,
                  withPlanningMode(
                      (LoadTableResponse) response,
                      RESTCatalogProperties.ScanPlanningMode.SERVER.modeName()));
            }

            return response;
          }
        });
  }

  @Override
  protected String catalogName() {
    return REST_CATALOG_NAME;
  }

  @Override
  protected Map<String, String> additionalCatalogProperties() {
    Configuration conf = new Configuration();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, REST_CATALOG_NAME);
    RestCatalogScanPlanning.setScanPlanningMode(conf, REST_CATALOG_NAME, "server");
    return IcebergCatalogProperties.getCatalogProperties(conf, REST_CATALOG_NAME);
  }

  @BeforeEach
  @Override
  public void before() throws Exception {
    super.before();
    adapterForRESTServer.setPlanningBehavior(
        new RESTCatalogAdapter.PlanningBehavior() {
          @Override
          public boolean shouldPlanTableScanAsync(Scan<?, FileScanTask, ?> scan) {
            return false;
          }

          @Override
          public int numberFileScanTasksPerPlanTask() {
            return 100;
          }
        });
  }

  /**
   * Negative path: do not reload from the REST catalog unless {@code scan-planning-mode=server}.
   * Even with a REST catalog and a serialized table in the job conf, split generation should keep
   * using the snapshot ({@link SerializableTable}) when server mode is off.
   */
  @Test
  void usesDeserializedTableWhenServerModeDisabled() {
    Table table = new HadoopTables().create(UNIT_SCHEMA, PartitionSpec.unpartitioned(), warehouse.toString());
    Configuration conf = buildUnitTableConf(table, false);

    Table resolved = HiveTableUtil.resolveTableForScanPlanning(conf, table.name());
    assertThat(resolved).isInstanceOf(SerializableTable.class);
  }

  /**
   * Negative path: intra-transaction read-after-write must not reload from the catalog even when
   * server mode is enabled. {@link InputFormatConfig#TABLE_METADATA_LOCATION} points at uncommitted
   * metadata that only exists in the job conf; reloading from the REST catalog would return stale
   * committed state and break same-txn visibility (e.g. INSERT then SELECT).
   */
  @Test
  void usesDeserializedTableForIntraTxnMetadataEvenInServerMode() {
    Table table = new HadoopTables().create(UNIT_SCHEMA, PartitionSpec.unpartitioned(), warehouse.toString());
    Configuration conf = buildUnitTableConf(table, true);
    conf.set(InputFormatConfig.TABLE_METADATA_LOCATION, warehouse + "/metadata/snapshot.metadata.json");

    Table resolved = HiveTableUtil.resolveTableForScanPlanning(conf, table.name());
    assertThat(resolved).isInstanceOf(BaseTable.class);
  }

  @Test
  void executorJobConfWithoutPropagationUsesSerializedTable() throws IOException {
    Table table = createTableWithData();
    Configuration jobConf = executorJobConf(table);

    Table resolved = HiveTableUtil.resolveTableForScanPlanning(jobConf, TABLE_ID.toString());
    assertThat(resolved).isInstanceOf(SerializableTable.class);
    assertThat(resolved.newScan()).isNotInstanceOf(RESTTableScan.class);
  }

  @Test
  void propagatedJobConfReloadsRestTableFromCatalog() throws IOException {
    Table table = createTableWithData();
    Configuration sessionConf = sessionConf();
    Configuration jobConf = executorJobConf(table);
    RestCatalogScanPlanning.propagateCatalogPropertiesToJob(sessionConf, REST_CATALOG_NAME, jobConf);

    ArgumentCaptor<HTTPRequest> requestCaptor = ArgumentCaptor.forClass(HTTPRequest.class);
    Table resolved = HiveTableUtil.resolveTableForScanPlanning(jobConf, TABLE_ID.toString());

    assertThat(resolved).isInstanceOf(RESTTable.class);
    assertThat(resolved.newScan()).isInstanceOf(RESTTableScan.class);

    verify(adapterForRESTServer, atLeastOnce())
        .execute(requestCaptor.capture(), any(), any(), any(), any());
    assertThat(
            requestCaptor.getAllValues().stream()
                .anyMatch(req -> req.path().contains("/tables/")))
        .as("Expected Hive split planning to reload the table from the REST catalog")
        .isTrue();
  }

  private Configuration buildUnitTableConf(Table table, boolean serverMode) {
    Configuration conf = new Configuration();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, UNIT_CATALOG_NAME);
    conf.set(InputFormatConfig.TABLE_IDENTIFIER, table.name());
    conf.set(InputFormatConfig.TABLE_LOCATION, warehouse.toString());
    conf.set(InputFormatConfig.CATALOG_NAME, UNIT_CATALOG_NAME);
    conf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey(UNIT_CATALOG_NAME, CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    if (serverMode) {
      RestCatalogScanPlanning.setScanPlanningMode(conf, UNIT_CATALOG_NAME, "server");
    }
    conf.set(
        InputFormatConfig.SERIALIZED_TABLE_PREFIX + table.name(),
        HiveTableUtil.serializeTable(table, conf, null, null));
    return conf;
  }

  private Table createTableWithData() {
    restCatalog.createNamespace(NS);
    Table table =
        restCatalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();
    table.newAppend().appendFile(FILE_A).commit();
    return table;
  }

  private Configuration sessionConf() {
    Configuration sessionConf = new Configuration();
    MetastoreConf.setVar(sessionConf, MetastoreConf.ConfVars.CATALOG_DEFAULT, REST_CATALOG_NAME);
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey(REST_CATALOG_NAME, CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    restCatalog.properties().forEach(
        (key, value) ->
            sessionConf.set(
                IcebergCatalogProperties.catalogPropertyConfigKey(REST_CATALOG_NAME, key), value));
    sessionConf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey(REST_CATALOG_NAME, CatalogProperties.URI),
        httpServer.getURI().toString());
    RestCatalogScanPlanning.setScanPlanningMode(sessionConf, REST_CATALOG_NAME, "server");
    return sessionConf;
  }

  private Configuration executorJobConf(Table table) throws IOException {
    Configuration jobConf = new Configuration();
    jobConf.set(InputFormatConfig.TABLE_IDENTIFIER, TABLE_ID.toString());
    jobConf.set(InputFormatConfig.CATALOG_NAME, REST_CATALOG_NAME);
    jobConf.set(
        InputFormatConfig.SERIALIZED_TABLE_PREFIX + TABLE_ID.toString(),
        HiveTableUtil.serializeTable(table, jobConf, null, null));
    return jobConf;
  }

  private static LoadTableResponse withPlanningMode(LoadTableResponse response, String mode) {
    return LoadTableResponse.builder()
        .withTableMetadata(response.tableMetadata())
        .addAllConfig(response.config())
        .addConfig(RESTCatalogProperties.SCAN_PLANNING_MODE, mode)
        .addAllCredentials(response.credentials())
        .build();
  }
}
