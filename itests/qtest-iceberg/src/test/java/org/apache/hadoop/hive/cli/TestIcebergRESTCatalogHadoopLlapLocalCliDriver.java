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

package org.apache.hadoop.hive.cli;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.control.CliAdapter;
import org.apache.hadoop.hive.cli.control.CliConfigs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.CatalogUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * LLAP CLI qtests with {@code HiveRESTCatalogClient}; REST catalog is
 * {@link org.apache.hive.iceberg.it.NativeIcebergRESTCatalogServer} (Jetty + {@code HadoopCatalog}).
 *
 * <p>Downstream uses this in place of upstream tests that run against an HMS-backed Iceberg REST
 * catalog ({@code hive-hms-catalog}).
 */
@RunWith(Parameterized.class)
public class TestIcebergRESTCatalogHadoopLlapLocalCliDriver {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestIcebergRESTCatalogHadoopLlapLocalCliDriver.class);
  private static final String CATALOG_NAME = "ice01";
  private static final CliAdapter CLI_ADAPTER =
      new CliConfigs.TestIcebergRESTCatalogHadoopLlapLocalCliDriver().getCliAdapter();

  private final String name;
  private final File qfile;

  public static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION =
      HiveRESTCatalogServerExtension.builder(HiveRESTCatalogServerExtension.AuthType.NONE).build();

  /** REST catalog must start before CLI setup so the endpoint is available when Hive connects. */
  @ClassRule
  public static final TestRule cliClassRule = RuleChain
      .outerRule(REST_CATALOG_EXTENSION)
      .around(CLI_ADAPTER.buildClassRule());

  @Rule
  public final TestRule cliTestRule = CLI_ADAPTER.buildTestRule();

  @Parameters(name = "{0}")
  public static List<Object[]> getParameters() throws Exception {
    return CLI_ADAPTER.getParameters();
  }

  public TestIcebergRESTCatalogHadoopLlapLocalCliDriver(String name, File qfile) {
    this.name = name;
    this.qfile = qfile;
  }

  @Before
  public void setupHiveConfig() {
    String restCatalogPrefix = String.format("%s%s.", CatalogUtils.CATALOG_CONFIG_PREFIX, CATALOG_NAME);

    Configuration conf = SessionState.get().getConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_IMPL,
        "org.apache.iceberg.hive.client.HiveRESTCatalogClient");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, CATALOG_NAME);
    conf.set(restCatalogPrefix + "uri", REST_CATALOG_EXTENSION.getRestEndpoint());
    conf.set(restCatalogPrefix + "type", CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    conf.set(restCatalogPrefix + "header.x-actor-username", System.getProperty("user.name", "anonymous"));
  }

  @Before
  public void cleanUpRestCatalogServerTmpDir() throws IOException {
    try (Stream<Path> children = Files.list(REST_CATALOG_EXTENSION.getRestCatalogServer().getWarehouseDir())) {
      children
          .filter(path -> !path.getFileName().toString().equals("derby.log"))
          .filter(path -> !path.getFileName().toString().equals("metastore_db"))
          .forEach(path -> {
            try {
              if (Files.isDirectory(path)) {
                FileUtils.deleteDirectory(path.toFile());
              } else {
                Files.delete(path);
              }
            } catch (IOException e) {
              LOG.error("Failed to delete path: {}", path, e);
            }
          });
    }
  }

  @Test
  public void testCliDriver() throws Exception {
    CLI_ADAPTER.runTest(name, qfile);
  }
}
