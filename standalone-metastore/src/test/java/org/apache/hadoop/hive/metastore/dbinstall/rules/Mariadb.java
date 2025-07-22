package org.apache.hadoop.hive.metastore.dbinstall.rules;

import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

public class Mariadb extends DatabaseRule {
  private final MariaDBContainer<?> container = new MariaDBContainer<>(
    DockerImageName.parse(EXTERNAL_DB_DOCKER_PREFIX + "mariadb:10.2").asCompatibleSubstituteFor("mariadb"));

  public void before() throws IOException, InterruptedException {
    container.start();
  }

  @Override
  public void after() {
    container.stop();
  }

  @Override
  public String getDbType() {
    return "mysql";
  }

  @Override
  public String getDbRootUser() {
    return "root";
  }

  @Override
  public String getDbRootPassword() {
    return container.getPassword();
  }

  @Override
  public String getJdbcDriver() {
    return container.getDriverClassName();
  }

  @Override
  public String getJdbcUrl() {
    return container.withDatabaseName(HIVE_DB).getJdbcUrl();
  }

  @Override
  public String getInitialJdbcUrl() {
    return container.withDatabaseName("").getJdbcUrl();
  }

  @Override
  public String getHivePassword() {
    return HIVE_PASSWORD;
  }
}
