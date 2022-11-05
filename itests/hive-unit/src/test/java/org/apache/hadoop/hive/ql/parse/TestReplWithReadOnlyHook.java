package org.apache.hadoop.hive.ql.parse;

import static org.apache.hadoop.hive.ql.hooks.EnforceReadOnlyDatabaseHook.READONLY;
import static org.apache.hadoop.hive.common.repl.ReplConst.READ_ONLY_HOOK;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.shims.Utils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestReplWithReadOnlyHook extends BaseReplicationScenariosAcidTables {

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
      GzipJSONMessageEncoder.class.getCanonicalName());

    conf = new HiveConf(TestReplWithReadOnlyHook.class);
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");

    MiniDFSCluster miniDFSCluster =
      new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).build();

    Map<String, String> acidEnableConf = new HashMap<String, String>() {{
      put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
      put("hive.support.concurrency", "true");
      put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
      put("hive.metastore.client.capability.check", "false");
      put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
      put("hive.exec.dynamic.partition.mode", "nonstrict");
      put("hive.strict.checks.bucketing", "false");
      put("hive.mapred.mode", "nonstrict");
      put("mapred.input.dir.recursive", "true");
      put("hive.metastore.disallow.incompatible.col.type.changes", "false");
      put("metastore.warehouse.tenant.colocation", "true");
      put("hive.in.repl.test", "true");
      put("hive.txn.readonly.enabled", "true");
      put(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname, "false");
      put(HiveConf.ConfVars.REPL_RETAIN_CUSTOM_LOCATIONS_FOR_DB_ON_TARGET.varname, "false");
      put(HiveConf.ConfVars.PREEXECHOOKS.varname, READ_ONLY_HOOK);
    }};

    acidEnableConf.putAll(overrides);

    primary = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    acidEnableConf.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);

    replica = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    Map<String, String> overridesForHiveConf1 = new HashMap<String, String>() {{
      put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
      put("hive.support.concurrency", "false");
      put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
      put("hive.metastore.client.capability.check", "false");
    }};
    overridesForHiveConf1.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    replicaNonAcid = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf1);
  }

  @After
  public void afterTest() throws Throwable {
    replica.run("alter database " + replicatedDbName + " set dbproperties('readonly'='false')");
  }

  @Test
  public void testDbSetToReadOnlyAfterBootstrapLoad() throws Throwable {
    primary.run("use " + primaryDbName)
      .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
      .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    Map<String, String> dbParams = replica.getDatabase(replicatedDbName).getParameters();
    assertEquals("true", dbParams.get(READONLY));
  }

  @Test
  public void testDbReadOnlyModeBackWardCompatible() throws Throwable {
    //do bootstrap
    primary.run("use " + primaryDbName)
      .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
      .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);

    // let's toggle 'readonly' of db to simulate the use case
    replica.run("ALTER DATABASE " + replicatedDbName + " SET DBPROPERTIES('readonly'='false')");

    // perform incremental
    primary.run("insert into " + primaryDbName +".t1 values(\"AAA\")")
      .dump(primaryDbName);

    // before REPL LOAD ensure db parameters is not "readonly"
    String isDbReadOnly = replica.getDatabase(replicatedDbName).getParameters().get(READONLY);
    assertEquals(Boolean.FALSE.toString() ,isDbReadOnly);

    // perform REPL LOAD
    replica.load(replicatedDbName, primaryDbName);

    isDbReadOnly = replica.getDatabase(replicatedDbName).getParameters().get(READONLY);
    assertEquals(Boolean.TRUE.toString() ,isDbReadOnly);
  }
}
