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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.cli;

import java.io.File;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.junit.Before;
import org.junit.Test;

/* Unit test for GitHub Howl issue #3 */
/**
 * TestUseDatabase.
 */
public class TestUseDatabase {

  private IDriver hcatDriver;

  @Before
  public void setUp() throws Exception {
    HiveConf hcatConf = new HiveConf(this.getClass());
    //TODO: HIVE-27998: hcatalog tests on Tez
    hcatConf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
    hcatConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hcatConf.set(ConfVars.PRE_EXEC_HOOKS.varname, "");
    hcatConf.set(ConfVars.POST_EXEC_HOOKS.varname, "");
    hcatConf.set(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    hcatConf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname, HCatSemanticAnalyzer.class.getName());
    hcatDriver = DriverFactory.newDriver(hcatConf);
    SessionState.start(new CliSessionState(hcatConf));
  }

  private final String dbName = "testUseDatabase_db";
  private final String tblName = "testUseDatabase_tbl";

  @Test
  public void testAlterTablePass() throws Exception {

    hcatDriver.run("create database " + dbName);
    hcatDriver.run("use " + dbName);
    hcatDriver.run("create table " + tblName + " (a int) partitioned by (b string) stored as RCFILE");

    String tmpDir = System.getProperty("test.tmp.dir");
    File dir = new File(tmpDir + "/hive-junit-" + System.nanoTime());
    hcatDriver.run("alter table " + tblName + " add partition (b='2') location '" + dir.toURI().getPath() + "'");

    hcatDriver.run("alter table " + tblName + " set fileformat " +
        "INPUTFORMAT  'org.apache.hadoop.hive.ql.io.RCFileInputFormat' " +
        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' " +
        "serde 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'" +
        "inputdriver 'mydriver'" +
        "outputdriver 'yourdriver'");

    hcatDriver.run("drop table " + tblName);
    hcatDriver.run("drop database " + dbName);
  }

}
