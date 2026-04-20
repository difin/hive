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

package org.apache.hadoop.hive.metastore;

import com.facebook.fb303.FacebookBase;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.AlterDataConnectorRequest;
import org.apache.hadoop.hive.metastore.api.CreateDataConnectorRequest;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.DropDataConnectorRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

/**
 * This class catches the API dropped in the upstream hive_metastore.thrift, so that the
 * old client can speak to the new server during the upgrade, and make the upgrade smooth.
 */
public abstract class DeprecatedThriftMetastore extends FacebookBase implements IHMSHandler {
  protected DeprecatedThriftMetastore(String name) {
    super(name);
  }

  @Override
  @Deprecated
  public final Table get_table(final String dbname, final String name) throws TException {
    GetTableRequest req = new GetTableRequest(dbname, name);
    return get_table_req(req).getTable();
  }

  @Override
  public final void create_dataconnector(DataConnector connector)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    create_dataconnector_req(new CreateDataConnectorRequest(connector));
  }

  @Override
  public final void drop_dataconnector(String name, boolean ifNotExists, boolean checkReferences)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    DropDataConnectorRequest req = new DropDataConnectorRequest(name);
    req.setCheckReferences(checkReferences);
    req.setIfNotExists(ifNotExists);
    drop_dataconnector_req(req);
  }

  @Override
  public final void alter_dataconnector(String name, DataConnector connector)
      throws MetaException, NoSuchObjectException, TException {
    AlterDataConnectorRequest req = new AlterDataConnectorRequest(name, connector);
    alter_dataconnector_req(req);
  }

  @Override
  public final List<Table> get_table_objects_by_name(String dbname, List<String> tbl_names) throws TException {
    GetTablesRequest req = new GetTablesRequest(dbname);
    req.setTblNames(tbl_names);
    return get_table_objects_by_name_req(req).getTables();
  }
}
