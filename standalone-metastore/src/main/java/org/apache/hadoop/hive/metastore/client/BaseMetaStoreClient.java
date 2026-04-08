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

package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

/**
 * Minimal base for alternate {@link IMetaStoreClient} implementations (e.g. REST-backed catalog).
 * Abstract methods from {@link IMetaStoreClient} default to {@link UnsupportedOperationException}
 * or neutral primitives; subclasses override what they support.
 * 
 * TODO: Replace this workaround once HIVE-27473 (Rewrite MetaStoreClients to be composable) is backported
 */
public abstract class BaseMetaStoreClient implements IMetaStoreClient {

  protected final Configuration conf;

  protected BaseMetaStoreClient(Configuration conf) {
    this.conf = conf != null ? new Configuration(conf) : new Configuration();
  }

  @Override
  public boolean isCompatibleWith(org.apache.hadoop.conf.Configuration p0) {
    return false;
  }

  @Override
  public void setHiveAddedJars(java.lang.String p0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isLocalMetaStore() {
    return false;
  }

  @Override
  public void reconnect() throws org.apache.hadoop.hive.metastore.api.MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setMetaConf(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.lang.String getMetaConf(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.lang.String getHMSAPIVersion() throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void createCatalog(org.apache.hadoop.hive.metastore.api.Catalog p0) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterCatalog(java.lang.String p0, org.apache.hadoop.hive.metastore.api.Catalog p1) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Catalog getCatalog(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getCatalogs() throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void dropCatalog(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropCatalog(java.lang.String p0, boolean p1) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.util.List<java.lang.String> getDatabases(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getDatabases(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getAllDatabases() throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getAllDatabases(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getTables(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getTables(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getTables(java.lang.String p0, java.lang.String p1, org.apache.hadoop.hive.metastore.TableType p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getTables(java.lang.String p0, java.lang.String p1, java.lang.String p2, org.apache.hadoop.hive.metastore.TableType p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Table> getAllMaterializedViewObjectsForRewriting() throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.ExtendedTableInfo> getTablesExt(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3, int p4) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getMaterializedViewsForRewriting(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getMaterializedViewsForRewriting(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.TableMeta> getTableMeta(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.TableMeta> getTableMeta(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getAllTables(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getAllTables(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> listTableNamesByFilter(java.lang.String p0, java.lang.String p1, short p2) throws org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> listTableNamesByFilter(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3) throws org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public void dropTable(java.lang.String p0, java.lang.String p1, boolean p2, boolean p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropTable(java.lang.String p0, java.lang.String p1, boolean p2, boolean p3, boolean p4) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropTable(org.apache.hadoop.hive.metastore.api.Table p0, boolean p1, boolean p2, boolean p3) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropTable(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropTable(java.lang.String p0, java.lang.String p1, java.lang.String p2, boolean p3, boolean p4, boolean p5) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void truncateTable(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void truncateTable(org.apache.hadoop.hive.common.TableName p0, java.util.List<java.lang.String> p1) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void truncateTable(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, java.lang.String p3, long p4) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void truncateTable(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, java.lang.String p3, long p4, boolean p5) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void truncateTable(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.CmRecycleResponse recycleDirToCmPath(org.apache.hadoop.hive.metastore.api.CmRecycleRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public boolean tableExists(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return false;
  }

  @Override
  public boolean tableExists(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return false;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Database getDatabase(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Database getDatabase(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTable(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetTableResult getTable(org.apache.hadoop.hive.metastore.api.GetTableRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTable(java.lang.String p0, java.lang.String p1, boolean p2, java.lang.String p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTable(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTable(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTable(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3, boolean p4, java.lang.String p5, boolean p6) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Table> getTableObjectsByName(java.lang.String p0, java.util.List<java.lang.String> p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.UnknownDBException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Table> getTables(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, org.apache.hadoop.hive.metastore.api.GetProjectionsSpec p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.UnknownDBException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Table> getTableObjectsByName(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.UnknownDBException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Materialization getMaterializationInvalidationInfo(org.apache.hadoop.hive.metastore.api.CreationMetadata p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.UnknownDBException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Materialization getMaterializationInvalidationInfo(org.apache.hadoop.hive.metastore.api.CreationMetadata p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.UnknownDBException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void updateCreationMetadata(java.lang.String p0, java.lang.String p1, org.apache.hadoop.hive.metastore.api.CreationMetadata p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCreationMetadata(java.lang.String p0, java.lang.String p1, java.lang.String p2, org.apache.hadoop.hive.metastore.api.CreationMetadata p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition appendPartition(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition appendPartition(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition appendPartition(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition appendPartition(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition add_partition(org.apache.hadoop.hive.metastore.api.Partition p0) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public int add_partitions(java.util.List<org.apache.hadoop.hive.metastore.api.Partition> p0) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return 0;
  }

  @Override
  public int add_partitions_pspec(org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy p0) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return 0;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> add_partitions(java.util.List<org.apache.hadoop.hive.metastore.api.Partition> p0, boolean p1, boolean p2) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartition(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetPartitionResponse getPartitionRequest(org.apache.hadoop.hive.metastore.api.GetPartitionRequest p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartition(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition exchange_partition(java.util.Map<java.lang.String, java.lang.String> p0, java.lang.String p1, java.lang.String p2, java.lang.String p3, java.lang.String p4) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition exchange_partition(java.util.Map<java.lang.String, java.lang.String> p0, java.lang.String p1, java.lang.String p2, java.lang.String p3, java.lang.String p4, java.lang.String p5, java.lang.String p6) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> exchange_partitions(java.util.Map<java.lang.String, java.lang.String> p0, java.lang.String p1, java.lang.String p2, java.lang.String p3, java.lang.String p4) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> exchange_partitions(java.util.Map<java.lang.String, java.lang.String> p0, java.lang.String p1, java.lang.String p2, java.lang.String p3, java.lang.String p4, java.lang.String p5, java.lang.String p6) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartition(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartition(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuthInfo(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, java.lang.String p3, java.util.List<java.lang.String> p4) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuthInfo(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, java.lang.String p4, java.util.List<java.lang.String> p5) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(java.lang.String p0, java.lang.String p1, short p2) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy listPartitionSpecs(java.lang.String p0, java.lang.String p1, int p2) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy listPartitionSpecs(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, short p3) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, int p4) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> listPartitionNames(java.lang.String p0, java.lang.String p1, short p2) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse listPartitionNamesRequest(org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> listPartitionNames(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> listPartitionNames(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, short p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> listPartitionNames(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, int p4) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.PartitionValuesResponse listPartitionValues(org.apache.hadoop.hive.metastore.api.PartitionValuesRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    return null;
  }

  @Override
  public int getNumPartitionsByFilter(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return 0;
  }

  @Override
  public int getNumPartitionsByFilter(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return 0;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsByFilter(java.lang.String p0, java.lang.String p1, java.lang.String p2, short p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsByFilter(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3, int p4) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy listPartitionSpecsByFilter(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy listPartitionSpecsByFilter(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3, int p4) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public boolean listPartitionsByExpr(java.lang.String p0, java.lang.String p1, byte[] p2, java.lang.String p3, short p4, java.util.List<org.apache.hadoop.hive.metastore.api.Partition> p5) throws org.apache.thrift.TException {
    return false;
  }

  @Override
  public boolean listPartitionsByExpr(java.lang.String p0, java.lang.String p1, java.lang.String p2, byte[] p3, java.lang.String p4, int p5, java.util.List<org.apache.hadoop.hive.metastore.api.Partition> p6) throws org.apache.thrift.TException {
    return false;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(java.lang.String p0, java.lang.String p1, short p2, java.lang.String p3, java.util.List<java.lang.String> p4) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthResponse listPartitionsWithAuthInfoRequest(org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3, java.lang.String p4, java.util.List<java.lang.String> p5) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.PartitionsResponse getPartitionsRequest(org.apache.hadoop.hive.metastore.api.PartitionsRequest p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, boolean p3, java.lang.String p4) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, boolean p4, java.lang.String p5) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult getPartitionsByNames(org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, short p3, java.lang.String p4, java.util.List<java.lang.String> p5) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, int p4, java.lang.String p5, java.util.List<java.lang.String> p6) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException {
    return null;
  }

  @Override
  public void markPartitionForEvent(java.lang.String p0, java.lang.String p1, java.util.Map<java.lang.String, java.lang.String> p2, org.apache.hadoop.hive.metastore.api.PartitionEventType p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.UnknownDBException, org.apache.hadoop.hive.metastore.api.UnknownPartitionException, org.apache.hadoop.hive.metastore.api.InvalidPartitionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markPartitionForEvent(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.Map<java.lang.String, java.lang.String> p3, org.apache.hadoop.hive.metastore.api.PartitionEventType p4) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.UnknownDBException, org.apache.hadoop.hive.metastore.api.UnknownPartitionException, org.apache.hadoop.hive.metastore.api.InvalidPartitionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPartitionMarkedForEvent(java.lang.String p0, java.lang.String p1, java.util.Map<java.lang.String, java.lang.String> p2, org.apache.hadoop.hive.metastore.api.PartitionEventType p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.UnknownDBException, org.apache.hadoop.hive.metastore.api.UnknownPartitionException, org.apache.hadoop.hive.metastore.api.InvalidPartitionException {
    return false;
  }

  @Override
  public boolean isPartitionMarkedForEvent(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.Map<java.lang.String, java.lang.String> p3, org.apache.hadoop.hive.metastore.api.PartitionEventType p4) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.UnknownDBException, org.apache.hadoop.hive.metastore.api.UnknownPartitionException, org.apache.hadoop.hive.metastore.api.InvalidPartitionException {
    return false;
  }

  @Override
  public void validatePartitionNameCharacters(java.util.List<java.lang.String> p0) throws org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTranslateTableDryrun(org.apache.hadoop.hive.metastore.api.Table p0) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void createTable(org.apache.hadoop.hive.metastore.api.Table p0) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTable(org.apache.hadoop.hive.metastore.api.CreateTableRequest p0) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_table(java.lang.String p0, java.lang.String p1, org.apache.hadoop.hive.metastore.api.Table p2) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_table(java.lang.String p0, java.lang.String p1, java.lang.String p2, org.apache.hadoop.hive.metastore.api.Table p3, org.apache.hadoop.hive.metastore.api.EnvironmentContext p4) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_table(java.lang.String p0, java.lang.String p1, org.apache.hadoop.hive.metastore.api.Table p2, boolean p3) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_table_with_environmentContext(java.lang.String p0, java.lang.String p1, org.apache.hadoop.hive.metastore.api.Table p2, org.apache.hadoop.hive.metastore.api.EnvironmentContext p3) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_table(java.lang.String p0, java.lang.String p1, java.lang.String p2, org.apache.hadoop.hive.metastore.api.Table p3, org.apache.hadoop.hive.metastore.api.EnvironmentContext p4, java.lang.String p5) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createDatabase(org.apache.hadoop.hive.metastore.api.Database p0) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropDatabase(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropDatabase(java.lang.String p0, boolean p1, boolean p2) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropDatabase(java.lang.String p0, boolean p1, boolean p2, boolean p3) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropDatabase(org.apache.hadoop.hive.metastore.api.DropDatabaseRequest p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterDatabase(java.lang.String p0, org.apache.hadoop.hive.metastore.api.Database p1) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterDatabase(java.lang.String p0, java.lang.String p1, org.apache.hadoop.hive.metastore.api.Database p2) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createDataConnector(org.apache.hadoop.hive.metastore.api.DataConnector p0) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropDataConnector(java.lang.String p0, boolean p1, boolean p2) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterDataConnector(java.lang.String p0, org.apache.hadoop.hive.metastore.api.DataConnector p1) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.DataConnector getDataConnector(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getAllDataConnectorNames() throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public boolean dropPartition(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, boolean p3) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public boolean dropPartition(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, boolean p4) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public boolean dropPartition(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, org.apache.hadoop.hive.metastore.PartitionDropOptions p3) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public boolean dropPartition(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, org.apache.hadoop.hive.metastore.PartitionDropOptions p4) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(java.lang.String p0, java.lang.String p1, java.util.List<org.apache.commons.lang3.tuple.Pair<java.lang.Integer, byte[]>> p2, boolean p3, boolean p4) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(java.lang.String p0, java.lang.String p1, java.util.List<org.apache.commons.lang3.tuple.Pair<java.lang.Integer, byte[]>> p2, boolean p3, boolean p4, boolean p5) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(java.lang.String p0, java.lang.String p1, java.util.List<org.apache.commons.lang3.tuple.Pair<java.lang.Integer, byte[]>> p2, org.apache.hadoop.hive.metastore.PartitionDropOptions p3) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<org.apache.commons.lang3.tuple.Pair<java.lang.Integer, byte[]>> p3, org.apache.hadoop.hive.metastore.PartitionDropOptions p4) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public boolean dropPartition(java.lang.String p0, java.lang.String p1, java.lang.String p2, boolean p3) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public boolean dropPartition(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3, boolean p4) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public void alter_partition(java.lang.String p0, java.lang.String p1, org.apache.hadoop.hive.metastore.api.Partition p2) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_partition(java.lang.String p0, java.lang.String p1, org.apache.hadoop.hive.metastore.api.Partition p2, org.apache.hadoop.hive.metastore.api.EnvironmentContext p3) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_partition(java.lang.String p0, java.lang.String p1, java.lang.String p2, org.apache.hadoop.hive.metastore.api.Partition p3, org.apache.hadoop.hive.metastore.api.EnvironmentContext p4, java.lang.String p5) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_partition(java.lang.String p0, java.lang.String p1, java.lang.String p2, org.apache.hadoop.hive.metastore.api.Partition p3, org.apache.hadoop.hive.metastore.api.EnvironmentContext p4) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_partitions(java.lang.String p0, java.lang.String p1, java.util.List<org.apache.hadoop.hive.metastore.api.Partition> p2) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_partitions(java.lang.String p0, java.lang.String p1, java.util.List<org.apache.hadoop.hive.metastore.api.Partition> p2, org.apache.hadoop.hive.metastore.api.EnvironmentContext p3) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_partitions(java.lang.String p0, java.lang.String p1, java.util.List<org.apache.hadoop.hive.metastore.api.Partition> p2, org.apache.hadoop.hive.metastore.api.EnvironmentContext p3, java.lang.String p4, long p5) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_partitions(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<org.apache.hadoop.hive.metastore.api.Partition> p3, org.apache.hadoop.hive.metastore.api.EnvironmentContext p4, java.lang.String p5, long p6) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void renamePartition(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, org.apache.hadoop.hive.metastore.api.Partition p3) throws org.apache.hadoop.hive.metastore.api.InvalidOperationException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void renamePartition(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, org.apache.hadoop.hive.metastore.api.Partition p4, java.lang.String p5, long p6, boolean p7) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.FieldSchema> getFields(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.FieldSchema> getFields(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetFieldsResponse getFieldsRequest(org.apache.hadoop.hive.metastore.api.GetFieldsRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.FieldSchema> getSchema(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.FieldSchema> getSchema(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetSchemaResponse getSchemaRequest(org.apache.hadoop.hive.metastore.api.GetSchemaRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.UnknownTableException, org.apache.hadoop.hive.metastore.api.UnknownDBException {
    return null;
  }

  @Override
  public java.lang.String getConfigValue(java.lang.String p0, java.lang.String p1) throws org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.ConfigValSecurityException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> partitionNameToVals(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.Map<java.lang.String, java.lang.String> partitionNameToSpec(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public boolean updateTableColumnStatistics(org.apache.hadoop.hive.metastore.api.ColumnStatistics p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return false;
  }

  @Override
  public boolean updatePartitionColumnStatistics(org.apache.hadoop.hive.metastore.api.ColumnStatistics p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return false;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj> getTableColumnStatistics(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, java.lang.String p3) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    // Mutable empty list: Hive stats code (e.g. StatsUtils) may add to the returned list.
    return new java.util.ArrayList<>();
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj> getTableColumnStatistics(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, java.lang.String p3, java.lang.String p4) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return new java.util.ArrayList<>();
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj> getTableColumnStatistics(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, java.lang.String p4) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return new java.util.ArrayList<>();
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj> getTableColumnStatistics(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, java.lang.String p4, java.lang.String p5) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return new java.util.ArrayList<>();
  }

  @Override
  public java.util.Map<java.lang.String, java.util.List<org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj>> getPartitionColumnStatistics(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, java.util.List<java.lang.String> p3, java.lang.String p4) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.Map<java.lang.String, java.util.List<org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj>> getPartitionColumnStatistics(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, java.util.List<java.lang.String> p3, java.lang.String p4, java.lang.String p5) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.Map<java.lang.String, java.util.List<org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj>> getPartitionColumnStatistics(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, java.util.List<java.lang.String> p4, java.lang.String p5) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.Map<java.lang.String, java.util.List<org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj>> getPartitionColumnStatistics(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, java.util.List<java.lang.String> p4, java.lang.String p5, java.lang.String p6) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public boolean deleteColumnStatistics(org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest p0) throws org.apache.thrift.TException {
    return false;
  }

  @Override
  public void updateTransactionalStatistics(org.apache.hadoop.hive.metastore.api.UpdateTransactionalStatsRequest p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean create_role(org.apache.hadoop.hive.metastore.api.Role p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public boolean drop_role(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public java.util.List<java.lang.String> listRoleNames() throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public boolean grant_role(java.lang.String p0, java.lang.String p1, org.apache.hadoop.hive.metastore.api.PrincipalType p2, java.lang.String p3, org.apache.hadoop.hive.metastore.api.PrincipalType p4, boolean p5) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public boolean revoke_role(java.lang.String p0, java.lang.String p1, org.apache.hadoop.hive.metastore.api.PrincipalType p2, boolean p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.Role> list_roles(java.lang.String p0, org.apache.hadoop.hive.metastore.api.PrincipalType p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet get_privilege_set(org.apache.hadoop.hive.metastore.api.HiveObjectRef p0, java.lang.String p1, java.util.List<java.lang.String> p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege> list_privileges(java.lang.String p0, org.apache.hadoop.hive.metastore.api.PrincipalType p1, org.apache.hadoop.hive.metastore.api.HiveObjectRef p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public boolean grant_privileges(org.apache.hadoop.hive.metastore.api.PrivilegeBag p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public boolean revoke_privileges(org.apache.hadoop.hive.metastore.api.PrivilegeBag p0, boolean p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public boolean refresh_privileges(org.apache.hadoop.hive.metastore.api.HiveObjectRef p0, java.lang.String p1, org.apache.hadoop.hive.metastore.api.PrivilegeBag p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public java.lang.String getDelegationToken(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public long renewDelegationToken(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return 0L;
  }

  @Override
  public void cancelDelegationToken(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.lang.String getTokenStrForm() throws java.io.IOException {
    return null;
  }

  @Override
  public boolean addToken(java.lang.String p0, java.lang.String p1) throws org.apache.thrift.TException {
    return false;
  }

  @Override
  public boolean removeToken(java.lang.String p0) throws org.apache.thrift.TException {
    return false;
  }

  @Override
  public java.lang.String getToken(java.lang.String p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getAllTokenIdentifiers() throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public int addMasterKey(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return 0;
  }

  @Override
  public void updateMasterKey(java.lang.Integer p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeMasterKey(java.lang.Integer p0) throws org.apache.thrift.TException {
    return false;
  }

  @Override
  public java.lang.String[] getMasterKeys() throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void createFunction(org.apache.hadoop.hive.metastore.api.Function p0) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterFunction(java.lang.String p0, java.lang.String p1, org.apache.hadoop.hive.metastore.api.Function p2) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterFunction(java.lang.String p0, java.lang.String p1, java.lang.String p2, org.apache.hadoop.hive.metastore.api.Function p3) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropFunction(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.InvalidInputException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropFunction(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.InvalidInputException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Function getFunction(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Function getFunction(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getFunctions(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetFunctionsResponse getFunctionsRequest(org.apache.hadoop.hive.metastore.api.GetFunctionsRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> getFunctions(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse getAllFunctions() throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse getOpenTxns() throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.common.ValidTxnList getValidTxns() throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.common.ValidTxnList getValidTxns(long p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.common.ValidTxnList getValidTxns(long p0, java.util.List<org.apache.hadoop.hive.metastore.api.TxnType> p1) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.common.ValidWriteIdList getValidWriteIds(java.lang.String p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.common.ValidWriteIdList getValidWriteIds(java.lang.String p0, java.lang.Long p1) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.TableValidWriteIds> getValidWriteIds(java.util.List<java.lang.String> p0, java.lang.String p1) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void addWriteIdsToMinHistory(long p0, java.util.Map<java.lang.String, java.lang.Long> p1) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long openTxn(java.lang.String p0) throws org.apache.thrift.TException {
    return 0L;
  }

  @Override
  public long openTxn(java.lang.String p0, org.apache.hadoop.hive.metastore.api.TxnType p1) throws org.apache.thrift.TException {
    return 0L;
  }

  @Override
  public java.util.List<java.lang.Long> replOpenTxn(java.lang.String p0, java.util.List<java.lang.Long> p1, java.lang.String p2, org.apache.hadoop.hive.metastore.api.TxnType p3) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.OpenTxnsResponse openTxns(java.lang.String p0, int p1) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void rollbackTxn(long p0) throws org.apache.hadoop.hive.metastore.api.NoSuchTxnException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollbackTxn(org.apache.hadoop.hive.metastore.api.AbortTxnRequest p0) throws org.apache.hadoop.hive.metastore.api.NoSuchTxnException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replRollbackTxn(long p0, java.lang.String p1, org.apache.hadoop.hive.metastore.api.TxnType p2) throws org.apache.hadoop.hive.metastore.api.NoSuchTxnException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.ReplayedTxnsForPolicyResult getReplayedTxnsForPolicy(java.lang.String p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void commitTxn(long p0) throws org.apache.hadoop.hive.metastore.api.NoSuchTxnException, org.apache.hadoop.hive.metastore.api.TxnAbortedException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitTxnWithKeyValue(long p0, long p1, java.lang.String p2, java.lang.String p3) throws org.apache.hadoop.hive.metastore.api.NoSuchTxnException, org.apache.hadoop.hive.metastore.api.TxnAbortedException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitTxn(org.apache.hadoop.hive.metastore.api.CommitTxnRequest p0) throws org.apache.hadoop.hive.metastore.api.NoSuchTxnException, org.apache.hadoop.hive.metastore.api.TxnAbortedException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void abortTxns(java.util.List<java.lang.Long> p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void abortTxns(org.apache.hadoop.hive.metastore.api.AbortTxnsRequest p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long allocateTableWriteId(long p0, java.lang.String p1, java.lang.String p2) throws org.apache.thrift.TException {
    return 0L;
  }

  @Override
  public long allocateTableWriteId(long p0, java.lang.String p1, java.lang.String p2, boolean p3) throws org.apache.thrift.TException {
    return 0L;
  }

  @Override
  public void replTableWriteIdState(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.TxnToWriteId> allocateTableWriteIdsBatch(java.util.List<java.lang.Long> p0, java.lang.String p1, java.lang.String p2) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.TxnToWriteId> replAllocateTableWriteIdsBatch(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<org.apache.hadoop.hive.metastore.api.TxnToWriteId> p3) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public long getMaxAllocatedWriteId(java.lang.String p0, java.lang.String p1) throws org.apache.thrift.TException {
    return 0L;
  }

  @Override
  public void seedWriteId(java.lang.String p0, java.lang.String p1, long p2) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seedTxnId(long p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse showTxns() throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.LockResponse lock(org.apache.hadoop.hive.metastore.api.LockRequest p0) throws org.apache.hadoop.hive.metastore.api.NoSuchTxnException, org.apache.hadoop.hive.metastore.api.TxnAbortedException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.LockResponse checkLock(long p0) throws org.apache.hadoop.hive.metastore.api.NoSuchTxnException, org.apache.hadoop.hive.metastore.api.TxnAbortedException, org.apache.hadoop.hive.metastore.api.NoSuchLockException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void unlock(long p0) throws org.apache.hadoop.hive.metastore.api.NoSuchLockException, org.apache.hadoop.hive.metastore.api.TxnOpenException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.ShowLocksResponse showLocks() throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.ShowLocksResponse showLocks(org.apache.hadoop.hive.metastore.api.ShowLocksRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void heartbeat(long p0, long p1) throws org.apache.hadoop.hive.metastore.api.NoSuchLockException, org.apache.hadoop.hive.metastore.api.NoSuchTxnException, org.apache.hadoop.hive.metastore.api.TxnAbortedException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse heartbeatTxnRange(long p0, long p1) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void compact(java.lang.String p0, java.lang.String p1, java.lang.String p2, org.apache.hadoop.hive.metastore.api.CompactionType p3) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void compact(java.lang.String p0, java.lang.String p1, java.lang.String p2, org.apache.hadoop.hive.metastore.api.CompactionType p3, java.util.Map<java.lang.String, java.lang.String> p4) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.CompactionResponse compact2(java.lang.String p0, java.lang.String p1, java.lang.String p2, org.apache.hadoop.hive.metastore.api.CompactionType p3, java.util.Map<java.lang.String, java.lang.String> p4) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.CompactionResponse compact2(org.apache.hadoop.hive.metastore.api.CompactionRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.ShowCompactResponse showCompactions() throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.ShowCompactResponse showCompactions(org.apache.hadoop.hive.metastore.api.ShowCompactRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public boolean submitForCleanup(org.apache.hadoop.hive.metastore.api.CompactionRequest p0, long p1, long p2) throws org.apache.thrift.TException {
    return false;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse getLatestCommittedCompactionInfo(org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void addDynamicPartitions(long p0, long p1, java.lang.String p2, java.lang.String p3, java.util.List<java.lang.String> p4) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addDynamicPartitions(long p0, long p1, java.lang.String p2, java.lang.String p3, java.util.List<java.lang.String> p4, org.apache.hadoop.hive.metastore.api.DataOperationType p5) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void insertTable(org.apache.hadoop.hive.metastore.api.Table p0, boolean p1) throws org.apache.hadoop.hive.metastore.api.MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLatestTxnIdInConflict(long p0) throws org.apache.thrift.TException {
    return 0L;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetDatabaseObjectsResponse get_databases_req(org.apache.hadoop.hive.metastore.api.GetDatabaseObjectsRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.NotificationEventResponse getNextNotification(long p0, int p1, org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter p2) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.NotificationEventResponse getNextNotification(org.apache.hadoop.hive.metastore.api.NotificationEventRequest p0, boolean p1, org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter p2) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId getCurrentNotificationEventId() throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse getNotificationEventsCount(org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.FireEventResponse fireListenerEvent(org.apache.hadoop.hive.metastore.api.FireEventRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void addWriteNotificationLog(org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addWriteNotificationLogInBatch(org.apache.hadoop.hive.metastore.api.WriteNotificationLogBatchRequest p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse get_principals_in_role(org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.AggrStats getAggrColStatsFor(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, java.util.List<java.lang.String> p3, java.lang.String p4) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.AggrStats getAggrColStatsFor(java.lang.String p0, java.lang.String p1, java.util.List<java.lang.String> p2, java.util.List<java.lang.String> p3, java.lang.String p4, java.lang.String p5) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.AggrStats getAggrColStatsFor(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, java.util.List<java.lang.String> p4, java.lang.String p5) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.AggrStats getAggrColStatsFor(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.util.List<java.lang.String> p3, java.util.List<java.lang.String> p4, java.lang.String p5, java.lang.String p6) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public boolean setPartitionColumnStatistics(org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return false;
  }

  @Override
  public void flushCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.lang.Iterable<java.util.Map.Entry<java.lang.Long, java.nio.ByteBuffer>> getFileMetadata(java.util.List<java.lang.Long> p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.lang.Iterable<java.util.Map.Entry<java.lang.Long, org.apache.hadoop.hive.metastore.api.MetadataPpdResult>> getFileMetadataBySarg(java.util.List<java.lang.Long> p0, java.nio.ByteBuffer p1, boolean p2) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void clearFileMetadata(java.util.List<java.lang.Long> p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFileMetadata(java.util.List<java.lang.Long> p0, java.util.List<java.nio.ByteBuffer> p1) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSameConfObj(org.apache.hadoop.conf.Configuration p0) {
    return false;
  }

  @Override
  public boolean cacheFileMetadata(java.lang.String p0, java.lang.String p1, java.lang.String p2, boolean p3) throws org.apache.thrift.TException {
    return false;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.SQLPrimaryKey> getPrimaryKeys(org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.SQLForeignKey> getForeignKeys(org.apache.hadoop.hive.metastore.api.ForeignKeysRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint> getUniqueConstraints(org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint> getNotNullConstraints(org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint> getDefaultConstraints(org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.SQLCheckConstraint> getCheckConstraints(org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints getAllTableConstraints(
      org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest p0)
      throws org.apache.hadoop.hive.metastore.api.MetaException,
          org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    // Match Thrift HMS behavior for tables with no constraints; callers (e.g. Hive#getTableConstraints)
    // assume a non-null response. Subclasses may override.
    org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints empty =
        new org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints();
    empty.setPrimaryKeys(java.util.Collections.emptyList());
    empty.setForeignKeys(java.util.Collections.emptyList());
    empty.setUniqueConstraints(java.util.Collections.emptyList());
    empty.setNotNullConstraints(java.util.Collections.emptyList());
    empty.setDefaultConstraints(java.util.Collections.emptyList());
    empty.setCheckConstraints(java.util.Collections.emptyList());
    return empty;
  }

  @Override
  public void createTableWithConstraints(org.apache.hadoop.hive.metastore.api.Table p0, java.util.List<org.apache.hadoop.hive.metastore.api.SQLPrimaryKey> p1, java.util.List<org.apache.hadoop.hive.metastore.api.SQLForeignKey> p2, java.util.List<org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint> p3, java.util.List<org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint> p4, java.util.List<org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint> p5, java.util.List<org.apache.hadoop.hive.metastore.api.SQLCheckConstraint> p6) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropConstraint(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropConstraint(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addPrimaryKey(java.util.List<org.apache.hadoop.hive.metastore.api.SQLPrimaryKey> p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addForeignKey(java.util.List<org.apache.hadoop.hive.metastore.api.SQLForeignKey> p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addUniqueConstraint(java.util.List<org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint> p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addNotNullConstraint(java.util.List<org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint> p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addDefaultConstraint(java.util.List<org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint> p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addCheckConstraint(java.util.List<org.apache.hadoop.hive.metastore.api.SQLCheckConstraint> p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.lang.String getMetastoreDbUuid() throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void createResourcePlan(org.apache.hadoop.hive.metastore.api.WMResourcePlan p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.WMFullResourcePlan getResourcePlan(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.WMResourcePlan> getAllResourcePlans() throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void dropResourcePlan(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.WMFullResourcePlan alterResourcePlan(java.lang.String p0, org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan p1, boolean p2, boolean p3, boolean p4) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.WMFullResourcePlan getActiveResourcePlan() throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse validateResourcePlan(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void createWMTrigger(org.apache.hadoop.hive.metastore.api.WMTrigger p0) throws org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterWMTrigger(org.apache.hadoop.hive.metastore.api.WMTrigger p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropWMTrigger(java.lang.String p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.WMTrigger> getTriggersForResourcePlan(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void createWMPool(org.apache.hadoop.hive.metastore.api.WMPool p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterWMPool(org.apache.hadoop.hive.metastore.api.WMNullablePool p0, java.lang.String p1) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropWMPool(java.lang.String p0, java.lang.String p1) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createOrUpdateWMMapping(org.apache.hadoop.hive.metastore.api.WMMapping p0, boolean p1) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropWMMapping(org.apache.hadoop.hive.metastore.api.WMMapping p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createOrDropTriggerToPoolMapping(java.lang.String p0, java.lang.String p1, java.lang.String p2, boolean p3) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.InvalidObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createISchema(org.apache.hadoop.hive.metastore.api.ISchema p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterISchema(java.lang.String p0, java.lang.String p1, java.lang.String p2, org.apache.hadoop.hive.metastore.api.ISchema p3) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.ISchema getISchema(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void dropISchema(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addSchemaVersion(org.apache.hadoop.hive.metastore.api.SchemaVersion p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.SchemaVersion getSchemaVersion(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.SchemaVersion getSchemaLatestVersion(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.SchemaVersion> getSchemaAllVersions(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void dropSchemaVersion(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp getSchemaByCols(org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void mapSchemaVersionToSerde(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3, java.lang.String p4) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSchemaVersionState(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3, org.apache.hadoop.hive.metastore.api.SchemaVersionState p4) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addSerDe(org.apache.hadoop.hive.metastore.api.SerDeInfo p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.SerDeInfo getSerDe(java.lang.String p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.LockResponse lockMaterializationRebuild(java.lang.String p0, java.lang.String p1, long p2) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public boolean heartbeatLockMaterializationRebuild(java.lang.String p0, java.lang.String p1, long p2) throws org.apache.thrift.TException {
    return false;
  }

  @Override
  public void addRuntimeStat(org.apache.hadoop.hive.metastore.api.RuntimeStat p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.RuntimeStat> getRuntimeStats(int p0, int p1) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.OptionalCompactionInfoStruct findNextCompact(java.lang.String p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.OptionalCompactionInfoStruct findNextCompact(org.apache.hadoop.hive.metastore.api.FindNextCompactRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void updateCompactorState(org.apache.hadoop.hive.metastore.api.CompactionInfoStruct p0, long p1) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.util.List<java.lang.String> findColumnsWithStats(org.apache.hadoop.hive.metastore.api.CompactionInfoStruct p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void markCleaned(org.apache.hadoop.hive.metastore.api.CompactionInfoStruct p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markCompacted(org.apache.hadoop.hive.metastore.api.CompactionInfoStruct p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markFailed(org.apache.hadoop.hive.metastore.api.CompactionInfoStruct p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean updateCompactionMetricsData(org.apache.hadoop.hive.metastore.api.CompactionMetricsDataStruct p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return false;
  }

  @Override
  public void removeCompactionMetricsData(org.apache.hadoop.hive.metastore.api.CompactionMetricsDataRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markRefused(org.apache.hadoop.hive.metastore.api.CompactionInfoStruct p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHadoopJobid(java.lang.String p0, long p1) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.lang.String getServerVersion() throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.ScheduledQuery getScheduledQuery(org.apache.hadoop.hive.metastore.api.ScheduledQueryKey p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void scheduledQueryMaintenance(org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException, java.sql.SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse scheduledQueryPoll(org.apache.hadoop.hive.metastore.api.ScheduledQueryPollRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void scheduledQueryProgress(org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addReplicationMetrics(org.apache.hadoop.hive.metastore.api.ReplicationMetricList p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.ReplicationMetricList getReplicationMetrics(org.apache.hadoop.hive.metastore.api.GetReplicationMetricsRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetPartitionsResponse getPartitionsWithSpecs(org.apache.hadoop.hive.metastore.api.GetPartitionsRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void createStoredProcedure(org.apache.hadoop.hive.metastore.api.StoredProcedure p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.StoredProcedure getStoredProcedure(org.apache.hadoop.hive.metastore.api.StoredProcedureRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void dropStoredProcedure(org.apache.hadoop.hive.metastore.api.StoredProcedureRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.util.List<java.lang.String> getAllStoredProcedures(org.apache.hadoop.hive.metastore.api.ListStoredProcedureRequest p0) throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return null;
  }

  @Override
  public void addPackage(org.apache.hadoop.hive.metastore.api.AddPackageRequest p0) throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException, org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Package findPackage(org.apache.hadoop.hive.metastore.api.GetPackageRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public java.util.List<java.lang.String> listPackages(org.apache.hadoop.hive.metastore.api.ListPackageRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public void dropPackage(org.apache.hadoop.hive.metastore.api.DropPackageRequest p0) throws org.apache.thrift.TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.util.List<org.apache.hadoop.hive.metastore.api.WriteEventInfo> getAllWriteEventInfo(org.apache.hadoop.hive.metastore.api.GetAllWriteEventInfoRequest p0) throws org.apache.thrift.TException {
    return null;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Client getThriftClient() throws org.apache.hadoop.hive.metastore.api.MetaException {
    return null;
  }
}
