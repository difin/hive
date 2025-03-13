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

package org.apache.iceberg;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveActor;
import org.apache.iceberg.hive.HiveCatalogFriend;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Whether a given user can read/write a given table.
 */
public interface TableAccess {
  Logger LOG = LoggerFactory.getLogger(TableAccess.class);

  default boolean isReadAllowed(TableIdentifier ident, String user) { return false; }
  default boolean isWriteAllowed(TableIdentifier ident, String user) { return false; }

  default boolean isReadAllowed(TableIdentifier ident) {
    try {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      return isReadAllowed(ident, ugi.getUserName());
    } catch (IOException e) {
      LOG.error("Unable to get UGI");
    }
    return false;
  }

  static boolean isReadAllowed(Catalog catalog, TableIdentifier ident) {
    HiveActor actor = HiveCatalogFriend.getActorOf(catalog);
    if (actor != null) {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using actor {}@{}", catalog.getClass().getSimpleName(), catalog.name());
        }
        final String dbName = ident.namespace().level(0);
        final String tableName = ident.name();
        return actor.isTableAccessible(dbName, tableName);
      } catch (TException | InterruptedException e) {
        LOG.warn("Table is not accessible {}", ident, e);
      }
    } else {
      LOG.warn("Could not retrieve actor for catalog {}@{}", catalog.getClass().getSimpleName(), catalog.name());
    }
    return false;
  }
}

