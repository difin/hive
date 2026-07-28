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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSServletSecurity;
import org.apache.hadoop.hive.metastore.RangerPrivilegeHelper;
import org.apache.hadoop.hive.metastore.RangerPrivilegeHelper.AccessLevel;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Class that wraps an Iceberg Catalog to cache tables + authz cache.
 */
public class HMSCachingCatalog extends CachingCatalog implements SupportsNamespaces, ViewCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(HMSCachingCatalog.class);

  private final HiveCatalog hiveCatalog;
  private final Configuration configuration;

  //Authz Cache
  private final Cache<TableIdentifier, ConcurrentMap<String, AccessLevel>> accessLevelCache;
  private final boolean caseSensitive;

  public HMSCachingCatalog(HiveCatalog catalog, long expirationMs) {
    this(catalog, expirationMs, /*cacheAuthorization*/ false, /*caseSensitive*/ true, null);
  }

  public HMSCachingCatalog(HiveCatalog catalog, long expirationMs, boolean cacheAuthorization) {
    this(catalog, expirationMs, cacheAuthorization, /*caseSensitive*/ true, null);
  }

  public HMSCachingCatalog(HiveCatalog catalog, long expirationMs, boolean cacheAuthorization, boolean caseSensitive) {
    this(catalog, expirationMs, cacheAuthorization, caseSensitive, null);
  }

  public HMSCachingCatalog(HiveCatalog catalog, long expirationMs, boolean cacheAuthorization, boolean caseSensitive, Configuration conf) {

    super(catalog, caseSensitive, expirationMs, Ticker.systemTicker());
    this.hiveCatalog = catalog;
    this.caseSensitive = caseSensitive;
    this.configuration = conf;

    if (cacheAuthorization) {
      this.accessLevelCache = Caffeine.newBuilder().expireAfterWrite(Duration.ofMillis(expirationMs)).ticker(Ticker.systemTicker()).build();
    } else {
      this.accessLevelCache = null;
    }
  }

  /**
   * Returns the underlying HiveCatalog.
   */
  public HiveCatalog unwrap() {
    return hiveCatalog;
  }

  private TableIdentifier canonicalize(TableIdentifier ident) {
    return caseSensitive ? ident : ident.toLowerCase();
  }

  /**
   * Gets the access level for a user on a table.
   *
   * @param ident the table identifier
   * @param user  the user name
   * @return the access level
   */
  public AccessLevel getAccessLevel(TableIdentifier ident, String user) {
    if (accessLevelCache == null) {
      return computeAccessLevel(ident, user);
    }

    TableIdentifier key = canonicalize(ident);
    ConcurrentMap<String, AccessLevel> perTable = accessLevelCache.get(key, k -> new ConcurrentHashMap<>());

    return perTable.computeIfAbsent(user, u -> computeAccessLevel(ident, u));
  }

  public boolean isReadAllowed(TableIdentifier ident, String user) {
    AccessLevel level = getAccessLevel(ident, user);
    return level == AccessLevel.READ_ONLY || level == AccessLevel.READ_WRITE;
  }

  public boolean isWriteAllowed(TableIdentifier ident, String user) {
    return getAccessLevel(ident, user) == AccessLevel.READ_WRITE;
  }

  public boolean isReadAllowed(TableIdentifier ident) {
    try {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      return isReadAllowed(ident, ugi.getUserName());
    } catch (IOException e) {
      LOG.error("Unable to get UGI");
    }
    return false;
  }

  private AccessLevel computeAccessLevel(TableIdentifier ident, String user) {
    if (configuration == null) {
      LOG.warn("No configuration available for access level computation on {}", ident);
      return AccessLevel.NONE;
    }
    try {
      RangerPrivilegeHelper helper = HMSServletSecurity.getPrivilegeHelper(configuration);
      if (helper != null && helper.isAvailable()) {
        String dbName = ident.namespace().level(0);
        String tableName = ident.name();
        return helper.getAccessLevel(dbName, tableName, user);
      } else {
        LOG.warn("RangerPrivilegeHelper not available for access check on {}", ident);
      }
    } catch (Exception e) {
      LOG.warn("Access level check failed for {}", ident, e);
    }
    return AccessLevel.NONE;
  }


  @Override
  public void invalidateTable(TableIdentifier ident) {
    super.invalidateTable(ident);
    if (accessLevelCache != null) {
      accessLevelCache.invalidate(canonicalize(ident));
    }
  }

  @Override
  public boolean tableExists(TableIdentifier identifier) {
    return hiveCatalog.tableExists(identifier);
  }

  @Override
  public Catalog.TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    // TODO: check upstream
    return super.buildTable(identifier, schema);
  }

  @Override
  public void createNamespace(Namespace nmspc, Map<String, String> map) {
    hiveCatalog.createNamespace(nmspc, map);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace nmspc) throws NoSuchNamespaceException {
    return hiveCatalog.listNamespaces(nmspc);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace nmspc) throws NoSuchNamespaceException {
    return hiveCatalog.loadNamespaceMetadata(nmspc);
  }

  @Override
  public boolean dropNamespace(Namespace nmspc) throws NamespaceNotEmptyException {
    List<TableIdentifier> tables = listTables(nmspc);
    for (TableIdentifier ident : tables) {
      invalidateTable(ident);
    }
    return hiveCatalog.dropNamespace(nmspc);
  }

  @Override
  public boolean setProperties(Namespace nmspc, Map<String, String> map) throws NoSuchNamespaceException {
    return hiveCatalog.setProperties(nmspc, map);
  }

  @Override
  public boolean removeProperties(Namespace nmspc, Set<String> set) throws NoSuchNamespaceException {
    return hiveCatalog.removeProperties(nmspc, set);
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return hiveCatalog.namespaceExists(namespace);
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    return hiveCatalog.listViews(namespace);
  }

  @Override
  public View loadView(TableIdentifier identifier) {
    return hiveCatalog.loadView(identifier);
  }

  @Override
  public boolean viewExists(TableIdentifier identifier) {
    return hiveCatalog.viewExists(identifier);
  }

  @Override
  public ViewBuilder buildView(TableIdentifier identifier) {
    return hiveCatalog.buildView(identifier);
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    return hiveCatalog.dropView(identifier);
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    hiveCatalog.renameView(from, to);
  }

  @Override
  public void invalidateView(TableIdentifier identifier) {
    hiveCatalog.invalidateView(identifier);
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    hiveCatalog.initialize(name, properties);
  }
}
