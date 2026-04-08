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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies {@link HiveMetaHook} around table creation for alternate {@link IMetaStoreClient}
 * implementations (such as {@code HiveRESTCatalogClient}) that do not embed hook logic.
 *
 * <p>Upstream Hive does this via {@code HiveMetaStoreClientBuilder.withHooks} wrapping the
 * delegate in {@code HookEnabledMetaStoreClient} (see apache/hive {@code metastore-client}).
 * This tree uses a JDK proxy with the same {@code createTable} / {@code createTableWithConstraints}
 * contract as {@link org.apache.hadoop.hive.metastore.HiveMetaStoreClient#createTable}.
 *
 * <p>Delegate calls use {@link Method#invoke}; checked exceptions from the delegate are wrapped in
 * {@link InvocationTargetException}. Those must be unwrapped and rethrown so callers (e.g.
 * {@code Hive#getTable}) receive {@code NoSuchObjectException} directly, matching non-proxy
 * {@link org.apache.hadoop.hive.metastore.HiveMetaStoreClient} behavior and avoiding
 * {@link java.lang.reflect.UndeclaredThrowableException} from the proxy layer.
 * 
 * TODO: Replace this workaround once HIVE-27473 (Rewrite MetaStoreClients to be composable) is backported
 */
public final class HookEnabledMetaStoreClient {

  private static final Logger LOG = LoggerFactory.getLogger(HookEnabledMetaStoreClient.class);

  /**
   * Iceberg's {@code HiveIcebergMetaHook} runs {@code Catalogs.createTable} in {@code commitCreateTable}
   * for Thrift HMS; skipping the delegate avoids duplicate creates when the delegate is a
   * {@link BaseMetaStoreClient}. {@code BaseHiveIcebergMetaHook} (used for non-Hive session catalogs)
   * leaves {@code commitCreateTable} empty and must still call the delegate — detect the full hook
   * class reflectively so {@code standalone-metastore} does not depend on Iceberg at compile time
   * and {@link HiveMetaHook} stays aligned with upstream.
   */
  private static final String HIVE_ICEBERG_META_HOOK_CLASS =
      "org.apache.iceberg.mr.hive.HiveIcebergMetaHook";

  private static boolean isHiveIcebergMetaHook(HiveMetaHook hook) {
    if (hook == null) {
      return false;
    }
    try {
      Class<?> c = Class.forName(HIVE_ICEBERG_META_HOOK_CLASS);
      return c.isInstance(hook);
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  private HookEnabledMetaStoreClient() {
  }

  public static IMetaStoreClient newClient(Configuration conf, @Nullable HiveMetaHookLoader hookLoader,
      IMetaStoreClient delegate) {
    Objects.requireNonNull(conf, "conf");
    Objects.requireNonNull(delegate, "delegate");
    if (hookLoader == null) {
      return delegate;
    }
    return (IMetaStoreClient) Proxy.newProxyInstance(
        IMetaStoreClient.class.getClassLoader(),
        new Class<?>[] {IMetaStoreClient.class},
        new CreateTableHookHandler(conf, hookLoader, delegate));
  }

  /**
   * After {@code JavaUtils.newInstance(msClientClass, ...)}, wraps {@code delegate} with hooks when
   * constructor args match {@link org.apache.hadoop.hive.metastore.RetryingMetaStoreClient}'s
   * {@code (Configuration, HiveMetaHookLoader, ...)} pattern and the delegate is a
   * {@link BaseMetaStoreClient} (alternate implementation). Leaves {@code HiveMetaStoreClient} and
   * other delegates unchanged.
   */
  public static IMetaStoreClient wrapAfterMsClientConstruction(IMetaStoreClient delegate,
      Object[] constructorArgs) {
    Objects.requireNonNull(delegate, "delegate");
    if (constructorArgs == null || constructorArgs.length < 2
        || !(constructorArgs[0] instanceof Configuration)
        || !(constructorArgs[1] instanceof HiveMetaHookLoader)) {
      return delegate;
    }
    HiveMetaHookLoader hl = (HiveMetaHookLoader) constructorArgs[1];
    Configuration cfg = (Configuration) constructorArgs[0];
    if (hl == null || !(delegate instanceof BaseMetaStoreClient)) {
      return delegate;
    }
    return newClient(cfg, hl, delegate);
  }

  private static final class CreateTableHookHandler implements InvocationHandler {
    private final Configuration conf;
    private final HiveMetaHookLoader hookLoader;
    private final IMetaStoreClient delegate;

    CreateTableHookHandler(Configuration conf, HiveMetaHookLoader hookLoader, IMetaStoreClient delegate) {
      this.conf = conf;
      this.hookLoader = hookLoader;
      this.delegate = delegate;
    }

    private HiveMetaHook getHook(Table tbl) throws MetaException {
      return hookLoader.getHook(tbl);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (method.getDeclaringClass() == Object.class) {
        if ("equals".equals(method.getName())) {
          return proxy == args[0];
        }
        if ("hashCode".equals(method.getName())) {
          return System.identityHashCode(proxy);
        }
        if ("toString".equals(method.getName())) {
          return "HookEnabledMetaStoreClient(" + delegate + ")";
        }
      }

      if ("createTable".equals(method.getName()) && args != null && args.length == 1) {
        if (args[0] instanceof CreateTableRequest) {
          invokeCreateTable((CreateTableRequest) args[0]);
          return null;
        }
        if (args[0] instanceof Table) {
          // Same hook path as createTable(CreateTableRequest); callers often use the Table overload.
          invokeCreateTable(new CreateTableRequest((Table) args[0]));
          return null;
        }
      }

      if ("createTableWithConstraints".equals(method.getName()) && args != null && args.length == 7) {
        @SuppressWarnings("unchecked")
        List<SQLPrimaryKey> pk = (List<SQLPrimaryKey>) args[1];
        @SuppressWarnings("unchecked")
        List<SQLForeignKey> fk = (List<SQLForeignKey>) args[2];
        @SuppressWarnings("unchecked")
        List<SQLUniqueConstraint> uq = (List<SQLUniqueConstraint>) args[3];
        @SuppressWarnings("unchecked")
        List<SQLNotNullConstraint> nn = (List<SQLNotNullConstraint>) args[4];
        @SuppressWarnings("unchecked")
        List<SQLDefaultConstraint> def = (List<SQLDefaultConstraint>) args[5];
        @SuppressWarnings("unchecked")
        List<SQLCheckConstraint> chk = (List<SQLCheckConstraint>) args[6];
        invokeCreateTableWithConstraints((Table) args[0], pk, fk, uq, nn, def, chk);
        return null;
      }

      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException e) {
        Throwable cause = e.getCause();
        if (cause != null) {
          throw cause;
        }
        throw e;
      }
    }

    private void invokeCreateTable(CreateTableRequest request) throws Throwable {
      Table tbl = request.getTable();
      HiveMetaHook hook = getHook(tbl);
      if (hook != null) {
        hook.preCreateTable(request);
      }
      boolean success = false;
      try {
        if (hook == null || !hook.createHMSTableInHook()) {
          // Only HiveIcebergMetaHook (not BaseHiveIcebergMetaHook) creates the Iceberg table in
          // commitCreateTable for embedded clients; see isHiveIcebergMetaHook javadoc.
          boolean skipDelegatePhysicalCreate =
              delegate instanceof BaseMetaStoreClient && isHiveIcebergMetaHook(hook);
          if (!skipDelegatePhysicalCreate) {
            delegate.createTable(request);
          }
        }
        if (hook != null) {
          hook.commitCreateTable(tbl);
        }
        success = true;
      } finally {
        if (!success && hook != null) {
          try {
            hook.rollbackCreateTable(tbl);
          } catch (Exception e) {
            LOG.error("Create rollback failed with", e);
          }
        }
      }
    }

    private void invokeCreateTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys,
        List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints,
        List<SQLNotNullConstraint> notNullConstraints, List<SQLDefaultConstraint> defaultConstraints,
        List<SQLCheckConstraint> checkConstraints) throws Throwable {

      CreateTableRequest createTableRequest = new CreateTableRequest(tbl);

      if (!tbl.isSetCatName()) {
        String defaultCat = MetaStoreUtils.getDefaultCatalog(conf);
        tbl.setCatName(defaultCat);
        if (primaryKeys != null) {
          primaryKeys.forEach(pk -> pk.setCatName(defaultCat));
        }
        if (foreignKeys != null) {
          foreignKeys.forEach(fk -> fk.setCatName(defaultCat));
        }
        if (uniqueConstraints != null) {
          uniqueConstraints.forEach(uc -> uc.setCatName(defaultCat));
          createTableRequest.setUniqueConstraints(uniqueConstraints);
        }
        if (notNullConstraints != null) {
          notNullConstraints.forEach(nn -> nn.setCatName(defaultCat));
        }
        if (defaultConstraints != null) {
          defaultConstraints.forEach(def -> def.setCatName(defaultCat));
        }
        if (checkConstraints != null) {
          checkConstraints.forEach(cc -> cc.setCatName(defaultCat));
        }
      }

      if (primaryKeys != null) {
        createTableRequest.setPrimaryKeys(primaryKeys);
      }
      if (foreignKeys != null) {
        createTableRequest.setForeignKeys(foreignKeys);
      }
      if (uniqueConstraints != null) {
        createTableRequest.setUniqueConstraints(uniqueConstraints);
      }
      if (notNullConstraints != null) {
        createTableRequest.setNotNullConstraints(notNullConstraints);
      }
      if (defaultConstraints != null) {
        createTableRequest.setDefaultConstraints(defaultConstraints);
      }
      if (checkConstraints != null) {
        createTableRequest.setCheckConstraints(checkConstraints);
      }

      invokeCreateTable(createTableRequest);
    }
  }
}
