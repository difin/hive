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

/**
 * Minimal base for alternate {@link IMetaStoreClient} implementations (e.g. REST-backed catalog).
 * Abstract methods from {@link IMetaStoreClient} default to {@link UnsupportedOperationException}
 * or neutral primitives; subclasses override what they support.
 * 
 * TODO: Replace this stub with the implementation from HIVE-27473 (composable MetaStoreClients) once it is backported
 */
public abstract class BaseMetaStoreClient implements IMetaStoreClient {

  protected final Configuration conf;

  protected BaseMetaStoreClient(Configuration conf) {
    this.conf = conf != null ? new Configuration(conf) : new Configuration();
  }
}
