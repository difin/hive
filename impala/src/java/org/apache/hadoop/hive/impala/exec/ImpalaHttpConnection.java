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
package org.apache.hadoop.hive.impala.exec;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import org.apache.impala.thrift.ImpalaHiveServer2Service;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransportException;

import java.net.URI;
import java.net.URISyntaxException;

class ImpalaHttpConnection implements ImpalaConnection {
  private final URI url;
  private final THttpClient client;
  private final int timeout;

  ImpalaHttpConnection(String address, String path, int timeout) throws HiveException,
      TTransportException {
    String host_url = "http://" + address + "/" + path;
    try {
      this.url = new URI(host_url);
    } catch (URISyntaxException e) {
      throw new HiveException(e);
    }
    this.timeout = timeout;
    this.client = new THttpClient(url.toString());
    this.client.setConnectTimeout(timeout);
    this.client.setReadTimeout(timeout);
  }

  public void open() throws TException {
    client.open();
  }

  public void close() {
    client.close();
  }

  public String toString() {
    return "ImpalaHttpConnection{" + url + ", timeout: " + timeout + "}";
  }

  public ImpalaHiveServer2Service.Client getClient() throws HiveException {
    return new ImpalaHiveServer2Service.Client(new TBinaryProtocol(client));
  }
}
