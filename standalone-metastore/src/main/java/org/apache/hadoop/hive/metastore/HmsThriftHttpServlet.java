/* * Licensed to the Apache Software Foundation (ASF) under one
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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;

import java.util.Optional;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.auth.jwt.JWTValidator;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.pac4j.core.context.JEEContext;
import org.pac4j.core.credentials.TokenCredentials;
import org.pac4j.core.credentials.extractor.BearerAuthExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
public class HmsThriftHttpServlet extends TServlet {
  private final ServletSecurity security;

  public HmsThriftHttpServlet(TProcessor processor,
                              TProtocolFactory protocolFactory, Configuration conf) {
    super(processor, protocolFactory);
    boolean jwt = MetastoreConf.getVar(conf,ConfVars.THRIFT_METASTORE_AUTHENTICATION).equalsIgnoreCase("jwt");
    security = new ServletSecurity(conf, jwt);
  }

  public void init() throws ServletException {
    super.init();
    security.init();
  }

  @Override
  protected void doPost(HttpServletRequest request,
                        HttpServletResponse response) throws ServletException, IOException {
    security.execute(request, response, (q,a)->super.doPost(q,a));
  }
}

