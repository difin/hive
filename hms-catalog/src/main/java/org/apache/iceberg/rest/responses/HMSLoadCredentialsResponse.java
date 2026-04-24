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

package org.apache.iceberg.rest.responses;

import org.apache.iceberg.rest.credentials.Credential;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Response for the LOAD_CREDENTIALS endpoint.
 * Returns a list of credentials for accessing table storage.
 */
public class HMSLoadCredentialsResponse implements LoadCredentialsResponse {
  private final List<Credential> credentials;

  private HMSLoadCredentialsResponse(List<Credential> credentials) {
    this.credentials = Collections.unmodifiableList(new ArrayList<>(credentials));
  }

  public List<Credential> credentials() {
    return credentials;
  }

  @Override
  public void validate() {
    // no required fields
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final List<Credential> credentials = new ArrayList<>();

    public Builder addCredential(Credential credential) {
      this.credentials.add(credential);
      return this;
    }

    public HMSLoadCredentialsResponse build() {
      return new HMSLoadCredentialsResponse(credentials);
    }
  }
}
