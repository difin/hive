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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.engine.EngineSession;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.impala.plan.ImpalaCompiledPlan;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.rpc.thrift.TCancelOperationReq;
import org.apache.hive.service.rpc.thrift.TCancelOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseOperationReq;
import org.apache.hive.service.rpc.thrift.TCloseOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TCloseSessionResp;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusReq;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusResp;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationState;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TGetBackendConfigReq;
import org.apache.impala.thrift.TGetBackendConfigResp;
import org.apache.impala.thrift.ImpalaHiveServer2Service;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExecutePlannedStatementReq;
import org.apache.impala.thrift.TPingImpalaHS2ServiceReq;
import org.apache.impala.thrift.TPingImpalaHS2ServiceResp;
import org.apache.impala.thrift.TGetExecutorMembershipReq;
import org.apache.impala.thrift.TGetExecutorMembershipResp;
import org.apache.impala.thrift.TInitQueryContextResp;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TUpdateExecutorMembershipRequest;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.Map;
import java.util.Random;

/**
 * Provides an interface for a user's Impala session.
 */
public class ImpalaSessionImpl implements EngineSession {
    private static final Logger LOG = LoggerFactory.getLogger(ImpalaSessionImpl.class);

    /* Connection to Impala coordinator */
    private ImpalaConnection connection;
    /* HS2 client to Impala coordinator */
    private ImpalaHiveServer2Service.Client client;
    /* Session handle. Only valid after successful open */
    private TSessionHandle sessionHandle;
    /* Query options */
    private Map<String,String> sessionConfig;
    /* Address of server this session was created for */
    private String address;
    /* Generates random numbers for RPC retry sleeps */
    private final Random randGen = new Random();
    /* Underlying configured TSocket timeout */
    private int connectionTimeout;
    /* Maximum timer error on the Impala server */
    private long impalaMaxTimerError;
    /* Fetch EOF status */
    private boolean fetchEOF = false;
    /* Shutdown request */
    private Boolean pendingCancel = new Boolean(false);
    /* Buffer size for socket stream */
    private int socketBufferSize;

    public ImpalaSessionImpl(HiveConf conf) { init(conf); }

    @Override
    public void init(HiveConf conf) {
      this.address = conf.getVar(HiveConf.ConfVars.HIVE_IMPALA_ADDRESS);
      this.connectionTimeout = conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_CONNECT_TIMEOUT);
      this.socketBufferSize = conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_SOCKET_BUFFER_SIZE);
    }

    /* Used to provide an interface for RPC calls consumed by retryRPC. */
    private interface RPCCall<T> {
        T execute(ImpalaHiveServer2Service.Client c) throws TException, HiveException;
    }

    /* Used to provide an interface for RPC calls consumed by retryRPC with number of
     * times the call has been retried provided to callback */
    private interface RPCCallWithRetry<T> {
        T execute(ImpalaHiveServer2Service.Client c) throws TException, HiveException;
    }

    /* Retries RPC calls that are still executing or fail due to TException. */
    private <T> T retryRPC(String rpcCallName, boolean canReOpenSession, RPCCallWithRetry<T> call) throws HiveException {
        T resp = null;

        try {
            resp = call.execute(client);
            return resp;
        } catch (Exception e) {
            if (canReOpenSession) {
                close(); // Close session, client, and connection
                openImpl(false);     // Reopen connection, client, and session
            } else if (e instanceof TTransportException) {
                disconnectClient();
                connectClient(); // Reopen connection and client
            }
        }

        try {
            resp = call.execute(client);
        } catch (Exception e) {
            throw new HiveException(e);
        }
        return resp;
    }

    /* Checks TStatus status code, returns false if status is success, true if status suggests retrying, or throws
     * an HiveException if an error is encountered.
     */
    private void checkThriftStatus(TStatus status) throws HiveException {
        String errmsg;
        switch (status.getStatusCode()) {
            case ERROR_STATUS:
                throw new HiveException(status.getErrorMessage());
            case INVALID_HANDLE_STATUS:
                errmsg = "Invalid handle for server " + connection;
                throw new HiveException(errmsg);
        }

    }

    /* Given a valid TOperationHandle attempts to retrieve rows from Impala. */
    @Override
    public TRowSet fetch(TOperationHandle opHandle, long fetchSize) throws HiveException {
        if(fetchEOF) {
            return null;
        }

        Preconditions.checkNotNull(opHandle);
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);
        Preconditions.checkArgument(fetchSize > 0);

        TFetchResultsReq req = new TFetchResultsReq();
        req.setOperationHandle(opHandle);
        req.setMaxRows(fetchSize);

        TFetchResultsResp resp = null;

        do {
            resp = retryRPC("FetchResults", false, (c) -> c.FetchResults(req));
            checkThriftStatus(resp.getStatus());
            // A query can return STILL_EXECUTING_STATUS when the fetch time
            // exceeds FETCH_ROWS_TIMEOUT_MS. This could be propagated to the
            // user in the future but for now just retry the operation with
            // the same timeout.
        } while (resp.getStatus().getStatusCode() == TStatusCode.STILL_EXECUTING_STATUS ||
            (resp.isHasMoreRows() && resp.getResults().getRowsSize() == 0));

        if(!resp.isHasMoreRows()) {
              fetchEOF = true;
        }

        return resp.getResults();
    }

    @Override
    public void notifyShutdown() {
      synchronized (pendingCancel) {
        pendingCancel = true;
      }
    }

    @Override
    public void closeOperation(TOperationHandle opHandle) throws HiveException {
        Preconditions.checkNotNull(opHandle);
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TCloseOperationReq req = new TCloseOperationReq();
        req.setOperationHandle(opHandle);
        TCloseOperationResp resp = retryRPC("CloseOperation", false,
                (c) -> {
                  TCloseOperationResp resp2 = c.CloseOperation(req);
                  checkThriftStatus(resp2.getStatus()); // Check errors on every iteration
                  return resp2;
                });
    }

    private TPingImpalaHS2ServiceResp PrepareForExecution() throws HiveException {
          TPingImpalaHS2ServiceReq req = new TPingImpalaHS2ServiceReq(sessionHandle);
          return retryRPC("PingImpalaHS2Service", true,
                (c) -> {
                  req.setSessionHandle(sessionHandle); // Set latest session handle since Ping retry may reopen session
                  long ping_send_ts = System.nanoTime();
                  TPingImpalaHS2ServiceResp resp = client.PingImpalaHS2Service(req);
                  checkThriftStatus(resp.getStatus()); // Check errors on every iteration
                  // Move the coordinator timestamp forward by half of the RPC latency
                  // to compensate for overlap between the frontend and backend timelines.
                  resp.timestamp += ((System.nanoTime() - ping_send_ts) / 2);
                  return resp; 
                });
    }

    /* Executes an Impala plan */
    public TOperationHandle executePlan(String sql, ImpalaCompiledPlan plan) throws HiveException {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TExecuteStatementReq statementRequest = new TExecuteStatementReq();
        statementRequest.setRunAsync(true);
        statementRequest.setStatement(sql);

// TODO: Parameters could be passed here instead of during session open to
// avoid the need to reconnect when parameters change. However, parameters
// would then not be set in the Impala session. Need further investigation to
// determine if session parameters are accessed elsewhere in the backend.
/*
        statementRequest.setConfOverlay(
             SessionState.get().getConf().subtree("impala").entrySet().stream()
             .filter(e -> !e.getKey().equals("core-site.overridden"))
             .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue)));
*/

        TExecutePlannedStatementReq req = new TExecutePlannedStatementReq();
        req.setStatementReq(statementRequest);
        req.setPlan(plan.getExecRequest());

        req.plan.setRemote_submit_time(PrepareForExecution().timestamp);
        // Don't retry the Execute itself in case the statment was DML or modified state
        TExecuteStatementResp resp;
        statementRequest.setSessionHandle(sessionHandle);
        try {
          plan.getTimeline().markEvent("Submit request");
          resp = client.ExecutePlannedStatement(req);
          checkThriftStatus(resp.getStatus());
        } catch (TException e) {
          throw new HiveException(e);
        }

        fetchEOF = false;
        TOperationHandle opHandle = resp.getOperationHandle();
        TGetOperationStatusReq statusReq = new TGetOperationStatusReq(opHandle);

        while (true) {
          synchronized (pendingCancel) {
            if (pendingCancel) {
              pendingCancel = false;
              TCancelOperationReq cancelReq = new TCancelOperationReq(opHandle);
              TCancelOperationResp cancelResp;
              try {
                  cancelResp = client.CancelOperation(cancelReq);
              } catch (TException e) {
                throw new HiveException(e);
              }
              checkThriftStatus(cancelResp.getStatus());
            }
          }
          TGetOperationStatusResp statusResp = retryRPC("GetOperationStatus", false,
                  (c) -> {
                    TGetOperationStatusResp resp2 = c.GetOperationStatus(statusReq);
                    checkThriftStatus(resp2.getStatus()); // Check errors on every iteration
                    return resp2;
                  });
          if (statusResp.getOperationState() == TOperationState.FINISHED_STATE) {
            break;
          } else if (statusResp.getOperationState() == TOperationState.ERROR_STATE ||
                    statusResp.getOperationState() == TOperationState.CANCELED_STATE) {
            String errMsg = statusResp.getErrorMessage();
            if (errMsg != null && !errMsg.isEmpty()) {
              throw new HiveException("Query was cancelled: " + errMsg);
            } else {
              throw new HiveException("Query was cancelled");
            }
          }
        }

        return opHandle;
    }

    /* Executes a query string */
    @Override
    public TOperationHandle execute(String sql, boolean runAsync) throws HiveException {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TExecuteStatementReq req = new TExecuteStatementReq();
        req.setRunAsync(runAsync);
        req.setStatement(sql);

        PrepareForExecution();
        // Don't retry the Execute itself in case the statement was DML or modified state
        TExecuteStatementResp resp;
        req.setSessionHandle(sessionHandle);
        try {
          resp = client.ExecuteStatement(req);
          checkThriftStatus(resp.getStatus());
        } catch (TException e) {
          throw new HiveException(e);
        }

        fetchEOF = false;
        return resp.getOperationHandle();
    }

    private void connectClient() throws HiveException {
        connection = new ImpalaConnection(socketBufferSize, address, connectionTimeout);
        client = connection.getClient();
    }
    /* Retrieve BackendConfig from impalad */
    public TBackendGflags getBackendConfig() throws HiveException {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TGetBackendConfigReq req = new TGetBackendConfigReq();
        req.setSessionHandle(sessionHandle);
        TGetBackendConfigResp resp;
        try {
            resp = client.GetBackendConfig(req);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        checkThriftStatus(resp.getStatus());
        return resp.getBackend_config();
    }

    /**
     * Retrieve the executor membership from impalad
     */
    public TUpdateExecutorMembershipRequest getExecutorMembership() throws HiveException {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TGetExecutorMembershipReq req = new TGetExecutorMembershipReq();
        TGetExecutorMembershipResp resp = retryRPC("GetExecutorMembership", true,
            (c) -> {
                req.setSessionHandle(sessionHandle); // Set the latest handle, since retry may reopen
                TGetExecutorMembershipResp respInternal = client.GetExecutorMembership(req);
                checkThriftStatus(respInternal.getStatus());
                return respInternal;
            });

        return resp.getExecutor_membership();
    }

    /**
     * Retrieve a new query context from Impala
     */
    public TQueryCtx getQueryContext() throws HiveException {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TInitQueryContextResp resp = retryRPC("InitQueryContext", true,
            (c) -> {
                TInitQueryContextResp respInternal = client.InitQueryContext();
                checkThriftStatus(respInternal.getStatus());
                return respInternal;
            });

        return resp.getQuery_ctx();
    }

    @Override
    public boolean isOpen() {
        return client != null;
    }

    /* Opens an Impala session */
    @Override
    public void open() throws HiveException {
      openImpl(true);
    }

    private void openImpl(boolean retryOpen) throws HiveException {
        // we've already called open
        if (client != null) {
            return;
        }

        connectClient();

        TOpenSessionReq req = new TOpenSessionReq();
        req.setUsername(SessionState.get().getUserName());

        // Copy Impala conf variables to Open request
        req.setConfiguration(
             SessionState.get().getConf().subtree("impala").entrySet().stream()
             .filter(e -> !e.getKey().equals("core-site.overridden"))
             .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue)));

        // CDPD-6958: Investigate columnar vs row oriented result sets from Impala
        // This is to force Impala to send back row oriented data (V6 and above returns columnar
        req.setClient_protocol(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5);

        TOpenSessionResp resp;
        if (retryOpen) {
          resp = retryRPC("OpenSession", true,
              (c) -> {
              TOpenSessionResp resp2 = c.OpenSession(req);
              checkThriftStatus(resp2.getStatus()); // Check errors on every iteration
              return resp2;
              });
        } else {
          try {
            resp = client.OpenSession(req);
            checkThriftStatus(resp.getStatus());
          } catch (TException e) {
            throw new HiveException(e);
          }
        }
        sessionHandle = resp.getSessionHandle();
        sessionConfig = resp.getConfiguration();
    }

    private void disconnectClient() {
        client = null;
        if (connection != null) {
          connection.close();
          connection = null;
        }
    }

    @Override
    public THandleIdentifier getSessionId() {
        return sessionHandle.getSessionId();
    }

    @Override
    public Map<String,String> getSessionConfig() {
        return sessionConfig;
    }

    /* Closes an Impala session */
    @Override
    public void close() {
        Preconditions.checkNotNull(client);
        if (sessionHandle != null) {

          TCloseSessionReq req = new TCloseSessionReq();
          req.setSessionHandle(sessionHandle);
          try {
              // we retry CloseSession to cleanup resources on the Impala side
              TCloseSessionResp resp = retryRPC("CloseSession", false,
                  (c) -> {
                    TCloseSessionResp resp2 = client.CloseSession(req);
                    checkThriftStatus(resp2.getStatus());
                    return resp2;
                  });
          } catch (Exception e) {
              // ignore TStatus error on close because there is nothing user actionable, but report it in log
              LOG.warn("Failed to close session ({}) to Impala coordinator ({})", getSessionId(), address,
                      e);
          }
        }
        if (connection != null) {
            disconnectClient();
        }
    }
}
