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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSServletSecurity;
import org.apache.hadoop.hive.metastore.RangerPrivilegeHelper;
import org.apache.hadoop.hive.metastore.RangerPrivilegeHelper.AccessLevel;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.metering.CatalogMeteringEventPublisher;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * Original @ <a href="https://github.com/apache/iceberg/blob/1.6.x/core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java">...</a>
 * Adaptor class to translate REST requests into {@link Catalog} API calls.
 */
public class HMSCatalogAdapter implements RESTClient {
  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalogAdapter.class);
  private static final Splitter SLASH = Splitter.on('/');
  private static final AtomicInteger REQUEST_ID_COUNTER = new AtomicInteger(0);

  private static final String VENDED_CREDENTIALS_ENABLED = "vended-credentials-enabled";
  private static final String ICEBERG_ACCESS_DELEGATION_HEADER = "X-Iceberg-Access-Delegation";
  private static final String ICEBERG_ACCESS_DELEGATION_VENDED_CREDENTIALS = "vended-credentials";
  private static final String ADLS_REFRESH_CREDENTIALS_ENDPOINT = "adls.refresh-credentials-endpoint";
  private static final String ADLS_REFRESH_CREDENTIALS_ENABLED = "adls.refresh-credentials-enabled";
  private static final String LOAD_CREDENTIALS_PATH_TEMPLATE = "v1/namespaces/%s/tables/%s/credentials";

  private static final Map<Class<? extends Exception>, Integer> EXCEPTION_ERROR_CODES =
      ImmutableMap.<Class<? extends Exception>, Integer>builder()
          .put(IllegalArgumentException.class, 400)
          .put(ValidationException.class, 400)
          .put(NamespaceNotEmptyException.class, 409)
          .put(NotAuthorizedException.class, 401)
          .put(ForbiddenException.class, 403)
          .put(NoSuchNamespaceException.class, 404)
          .put(NoSuchTableException.class, 404)
          .put(NoSuchIcebergTableException.class, 404)
          .put(UnsupportedOperationException.class, 406)
          .put(AlreadyExistsException.class, 409)
          .put(CommitFailedException.class, 409)
          .put(UnprocessableEntityException.class, 422)
          .put(CommitStateUnknownException.class, 500)
          .buildOrThrow();

  private static final String VIEWS_PATH = "v1/namespaces/{namespace}/views/{name}";
  private static final String TABLES_PATH = "v1/namespaces/{namespace}/tables/{table}";

  private final Catalog catalog;
  private final SupportsNamespaces asNamespaceCatalog;
  private final ViewCatalog asViewCatalog;

  private final CatalogMeteringEventPublisher catalogMeteringEventPublisher;

  private final Configuration configuration;

  public HMSCatalogAdapter(Catalog catalog, Configuration configuration) {
    this.catalog = catalog;
    this.configuration = configuration;
    this.catalogMeteringEventPublisher = new CatalogMeteringEventPublisher(configuration);
    this.asNamespaceCatalog =
        catalog instanceof SupportsNamespaces ? (SupportsNamespaces) catalog : null;
    this.asViewCatalog = catalog instanceof ViewCatalog ? (ViewCatalog) catalog : null;
  }

  enum HTTPMethod {
    GET,
    HEAD,
    POST,
    DELETE
  }

  enum Route {
    TOKENS(HTTPMethod.POST, "v1/oauth/tokens",
        null, OAuthTokenResponse.class),
    CONFIG(HTTPMethod.GET, "v1/config",
        null, ConfigResponse.class),
    LIST_NAMESPACES(HTTPMethod.GET, "v1/namespaces",
        null, ListNamespacesResponse.class),
    CREATE_NAMESPACE(HTTPMethod.POST, "v1/namespaces",
        CreateNamespaceRequest.class, CreateNamespaceResponse.class),
    LOAD_NAMESPACE(HTTPMethod.GET, "v1/namespaces/{namespace}",
        null, GetNamespaceResponse.class),
    NAMESPACE_EXISTS(HTTPMethod.HEAD, "v1/namespaces/{namespace}"),
    DROP_NAMESPACE(HTTPMethod.DELETE, "v1/namespaces/{namespace}"),
    UPDATE_NAMESPACE(HTTPMethod.POST, "v1/namespaces/{namespace}/properties",
        UpdateNamespacePropertiesRequest.class, UpdateNamespacePropertiesResponse.class),
    LIST_TABLES(HTTPMethod.GET, "v1/namespaces/{namespace}/tables",
        null, ListTablesResponse.class),
    CREATE_TABLE(HTTPMethod.POST, "v1/namespaces/{namespace}/tables",
        CreateTableRequest.class, LoadTableResponse.class),
    LOAD_TABLE(HTTPMethod.GET, TABLES_PATH,
        null, LoadTableResponse.class),
    TABLE_EXISTS(HTTPMethod.HEAD, TABLES_PATH),
    REGISTER_TABLE(HTTPMethod.POST, "v1/namespaces/{namespace}/register",
        RegisterTableRequest.class, LoadTableResponse.class),
    UPDATE_TABLE(HTTPMethod.POST, TABLES_PATH,
        UpdateTableRequest.class, LoadTableResponse.class),
    DROP_TABLE(HTTPMethod.DELETE, TABLES_PATH),
    LOAD_CREDENTIALS(HTTPMethod.GET, "v1/namespaces/{namespace}/tables/{table}/credentials",
        null, LoadCredentialsResponse.class),
    RENAME_TABLE(HTTPMethod.POST, "v1/tables/rename",
        RenameTableRequest.class, null),
    REPORT_METRICS(HTTPMethod.POST, "v1/namespaces/{namespace}/tables/{table}/metrics",
        ReportMetricsRequest.class, null),
    COMMIT_TRANSACTION(HTTPMethod.POST, "v1/transactions/commit",
        CommitTransactionRequest.class, null),
    LIST_VIEWS(HTTPMethod.GET, "v1/namespaces/{namespace}/views",
        null, ListTablesResponse.class),
    LOAD_VIEW(HTTPMethod.GET, VIEWS_PATH,
        null, LoadViewResponse.class),
    VIEW_EXISTS(HTTPMethod.HEAD, VIEWS_PATH),
    CREATE_VIEW(HTTPMethod.POST, "v1/namespaces/{namespace}/views",
        CreateViewRequest.class, LoadViewResponse.class),
    UPDATE_VIEW(HTTPMethod.POST, VIEWS_PATH,
        UpdateTableRequest.class, LoadViewResponse.class),
    RENAME_VIEW(HTTPMethod.POST, "v1/views/rename",
        RenameTableRequest.class, null),
    DROP_VIEW(HTTPMethod.DELETE, VIEWS_PATH);

    private final HTTPMethod method;
    private final int requiredLength;
    private final Map<Integer, String> requirements;
    private final Map<Integer, String> variables;
    private final Class<? extends RESTRequest> requestClass;
    private final Class<? extends RESTResponse> responseClass;

    /**
     * An exception safe way of getting a route by name.
     *
     * @param name the route name
     * @return the route instance or null if it could not be found
     */
    static Route byName(String name) {
      try {
        return valueOf(name.toUpperCase());
      } catch (IllegalArgumentException xill) {
        return null;
      }
    }

    Route(HTTPMethod method, String pattern) {
      this(method, pattern, null, null);
    }

    Route(
        HTTPMethod method,
        String pattern,
        Class<? extends RESTRequest> requestClass,
        Class<? extends RESTResponse> responseClass) {
      this.method = method;

      // parse the pattern into requirements and variables
      List<String> parts = SLASH.splitToList(pattern);
      ImmutableMap.Builder<Integer, String> requirementsBuilder = ImmutableMap.builder();
      ImmutableMap.Builder<Integer, String> variablesBuilder = ImmutableMap.builder();
      for (int pos = 0; pos < parts.size(); pos += 1) {
        String part = parts.get(pos);
        if (part.startsWith("{") && part.endsWith("}")) {
          variablesBuilder.put(pos, part.substring(1, part.length() - 1));
        } else {
          requirementsBuilder.put(pos, part);
        }
      }

      this.requestClass = requestClass;
      this.responseClass = responseClass;
      this.requiredLength = parts.size();
      this.requirements = requirementsBuilder.build();
      this.variables = variablesBuilder.build();
    }

    private boolean matches(HTTPMethod requestMethod, List<String> requestPath) {
      return method == requestMethod &&
          requiredLength == requestPath.size() &&
          requirements.entrySet().stream()
              .allMatch(
                  requirement ->
                      requirement
                          .getValue()
                          .equalsIgnoreCase(requestPath.get(requirement.getKey())));
    }

    private Map<String, String> variables(List<String> requestPath) {
      ImmutableMap.Builder<String, String> vars = ImmutableMap.builder();
      variables.forEach((key, value) -> vars.put(value, requestPath.get(key)));
      return vars.build();
    }

    public static Pair<Route, Map<String, String>> from(HTTPMethod method, String path) {
      List<String> parts = SLASH.splitToList(path);
      for (Route candidate : Route.values()) {
        if (candidate.matches(method, parts)) {
          return Pair.of(candidate, candidate.variables(parts));
        }
      }

      return null;
    }

    public Class<? extends RESTRequest> requestClass() {
      return requestClass;
    }

    public Class<? extends RESTResponse> responseClass() {
      return responseClass;
    }
  }


  private ConfigResponse config() {
    return castResponse(ConfigResponse.class, ConfigResponse.builder()
        .withOverride(VENDED_CREDENTIALS_ENABLED, "true")
        .build());
  }

  /**
   * Whether the given table is an Ozone table and in-line rewriting to s3a:// is disabled.
   * In that case S3 vended credentials do not apply and must not be vended. This check is
   * per-table so that  s3a:// tables in a mixed catalog are unaffected.
   */
  private boolean isServedAsNativeOfs(String tableLocation) {
    if (!OzonePathUtils.isOfsScheme(tableLocation)) {
      return false;
    }
    OzoneLinkBucketManager linkMgr = OzoneLinkBucketManager.getInstance(configuration);
    return linkMgr != null && !linkMgr.isOzoneInlineRewriteEnabled();
  }

  private ListNamespacesResponse listNamespaces(Map<String, String> vars) {
    if (asNamespaceCatalog != null) {
      Namespace namespace;
      if (vars.containsKey("parent")) {
        namespace = Namespace.of(RESTUtil.NAMESPACE_SPLITTER.splitToStream(vars.get("parent")).toArray(String[]::new));
      } else {
        namespace = Namespace.empty();
      }
      return castResponse(ListNamespacesResponse.class, CatalogHandlers.listNamespaces(asNamespaceCatalog, namespace));
    }
    throw new NamespaceNotSupported(catalog.toString());
  }

  private CreateNamespaceResponse createNamespace(Object body) {
    if (asNamespaceCatalog != null) {
      CreateNamespaceRequest request = castRequest(CreateNamespaceRequest.class, body);
      return castResponse(
          CreateNamespaceResponse.class, CatalogHandlers.createNamespace(asNamespaceCatalog, request));
    }
    throw new NamespaceNotSupported(catalog.toString());
  }

  private GetNamespaceResponse loadNamespace(Map<String, String> vars) {
    if (asNamespaceCatalog != null) {
      Namespace namespace = namespaceFromPathVars(vars);
      return castResponse(
          GetNamespaceResponse.class, CatalogHandlers.loadNamespace(asNamespaceCatalog, namespace));
    }
    throw new NamespaceNotSupported(catalog.toString());
  }

  private RESTResponse namespaceExists(Map<String, String> vars) {
    if (asNamespaceCatalog != null) {
      Namespace namespace = namespaceFromPathVars(vars);
      if (!asNamespaceCatalog.namespaceExists(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
      return null;
    }
    throw new NamespaceNotSupported(catalog.toString());
  }

  private RESTResponse dropNamespace(Map<String, String> vars) {
    if (asNamespaceCatalog != null) {
      CatalogHandlers.dropNamespace(asNamespaceCatalog, namespaceFromPathVars(vars));
      return null;
    }
    throw new NamespaceNotSupported(catalog.toString());
  }

  private UpdateNamespacePropertiesResponse updateNamespace(Map<String, String> vars, Object body) {
    if (asNamespaceCatalog != null) {
      Namespace namespace = namespaceFromPathVars(vars);
      UpdateNamespacePropertiesRequest request =
          castRequest(UpdateNamespacePropertiesRequest.class, body);
      return castResponse(
          UpdateNamespacePropertiesResponse.class,
          CatalogHandlers.updateNamespaceProperties(asNamespaceCatalog, namespace, request));
    }
    throw new NamespaceNotSupported(catalog.toString());
  }

  private ListTablesResponse listTables(Map<String, String> vars) {
    Namespace namespace = namespaceFromPathVars(vars);
    return castResponse(ListTablesResponse.class, CatalogHandlers.listTables(catalog, namespace));
  }

  private LoadTableResponse createTable(Map<String, String> vars, Object body) {
    final Class<LoadTableResponse> responseType = LoadTableResponse.class;
    Namespace namespace = namespaceFromPathVars(vars);
    CreateTableRequest request = castRequest(CreateTableRequest.class, body);
    request.validate();
    requireNamespaceWriteAccess(namespace);
    if (request.stageCreate()) {
      return castResponse(
          responseType, CatalogHandlers.stageTableCreate(catalog, namespace, request));
    } else {
      return castResponse(
          responseType, CatalogHandlers.createTable(catalog, namespace, request));
    }
  }

  private RESTResponse dropTable(Map<String, String> vars) {
    TableIdentifier ident = identFromPathVars(vars);
    requireWriteAccess(ident);
    if (PropertyUtil.propertyAsBoolean(vars, "purgeRequested", false)) {
      CatalogHandlers.purgeTable(catalog, ident);
    } else {
      CatalogHandlers.dropTable(catalog, ident);
    }
    catalog.invalidateTable(ident);
    return null;
  }

  private LoadTableResponse loadTable(Map<String, String> vars) {
    TableIdentifier ident = identFromPathVars(vars);
    return castResponse(LoadTableResponse.class, CatalogHandlers.loadTable(catalog, ident));
  }

  private RESTResponse tableExists(Map<String, String> vars) {
    TableIdentifier ident = identFromPathVars(vars);
    AccessLevel accessLevel = getAccessLevel(catalog, ident, configuration);
    if (accessLevel == AccessLevel.NONE) {
      throw new NoSuchTableException("Table is not accessible %s", ident.toString());
    }
    if (!Boolean.TRUE.equals(runAsHive(() -> catalog.tableExists(ident)))) {
      throw new NoSuchTableException("Table does not exist: %s", ident);
    }
    return null;
  }

  private LoadTableResponse registerTable(Map<String, String> vars, Object body) {
    Namespace namespace = namespaceFromPathVars(vars);
    RegisterTableRequest request = castRequest(RegisterTableRequest.class, body);
    return castResponse(LoadTableResponse.class, CatalogHandlers.registerTable(catalog, namespace, request));
  }

  private LoadTableResponse updateTable(Map<String, String> vars, Object body) {
    TableIdentifier ident = identFromPathVars(vars);
    requireWriteAccess(ident);
    UpdateTableRequest request = castRequest(UpdateTableRequest.class, body);
    LoadTableResponse response = castResponse(LoadTableResponse.class,
        runAsHive(() -> CatalogHandlers.updateTable(catalog, ident, request)));
    catalog.invalidateTable(ident);
    return response;
  }

  private RESTResponse renameTable(Object body) {
    RenameTableRequest request = castRequest(RenameTableRequest.class, body);
    TableIdentifier from = request.source();
    TableIdentifier to = request.destination();
    requireWriteAccess(from);
    requireWriteAccess(to);
    CatalogHandlers.renameTable(catalog, request);
    catalog.invalidateTable(from);
    catalog.invalidateTable(to);
    return null;
  }

  private RESTResponse reportMetrics(Object body) {
    // nothing to do here other than checking that we're getting the correct request
    castRequest(ReportMetricsRequest.class, body);
    return null;
  }

  private RESTResponse commitTransaction(Object body) {
    CommitTransactionRequest request = castRequest(CommitTransactionRequest.class, body);
    // Enforce write access for every table involved before attempting any commit.
    for (UpdateTableRequest tableChange : request.tableChanges()) {
      requireWriteAccess(tableChange.identifier());
    }
    commitTransaction(catalog, request);
    // Invalidate cached metadata for each committed table.
    for (UpdateTableRequest tableChange : request.tableChanges()) {
      catalog.invalidateTable(tableChange.identifier());
    }
    return null;
  }

  private ListTablesResponse listViews(Map<String, String> vars) {
    if (null != asViewCatalog) {
      Namespace namespace = namespaceFromPathVars(vars);
      // TODO: Add pagination support when Iceberg is upgraded to 1.7.0+
      // String pageToken = PropertyUtil.propertyAsString(vars, "pageToken", null);
      // String pageSize = PropertyUtil.propertyAsString(vars, "pageSize", null);
      return castResponse(
          ListTablesResponse.class,
          CatalogHandlers.listViews(asViewCatalog, namespace));
    }
    throw new ViewNotSupported(catalog.toString());
  }

  private LoadViewResponse createView(Map<String, String> vars, Object body) {
    if (null != asViewCatalog) {
      Namespace namespace = namespaceFromPathVars(vars);
      CreateViewRequest request = castRequest(CreateViewRequest.class, body);
      return castResponse(
          LoadViewResponse.class, CatalogHandlers.createView(asViewCatalog, namespace, request));
    }
    throw new ViewNotSupported(catalog.toString());
  }

  private LoadViewResponse loadView(Map<String, String> vars) {
    if (null != asViewCatalog) {
      TableIdentifier ident = identFromPathVars(vars);
      return castResponse(LoadViewResponse.class, CatalogHandlers.loadView(asViewCatalog, ident));
    }
    throw new ViewNotSupported(catalog.toString());
  }

  private RESTResponse viewExists(Map<String, String> vars) {
    if (null != asViewCatalog) {
      TableIdentifier ident = viewIdentFromPathVars(vars);
      if (!asViewCatalog.viewExists(ident)) {
        throw new NoSuchTableException("View does not exist: %s", ident);
      }
      return null;
    }
    throw new ViewNotSupported(catalog.toString());
  }

  private LoadViewResponse updateView(Map<String, String> vars, Object body) {
    if (null != asViewCatalog) {
      TableIdentifier ident = identFromPathVars(vars);
      UpdateTableRequest request = castRequest(UpdateTableRequest.class, body);
      return castResponse(
          LoadViewResponse.class, CatalogHandlers.updateView(asViewCatalog, ident, request));
    }
    throw new ViewNotSupported(catalog.toString());
  }

  private RESTResponse renameView(Object body) {
    if (null != asViewCatalog) {
      RenameTableRequest request = castRequest(RenameTableRequest.class, body);
      CatalogHandlers.renameView(asViewCatalog, request);
      return null;
    }
    throw new ViewNotSupported(catalog.toString());
  }

  private RESTResponse dropView(Map<String, String> vars) {
    if (null != asViewCatalog) {
      CatalogHandlers.dropView(asViewCatalog, identFromPathVars(vars));
      return null;
    }
    throw new ViewNotSupported(catalog.toString());
  }

  /**
   * This is a very simplistic approach that only validates the requirements for each table and does
   * not do any other conflict detection. Therefore, it does not guarantee true transactional
   * atomicity, which is left to the implementation details of a REST server.
   */
  private static void commitTransaction(Catalog catalog, CommitTransactionRequest request) {
    List<Transaction> transactions = Lists.newArrayList();

    for (UpdateTableRequest tableChange : request.tableChanges()) {
      Table table = runAsHive(() -> catalog.loadTable(tableChange.identifier()));
      if (table instanceof BaseTable) {
        Transaction transaction =
            Transactions.newTransaction(
                tableChange.identifier().toString(), ((BaseTable) table).operations());
        transactions.add(transaction);

        BaseTransaction.TransactionTable txTable =
            (BaseTransaction.TransactionTable) transaction.table();

        // this performs validations and makes temporary commits that are in-memory
        CatalogHandlers.commit(txTable.operations(), tableChange);
      } else {
        throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
      }
    }
    // only commit if validations passed previously
    runAsHive(() -> { transactions.forEach(Transaction::commitTransaction); return null; });
  }

  @SuppressWarnings({"MethodLength", "unchecked"})
  private <T extends RESTResponse> T handleRequest(
      Route route, Map<String, String> vars, Object body, String requestId, Map<String, String> headers) {
    catalogMeteringEventPublisher.updateAPICount();
    switch (route) {
      case TOKENS: {
        LOG.debug("API_OPERATION [{}] Processing OAuth token request", requestId);
        @SuppressWarnings("unchecked")
        Map<String, String> request = (Map<String, String>) castRequest(Map.class, body);
        String grantType = request.get("grant_type");
        switch (grantType) {
          case "client_credentials":
            return (T) OAuthTokenResponse.builder()
                .withToken("client-credentials-token:sub=" + request.get("client_id"))
                .withIssuedTokenType("urn:ietf:params:oauth:token-type:access_token")
                .withTokenType("Bearer")
                .build();

          case "urn:ietf:params:oauth:grant-type:token-exchange":
            String actor = request.get("actor_token");
            String token =
                String.format(
                    "token-exchange-token:sub=%s%s",
                    request.get("subject_token"), actor != null ? ",act=" + actor : "");
            return (T) OAuthTokenResponse.builder()
                .withToken(token)
                .withIssuedTokenType("urn:ietf:params:oauth:token-type:access_token")
                .withTokenType("Bearer")
                .build();

          default:
            throw new UnsupportedOperationException("Unsupported grant_type: " + grantType);
        }
      }

      case CONFIG:
        LOG.debug("API_OPERATION [{}] Retrieving catalog configuration", requestId);
        return (T) config();

      case LIST_NAMESPACES:
        LOG.debug("API_OPERATION [{}] Listing namespaces", requestId);
        return (T) listNamespaces(vars);

      case CREATE_NAMESPACE:
        LOG.debug("API_OPERATION [{}] Creating namespace", requestId);
        return (T) createNamespace(body);

      case LOAD_NAMESPACE:
        LOG.debug("API_OPERATION [{}] Loading namespace", requestId);
        return (T) loadNamespace(vars);

      case NAMESPACE_EXISTS:
        LOG.debug("API_OPERATION [{}] Checking namespace exists", requestId);
        return (T) namespaceExists(vars);

      case DROP_NAMESPACE:
        LOG.debug("API_OPERATION [{}] Dropping namespace", requestId);
        return (T) dropNamespace(vars);

      case UPDATE_NAMESPACE:
        LOG.debug("API_OPERATION [{}] Updating namespace properties", requestId);
        return (T) updateNamespace(vars, body);

      case LIST_TABLES:
        LOG.debug("API_OPERATION [{}] Listing tables", requestId);
        return (T) listTables(vars);

      case CREATE_TABLE:
        LOG.debug("API_OPERATION [{}] Creating table", requestId);
        return (T) createTable(vars, body);

      case DROP_TABLE:
        LOG.debug("API_OPERATION [{}] Dropping table", requestId);
        return (T) dropTable(vars);

      case LOAD_TABLE: {
        TableIdentifier ident = identFromPathVars(vars);
        LOG.debug("API_OPERATION [{}] Loading table: {}", requestId, ident);
        return (T) loadTable(catalog, ident, configuration, headers);
      }

      case TABLE_EXISTS:
        LOG.debug("API_OPERATION [{}] Checking table exists", requestId);
        return (T) tableExists(vars);

      case LOAD_CREDENTIALS: {
        TableIdentifier credIdent = identFromPathVars(vars);
        LOG.debug("API_OPERATION [{}] Loading credentials for table: {}", requestId, credIdent);
        return (T) loadCredentials(credIdent);
      }


      case REGISTER_TABLE:
        LOG.debug("API_OPERATION [{}] Registering table", requestId);
        return (T) registerTable(vars, body);

      case UPDATE_TABLE:
        LOG.debug("API_OPERATION [{}] Updating table", requestId);
        return (T) updateTable(vars, body);

      case RENAME_TABLE:
        LOG.debug("API_OPERATION [{}] Renaming table", requestId);
        return (T) renameTable(body);

      case REPORT_METRICS:
        LOG.debug("API_OPERATION [{}] Reporting metrics", requestId);
        return (T) reportMetrics(body);

      case COMMIT_TRANSACTION:
        LOG.debug("API_OPERATION [{}] Committing transaction", requestId);
        return (T) commitTransaction(body);

      case LIST_VIEWS:
        LOG.debug("API_OPERATION [{}] Listing views", requestId);
        return (T) listViews(vars);

      case CREATE_VIEW:
        LOG.debug("API_OPERATION [{}] Creating view", requestId);
        return (T) createView(vars, body);

      case LOAD_VIEW:
        LOG.debug("API_OPERATION [{}] Loading view", requestId);
        return (T) loadView(vars);

      case VIEW_EXISTS:
        LOG.debug("API_OPERATION [{}] Checking view exists", requestId);
        return (T) viewExists(vars);

      case UPDATE_VIEW:
        LOG.debug("API_OPERATION [{}] Updating view", requestId);
        return (T) updateView(vars, body);

      case RENAME_VIEW:
        LOG.debug("API_OPERATION [{}] Renaming view", requestId);
        return (T) renameView(vars);

      case DROP_VIEW:
        LOG.debug("API_OPERATION [{}] Dropping view", requestId);
        return (T) dropView(vars);

      default:
        LOG.error("API_UNHANDLED_ROUTE [{}] Route matched but no handler found: {}", requestId, route);
    }
    return null;
  }

  /**
   * Runs a function as Hive.
   *
   * @param func the function
   * @param <T>  the function return type
   * @return the function result
   * @throws NotAuthorizedException if authorization fails during the execution
   */
  public static <T> T runAsHive(java.util.function.Supplier<T> func) {
    try {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      PrivilegedExceptionAction<T> action = () -> {
        try {
          return func.get();
        } catch (NotAuthorizedException e) {
          LOG.error("Not authorized to perform action as hive", e);
          throw e;
        }
      };
      return ugi.doAs(action);
    } catch (IOException e) {
      Throwable cause = e.getCause();
      if (cause instanceof NotAuthorizedException) {
        throw (NotAuthorizedException) cause;
      }
      LOG.error("unable to perform action as hive", e);
      return null;
    } catch (InterruptedException e) {
      LOG.error("interrupted action as hive", e);
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * Gets the access level for the current user on a table, using RangerPrivilegeHelper
   * directly or via the HMSCachingCatalog cache.
   *
   * @param catalog the catalog
   * @param ident   the table identifier
   * @param conf    the configuration
   * @return the access level
   */
  private static AccessLevel getAccessLevel(Catalog catalog, TableIdentifier ident, Configuration conf) {
    try {
      String userName = UserGroupInformation.getCurrentUser().getUserName();
      if (catalog instanceof HMSCachingCatalog) {
        AccessLevel level = ((HMSCachingCatalog) catalog).getAccessLevel(ident, userName);
        LOG.info(
            "ACCESS_CHECK source=cache user={} table={} access={}",
            userName,
            ident,
            level);
        return level;
      }
      // Fallback: use RangerPrivilegeHelper directly
      RangerPrivilegeHelper helper = HMSServletSecurity.getPrivilegeHelper(conf);
      if (helper != null && helper.isAvailable()) {
        String dbName = ident.namespace().level(0);
        String tableName = ident.name();
        AccessLevel level = helper.getAccessLevel(dbName, tableName, userName);
        LOG.info(
            "ACCESS_CHECK source=ranger user={} db={} table={} access={}",
            userName,
            dbName,
            tableName,
            level);
        return level;
      }
      LOG.warn(
          "ACCESS_CHECK source=none user={} table={} reason=helper-unavailable returning={}",
          userName,
          ident,
          AccessLevel.NONE);
    } catch (IOException e) {
      LOG.error("Unable to get current user for access check on {}", ident, e);
    }
    LOG.warn(
        "ACCESS_CHECK source=none user=unknown table={} reason=exception-or-missing-context returning={}",
        ident,
        AccessLevel.NONE);
    return AccessLevel.NONE;
  }

  /**
   * Throws ForbiddenException when the current user does not have written (READ_WRITE)
   * access on the given table according to Ranger.
   */
  private void requireWriteAccess(TableIdentifier ident) {
    AccessLevel level = getAccessLevel(catalog, ident, configuration);
    if (level != AccessLevel.READ_WRITE) {
      try {
        String user = UserGroupInformation.getCurrentUser().getUserName();
        LOG.warn("Write access denied for user {} on table {}", user, ident);
      } catch (IOException ignored) {
        // best-effort logging only
      }
      throw new ForbiddenException("Write access denied on table %s", ident);
    }
  }

  /**
   * Throws ForbiddenException when the current user does not have write access
   * on any table in the given namespace (used for DDL operations like CREATE TABLE).
   *
   * The check uses a synthetic table identifier <namespace>.* to let Ranger
   * evaluate namespace-level policies. If no such policy exists, the check falls back to
   * requiring at least READ_WRITE on the namespace itself.
   */
  private void requireNamespaceWriteAccess(Namespace namespace) {
    // Use a wildcard table name so Ranger evaluates namespace-level "CREATE" policies.
    TableIdentifier nsIdent = TableIdentifier.of(namespace, "*");
    AccessLevel level = getAccessLevel(catalog, nsIdent, configuration);
    if (level != AccessLevel.READ_WRITE) {
      try {
        String user = UserGroupInformation.getCurrentUser().getUserName();
        LOG.warn("Write access denied for user {} on namespace {}", user, namespace);
      } catch (IOException ignored) {
        // best-effort logging only
      }
      throw new ForbiddenException("Write access denied on namespace %s", namespace);
    }
  }

  /**
   * Creates a load table response builder with table metadata.
   * If Azure data-sharing is active, include ADLS credential refresh config for ADLSFileIO.
   * When the client requests vended credentials via {@code X-Iceberg-Access-Delegation},
   * initial credentials are also included in the {@code storage-credentials} field.
   *
   * @param table       the table instance (can not be null)
   * @param ident       the table identifier
   * @return the load table response
   */
  static LoadTableResponse.Builder dataSharingBuilder(BaseTable table, TableIdentifier ident, Configuration conf) {
    // Must be performed as Hive user: reading file metadata requires Hive privileges.
    // In Iceberg 1.10, credentials are vended via the dedicated LOAD_CREDENTIALS endpoint —
    // see loadCredentials(). Protocol translation for ofs:// tables is delegated to
    // OzoneLinkBucketManager to keep Ozone-specific logic out of this class.
    return runAsHive(() -> {
      OzoneLinkBucketManager linkMgr = OzoneLinkBucketManager.getInstance(conf);
      TableMetadata metaData = linkMgr != null
          ? linkMgr.translateMetadataForExternalClient(table)
          : table.operations().current();
      LoadTableResponse.Builder builder = LoadTableResponse.builder().withTableMetadata(metaData);
      DataSharing dataSharing = HMSCatalogFactory.getDataSharing();
      if (dataSharing instanceof AzureCredentialProvider) {
        builder
            .addConfig(ADLS_REFRESH_CREDENTIALS_ENDPOINT, buildLoadCredentialsPath(ident))
            .addConfig(ADLS_REFRESH_CREDENTIALS_ENABLED, "true");
      }
      return builder;
    });
  }

  /**
   * Returns whether the client requested vended storage credentials via the Iceberg REST
   * {@code X-Iceberg-Access-Delegation} header.
   */
  static boolean isVendedCredentialsRequested(Map<String, String> headers) {
    String delegation = getHeaderIgnoreCase(headers, ICEBERG_ACCESS_DELEGATION_HEADER);
    if (delegation == null || delegation.isBlank()) {
      return false;
    }
    return Arrays.stream(delegation.split(","))
        .map(String::trim)
        .anyMatch(ICEBERG_ACCESS_DELEGATION_VENDED_CREDENTIALS::equals);
  }

  /**
   * Reads a request header using case-insensitive name matching.
   */
  private static String getHeaderIgnoreCase(Map<String, String> headers, String name) {
    if (headers == null) {
      return null;
    }
    String value = headers.get(name);
    if (value != null) {
      return value;
    }
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      if (name.equalsIgnoreCase(entry.getKey())) {
        return entry.getValue();
      }
    }
    return null;
  }

  static org.apache.iceberg.rest.credentials.Credential buildVendedCredential(
      BaseTable table, AccessLevel accessLevel) {
    DataSharing dataSharing = HMSCatalogFactory.getDataSharing();
    if (dataSharing == null) {
      return null;
    }
    String policy = (accessLevel == AccessLevel.READ_WRITE) ? "read-write" : "read-only";
    Map<String, String> token = dataSharing.getAccessTokenWithPolicy(table.location(), policy);
    if (token == null || token.isEmpty()) {
      return null;
    }
    return ImmutableCredential.builder()
        .prefix(table.location())
        .config(token)
        .build();
  }

  static String buildLoadCredentialsPath(TableIdentifier ident) {
    return String.format(
        LOAD_CREDENTIALS_PATH_TEMPLATE,
        RESTUtil.encodeNamespace(ident.namespace()),
        RESTUtil.encodeString(ident.name()));
  }

  /**
   * Ensures that the current user has access to the table it is trying to load.
   * <p>If configured for data sharing and the client requests vended credentials, the response
   * includes storage credentials for reading table files. Token refresh remains available via
   * the dedicated LOAD_CREDENTIALS endpoint.</p>
   *
   * @param catalog the catalog
   * @param ident   the table identifier
   * @param table   the table instance (can be null)
   * @param conf    the configuration
   * @param headers request headers
   * @return the load table response
   */
  private static LoadTableResponse accessTable(
      Catalog catalog,
      TableIdentifier ident,
      Table table,
      Configuration conf,
      Map<String, String> headers) {
    if (table instanceof BaseTable) {
      AccessLevel accessLevel = getAccessLevel(catalog, ident, conf);
      if (accessLevel == AccessLevel.NONE) {
        throw new NoSuchTableException("Table is not accessible %s", ident.toString());
      }
      LoadTableResponse.Builder builder = dataSharingBuilder((BaseTable) table, ident, conf);
      if (builder != null) {
        if (isVendedCredentialsRequested(headers)) {
          org.apache.iceberg.rest.credentials.Credential credential =
              buildVendedCredential((BaseTable) table, accessLevel);
          if (credential != null) {
            builder.addCredential(credential);
          }
        }
        return builder.build();
      }
    }
    if (table instanceof BaseMetadataTable) {
      throw new NoSuchTableException("Table does not exist: %s", ident.toString());
    }
    throw new IllegalStateException("Can not access table: " + ident.toString());
  }

  /**
   * Loads credentials for a table based on the user's access level.
   *
   * @param ident the table identifier
   * @return the load credentials response
   */
  private LoadCredentialsResponse loadCredentials(TableIdentifier ident) {
    AccessLevel accessLevel = getAccessLevel(catalog, ident, configuration);
    if (accessLevel == AccessLevel.NONE) {
      throw new NoSuchTableException("Table is not accessible %s", ident.toString());
    }
    Table table = runAsHive(() -> catalog.loadTable(ident));
    if (!(table instanceof BaseTable)) {
      throw new NoSuchTableException("Table does not exist: %s", ident.toString());
    }
    BaseTable baseTable = (BaseTable) table;
    if (isServedAsNativeOfs(baseTable.location())) {
      // This is ozone table and in-line rewrite is disabled, so S3 vended credentials does not apply. 
      // No credentials are vended
      LOG.info("In-line path rewrite disabled for ofs:// table {}; skipping S3 credential vending",
          baseTable.location());
      return ImmutableLoadCredentialsResponse.builder().build();
    }
    return runAsHive(() -> {
      ImmutableLoadCredentialsResponse.Builder responseBuilder = ImmutableLoadCredentialsResponse.builder();
      String credentialPrefix = toClientCredentialPrefix(baseTable.location());
      DataSharing dataSharing = HMSCatalogFactory.getDataSharing();
      if (dataSharing == null) {
        dataSharing = HMSCatalogServer.getDataSharing();
      }
      if (dataSharing != null) {
        String policy = (accessLevel == AccessLevel.READ_WRITE) ? "read-write" : "read-only";
        Map<String, String> token = dataSharing.getAccessTokenWithPolicy(baseTable.location(), policy);
        if (token != null && !token.isEmpty()) {
          responseBuilder.addCredentials(
              ImmutableCredential.builder()
                  .prefix(credentialPrefix)
                  .config(token)
                  .build());
        }
      }
      return responseBuilder.build();
    });
  }

  /**
   * Return the path prefix that external clients will actually use when applying vendored
   * credentials. Ozone tables are exposed through rewritten {@code s3a://} metadata, so the
   * credential prefix must also use the translated S3A location instead of the original
   * {@code ofs://} table location.
   */
  private String toClientCredentialPrefix(String tableLocation) {
    if (tableLocation != null && OzonePathUtils.isOfsScheme(tableLocation)) {
      try {
        return OzonePathUtils.translateOfsToS3a(tableLocation);
      } catch (RuntimeException e) {
        LOG.warn("Failed to translate Ozone credential prefix {}, falling back to original location",
            tableLocation, e);
      }
    }
    return tableLocation;
  }

  /**
   * Loads a table.
   *
   * @param catalog the catalog
   * @param ident   the table identifier
   * @return the load table response
   */
  LoadTableResponse loadTable(
      Catalog catalog, TableIdentifier ident, Configuration conf, Map<String, String> headers) {
    return accessTable(catalog, ident, runAsHive(() -> catalog.loadTable(ident)), conf, headers);
  }

  <T extends RESTResponse> T execute(
      HTTPMethod method,
      String path,
      Map<String, String> queryParams,
      Object body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    String requestId = generateRequestId();
    // X-HTTP-Method-Override is used by some clients that send certain HTTP verbs
    // (e.g. DELETE/PATCH via POST) to override the effective method.
    if (headers != null) {
      String methodOverride = headers.get("X-HTTP-Method-Override");
      if (methodOverride != null && !methodOverride.isBlank()) {
        try {
          HTTPMethod overridden = HTTPMethod.valueOf(methodOverride.trim().toUpperCase());
          LOG.debug("API_METHOD_OVERRIDE [{}] Overriding HTTP method {} -> {} for path {}",
              requestId, method, overridden, path);
          method = overridden;
        } catch (IllegalArgumentException e) {
          throw new RESTException("Unhandled http method override: %s", methodOverride);
        }
      }
    }
    String clientInfo = extractClientInfo(headers);
    LOG.info("API_CALL [{}] {} {} from client [{}]", requestId, method, path, clientInfo);
    ErrorResponse.Builder errorBuilder = ErrorResponse.builder();
    Pair<Route, Map<String, String>> routeAndVars = Route.from(method, path);
    if (routeAndVars != null) {
      LOG.debug("API_ROUTE_MATCHED [{}] Route: {}", requestId, routeAndVars.first());
      try {
        ImmutableMap.Builder<String, String> vars = ImmutableMap.builder();
        if (queryParams != null) {
          vars.putAll(queryParams);
        }
        vars.putAll(routeAndVars.second());
        T result = handleRequest(routeAndVars.first(), vars.build(), body, requestId, headers);
        LOG.info("API_SUCCESS [{}] {} {} completed successfully", requestId, method, path);
        return result;
      } catch (RuntimeException e) {
        LOG.warn("API_ERROR [{}] {} {} failed: {} - {}",
            requestId, method, path, e.getClass().getSimpleName(), e.getMessage());
        configureResponseFromException(e, errorBuilder);
      }
    } else {
      LOG.warn("API_UNSUPPORTED [{}] {} {} from client [{}] - No matching route found", requestId, method, path, clientInfo);
      if (LOG.isDebugEnabled()) {
        LOG.debug("API_UNSUPPORTED_DETAILS [{}] Available routes: {}",
            requestId, getSupportedRoutesSummary());
      }
      errorBuilder
          .responseCode(400)
          .withType("BadRequestException")
          .withMessage(String.format("No route for request: %s %s. Supported API version: v1. " +
              "Please check the API documentation for valid endpoints.", method, path));
    }

    ErrorResponse error = errorBuilder.build();
    LOG.info("API_ERROR_RESPONSE [{}] Returning error: {} - {}", requestId, error.type(), error.message());
    errorHandler.accept(error);

    // if the error handler doesn't throw an exception, throw a generic one
    throw new RESTException("Unhandled error: %s", error);
  }

  // helper methods for logging

  /**
   * Generates a unique request ID for tracking API calls through the system.
   * This helps correlate log entries for the same request.
   */
  private String generateRequestId() {
    int queryId = REQUEST_ID_COUNTER.incrementAndGet() % 10000;
    return String.format("%d-%04d", System.currentTimeMillis(), queryId);
  }

  /**
   * Extracts client information from request headers for monitoring.
   * This helps identify which clients are using which APIs.
   */
  private String extractClientInfo(Map<String, String> headers) {
    if (headers == null) return "unknown";

    StringBuilder clientInfo = new StringBuilder();

    String userAgent = headers.get("User-Agent");
    if (userAgent != null) {
      // Keep user agent info concise for logging
      String shortUA = userAgent.length() > 50 ? userAgent.substring(0, 50) + "..." : userAgent;
      clientInfo.append(shortUA);
    }

    String forwarded = headers.get("X-Forwarded-For");
    if (forwarded != null) {
      if (!clientInfo.isEmpty()) clientInfo.append(" from ");
      clientInfo.append(forwarded);
    }

    return !clientInfo.isEmpty() ? clientInfo.toString() : "unknown";
  }

  /**
   * Provides a summary of supported routes for debugging unsupported API calls.
   */
  private String getSupportedRoutesSummary() {
    return Arrays.stream(Route.values())
        .map(Enum::toString)
        .collect(Collectors.joining(", "));
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.DELETE, path, null, null, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.DELETE, path, queryParams, null, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.POST, path, null, body, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.GET, path, queryParams, null, responseType, headers, errorHandler);
  }

  @Override
  public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
    execute(HTTPMethod.HEAD, path, null, null, null, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.POST, path, null, formData, responseType, headers, errorHandler);
  }

  @Override
  public void close() {
    // The caller is responsible for closing the underlying catalog backing this REST catalog.
  }

  private static class NamespaceNotSupported extends RuntimeException {
    NamespaceNotSupported(String catalog) {
      super("catalog " + catalog + " does not support namespace");
    }
  }

  private static class ViewNotSupported extends RuntimeException {
    ViewNotSupported(String catalog) {
      super("catalog " + catalog + " does not support views");
    }
  }


  private static class BadResponseType extends RuntimeException {
    private BadResponseType(Class<?> responseType, Object response) {
      super(
          String.format("Invalid response object, not a %s: %s", responseType.getName(), response));
    }
  }

  private static class BadRequestType extends RuntimeException {
    private BadRequestType(Class<?> requestType, Object request) {
      super(String.format("Invalid request object, not a %s: %s", requestType.getName(), request));
    }
  }

  public static <T> T castRequest(Class<T> requestType, Object request) {
    if (requestType.isInstance(request)) {
      return requestType.cast(request);
    }

    throw new BadRequestType(requestType, request);
  }

  public static <T extends RESTResponse> T castResponse(Class<T> responseType, Object response) {
    if (responseType.isInstance(response)) {
      return responseType.cast(response);
    }

    throw new BadResponseType(responseType, response);
  }

  public static void configureResponseFromException(
      Exception exc, ErrorResponse.Builder errorBuilder) {
    errorBuilder
        .responseCode(EXCEPTION_ERROR_CODES.getOrDefault(exc.getClass(), 500))
        .withType(exc.getClass().getSimpleName())
        .withMessage(exc.getMessage())
        .withStackTrace(exc);
  }

  private static Namespace namespaceFromPathVars(Map<String, String> pathVars) {
    return RESTUtil.decodeNamespace(pathVars.get("namespace"));
  }

  private static TableIdentifier identFromPathVars(Map<String, String> pathVars) {
    return TableIdentifier.of(
        namespaceFromPathVars(pathVars), RESTUtil.decodeString(pathVars.get("table")));
  }

  private static TableIdentifier viewIdentFromPathVars(Map<String, String> pathVars) {
    return TableIdentifier.of(
        namespaceFromPathVars(pathVars), RESTUtil.decodeString(pathVars.get("name")));
  }
}
