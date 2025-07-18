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

package org.apache.hadoop.hive.ql.parse;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.DYNAMIC_PARTITION_CONVERT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ARCHIVE_ENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_DEFAULT_STORAGE_HANDLER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_STATS_DBCLASS;
import static org.apache.hadoop.hive.conf.HiveConf.shouldComputeLineage;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_CTAS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.DEFAULT_TABLE_TYPE;
import static org.apache.hadoop.hive.ql.ddl.view.create.AbstractCreateViewAnalyzer.validateTablesUsed;
import static org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTConverter.NON_FK_FILTERED;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.antlr.runtime.ClassicToken;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.Tree;
import org.antlr.runtime.tree.TreeVisitor;
import org.antlr.runtime.tree.TreeVisitorAction;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.StatsSetupConst.StatDB;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConf.ResultFileFormat;
import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.TransactionalValidationListener;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.cache.results.CacheUsage;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.ddl.DDLDescWithTableProperties;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.misc.hooks.InsertCommitHookDesc;
import org.apache.hadoop.hive.ql.ddl.misc.sortoder.SortFieldDesc;
import org.apache.hadoop.hive.ql.ddl.misc.sortoder.SortFields;
import org.apache.hadoop.hive.ql.ddl.table.constraint.ConstraintsUtils;
import org.apache.hadoop.hive.ql.ddl.table.convert.AlterTableConvertOperation;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.create.like.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.preinsert.PreInsertTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.properties.AlterTableUnsetPropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.skewed.SkewedTableUtils;
import org.apache.hadoop.hive.ql.ddl.view.create.CreateMaterializedViewDesc;
import org.apache.hadoop.hive.ql.ddl.view.materialized.update.MaterializedViewUpdateDesc;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RecordReader;
import org.apache.hadoop.hive.ql.exec.RecordWriter;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.ReduceField;
import org.apache.hadoop.hive.ql.exec.WindowFunctionInfo;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.NullRowsInputFormat;
import org.apache.hadoop.hive.ql.io.SchemaInferenceUtils;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.MaterializationValidationResult;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.Optimizer;
import org.apache.hadoop.hive.ql.optimizer.QueryPlanPostProcessor;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveOpConverterPostProc;
import org.apache.hadoop.hive.ql.optimizer.lineage.Generator;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec.SpecType;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputType;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionedTableFunctionSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitioningSpec;
import org.apache.hadoop.hive.ql.parse.QBSubQuery.SubQueryType;
import org.apache.hadoop.hive.ql.parse.SubQueryUtils.ISubQueryJoinInfo;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowExpressionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFrameSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckCtx;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc.SampleDesc;
import org.apache.hadoop.hive.ql.plan.ForwardDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewForwardDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewJoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.plan.UnionDesc;
import org.apache.hadoop.hive.ql.plan.mapper.AuxOpTreeSignature;
import org.apache.hadoop.hive.ql.plan.ptf.OrderExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.ResourceType;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFArray;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCardinalityViolation;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMurmurHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSurrogateKey;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFInline;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.ql.util.ResourceDownloader;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.DelimitedJSONSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.NoOpFetchFormatter;
import org.apache.hadoop.hive.serde2.NullStructSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe2;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.thrift.ThriftJDBCBinarySerDe;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;
import com.google.common.math.LongMath;
/**
 * Implementation of the semantic analyzer. It generates the query plan.
 * There are other specific semantic analyzers for some hive operations such as
 * various analyzers for DDL commands.
 */

public class SemanticAnalyzer extends BaseSemanticAnalyzer {


  public static final String DUMMY_DATABASE = "_dummy_database";
  public static final String DUMMY_TABLE = "_dummy_table";
  public static final String SUBQUERY_TAG_1 = "-subquery1";
  public static final String SUBQUERY_TAG_2 = "-subquery2";

  // Max characters when auto generating the column name with func name
  private static final int AUTOGEN_COLALIAS_PRFX_MAXLENGTH = 20;

  public static final String VALUES_TMP_TABLE_NAME_PREFIX = "Values__Tmp__Table__";

  /** Marks the temporary table created for a serialized CTE. The table is scoped to the query. */
  static final String MATERIALIZATION_MARKER = "$MATERIALIZATION";
  private static final String RESULTS_CACHE_KEY_TOKEN_REWRITE_PROGRAM = "RESULTS_CACHE_KEY_PROGRAM";

  private Map<TableScanOperator, ExprNodeDesc> opToPartPruner;
  private Map<TableScanOperator, PrunedPartitionList> opToPartList;
  protected Map<String, TableScanOperator> topOps;
  protected Map<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx;
  private List<LoadTableDesc> loadTableWork;
  private List<LoadFileDesc> loadFileWork;
  private final List<ColumnStatsAutoGatherContext> columnStatsAutoGatherContexts;
  private final Map<JoinOperator, QBJoinTree> joinContext;
  private final Map<SMBMapJoinOperator, QBJoinTree> smbMapJoinContext;
  private final List<ReduceSinkOperator> reduceSinkOperatorsAddedByEnforceBucketingSorting;
  private QB qb;
  protected ASTNode ast;
  private int destTableId;
  private UnionProcContext uCtx;
  private List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinOpsNoReducer;
  private Map<TableScanOperator, SampleDesc> opToSamplePruner;
  private final Map<TableScanOperator, Map<String, ExprNodeDesc>> opToPartToSkewedPruner;
  private Map<SelectOperator, Table> viewProjectToTableSchema;
  private Operator<? extends OperatorDesc> sinkOp;
  private final CacheTableHelper cacheTableHelper = new CacheTableHelper();

  /**
   * a map for the split sampling, from alias to an instance of SplitSample
   * that describes percentage and number.
   */
  private final Map<String, SplitSample> nameToSplitSample;
  private final Map<GroupByOperator, Set<String>> groupOpToInputTables;
  protected Map<String, PrunedPartitionList> prunedPartitions;
  protected List<FieldSchema> resultSchema;
  protected List<FieldSchema> originalResultSchema;
  protected CreateMaterializedViewDesc createVwDesc;
  private MaterializedViewUpdateDesc materializedViewUpdateDesc;
  private List<String> viewsExpanded;
  protected ASTNode viewSelect;
  protected final UnparseTranslator unparseTranslator;
  private final GlobalLimitCtx globalLimitCtx;

  // prefix for column names auto generated by hive
  protected final String autogenColAliasPrfxLbl;
  private final boolean autogenColAliasPrfxIncludeFuncName;

  // Keep track of view alias to read entity corresponding to the view
  // For eg: for a query like 'select * from V3', where V3 -> V2, V2 -> V1, V1 -> T
  // keeps track of aliases for V3, V3:V2, V3:V2:V1.
  // This is used when T is added as an input for the query, the parents of T is
  // derived from the alias V3:V2:V1:T
  private final Map<String, ReadEntity> viewAliasToInput;

  //need merge isDirect flag to input even if the newInput does not have a parent
  private boolean mergeIsDirect;

  // flag for no scan during analyze ... compute statistics
  private boolean noscan;

  // flag indicating that the analyzations should go only till resultSchema is ready
  protected boolean forViewCreation;
  private String fqViewName;

  // whether this is a mv rebuild rewritten expression
  protected MaterializationRebuildMode mvRebuildMode = MaterializationRebuildMode.NONE;

  protected volatile boolean disableJoinMerge = false;
  protected final boolean defaultJoinMerge;

  /**
   * This is required by prepare/execute statement
   * Original operator tree { @link topOps} shape is changed when going through transformations
   * and task generation, as a result original operator tree can not be used later to
   * e.g. regenerate tasks or re-running physical transformations.
   * Therefore we need to make a copy and cache it after operator tree is generated.
   */
  protected Map<String, TableScanOperator> topOpsCopy = null;

  /*
   * Capture the CTE definitions in a Query.
   */
  protected final Map<String, CTEClause> aliasToCTEs;

  /*
   * Used to check recursive CTE invocations. Similar to viewsExpanded
   */
  private List<String> ctesExpanded = new ArrayList<>();

  /*
   * Whether root tasks after materialized CTE linkage have been resolved
   */
  private boolean rootTasksResolved;

  private TableMask tableMask;

  CreateTableDesc tableDesc;

  protected AnalyzeRewriteContext analyzeRewrite;

  private WriteEntity acidAnalyzeTable;

  // A mapping from a tableName to a table object in metastore.
  QueryTables tabNameToTabObject;

  // The tokens we should ignore when we are trying to do table masking.
  private static final Set<Integer> IGNORED_TOKENS = Sets.newHashSet(HiveParser.TOK_GROUPBY,
      HiveParser.TOK_ORDERBY, HiveParser.TOK_WINDOWSPEC, HiveParser.TOK_CLUSTERBY,
      HiveParser.TOK_DISTRIBUTEBY, HiveParser.TOK_SORTBY);

  private String invalidResultCacheReason;
  private MaterializationValidationResult materializationValidationResult;

  private final NullOrdering defaultNullOrder;

  private static final CommonToken SELECTDI_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_SELECTDI, "TOK_SELECTDI");
  private static final CommonToken SELEXPR_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
  private static final CommonToken TABLEORCOL_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
  private static final CommonToken DOT_TOKEN =
      new ImmutableCommonToken(HiveParser.DOT, ".");

  private static final String[] UPDATED_TBL_PROPS = {
      hive_metastoreConstants.TABLE_IS_TRANSACTIONAL,
      hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES,
      hive_metastoreConstants.TABLE_BUCKETING_VERSION
  };

  private int subQueryExpressionAliasCounter = 0;
  private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();
  static class Phase1Ctx {
    String dest;
    int nextNum;
  }

  public SemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    opToPartPruner = new HashMap<TableScanOperator, ExprNodeDesc>();
    opToPartList = new HashMap<TableScanOperator, PrunedPartitionList>();
    opToSamplePruner = new HashMap<TableScanOperator, SampleDesc>();
    nameToSplitSample = new HashMap<String, SplitSample>();
    // Must be deterministic order maps - see HIVE-8707
    topOps = new LinkedHashMap<String, TableScanOperator>();
    loadTableWork = new ArrayList<LoadTableDesc>();
    loadFileWork = new ArrayList<LoadFileDesc>();
    columnStatsAutoGatherContexts = new ArrayList<ColumnStatsAutoGatherContext>();
    opParseCtx = new LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext>();
    joinContext = new HashMap<JoinOperator, QBJoinTree>();
    smbMapJoinContext = new HashMap<SMBMapJoinOperator, QBJoinTree>();
    // Must be deterministic order map for consistent q-test output across Java versions
    reduceSinkOperatorsAddedByEnforceBucketingSorting = new ArrayList<ReduceSinkOperator>();
    destTableId = 1;
    uCtx = null;
    listMapJoinOpsNoReducer = new ArrayList<AbstractMapJoinOperator<? extends MapJoinDesc>>();
    groupOpToInputTables = new HashMap<GroupByOperator, Set<String>>();
    prunedPartitions = new HashMap<String, PrunedPartitionList>();
    unparseTranslator = new UnparseTranslator(conf);
    autogenColAliasPrfxLbl = HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_LABEL);
    autogenColAliasPrfxIncludeFuncName = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_INCLUDEFUNCNAME);
    queryProperties = new QueryProperties();
    opToPartToSkewedPruner = new HashMap<TableScanOperator, Map<String, ExprNodeDesc>>();
    aliasToCTEs = new HashMap<String, CTEClause>();
    globalLimitCtx = new GlobalLimitCtx();
    viewAliasToInput = new HashMap<String, ReadEntity>();
    mergeIsDirect = true;
    noscan = false;
    tabNameToTabObject = new QueryTables();
    defaultJoinMerge = !HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MERGE_NWAY_JOINS);
    disableJoinMerge = defaultJoinMerge;
    defaultNullOrder = NullOrdering.defaultNullOrder(conf);
  }

  @Override
  protected void reset(boolean clearCache) {
    super.reset(true);
    if(clearCache) {
      prunedPartitions.clear();
      if (ctx != null) {
        ctx.getOpContext().getColStatsCache().clear();
      }

      //When init(true) combine with genResolvedParseTree, it will generate Resolved Parse tree from syntax tree
      //ReadEntity created under these conditions should be all relevant to the syntax tree even the ones without parents
      //set mergeIsDirect to true here.
      mergeIsDirect = true;
    } else {
      mergeIsDirect = false;
    }
    loadTableWork.clear();
    loadFileWork.clear();
    columnStatsAutoGatherContexts.clear();
    topOps.clear();
    destTableId = 1;
    idToTableNameMap.clear();
    qb = null;
    ast = null;
    uCtx = null;
    joinContext.clear();
    smbMapJoinContext.clear();
    opParseCtx.clear();
    groupOpToInputTables.clear();
    disableJoinMerge = defaultJoinMerge;
    aliasToCTEs.clear();
    opToPartPruner.clear();
    opToPartList.clear();
    opToPartToSkewedPruner.clear();
    opToSamplePruner.clear();
    nameToSplitSample.clear();
    resultSchema = null;
    createVwDesc = null;
    materializedViewUpdateDesc = null;
    viewsExpanded = null;
    viewSelect = null;
    ctesExpanded.clear();
    globalLimitCtx.disableOpt();
    viewAliasToInput.clear();
    reduceSinkOperatorsAddedByEnforceBucketingSorting.clear();
    listMapJoinOpsNoReducer.clear();
    unparseTranslator.clear();
    queryProperties.clear();
    outputs.clear();

    if (ctx != null && ctx.enableUnparse()) {
      unparseTranslator.enable();
    }
  }

  void initParseCtx(ParseContext pctx) {
    opToPartPruner = pctx.getOpToPartPruner();
    opToPartList = pctx.getOpToPartList();
    opToSamplePruner = pctx.getOpToSamplePruner();
    topOps = pctx.getTopOps();
    loadTableWork = pctx.getLoadTableWork();
    loadFileWork = pctx.getLoadFileWork();
    ctx = pctx.getContext();
    destTableId = pctx.getDestTableId();
    idToTableNameMap = pctx.getIdToTableNameMap();
    uCtx = pctx.getUCtx();
    listMapJoinOpsNoReducer = pctx.getListMapJoinOpsNoReducer();
    prunedPartitions = pctx.getPrunedPartitions();
    tabNameToTabObject = pctx.getTabNameToTabObject();
    fetchTask = pctx.getFetchTask();
    setLineageInfo(pctx.getLineageInfo());
  }

  public ParseContext getParseContext() {
    // Make sure the basic query properties are initialized
    copyInfoToQueryProperties(queryProperties);
    return new ParseContext(queryState, opToPartPruner, opToPartList, topOps,
        new HashSet<JoinOperator>(joinContext.keySet()),
        new HashSet<SMBMapJoinOperator>(smbMapJoinContext.keySet()),
        loadTableWork, loadFileWork, columnStatsAutoGatherContexts,
        ctx, idToTableNameMap, destTableId, uCtx,
        listMapJoinOpsNoReducer, prunedPartitions, tabNameToTabObject,
        opToSamplePruner, globalLimitCtx, nameToSplitSample, inputs, rootTasks,
        opToPartToSkewedPruner, viewAliasToInput, reduceSinkOperatorsAddedByEnforceBucketingSorting,
        analyzeRewrite, tableDesc, createVwDesc, materializedViewUpdateDesc,
        queryProperties, viewProjectToTableSchema);
  }

  public CompilationOpContext getOpContext() {
    return ctx.getOpContext();
  }

  static String genPartValueString(String partColType, String partVal) {
    String returnVal = partVal;
    if (partColType.equals(serdeConstants.STRING_TYPE_NAME) ||
        partColType.contains(serdeConstants.VARCHAR_TYPE_NAME) ||
        partColType.contains(serdeConstants.CHAR_TYPE_NAME)) {
      returnVal = "'" + escapeSQLString(partVal) + "'";
    } else if (partColType.equals(serdeConstants.TINYINT_TYPE_NAME)) {
      returnVal = partVal + "Y";
    } else if (partColType.equals(serdeConstants.SMALLINT_TYPE_NAME)) {
      returnVal = partVal + "S";
    } else if (partColType.equals(serdeConstants.INT_TYPE_NAME)) {
      returnVal = partVal;
    } else if (partColType.equals(serdeConstants.BIGINT_TYPE_NAME)) {
      returnVal = partVal + "L";
    } else if (partColType.contains(serdeConstants.DECIMAL_TYPE_NAME)) {
      returnVal = partVal + "BD";
    } else if (partColType.equals(serdeConstants.DATE_TYPE_NAME) ||
        partColType.equals(serdeConstants.TIMESTAMP_TYPE_NAME)) {
      returnVal = partColType + " '" + escapeSQLString(partVal) + "'";
    } else {
      //for other usually not used types, just quote the value
      returnVal = "'" + escapeSQLString(partVal) + "'";
    }

    return returnVal;
  }

  private void doPhase1QBExpr(ASTNode ast, QBExpr qbexpr, String id, String alias, ASTNode tabColNames,
                              Map<String, CTEClause> aliasToCTEs) throws SemanticException {
    doPhase1QBExpr(ast, qbexpr, id, alias, false, tabColNames, aliasToCTEs);
  }

  private void doPhase1QBExpr(ASTNode ast, QBExpr qbexpr, String id, String alias, ASTNode tabColNames)
      throws SemanticException {
    doPhase1QBExpr(ast, qbexpr, id, alias, false, tabColNames, aliasToCTEs);
  }

  @SuppressWarnings("nls")
  void doPhase1QBExpr(ASTNode ast, QBExpr qbexpr, String id, String alias, boolean insideView, ASTNode tabColNames)
      throws SemanticException {
    doPhase1QBExpr(ast, qbexpr, id, alias, insideView, tabColNames, this.aliasToCTEs);
  }

  @SuppressWarnings("nls")
  void doPhase1QBExpr(ASTNode ast, QBExpr qbexpr, String id, String alias, boolean insideView, ASTNode tabColNames,
                      Map<String, CTEClause> aliasToCTEs) throws SemanticException {

    assert (ast.getToken() != null);
    if (ast.getToken().getType() == HiveParser.TOK_QUERY) {
      QB qb = new QB(id, alias, true);
      qb.setInsideView(insideView);
      Phase1Ctx ctx_1 = initPhase1Ctx();
      qb.getParseInfo().setColAliases(tabColNames);
      doPhase1(ast, qb, ctx_1, null, aliasToCTEs);

      qbexpr.setOpcode(QBExpr.Opcode.NULLOP);
      qbexpr.setQB(qb);
    }
    // setop
    else {
      switch (ast.getToken().getType()) {
      case HiveParser.TOK_UNIONALL:
        qbexpr.setOpcode(QBExpr.Opcode.UNION);
        break;
      case HiveParser.TOK_INTERSECTALL:
        queryProperties.setHasIntersect(true);
        qbexpr.setOpcode(QBExpr.Opcode.INTERSECTALL);
        break;
      case HiveParser.TOK_INTERSECTDISTINCT:
        queryProperties.setHasIntersect(true);
        qbexpr.setOpcode(QBExpr.Opcode.INTERSECT);
        break;
      case HiveParser.TOK_EXCEPTALL:
        queryProperties.setHasExcept(true);
        qbexpr.setOpcode(QBExpr.Opcode.EXCEPTALL);
        break;
      case HiveParser.TOK_EXCEPTDISTINCT:
        queryProperties.setHasExcept(true);
        qbexpr.setOpcode(QBExpr.Opcode.EXCEPT);
        break;
      default:
        throw new SemanticException(ErrorMsg.UNSUPPORTED_SET_OPERATOR.getMsg("Type "
            + ast.getToken().getType()));
      }
      // query 1
      assert (ast.getChild(0) != null);
      QBExpr qbexpr1 = new QBExpr(alias + SUBQUERY_TAG_1);
      doPhase1QBExpr((ASTNode) ast.getChild(0), qbexpr1, id,
          alias + SUBQUERY_TAG_1, insideView, tabColNames, aliasToCTEs);
      qbexpr.setQBExpr1(qbexpr1);

      // query 2
      assert (ast.getChild(1) != null);
      QBExpr qbexpr2 = new QBExpr(alias + SUBQUERY_TAG_2);
      doPhase1QBExpr((ASTNode) ast.getChild(1), qbexpr2, id,
          alias + SUBQUERY_TAG_2, insideView, tabColNames, aliasToCTEs);
      qbexpr.setQBExpr2(qbexpr2);
    }
  }

  private Map<String, ASTNode> doPhase1GetAggregationsFromSelect(
      ASTNode selExpr, QB qb, String dest) throws SemanticException {

    // Iterate over the selects search for aggregation Trees.
    // Use String as keys to eliminate duplicate trees.
    Map<String, ASTNode> aggregationTrees = new LinkedHashMap<String, ASTNode>();
    List<ASTNode> wdwFns = new ArrayList<ASTNode>();
    for (int i = 0; i < selExpr.getChildCount(); ++i) {
      ASTNode function = (ASTNode) selExpr.getChild(i);
      if (function.getType() == HiveParser.TOK_SELEXPR ||
          function.getType() == HiveParser.TOK_SUBQUERY_EXPR) {
        function = (ASTNode)function.getChild(0);
      }
      doPhase1GetAllAggregations(function, qb, aggregationTrees, wdwFns, null);
    }

    // window based aggregations are handled differently
    for (ASTNode wdwFn : wdwFns) {
      WindowingSpec spec = qb.getWindowingSpec(dest);
      if(spec == null) {
        queryProperties.setHasWindowing(true);
        spec = new WindowingSpec();
        qb.addDestToWindowingSpec(dest, spec);
      }
      Map<String, ASTNode> wExprsInDest = qb.getParseInfo().getWindowingExprsForClause(dest);
      int wColIdx = spec.getWindowExpressions() == null ? 0 : spec.getWindowExpressions().size();
      WindowFunctionSpec wFnSpec = processWindowFunction(wdwFn,
          (ASTNode)wdwFn.getChild(wdwFn.getChildCount()-1));
      // If this is a duplicate invocation of a function; don't add to WindowingSpec.
      if ( wExprsInDest != null &&
          wExprsInDest.containsKey(wFnSpec.getExpression().toStringTree())) {
        continue;
      }
      wFnSpec.setAlias(wFnSpec.getName() + "_window_" + wColIdx);
      spec.addWindowFunction(wFnSpec);
      qb.getParseInfo().addWindowingExprToClause(dest, wFnSpec.getExpression());
    }

    return aggregationTrees;
  }

  private void doPhase1WhereClause(ASTNode expressionTree, QB qb) throws SemanticException {
    int exprTokenType = expressionTree.getToken().getType();
    if(exprTokenType == HiveParser.TOK_SUBQUERY_EXPR) {
      qb.addSubqExprAlias(expressionTree, this);
      return;
    }

    for (int i = 0; i < expressionTree.getChildCount(); i++) {
      doPhase1WhereClause((ASTNode) expressionTree.getChild(i), qb);
    }
  }

  /**
   * This method figures out if current AST is for INSERT INTO
   * @param qbp qbParseInfo
   * @param dest destination clause
   * @return true or false
   */
  protected boolean isInsertInto(QBParseInfo qbp, String dest) {
    // get the destination and check if it is TABLE
    if(qbp == null || dest == null ) {
      return false;
    }
    ASTNode destNode = qbp.getDestForClause(dest);
    return destNode != null && destNode.getType() == HiveParser.TOK_TAB;
  }

  /**
   * Given an AST this method figures out if it is a value clause
   * e.g. VALUES(1,3..)
   */
  private boolean isValueClause(ASTNode select) {
    if(select == null) {
      return false;
    }
    if(select.getChildCount() == 1) {
      ASTNode selectExpr = (ASTNode)select.getChild(0);
      if(selectExpr.getChildCount() == 1 ) {
        ASTNode selectChildExpr = (ASTNode)selectExpr.getChild(0);
        if(selectChildExpr.getType() == HiveParser.TOK_FUNCTION) {
          ASTNode inline = (ASTNode)selectChildExpr.getChild(0);
            ASTNode func = (ASTNode)selectChildExpr.getChild(1);
          if(inline.getText().equals(GenericUDTFInline.class.getAnnotation(Description.class).name())
              && func.getType() == HiveParser.TOK_FUNCTION) {
            ASTNode arrayNode = (ASTNode)func.getChild(0);
            ASTNode funcNode= (ASTNode)func.getChild(1);
            if(arrayNode.getText().equals(GenericUDFArray.class.getAnnotation(Description.class).name() )
                && funcNode.getType() == HiveParser.TOK_FUNCTION) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  /**
   * This method creates a list of default constraints which corresponds to
   *  given schema (targetSchema) or target table's column schema (if targetSchema is null)
   * @param tbl
   * @param targetSchema
   * @return List of default constraints (including NULL if there is no default)
   * @throws SemanticException
   */
  protected List<String> getDefaultConstraints(Table tbl, List<String> targetSchema) throws SemanticException{
    Map<String, String> colNameToDefaultVal = getColNameToDefaultValueMap(tbl);
    List<String> defaultConstraints = new ArrayList<>();
    if(targetSchema != null && !targetSchema.isEmpty()) {
      for (String colName : targetSchema) {
        defaultConstraints.add(colNameToDefaultVal.get(colName));
      }
    }
    else {
      for(FieldSchema fs:tbl.getCols()) {
        defaultConstraints.add(colNameToDefaultVal.get(fs.getName()));
      }
    }
    return defaultConstraints;
  }

  protected Map<String, String> getColNameToDefaultValueMap(Table tbl) throws SemanticException {
    Map<String, String> colNameToDefaultVal = null;
    try {
      DefaultConstraint dc = Hive.get().getEnabledDefaultConstraints(tbl.getDbName(), tbl.getTableName());
      colNameToDefaultVal = dc.getColNameToDefaultValueMap();
    } catch (Exception e) {
      if (e instanceof SemanticException) {
        throw (SemanticException) e;
      } else {
        throw (new RuntimeException(e));
      }
    }
    return colNameToDefaultVal;
  }

  /**
   * Constructs an AST for given DEFAULT string
   * @param newValue
   * @throws SemanticException
   */
  private ASTNode getNodeReplacementforDefault(String newValue) throws SemanticException {
    ASTNode newNode = null;
    if(newValue== null) {
      newNode = ASTBuilder.construct(HiveParser.TOK_NULL, "TOK_NULL").node();
    }
    else {
      try {
        newNode = new ParseDriver().parseExpression(newValue);
      } catch(Exception e) {
        throw new SemanticException("Error while parsing default value for DEFAULT keyword: " + newValue
                                        + ". Error message: " + e.getMessage());
      }
    }
    return newNode;
  }

  /**
   * This method replaces ASTNode corresponding to DEFAULT keyword with either DEFAULT constraint
   *  expression if exists or NULL otherwise
   * @param selectExprs
   * @param targetTable
   * @throws SemanticException
   */
  private void replaceDefaultKeywordForUpdate(ASTNode selectExprs, Table targetTable) throws SemanticException {
    List<String> defaultConstraints = null;
    for (int i = 0; i < selectExprs.getChildCount(); i++) {
      ASTNode selectExpr = (ASTNode) selectExprs.getChild(i);
      if (selectExpr.getChildCount() == 1 && selectExpr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
        //first child should be rowid
        if (i != 0 || selectExpr.getChild(0).getChild(0).getText().equals("ROW__ID")) {
          if (selectExpr.getChild(0).getChild(0).getType() == HiveParser.TOK_DEFAULT_VALUE) {
            if (defaultConstraints == null) {
              defaultConstraints = getDefaultConstraints(targetTable, null);
            }
            ASTNode newNode = getNodeReplacementforDefault(defaultConstraints.get(i - 1));
            // replace the node in place
            selectExpr.replaceChildren(0, 0, newNode);
            if (LOG.isDebugEnabled()) {
              LOG.debug("DEFAULT keyword replacement - Inserted {} for table: {}", newNode.getText(),
                  targetTable.getTableName());
            }
          }
        }
      }
    }
  }

  /**
   * This method replaces DEFAULT AST node with DEFAULT expression
   * @param valueArrClause This is AST for value clause
   * @param targetTable
   * @param targetSchema this is target schema/column schema if specified in query
   */
  private void replaceDefaultKeyword(ASTNode valueArrClause, Table targetTable, List<String> targetSchema) throws SemanticException {
    List<String> defaultConstraints = null;
    for (int i = 1; i < valueArrClause.getChildCount(); i++) {
      ASTNode valueClause = (ASTNode) valueArrClause.getChild(i);
      //skip first child since it is struct
      for (int j = 1; j < valueClause.getChildCount(); j++) {
        if (valueClause.getChild(j).getType() == HiveParser.TOK_TABLE_OR_COL
                && valueClause.getChild(j).getChild(0).getType() == HiveParser.TOK_DEFAULT_VALUE) {
          if (defaultConstraints == null) {
            defaultConstraints = getDefaultConstraints(targetTable, targetSchema);
          }
          ASTNode newNode = getNodeReplacementforDefault(defaultConstraints.get(j - 1));
          // replace the node in place
          valueClause.replaceChildren(j, j, newNode);
          LOG.debug("DEFAULT keyword replacement - Inserted {} for table: {}", newNode.getText(),
                  targetTable.getTableName());
        }
      }
    }
  }

  private void doPhase1GetColumnAliasesFromSelect(
      ASTNode selectExpr, QBParseInfo qbp, String dest) throws SemanticException {
    if (isInsertInto(qbp, dest)) {
      ASTNode tblAst = qbp.getDestForClause(dest);
      String tableName = getUnescapedName((ASTNode) tblAst.getChild(0));
      Table targetTable;
      try {
        if (isValueClause(selectExpr)) {
          targetTable = getTableObjectByName(tableName);
          replaceDefaultKeyword((ASTNode) selectExpr.getChild(0).getChild(0).getChild(1), targetTable, qbp.getDestSchemaForClause(dest));
        } else if (updating(dest)) {
          targetTable = getTableObjectByName(tableName);
          replaceDefaultKeywordForUpdate(selectExpr, targetTable);
        }
      } catch (Exception e) {
        if (e instanceof SemanticException) {
          throw (SemanticException) e;
        } else {
          throw (new RuntimeException(e));
        }
      }
    }
    for (int i = 0; i < selectExpr.getChildCount(); ++i) {
      ASTNode selExpr = (ASTNode) selectExpr.getChild(i);
      if ((selExpr.getToken().getType() == HiveParser.TOK_SELEXPR)
          && (selExpr.getChildCount() == 2)) {
        String columnAlias = unescapeIdentifier(selExpr.getChild(1).getText());
        qbp.setExprToColumnAlias((ASTNode) selExpr.getChild(0), columnAlias);
      }
    }
  }

  /**
   * DFS-scan the expressionTree to find all aggregation subtrees and put them
   * in aggregations.
   *
   * @param expressionTree
   * @param aggregations
   *          the key to the HashTable is the toStringTree() representation of
   *          the aggregation subtree.
   * @throws SemanticException
   */
  private void doPhase1GetAllAggregations(ASTNode expressionTree, QB qb,
                                          Map<String, ASTNode> aggregations, List<ASTNode> wdwFns,
                                          ASTNode wndParent) throws SemanticException {
    int exprTokenType = expressionTree.getToken().getType();
    if(exprTokenType == HiveParser.TOK_SUBQUERY_EXPR) {
      //since now we have scalar subqueries we can get subquery expression in having
      // we don't want to include aggregate from within subquery
      qb.addSubqExprAlias(expressionTree, this);
      return;
    }

    boolean parentIsWindowSpec = wndParent != null;

    if (exprTokenType == HiveParser.TOK_FUNCTION
        || exprTokenType == HiveParser.TOK_FUNCTIONDI
        || exprTokenType == HiveParser.TOK_FUNCTIONSTAR) {
      assert (expressionTree.getChildCount() != 0);
      Tree lastChild = expressionTree.getChild(expressionTree.getChildCount() - 1);
      if (lastChild.getType() == HiveParser.TOK_WINDOWSPEC) {
        // If it is a windowing spec, we include it in the list
        // Further, we will examine its children AST nodes to check whether
        // there are aggregation functions within
        wdwFns.add(expressionTree);
        for(Node child : expressionTree.getChildren()) {
          doPhase1GetAllAggregations((ASTNode) child, qb, aggregations, wdwFns, expressionTree);
        }
        return;
      } else if (lastChild.getType() == HiveParser.TOK_WITHIN_GROUP) {
        transformWithinGroup(expressionTree, lastChild);
      }
      if (expressionTree.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = unescapeIdentifier(expressionTree.getChild(0)
            .getText());
        // Validate the function name
        if (FunctionRegistry.getFunctionInfo(functionName) == null) {
          throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(functionName));
        }
        if(FunctionRegistry.impliesOrder(functionName) && !parentIsWindowSpec) {
          throw new SemanticException(ErrorMsg.MISSING_OVER_CLAUSE.getMsg(functionName));
        }
        if (FunctionRegistry.getGenericUDAFResolver(functionName) != null) {
          if(containsLeadLagUDF(expressionTree) && !parentIsWindowSpec) {
            throw new SemanticException(ErrorMsg.MISSING_OVER_CLAUSE.getMsg(functionName));
          }
          aggregations.put(expressionTree.toStringTree(), expressionTree);
          FunctionInfo fi = FunctionRegistry.getFunctionInfo(functionName);
          if (!fi.isNative()) {
            unparseTranslator.addIdentifierTranslation((ASTNode) expressionTree
                .getChild(0));
          }
          return;
        }
      }
    }
    for (int i = 0; i < expressionTree.getChildCount(); i++) {
      doPhase1GetAllAggregations((ASTNode) expressionTree.getChild(i), qb,
          aggregations, wdwFns, wndParent);
    }
  }

  private void transformWithinGroup(ASTNode expressionTree, Tree withinGroupNode) throws SemanticException {
    if (isCBOExecuted()) {
      return;
    }

    Tree functionNameNode = expressionTree.getChild(0);
    if (!FunctionRegistry.isOrderedAggregate(functionNameNode.getText())) {
      throw new SemanticException(ErrorMsg.WITHIN_GROUP_NOT_ALLOWED, functionNameNode.getText());
    }

    List<Tree> parameters = new ArrayList<>(expressionTree.getChildCount() - 2);
    for (int i = 1; i < expressionTree.getChildCount() - 1; ++i) {
      parameters.add(expressionTree.getChild(i));
    }
    while (expressionTree.getChildCount() > 1) {
      expressionTree.deleteChild(1);
    }

    Tree orderByNode = withinGroupNode.getChild(0);
    if (parameters.size() != orderByNode.getChildCount()) {
      throw new SemanticException(ErrorMsg.WITHIN_GROUP_PARAMETER_MISMATCH,
              Integer.toString(parameters.size()), Integer.toString(orderByNode.getChildCount()));
    }

    for (int i = 0; i < orderByNode.getChildCount(); ++i) {
      expressionTree.addChild(parameters.get(i));
      Tree tabSortColNameNode = orderByNode.getChild(i);
      Tree nullsNode = tabSortColNameNode.getChild(0);
      ASTNode sortKey = (ASTNode) tabSortColNameNode.getChild(0).getChild(0);
      expressionTree.addChild(sortKey);
      expressionTree.addChild(ASTBuilder.createAST(HiveParser.NumberLiteral,
              Integer.toString(DirectionUtils.tokenToCode(tabSortColNameNode.getType()))));
      expressionTree.addChild(ASTBuilder.createAST(HiveParser.NumberLiteral,
              Integer.toString(NullOrdering.fromToken(nullsNode.getType()).getCode())));
    }
  }

  private List<ASTNode> doPhase1GetDistinctFuncExprs(Map<String, ASTNode> aggregationTrees) {
    List<ASTNode> exprs = new ArrayList<ASTNode>();
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      assert (value != null);
      if (value.getToken().getType() == HiveParser.TOK_FUNCTIONDI) {
        exprs.add(value);
      }
    }
    return exprs;
  }

  public static String generateErrorMessage(ASTNode ast, String message) {
    StringBuilder sb = new StringBuilder();
    if (ast == null) {
      sb.append(message).append(". Cannot tell the position of null AST.");
      return sb.toString();
    }
    sb.append(ast.getLine());
    sb.append(":");
    sb.append(ast.getCharPositionInLine());
    sb.append(" ");
    sb.append(message);
    sb.append(". Error encountered near token '");
    sb.append(ASTErrorUtils.getText(ast));
    sb.append("'");
    return sb.toString();
  }

  ASTNode getAST() {
    return this.ast;
  }

  protected void setAST(ASTNode newAST) {
    this.ast = newAST;
  }

  private String findSimpleTableName(ASTNode tabref, int aliasIndex) throws SemanticException {
    assert tabref.getType() == HiveParser.TOK_TABREF;
    ASTNode tableTree = (ASTNode) (tabref.getChild(0));

    String alias;
    if (aliasIndex != 0) {
      alias = unescapeIdentifier(tabref.getChild(aliasIndex).getText());
    }
    else {
      alias = getUnescapedUnqualifiedTableName(tableTree);
    }
    return alias;
  }
  /**
   * Goes though the tabref tree and finds the alias for the table. Once found,
   * it records the table name-> alias association in aliasToTabs. It also makes
   * an association from the alias to the table AST in parse info.
   *
   * @return the alias of the table
   */
  private String processTable(QB qb, ASTNode tabref) throws SemanticException {
    // For each table reference get the table name
    // and the alias (if alias is not present, the table name
    // is used as an alias)
    int[] indexes = findTabRefIdxs(tabref);
    int aliasIndex = indexes[0];
    int propsIndex = indexes[1];
    int tsampleIndex = indexes[2];
    int ssampleIndex = indexes[3];
    int asOfTimeIndex = indexes[4];
    int asOfVersionIndex = indexes[5];
    int asOfVersionFromIndex = indexes[6];

    ASTNode tableTree = (ASTNode) (tabref.getChild(0));

    String tabIdName = HiveUtils.getLowerCaseTableName(getUnescapedName(tableTree));

    String alias = findSimpleTableName(tabref, aliasIndex);

    if (propsIndex >= 0) {
      Tree propsAST = tabref.getChild(propsIndex);
      Map<String, String> props = getProps((ASTNode) propsAST.getChild(0));
      // We get the information from Calcite.
      if ("TRUE".equals(props.get("insideView"))) {
        qb.getAliasInsideView().add(alias.toLowerCase());
      }
      qb.setTabProps(alias, props);
    }

    if (asOfTimeIndex != -1 || asOfVersionIndex != -1 || asOfVersionFromIndex != -1) {
      String asOfVersion = asOfVersionIndex == -1 ? null : getAsOfValue(tabref, asOfVersionIndex);
      String asOfVersionFrom =
          asOfVersionFromIndex == -1 ? null : tabref.getChild(asOfVersionFromIndex).getChild(0).getText();
      String asOfTime = asOfTimeIndex == -1 ? null : getAsOfValue(tabref, asOfTimeIndex);
      qb.setSystemVersion(alias, new QBSystemVersion(asOfVersion, asOfVersionFrom, asOfTime));
    }

    // If the alias is already there then we have a conflict
    if (qb.exists(alias)) {
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(),
          tabref.getChild(aliasIndex)));
    }
    if (tsampleIndex >= 0) {
      ASTNode sampleClause = (ASTNode) tabref.getChild(tsampleIndex);
      List<ASTNode> sampleCols = new ArrayList<ASTNode>();
      if (sampleClause.getChildCount() > 2) {
        for (int i = 2; i < sampleClause.getChildCount(); i++) {
          sampleCols.add((ASTNode) sampleClause.getChild(i));
        }
      }
      // TODO: For now only support sampling on up to two columns
      // Need to change it to list of columns
      if (sampleCols.size() > 2) {
        throw new SemanticException(generateErrorMessage(
            (ASTNode) tabref.getChild(0),
            ErrorMsg.SAMPLE_RESTRICTION.getMsg()));
      }
      TableSample tabSample = new TableSample(
          unescapeIdentifier(sampleClause.getChild(0).getText()),
          unescapeIdentifier(sampleClause.getChild(1).getText()),
          sampleCols);
      qb.getParseInfo().setTabSample(alias, tabSample);
      if (unparseTranslator.isEnabled()) {
        for (ASTNode sampleCol : sampleCols) {
          unparseTranslator.addIdentifierTranslation((ASTNode) sampleCol
              .getChild(0));
        }
      }
    } else if (ssampleIndex >= 0) {
      ASTNode sampleClause = (ASTNode) tabref.getChild(ssampleIndex);

      Tree type = sampleClause.getChild(0);
      Tree numerator = sampleClause.getChild(1);
      String value = unescapeIdentifier(numerator.getText());


      SplitSample sample;
      if (type.getType() == HiveParser.TOK_PERCENT) {
        assertCombineInputFormat(numerator, "Percentage");
        double percent = Double.valueOf(value);
        if (percent < 0  || percent > 100) {
          throw new SemanticException(generateErrorMessage((ASTNode) numerator,
              "Sampling percentage should be between 0 and 100"));
        }
        int seedNum = conf.getIntVar(ConfVars.HIVE_SAMPLE_RANDOM_NUM);
        sample = new SplitSample(percent, seedNum);
      } else if (type.getType() == HiveParser.TOK_ROWCOUNT) {
        sample = new SplitSample(Integer.parseInt(value));
      } else {
        assert type.getType() == HiveParser.TOK_LENGTH;
        assertCombineInputFormat(numerator, "Total Length");
        long length = Integer.parseInt(value.substring(0, value.length() - 1));
        char last = value.charAt(value.length() - 1);
        if (last == 'k' || last == 'K') {
          length <<= 10;
        } else if (last == 'm' || last == 'M') {
          length <<= 20;
        } else if (last == 'g' || last == 'G') {
          length <<= 30;
        }
        int seedNum = conf.getIntVar(ConfVars.HIVE_SAMPLE_RANDOM_NUM);
        sample = new SplitSample(length, seedNum);
      }
      String alias_id = getAliasId(alias, qb);
      nameToSplitSample.put(alias_id, sample);
    }
    // Insert this map into the stats
    qb.setTabAlias(alias, tabIdName);
    if (qb.isInsideView()) {
      qb.getAliasInsideView().add(alias.toLowerCase());
    }
    qb.addAlias(alias);

    qb.getParseInfo().setSrcForAlias(alias, tableTree);

    // if alias to CTE contains the table name, we do not do the translation because
    // cte is actually a subquery.
    if (!this.aliasToCTEs.containsKey(tabIdName)) {
      unparseTranslator.addTableNameTranslation(tableTree, SessionState.get().getCurrentDatabase());
      if (aliasIndex != 0) {
        unparseTranslator.addIdentifierTranslation((ASTNode) tabref.getChild(aliasIndex));
      }
    }

    return alias;
  }

  private String getAsOfValue(ASTNode tabref, int asOfIndex) throws SemanticException {
    String asOfValue = null;
    if (asOfIndex != -1) {
      ASTNode expr = (ASTNode) tabref.getChild(asOfIndex).getChild(0);
      if (expr.getChildCount() > 0) {
        ExprNodeDesc desc = genExprNodeDesc(expr, new RowResolver(), false, true);
        ExprNodeConstantDesc c = (ExprNodeConstantDesc) desc;
        asOfValue = String.valueOf(c.getValue());
      } else {
        asOfValue = stripQuotes(expr.getText());
      }
    }
    return asOfValue;
  }

  Map<String, SplitSample> getNameToSplitSampleMap() {
    return this.nameToSplitSample;
  }

  private void assertCombineInputFormat(Tree numerator, String message) throws SemanticException {
    String inputFormat = conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez") ?
        HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_TEZ_INPUT_FORMAT):
        HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_INPUT_FORMAT);
    if (!inputFormat.equals(CombineHiveInputFormat.class.getName())) {
      throw new SemanticException(generateErrorMessage((ASTNode) numerator,
          message + " sampling is not supported in " + inputFormat));
    }
  }

  private String processSubQuery(QB qb, ASTNode subq) throws SemanticException {

    // This is a subquery and must have an alias
    if (subq.getChildCount() != 2) {
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.NO_SUBQUERY_ALIAS.getMsg(), subq));
    }
    ASTNode subqref = (ASTNode) subq.getChild(0);
    String alias = unescapeIdentifier(subq.getChild(1).getText());

    // Recursively do the first phase of semantic analysis for the subquery
    QBExpr qbexpr = new QBExpr(alias, subqref);

    doPhase1QBExpr(subqref, qbexpr, qb.getId(), alias, qb.isInsideView(), null);

    // If the alias is already there then we have a conflict
    if (qb.exists(alias)) {
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(),
          subq.getChild(1)));
    }
    // Insert this map into the stats
    qb.setSubqAlias(alias, qbexpr);
    qb.addAlias(alias);

    unparseTranslator.addIdentifierTranslation((ASTNode) subq.getChild(1));

    return alias;
  }

  private void processLateralViewSelect(ASTNode lateralViewSelect) throws SemanticException {
    ASTNode selExprToken = (ASTNode) lateralViewSelect.getChild(0);
    if (selExprToken.getToken().getType() != HiveParser.TOK_SELEXPR) {
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.LATERAL_VIEW_INVALID_CHILD.getMsg(), selExprToken));
    }
    for (Object o : selExprToken.getChildren()) {
      ASTNode node = (ASTNode) o;
      switch (node.getToken().getType()) {
        case HiveParser.TOK_FUNCTION:
          break;
        case HiveParser.Identifier:
          unparseTranslator.addIdentifierTranslation(node);
          break;
        case HiveParser.TOK_TABALIAS:
          unparseTranslator.addIdentifierTranslation((ASTNode) node.getChild(0));
          break;
        default:
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.LATERAL_VIEW_INVALID_CHILD.getMsg(), selExprToken));
      }
    }
  }

  /*
   * Phase1: hold onto any CTE definitions in aliasToCTE.
   * CTE definitions are global to the Query.
   */
  private void processCTE(QB qb, ASTNode ctes, Map<String, CTEClause> aliasToCTEs) throws SemanticException {

    int numCTEs = ctes.getChildCount();

    for(int i=0; i <numCTEs; i++) {
      ASTNode cte = (ASTNode) ctes.getChild(i);
      ASTNode cteQry = (ASTNode) cte.getChild(0);
      String alias = unescapeIdentifier(cte.getChild(1).getText());
      ASTNode withColList = cte.getChildCount() == 3 ? (ASTNode) cte.getChild(2) : null;
      String qName = getAliasId(alias, qb);

      if ( aliasToCTEs.containsKey(qName)) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(),
            cte.getChild(1)));
      }
      aliasToCTEs.put(qName, new CTEClause(qName, cteQry, withColList));
    }
  }

  /*
   * We allow CTE definitions in views. So we can end up with a hierarchy of CTE definitions:
   * - at the top level of a query statement
   * - where a view is referenced.
   * - views may refer to other views.
   *
   * The scoping rules we use are: to search for a CTE from the current QB outwards. In order to
   * disambiguate between CTES are different levels we qualify(prefix) them with the id of the QB
   * they appear in when adding them to the <code>aliasToCTEs</code> map.
   *
   */
  private CTEClause findCTEFromName(QB qb, String cteName, Map<String, CTEClause> aliasToCTEs) {
    StringBuilder qId = new StringBuilder();
    if (qb.getId() != null) {
      qId.append(qb.getId());
    }

    while (qId.length() > 0) {
      String nm = qId + ":" + cteName;
      CTEClause cte = aliasToCTEs.get(nm);
      if (cte != null) {
        return cte;
      }
      int lastIndex = qId.lastIndexOf(":");
      lastIndex = lastIndex < 0 ? 0 : lastIndex;
      qId.setLength(lastIndex);
    }
    return aliasToCTEs.get(cteName);
  }

  /*
   * If a CTE is referenced in a QueryBlock:
   * - add it as a SubQuery for now.
   *   - SQ.alias is the alias used in QB. (if no alias is specified,
   *     it used the CTE name. Works just like table references)
   *   - Adding SQ done by:
   *     - copying AST of CTE
   *     - setting ASTOrigin on cloned AST.
   *   - trigger phase 1 on new QBExpr.
   *   - update QB data structs: remove this as a table reference, move it to a SQ invocation.
   */
  private void addCTEAsSubQuery(QB qb, String cteName, String cteAlias)
      throws SemanticException {
    cteAlias = cteAlias == null ? cteName : cteAlias;
    CTEClause cte = findCTEFromName(qb, cteName, aliasToCTEs);
    ASTNode cteQryNode = cte.cteNode;
    QBExpr cteQBExpr = new QBExpr(cteAlias);
    doPhase1QBExpr(cteQryNode, cteQBExpr, qb.getId(), cteAlias, cte.withColList);
    qb.rewriteCTEToSubq(cteAlias, cteName, cteQBExpr);
  }

  private final CTEClause rootClause = new CTEClause(null, null, null);

  @Override
  public List<Task<?>> getAllRootTasks() {
    if (!rootTasksResolved) {
      LinkedHashMap<CTEClause, LinkedHashSet<CTEClause>> realDependencies = listRealDependencies();
      linkRealDependencies(realDependencies);
      rootTasks = toRealRootTasks(realDependencies);
      rootTasksResolved = true;
    }
    return rootTasks;
  }

  @Override
  public Set<ReadEntity> getAllInputs() {
    Set<ReadEntity> readEntities = new HashSet<ReadEntity>(getInputs());
    for (CTEClause cte : rootClause.asExecutionOrder()) {
      if (cte.source != null) {
        readEntities.addAll(cte.source.getInputs());
      }
    }
    return readEntities;
  }

  @Override
  public Set<WriteEntity> getAllOutputs() {
    Set<WriteEntity> writeEntities = new HashSet<WriteEntity>(getOutputs());
    for (CTEClause cte : rootClause.asExecutionOrder()) {
      if (cte.source != null) {
        writeEntities.addAll(cte.source.getOutputs());
      }
    }
    return writeEntities;
  }

  class CTEClause {
    CTEClause(String alias, ASTNode cteNode, ASTNode withColList) {
      this.alias = alias;
      this.cteNode = cteNode;
      this.withColList = withColList;
    }
    String alias;
    ASTNode cteNode;
    ASTNode withColList;
    boolean materialize;
    int reference;
    QBExpr qbExpr;
    List<CTEClause> parents = new ArrayList<CTEClause>();

    // materialized
    SemanticAnalyzer source;

    List<Task<?>> getTasks() {
      return source == null ? null : source.rootTasks;
    }

    List<CTEClause> asExecutionOrder() {
      List<CTEClause> execution = new ArrayList<CTEClause>();
      asExecutionOrder(new HashSet<CTEClause>(), execution);
      return execution;
    }

    void asExecutionOrder(Set<CTEClause> visited, List<CTEClause> execution) {
      for (CTEClause parent : parents) {
        if (visited.add(parent)) {
          parent.asExecutionOrder(visited, execution);
        }
      }
      execution.add(this);
    }

    @Override
    public String toString() {
      return alias == null ? "<root>" : alias;
    }
  }

  private List<Task<?>> getRealTasks(CTEClause cte) {
    if (cte == rootClause) {
      return rootTasks;
    } else {
      return cte.getTasks();
    }
  }

  /**
   * Links tasks based on dependencies among CTEs which have actual tasks.
   * For example, when materialized CTE X depends on materialized CTE Y,
   * the leaf tasks of Y must have the root tasks of X as its child tasks.
   */
  private void linkRealDependencies(LinkedHashMap<CTEClause, LinkedHashSet<CTEClause>> realDependencies) {
    LinkedHashMap<CTEClause, List<Task<?>>> dependentTasks = new LinkedHashMap<>();
    for (CTEClause child : realDependencies.keySet()) {
      for (CTEClause parent : realDependencies.get(child)) {
        if (!dependentTasks.containsKey(parent)) {
          dependentTasks.put(parent, new ArrayList<>());
        }
        dependentTasks.get(parent).addAll(getRealTasks(child));
      }
    }
    // This operation must be performed only once per CTE since it creates new leaves
    for (CTEClause parent : dependentTasks.keySet()) {
      List<Task<?>> sources = Task.findLeafs(getRealTasks(parent));
      linkTasks(sources, dependentTasks.get(parent));
    }
  }

  private static void linkTasks(List<Task<?>> sources, Iterable<Task<?>> sinks) {
    for (Task<?> source : sources) {
      for (Task<?> sink : sinks) {
        source.addDependentTask(sink);
      }
    }
  }

  // Returns tasks which have no dependencies and can start without waiting for any tasks
  private List<Task<?>> toRealRootTasks(LinkedHashMap<CTEClause, LinkedHashSet<CTEClause>> realDependencies) {
    List<Task<?>> realRootTasks = new ArrayList<>();
    for (CTEClause cte : realDependencies.keySet()) {
      if (realDependencies.get(cte).isEmpty()) {
        realRootTasks.addAll(getRealTasks(cte));
      }
    }
    return realRootTasks;
  }

  // child with tasks -> list of parents with tasks
  private LinkedHashMap<CTEClause, LinkedHashSet<CTEClause>> listRealDependencies() {
    LinkedHashMap<CTEClause, LinkedHashSet<CTEClause>> realDependencies = new LinkedHashMap<>();
    for (CTEClause child : rootClause.asExecutionOrder()) {
      if (getRealTasks(child) == null) {
        // This CTE will be executed as a part of other CTEs or a root statement
        continue;
      }
      LinkedHashSet<CTEClause> parents = new LinkedHashSet<>();
      collectRealDependencies(child, parents);
      realDependencies.put(child, parents);
    }
    return realDependencies;
  }

  private void collectRealDependencies(CTEClause cte, LinkedHashSet<CTEClause> realDependencies) {
    for (CTEClause parent : cte.parents) {
      if (getRealTasks(parent) == null) {
        collectRealDependencies(parent, realDependencies);
      } else {
        realDependencies.add(parent);
      }
    }
  }

  Table materializeCTE(String cteName, CTEClause cte) throws HiveException {

    ASTNode createTable = new ASTNode(new ClassicToken(HiveParser.TOK_CREATETABLE));

    ASTNode tableName = new ASTNode(new ClassicToken(HiveParser.TOK_TABNAME));
    tableName.addChild(new ASTNode(new ClassicToken(HiveParser.Identifier, cteName)));

    ASTNode temporary = new ASTNode(new ClassicToken(HiveParser.KW_TEMPORARY, MATERIALIZATION_MARKER));

    createTable.addChild(tableName);
    createTable.addChild(temporary);
    createTable.addChild(cte.cteNode);

    SemanticAnalyzer analyzer = new SemanticAnalyzer(queryState);
    analyzer.initCtx(ctx);
    analyzer.init(false);

    // should share cte contexts
    analyzer.aliasToCTEs.putAll(aliasToCTEs);

    HiveOperation operation = queryState.getHiveOperation();
    try {
      analyzer.analyzeInternal(createTable);
    } finally {
      queryState.setCommandType(operation);
    }

    Table table = analyzer.tableDesc.toTable(conf);
    Path location = table.getDataLocation();
    try {
      location.getFileSystem(conf).mkdirs(location);
    } catch (IOException e) {
      throw new HiveException(e);
    }
    table.setMaterializedTable(true);

    LOG.info("{} will be materialized into {}", cteName, location);
    cte.source = analyzer;

    ctx.addMaterializedTable(cteName, table, getMaterializedTableStats(analyzer.getSinkOp()));

    return table;
  }

  protected Statistics getMaterializedTableStats(Operator<?> sinkOp) {
    final Statistics tableStats = sinkOp.getStatistics().clone();
    if (tableStats.getColumnStatsState() == Statistics.State.NONE || sinkOp.getNumParent() == 0) {
      return tableStats;
    }

    final List<String> parentColumnNames = sinkOp.getParentOperators().get(0).getSchema().getColumnNames();
    final List<String> childColumnNames = sinkOp.getSchema().getColumnNames();
    if (parentColumnNames.size() != childColumnNames.size()) {
      LOG.warn("The number of columns of FileSinkOperator is inconsistent. Parent = {}, Child = {}",
          parentColumnNames, childColumnNames);
      tableStats.setColumnStatsState(Statistics.State.NONE);
      return tableStats;
    }
    final Map<String, String> mapping = new HashMap<>(parentColumnNames.size());
    for (int i = 0; i < parentColumnNames.size(); i++) {
      mapping.put(parentColumnNames.get(i), childColumnNames.get(i));
    }

    final List<ColStatistics> colStatsList = tableStats.getColumnStats();
    if (!mapping.keySet().equals(colStatsList.stream().map(ColStatistics::getColumnName).collect(Collectors.toSet()))) {
      LOG.warn("The column statistics are inconsistent with the expected column names. Actual = {}, Expected = {}",
          colStatsList, parentColumnNames);
      tableStats.setColumnStatsState(Statistics.State.NONE);
      return tableStats;
    }
    for (ColStatistics colStats : colStatsList) {
      colStats.setColumnName(mapping.get(colStats.getColumnName()));
    }
    tableStats.setColumnStats(colStatsList);
    return tableStats;
  }


  static boolean isJoinToken(ASTNode node) {
    return (node.getToken().getType() == HiveParser.TOK_JOIN)
        || (node.getToken().getType() == HiveParser.TOK_CROSSJOIN)
        || isOuterJoinToken(node)
        || (node.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN)
        || (node.getToken().getType() == HiveParser.TOK_LEFTANTISEMIJOIN)
        || (node.getToken().getType() == HiveParser.TOK_UNIQUEJOIN);
  }

  static private boolean isOuterJoinToken(ASTNode node) {
    return (node.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN);
  }

  /**
   * Given the AST with TOK_JOIN as the root, get all the aliases for the tables
   * or subqueries in the join.
   *
   * @param qb
   * @param join
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private void processJoin(QB qb, ASTNode join) throws SemanticException {
    int numChildren = join.getChildCount();
    if ((numChildren != 2) && (numChildren != 3) && (numChildren != 4)
        && join.getToken().getType() != HiveParser.TOK_UNIQUEJOIN) {
      throw new SemanticException(generateErrorMessage(join,
          "Join with multiple children"));
    }

    queryProperties.incrementJoinCount(isOuterJoinToken(join));
    for (int num = 0; num < numChildren; num++) {
      ASTNode child = (ASTNode) join.getChild(num);
      if (child.getToken().getType() == HiveParser.TOK_TABREF) {
        processTable(qb, child);
      } else if (child.getToken().getType() == HiveParser.TOK_SUBQUERY) {
        processSubQuery(qb, child);
      } else if (child.getToken().getType() == HiveParser.TOK_PTBLFUNCTION) {
        queryProperties.setHasPTF(true);
        processPTF(qb, child);
        PTFInvocationSpec ptfInvocationSpec = qb.getPTFInvocationSpec(child);
        String inputAlias = ptfInvocationSpec == null ? null :
            ptfInvocationSpec.getFunction().getAlias();;
        if ( inputAlias == null ) {
          throw new SemanticException(generateErrorMessage(child,
              "PTF invocation in a Join must have an alias"));
        }

      } else if (child.getToken().getType() == HiveParser.TOK_LATERAL_VIEW ||
          child.getToken().getType() == HiveParser.TOK_LATERAL_VIEW_OUTER) {
        // SELECT * FROM src1 LATERAL VIEW udtf() AS myTable JOIN src2 ...
        // is not supported. Instead, the lateral view must be in a subquery
        // SELECT * FROM (SELECT * FROM src1 LATERAL VIEW udtf() AS myTable) a
        // JOIN src2 ...
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.LATERAL_VIEW_WITH_JOIN.getMsg(), join));
      } else if (isJoinToken(child)) {
        processJoin(qb, child);
      }
    }
  }

  /**
   * Given the AST with TOK_LATERAL_VIEW as the root, get the alias for the
   * table or subquery in the lateral view and also make a mapping from the
   * alias to all the lateral view AST's.
   *
   * @param qb
   * @param lateralView
   * @return the alias for the table/subquery
   * @throws SemanticException
   */

  private String processLateralView(QB qb, ASTNode lateralView)
      throws SemanticException {
    int numChildren = lateralView.getChildCount();
    assert (numChildren == 2);

    if (!isCBOSupportedLateralView(lateralView)) {
      queryProperties.setCBOSupportedLateralViews(false);
    }

    ASTNode next = (ASTNode) lateralView.getChild(1);
    String alias = null;
    switch (next.getToken().getType()) {
    case HiveParser.TOK_TABREF:
      alias = processTable(qb, next);
      break;
    case HiveParser.TOK_SUBQUERY:
      alias = processSubQuery(qb, next);
      break;
    case HiveParser.TOK_LATERAL_VIEW:
    case HiveParser.TOK_LATERAL_VIEW_OUTER:
      alias = processLateralView(qb, next);
      break;
    default:
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.LATERAL_VIEW_INVALID_CHILD.getMsg(), lateralView));
    }
    processLateralViewSelect((ASTNode) lateralView.getChild(0));
    alias = alias.toLowerCase();
    qb.getParseInfo().addLateralViewForAlias(alias, lateralView);
    qb.addAlias(alias);
    return alias;
  }

  @SuppressWarnings({"fallthrough", "nls"})
  boolean doPhase1(ASTNode ast, QB qb, Phase1Ctx ctx_1, PlannerContext plannerCtx)
      throws SemanticException {
    return doPhase1(ast, qb, ctx_1, plannerCtx, this.aliasToCTEs);
  }

  /**
   * Phase 1: (including, but not limited to):
   *
   * 1. Gets all the aliases for all the tables / subqueries and makes the
   * appropriate mapping in aliasToTabs, aliasToSubq 2. Gets the location of the
   * destination and names the clause "inclause" + i 3. Creates a map from a
   * string representation of an aggregation tree to the actual aggregation AST
   * 4. Creates a mapping from the clause name to the select expression AST in
   * destToSelExpr 5. Creates a mapping from a table alias to the lateral view
   * AST's in aliasToLateralViews
   *
   * @param ast
   * @param qb
   * @param ctx_1
   * @throws SemanticException
   */
  @SuppressWarnings({"fallthrough", "nls"})
  boolean doPhase1(ASTNode ast, QB qb, Phase1Ctx ctx_1, PlannerContext plannerCtx, Map<String, CTEClause> aliasToCTEs)
      throws SemanticException {

    boolean phase1Result = true;
    QBParseInfo qbp = qb.getParseInfo();
    boolean skipRecursion = false;

    if (ast.getToken() != null) {
      skipRecursion = true;
      switch (ast.getToken().getType()) {
      case HiveParser.TOK_SELECTDI:
        qb.countSelDi();
        // fall through
      case HiveParser.TOK_SELECT:
        qb.countSel();
        qbp.setSelExprForClause(ctx_1.dest, ast);

        int posn = 0;
        if (((ASTNode) ast.getChild(0)).getType() == HiveParser.QUERY_HINT) {
          posn = processQueryHint((ASTNode)ast.getChild(0), qbp, posn);
        }

        if ((ast.getChild(posn).getChild(0).getType() == HiveParser.TOK_TRANSFORM)) {
          queryProperties.setUsesScript(true);
        }

        Map<String, ASTNode> aggregations = doPhase1GetAggregationsFromSelect(ast, qb, ctx_1.dest);
        doPhase1GetColumnAliasesFromSelect(ast, qbp, ctx_1.dest);
        qbp.setAggregationExprsForClause(ctx_1.dest, aggregations);
        qbp.setDistinctFuncExprsForClause(ctx_1.dest,
            doPhase1GetDistinctFuncExprs(aggregations));
        break;

      case HiveParser.TOK_WHERE:
        qbp.setWhrExprForClause(ctx_1.dest, ast);
        if (!SubQueryUtils.findSubQueries((ASTNode) ast.getChild(0)).isEmpty()) {
          queryProperties.setFilterWithSubQuery(true);
        }
        doPhase1WhereClause(ast, qb);
        break;

      case HiveParser.TOK_INSERT_INTO:
        String currentDatabase = SessionState.get().getCurrentDatabase();
        String tab_name = getUnescapedName((ASTNode) ast.getChild(0).getChild(0), currentDatabase);
        qbp.addInsertIntoTable(tab_name, ast);
        setSqlKind(SqlKind.INSERT);

      case HiveParser.TOK_DESTINATION:
        ctx_1.dest = this.ctx.getDestNamePrefix(ast, qb).toString() + ctx_1.nextNum;
        ctx_1.nextNum++;
        boolean isTmpFileDest = false;
        if (ast.getChildCount() > 0 && ast.getChild(0) instanceof ASTNode) {
          ASTNode ch = (ASTNode) ast.getChild(0);
          if (ch.getToken().getType() == HiveParser.TOK_DIR && ch.getChildCount() > 0
              && ch.getChild(0) instanceof ASTNode) {
            ch = (ASTNode) ch.getChild(0);
            isTmpFileDest = ch.getToken().getType() == HiveParser.TOK_TMP_FILE;
            if (ch.getToken().getType() == HiveParser.StringLiteral) {
              qbp.setInsertOverwriteDirectory(true);
              // set DML for IOWD here as that's not covered by other codepaths
              queryProperties.setQueryType(QueryProperties.QueryType.DML);
              setSqlKind(SqlKind.INSERT);
            }
          } else {
            if (ast.getToken().getType() == HiveParser.TOK_DESTINATION
                && ast.getChild(0).getType() == HiveParser.TOK_TAB) {
              String fullTableName = getUnescapedName((ASTNode) ast.getChild(0).getChild(0),
                  SessionState.get().getCurrentDatabase());
              qbp.getInsertOverwriteTables().put(fullTableName.toLowerCase(), ast);
              qbp.setDestToOpType(ctx_1.dest, true);
              setSqlKind(SqlKind.INSERT);
            }
          }
        }

        // is there a insert in the subquery
        if (qbp.getIsSubQ() && !isTmpFileDest) {
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.NO_INSERT_INSUBQUERY.getMsg(), ast));
        }

        qbp.setDestForClause(ctx_1.dest, (ASTNode) ast.getChild(0));
        handleInsertStatementSpecPhase1(ast, qbp, ctx_1);

        if (qbp.getClauseNamesForDest().size() == 2) {
          // From the moment that we have two destination clauses,
          // we know that this is a multi-insert query.
          // Thus, set property to right value.
          // Using qbp.getClauseNamesForDest().size() >= 2 would be
          // equivalent, but we use == to avoid setting the property
          // multiple times
          queryProperties.setMultiDestQuery(true);
        }

        if (plannerCtx != null && !queryProperties.hasMultiDestQuery()) {
          plannerCtx.setInsertToken(ast, isTmpFileDest);
        } else if (plannerCtx != null && qbp.getClauseNamesForDest().size() == 2) {
          // For multi-insert query, currently we only optimize the FROM clause.
          // Hence, introduce multi-insert token on top of it.
          // However, first we need to reset existing token (insert).
          // Using qbp.getClauseNamesForDest().size() >= 2 would be
          // equivalent, but we use == to avoid setting the property
          // multiple times
          plannerCtx.resetToken();
          plannerCtx.setMultiInsertToken((ASTNode) qbp.getQueryFrom().getChild(0));
        }
        break;

      case HiveParser.TOK_FROM:
        int child_count = ast.getChildCount();
        if (child_count != 1) {
          throw new SemanticException(generateErrorMessage(ast,
              "Multiple Children " + child_count));
        }

        if (!qbp.getIsSubQ()) {
          qbp.setQueryFromExpr(ast);
        }

        // Check if this is a subquery / lateral view
        ASTNode frm = (ASTNode) ast.getChild(0);
        if (frm.getToken().getType() == HiveParser.TOK_TABREF) {
          processTable(qb, frm);
        } else if (frm.getToken().getType() == HiveParser.TOK_SUBQUERY) {
          processSubQuery(qb, frm);
        } else if (frm.getToken().getType() == HiveParser.TOK_LATERAL_VIEW ||
            frm.getToken().getType() == HiveParser.TOK_LATERAL_VIEW_OUTER) {
          queryProperties.setHasLateralViews(true);
          processLateralView(qb, frm);
        } else if (isJoinToken(frm)) {
          processJoin(qb, frm);
          qbp.setJoinExpr(frm);
        }else if(frm.getToken().getType() == HiveParser.TOK_PTBLFUNCTION){
          queryProperties.setHasPTF(true);
          processPTF(qb, frm);
        }
        break;

      case HiveParser.TOK_CLUSTERBY:
        // Get the clusterby aliases - these are aliased to the entries in the
        // select list
        queryProperties.setHasClusterBy(true);
        qbp.setClusterByExprForClause(ctx_1.dest, ast);
        break;

      case HiveParser.TOK_DISTRIBUTEBY:
        // Get the distribute by aliases - these are aliased to the entries in
        // the select list
        queryProperties.setHasDistributeBy(true);
        qbp.setDistributeByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.CLUSTERBY_DISTRIBUTEBY_CONFLICT.getMsg()));
        } else if (qbp.getOrderByForClause(ctx_1.dest) != null) {
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.ORDERBY_DISTRIBUTEBY_CONFLICT.getMsg()));
        }
        break;

      case HiveParser.TOK_SORTBY:
        // Get the sort by aliases - these are aliased to the entries in the
        // select list
        queryProperties.setHasSortBy(true);
        qbp.setSortByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.CLUSTERBY_SORTBY_CONFLICT.getMsg()));
        } else if (qbp.getOrderByForClause(ctx_1.dest) != null) {
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.ORDERBY_SORTBY_CONFLICT.getMsg()));
        }

        break;

      case HiveParser.TOK_ORDERBY:
        // Get the order by aliases - these are aliased to the entries in the
        // select list
        queryProperties.setHasOrderBy(true);
        qbp.setOrderByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.CLUSTERBY_ORDERBY_CONFLICT.getMsg()));
        }
        // If there are aggregations in order by, we need to remember them in qb.
        qbp.addAggregationExprsForClause(ctx_1.dest,
            doPhase1GetAggregationsFromSelect(ast, qb, ctx_1.dest));
        break;

      case HiveParser.TOK_GROUPBY:
      case HiveParser.TOK_ROLLUP_GROUPBY:
      case HiveParser.TOK_CUBE_GROUPBY:
      case HiveParser.TOK_GROUPING_SETS:
        // Get the groupby aliases - these are aliased to the entries in the
        // select list
        queryProperties.setHasGroupBy(true);
        if (qbp.getJoinExpr() != null) {
          queryProperties.setHasJoinFollowedByGroupBy(true);
        }
        qbp.setGroupByExprForClause(ctx_1.dest, ast);
        skipRecursion = true;

        // Rollup and Cubes are syntactic sugar on top of grouping sets
        if (ast.getToken().getType() == HiveParser.TOK_ROLLUP_GROUPBY) {
          qbp.getDestRollups().add(ctx_1.dest);
        } else if (ast.getToken().getType() == HiveParser.TOK_CUBE_GROUPBY) {
          qbp.getDestCubes().add(ctx_1.dest);
        } else if (ast.getToken().getType() == HiveParser.TOK_GROUPING_SETS) {
          qbp.getDestGroupingSets().add(ctx_1.dest);
        }
        break;

      case HiveParser.TOK_HAVING:
        qbp.setHavingExprForClause(ctx_1.dest, ast);
        qbp.addAggregationExprsForClause(ctx_1.dest,
            doPhase1GetAggregationsFromSelect(ast, qb, ctx_1.dest));
        // Clause might also refer to aggregations with distinct
        qbp.setDistinctFuncExprsForClause(ctx_1.dest,
            doPhase1GetDistinctFuncExprs(qbp.getAggregationExprsForClause(ctx_1.dest)));
        break;

      case HiveParser.TOK_QUALIFY:
        queryProperties.setHasQualify(true);
        qbp.setQualifyExprForClause(ctx_1.dest, ast);
        qbp.addAggregationExprsForClause(ctx_1.dest,
                doPhase1GetAggregationsFromSelect(ast, qb, ctx_1.dest));
        break;

      case HiveParser.KW_WINDOW:
        if (!qb.hasWindowingSpec(ctx_1.dest) ) {
          throw new SemanticException(generateErrorMessage(ast,
              "Query has no Cluster/Distribute By; but has a Window definition"));
        }
        handleQueryWindowClauses(qb, ctx_1, ast);
        break;

      case HiveParser.TOK_LIMIT:
        queryProperties.setHasLimit(true);
        if (ast.getChildCount() == 2) {
          qbp.setDestLimit(ctx_1.dest,
              Integer.valueOf(ast.getChild(0).getText()), Integer.valueOf(ast.getChild(1).getText()));
        } else {
          qbp.setDestLimit(ctx_1.dest, Integer.valueOf(0), Integer.valueOf(ast.getChild(0).getText()));
        }
        break;

      case HiveParser.TOK_ANALYZE:
        // Case of analyze command

        String table_name = getUnescapedName((ASTNode) ast.getChild(0).getChild(0)).toLowerCase();


        qb.setTabAlias(table_name, table_name);
        qb.addAlias(table_name);
        qb.getParseInfo().setIsAnalyzeCommand(true);
        qb.getParseInfo().setNoScanAnalyzeCommand(this.noscan);
        // Allow analyze the whole table and dynamic partitions
        HiveConf.setVar(conf, HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");
        HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_MAPRED_MODE, "nonstrict");

        break;

      case HiveParser.TOK_UNIONALL:
        if (!qbp.getIsSubQ()) {
          // this shouldn't happen. The parser should have converted the union to be
          // contained in a subquery. Just in case, we keep the error as a fallback.
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.UNION_NOTIN_SUBQ.getMsg()));
        }
        skipRecursion = false;
        break;

      case HiveParser.TOK_INSERT:
        ASTNode destination = (ASTNode) ast.getChild(0);
        Tree tab = destination.getChild(0);

        // Proceed if AST contains partition & If Not Exists
        if (destination.getChildCount() == 2 &&
            tab.getChildCount() == 2 &&
            destination.getChild(1).getType() == HiveParser.TOK_IFNOTEXISTS) {
          final String tableName = getUnescapedName((ASTNode) tab.getChild(0), SessionState.get().getCurrentDatabase());

          Tree partitions = tab.getChild(1);
          int childCount = partitions.getChildCount();
          Map<String, String> partition = new HashMap<String, String>();
          for (int i = 0; i < childCount; i++) {
            String partitionName = partitions.getChild(i).getChild(0).getText();
            // Convert to lowercase for the comparison
            partitionName = partitionName.toLowerCase();
            Tree pvalue = partitions.getChild(i).getChild(1);
            if (pvalue == null) {
              break;
            }
            String partitionVal = stripQuotes(pvalue.getText());
            partition.put(partitionName, partitionVal);
          }
          // if it is a dynamic partition throw the exception
          if (childCount != partition.size()) {
            throw new SemanticException(ErrorMsg.INSERT_INTO_DYNAMICPARTITION_IFNOTEXISTS
                .getMsg(partition.toString()));
          }
          Table table = null;
          try {
            table = getTableObjectByName(tableName);
          } catch (HiveException ex) {
            throw new SemanticException(ex);
          }
          try {
            Partition parMetaData = db.getPartition(table, partition, false);
            // Check partition exists if it exists skip the overwrite
            if (parMetaData != null) {
              phase1Result = false;
              skipRecursion = true;
              LOG.info("Partition already exists so insert into overwrite " +
                  "skipped for partition : {}", parMetaData);
              break;
            }
          } catch (HiveException e) {
            LOG.info("Error while getting metadata : ", e);
          }
          validatePartSpec(table, partition, (ASTNode)tab, conf, false);
        }
        skipRecursion = false;
        break;
      case HiveParser.TOK_LATERAL_VIEW:
      case HiveParser.TOK_LATERAL_VIEW_OUTER:
        // todo: nested LV
        assert ast.getChildCount() == 1;
        qb.getParseInfo().getDestToLateralView().put(ctx_1.dest, ast);
        break;
      case HiveParser.TOK_CTE:
        processCTE(qb, ast, aliasToCTEs);
        break;
      case HiveParser.QUERY_HINT:
          processQueryHint(ast, qbp, 0);
      default:
        skipRecursion = false;
        break;
      }
    }

    if (!skipRecursion) {
      // Iterate over the rest of the children
      int child_count = ast.getChildCount();
      for (int child_pos = 0; child_pos < child_count && phase1Result; ++child_pos) {
        // Recurse
        phase1Result = doPhase1((ASTNode) ast.getChild(child_pos), qb, ctx_1, plannerCtx, aliasToCTEs);
      }
    }
    return phase1Result;
  }

  private int processQueryHint(ASTNode ast, QBParseInfo qbp, int posn) throws SemanticException{
    ParseDriver pd = new ParseDriver();
    String queryHintStr = ast.getText();
    LOG.debug("QUERY HINT: {} ", queryHintStr);
    try {
      ASTNode hintNode = pd.parseHint(queryHintStr);
      qbp.setHints(hintNode);
    } catch (ParseException e) {
      throw new SemanticException("failed to parse query hint: "+e.getMessage(), e);
    }
    return posn + 1;
  }

  /**
   * This is phase1 of supporting specifying schema in insert statement
   * insert into foo(z,y) select a,b from bar;
   * @see #handleInsertStatementSpec(java.util.List, String, RowResolver, QB, ASTNode)
   * @throws SemanticException
   */
  private void handleInsertStatementSpecPhase1(ASTNode ast, QBParseInfo qbp, Phase1Ctx ctx_1) throws SemanticException {
    ASTNode tabColName = (ASTNode)ast.getChild(1);
    boolean hasSpecificColumns = tabColName != null && tabColName.getType() == HiveParser.TOK_TABCOLNAME;
    if(ast.getType() == HiveParser.TOK_INSERT_INTO) {
      //we have "insert into foo(a,b)..."; parser will enforce that 1+ columns are listed if TOK_TABCOLNAME is present
      String fullTableName = getUnescapedName((ASTNode) ast.getChild(0).getChild(0),
          SessionState.get().getCurrentDatabase());
      List<String> targetColumnNames = hasSpecificColumns ? processTableColumnNames(tabColName, fullTableName) : new ArrayList<>();
      if (hasSpecificColumns) {
        qbp.setDestSchemaForClause(ctx_1.dest, targetColumnNames);
      }
      Table targetTable;
      try {
        targetTable = getTableObjectByName(fullTableName);
      } catch (HiveException ex) {
        LOG.error("Error processing HiveParser.TOK_DESTINATION: " + ex.getMessage(), ex);
        throw new SemanticException(ex);
      }
      if(targetTable == null) {
        throw new SemanticException(generateErrorMessage(ast,
            "Unable to access metadata for table " + fullTableName));
      }
      ColumnAccessInfo cai = new ColumnAccessInfo();
      if (!hasSpecificColumns) {
        for (FieldSchema col : targetTable.getCols()) {
          targetColumnNames.add(col.getName());
        }
      }
      String completeTableName = targetTable.getCompleteName();
      for (String colName : targetColumnNames) {
        cai.add(completeTableName, colName);
      }
      setUpdateColumnAccessInfo(cai);
      Set<String> targetColumns = new HashSet<>(targetColumnNames);
      for(FieldSchema f : targetTable.getCols()) {
        //parser only allows foo(a,b), not foo(foo.a, foo.b)
        targetColumns.remove(f.getName());
      }
      if(!targetColumns.isEmpty()) {//here we need to see if remaining columns are dynamic partition columns
            /* We just checked the user specified schema columns among regular table column and found some which are not
            'regular'.  Now check is they are dynamic partition columns
              For dynamic partitioning,
              Given "create table multipart(a int, b int) partitioned by (c int, d int);"
              for "insert into multipart partition(c='1',d)(d,a) values(2,3);" we expect parse tree to look like this
               (TOK_INSERT_INTO
                (TOK_TAB
                  (TOK_TABNAME multipart)
                  (TOK_PARTSPEC
                    (TOK_PARTVAL c '1')
                    (TOK_PARTVAL d)
                  )
                )
                (TOK_TABCOLNAME d a)
               )*/
        List<String> dynamicPartitionColumns = new ArrayList<String>();
        if(ast.getChild(0) != null && ast.getChild(0).getType() == HiveParser.TOK_TAB) {
          ASTNode tokTab = (ASTNode)ast.getChild(0);
          ASTNode tokPartSpec = (ASTNode)tokTab.getFirstChildWithType(HiveParser.TOK_PARTSPEC);
          if(tokPartSpec != null) {
            for(Node n : tokPartSpec.getChildren()) {
              ASTNode tokPartVal = null;
              if(n instanceof ASTNode) {
                tokPartVal = (ASTNode)n;
              }
              if(tokPartVal != null && tokPartVal.getType() == HiveParser.TOK_PARTVAL && tokPartVal.getChildCount() == 1) {
                assert tokPartVal.getChild(0).getType() == HiveParser.Identifier :
                    "Expected column name; found tokType=" + tokPartVal.getType();
                dynamicPartitionColumns.add(tokPartVal.getChild(0).getText());
              }
            }
            for(String colName : dynamicPartitionColumns) {
              targetColumns.remove(colName);
            }
          } else  {
            // partition spec is not specified but column schema can have partitions specified
            for(FieldSchema f : targetTable.getPartCols()) {
              //parser only allows foo(a,b), not foo(foo.a, foo.b)
              targetColumns.remove(f.getName());
            }
          }
        }

        if(!targetColumns.isEmpty()) {
          //Found some columns in user specified schema which are neither regular not dynamic partition columns
          throw new SemanticException(generateErrorMessage(tabColName,
              "'" + (targetColumns.size() == 1 ? targetColumns.iterator().next() : targetColumns) +
                  "' in insert schema specification " + (targetColumns.size() == 1 ? "is" : "are") +
                  " not found among regular columns of " +
                  fullTableName + " nor dynamic partition columns."));
        }
      }
    }
  }

  protected List<String> processTableColumnNames(ASTNode tabColName, String tableName) throws SemanticException {
    if (tabColName == null) {
      return Collections.emptyList();
    }
    List<String> targetColNames = new ArrayList<>(tabColName.getChildren().size());
    for(Node col : tabColName.getChildren()) {
      assert ((ASTNode)col).getType() == HiveParser.Identifier :
          "expected token " + HiveParser.Identifier + " found " + ((ASTNode)col).getType();
      targetColNames.add(((ASTNode)col).getText().toLowerCase());
    }
    Set<String> targetColumns = new HashSet<>(targetColNames);
    if(targetColNames.size() != targetColumns.size()) {
      throw new SemanticException(generateErrorMessage(tabColName,
              "Duplicate column name detected in " + tableName + " table schema specification"));
    }
    return targetColNames;
  }

  private Map<String, CTEClause> getMaterializationMetadata(QB qb) throws SemanticException {
    if (qb.isCTAS()) {
      return null;
    }
    Map<String, CTEClause> materializationAliasToCTEs = new HashMap<>(this.aliasToCTEs);
    try {
      gatherCTEReferences(qb, rootClause, materializationAliasToCTEs);
      int threshold = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_CTE_MATERIALIZE_THRESHOLD);
      for (CTEClause cte : Sets.newHashSet(materializationAliasToCTEs.values())) {
        if (threshold >= 0 && cte.reference >= threshold) {
          cte.materialize = !HiveConf.getBoolVar(conf, ConfVars.HIVE_CTE_MATERIALIZE_FULL_AGGREGATE_ONLY)
              || cte.qbExpr.getQB().getParseInfo().isFullyAggregate();
        }
      }
    } catch (HiveException e) {
      LOG.error("Failed to get Materialization Metadata", e);
      if (e instanceof SemanticException) {
        throw (SemanticException)e;
      }
      throw new SemanticException(e.getMessage(), e);
    }
    return materializationAliasToCTEs;
  }

  private void gatherCTEReferences(QBExpr qbexpr, CTEClause parent,
                                   Map<String, CTEClause> materializationAliasToCTEs) throws HiveException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      gatherCTEReferences(qbexpr.getQB(), parent, materializationAliasToCTEs);
    } else {
      gatherCTEReferences(qbexpr.getQBExpr1(), parent, materializationAliasToCTEs);
      gatherCTEReferences(qbexpr.getQBExpr2(), parent, materializationAliasToCTEs);
    }
  }

  // TODO: check view references, too
  private void gatherCTEReferences(QB qb, CTEClause current,
                                   Map<String, CTEClause> materializationAliasToCTEs) throws HiveException {
    for (String alias : qb.getTabAliases()) {
      String tabName = qb.getTabNameForAlias(alias);
      String cteName = tabName.toLowerCase();

      CTEClause cte = findCTEFromName(qb, cteName, materializationAliasToCTEs);
      if (cte != null) {
        cte.reference++;
        current.parents.add(cte);
        if (cte.qbExpr != null) {
          continue;
        }
        cte.qbExpr = new QBExpr(cteName);
        doPhase1QBExpr(cte.cteNode, cte.qbExpr, qb.getId(), cteName, cte.withColList, materializationAliasToCTEs);
        gatherCTEReferences(cte.qbExpr, cte, materializationAliasToCTEs);
      }
    }
    for (String alias : qb.getSubqAliases()) {
      gatherCTEReferences(qb.getSubqForAlias(alias), current, materializationAliasToCTEs);
    }
    for (String alias : qb.getSubqExprAliases()) {
      gatherCTEReferences(qb.getSubqExprForAlias(alias), current, materializationAliasToCTEs);
    }
  }

  private void checkRecursiveCTE(CTEClause current, Set<String> path) throws SemanticException {

    for (CTEClause child : current.parents) {
      if (path.contains(child.alias)) {
        throw new SemanticException("Recursive cte " + child.alias +
            " detected (cycle: " + StringUtils.join(path, " -> ") +
            " -> " + child.alias + ").");
      }
      path.add(child.alias);
      checkRecursiveCTE(child, path);
      path.remove(child.alias);
    }
  }

  void getMetaData(QB qb) throws SemanticException {
    getMetaData(qb, false);
  }

  void getMetaData(QB qb, boolean enableMaterialization) throws SemanticException {
    try {
      Map<String, CTEClause> materializationAliasToCTEs = null;
      if (enableMaterialization) {
        materializationAliasToCTEs = getMaterializationMetadata(qb);
      }
      checkRecursiveCTE(rootClause, new HashSet<>());
      getMetaData(qb, null);
      if (materializationAliasToCTEs != null && !materializationAliasToCTEs.isEmpty()) {
        this.aliasToCTEs.putAll(materializationAliasToCTEs);
      }
    } catch (HiveException e) {
      if (e instanceof SemanticException) {
        throw (SemanticException)e;
      }
      throw new SemanticException(e.getMessage(), e);
    }
  }

  private void getMetaData(QBExpr qbexpr, ReadEntity parentInput)
      throws HiveException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      getMetaData(qbexpr.getQB(), parentInput);
    } else {
      getMetaData(qbexpr.getQBExpr1(), parentInput);
      getMetaData(qbexpr.getQBExpr2(), parentInput);
    }
  }

  @SuppressWarnings("nls")
  private void getMetaData(QB qb, ReadEntity parentInput)
      throws HiveException {
    LOG.info("Get metadata for source tables");

    // Go over the tables and populate the related structures.
    // We have to materialize the table alias list since we might
    // modify it in the middle for view rewrite.
    List<String> tabAliases = new ArrayList<String>(qb.getTabAliases());

    // Keep track of view alias to view name and read entity
    // For eg: for a query like 'select * from V3', where V3 -> V2, V2 -> V1, V1 -> T
    // keeps track of full view name and read entity corresponding to alias V3, V3:V2, V3:V2:V1.
    // This is needed for tracking the dependencies for inputs, along with their parents.
    Map<String, Pair<String, ReadEntity>> aliasToViewInfo =
        new HashMap<String, Pair<String, ReadEntity>>();

    /*
     * used to capture view to SQ conversions. This is used to check for
     * recursive CTE invocations.
     */
    Map<String, String> sqAliasToCTEName = new HashMap<String, String>();

    for (String alias : tabAliases) {
      String tabName = qb.getTabNameForAlias(alias);
      String cteName = tabName.toLowerCase();

      // Get table details from tabNameToTabObject cache
      Table tab = aliasToCTEs.containsKey(tabName)? null: getTableObjectByName(tabName, false);
      if (tab != null) {
        Table newTab = tab.makeCopy();
        tab = newTab;
      }
      if (tab == null ||
          tab.getDbName().equals(SessionState.get().getCurrentDatabase())) {
        Table materializedTab = ctx.getMaterializedTable(cteName);
        if (materializedTab == null) {
          // we first look for this alias from CTE, and then from catalog.
          CTEClause cte = findCTEFromName(qb, cteName, aliasToCTEs);
          if (cte != null) {
            if (!cte.materialize) {
              addCTEAsSubQuery(qb, cteName, alias);
              sqAliasToCTEName.put(alias, cteName);
              continue;
            }
            tab = materializeCTE(cteName, cte);
          }
        } else {
          tab = materializedTab;
        }
      }

      if (tab == null) {
        if(tabName.equals(DUMMY_DATABASE + "." + DUMMY_TABLE)) {
          continue;
        }
        ASTNode src = qb.getParseInfo().getSrcForAlias(alias);
        if (null != src) {
          if (src.getChildCount() == 3) {
            throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg() + " '" + src.getChild(2).getText() + "'");
          }
          throw new SemanticException(ASTErrorUtils.getMsg(ErrorMsg.INVALID_TABLE.getMsg(), src));
        } else {
          throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(alias));
        }
      }

      QBSystemVersion asOf = qb.getSystemVersionForAlias(alias);
      if (asOf != null) {
        if (!Optional.ofNullable(tab.getStorageHandler()).map(HiveStorageHandler::isTimeTravelAllowed).orElse(false)) {
          throw new SemanticException(ErrorMsg.TIME_TRAVEL_NOT_ALLOWED, alias);
        }
        tab.setAsOfVersion(asOf.getAsOfVersion());
        tab.setVersionIntervalFrom(asOf.getFromVersion());
        tab.setAsOfTimestamp(asOf.getAsOfTime());
      }

      if (tab.isView()) {
        if (qb.getParseInfo().isAnalyzeCommand()) {
          throw new SemanticException(ErrorMsg.ANALYZE_VIEW.getMsg());
        }
        String fullViewName = tab.getFullyQualifiedName();
        // Prevent view cycles
        if (viewsExpanded.contains(fullViewName)) {
          throw new SemanticException("Recursive view " + fullViewName +
              " detected (cycle: " + StringUtils.join(viewsExpanded, " -> ") +
              " -> " + fullViewName + ").");
        }
        replaceViewReferenceWithDefinition(qb, tab, tabName, alias);
        // This is the last time we'll see the Table objects for views, so add it to the inputs
        // now. isInsideView will tell if this view is embedded in another view.
        // If the view is Inside another view, it should have at least one parent
        if (qb.isInsideView() && parentInput == null) {
          parentInput = PlanUtils.getParentViewInfo(getAliasId(alias, qb), viewAliasToInput);
        }
        ReadEntity viewInput = new ReadEntity(tab, parentInput, !qb.isInsideView());
        viewInput = PlanUtils.addInput(inputs, viewInput);
        aliasToViewInfo.put(alias, Pair.of(fullViewName, viewInput));
        String aliasId = getAliasId(alias, qb);
        if (aliasId != null) {
          aliasId = aliasId.replace(SemanticAnalyzer.SUBQUERY_TAG_1, "")
              .replace(SemanticAnalyzer.SUBQUERY_TAG_2, "");
        }
        viewAliasToInput.put(aliasId, viewInput);
        continue;
      }

      if (!InputFormat.class.isAssignableFrom(tab.getInputFormatClass())) {
        throw new SemanticException(generateErrorMessage(
            qb.getParseInfo().getSrcForAlias(alias),
            ErrorMsg.INVALID_INPUT_FORMAT_TYPE.getMsg()));
      }

      qb.getMetaData().setSrcForAlias(alias, tab);

      if (qb.getParseInfo().isAnalyzeCommand()) {
        // allow partial partition specification for nonscan since noscan is fast.
        TableSpec ts = new TableSpec(db, conf, (ASTNode) ast.getChild(0), true, this.noscan);
        if (ts.specType == SpecType.DYNAMIC_PARTITION) { // dynamic partitions
          try {
            ts.partitions = db.getPartitionsByNames(ts.tableHandle, ts.partSpec);
          } catch (HiveException e) {
            throw new SemanticException(generateErrorMessage(
                qb.getParseInfo().getSrcForAlias(alias),
                "Cannot get partitions for " + ts.partSpec), e);
          }
        }

        tab.setTableSpec(ts);
        qb.getParseInfo().addTableSpec(alias, ts);
      }

      ReadEntity parentViewInfo = PlanUtils.getParentViewInfo(getAliasId(alias, qb), viewAliasToInput);
      // Temporary tables created during the execution are not the input sources
      if (!PlanUtils.isValuesTempTable(alias)) {
        PlanUtils.addInput(inputs,
            new ReadEntity(tab, parentViewInfo, parentViewInfo == null), mergeIsDirect);
      }
    }

    LOG.info("Get metadata for subqueries");
    // Go over the subqueries and getMetaData for these
    for (String alias : qb.getSubqAliases()) {
      boolean wasView = aliasToViewInfo.containsKey(alias);
      boolean wasCTE = sqAliasToCTEName.containsKey(alias);
      ReadEntity newParentInput = null;
      if (wasView) {
        viewsExpanded.add(aliasToViewInfo.get(alias).getLeft());
        newParentInput = aliasToViewInfo.get(alias).getRight();
      } else if (wasCTE) {
        ctesExpanded.add(sqAliasToCTEName.get(alias));
      }
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      if (qbexpr.getQB() != null && (wasView || qb.isInsideView())) {
        qbexpr.getQB().setInsideView(true);
      }
      getMetaData(qbexpr, newParentInput);
      if (wasView) {
        viewsExpanded.remove(viewsExpanded.size() - 1);
      } else if (wasCTE) {
        ctesExpanded.remove(ctesExpanded.size() - 1);
      }
    }

    RowFormatParams rowFormatParams = new RowFormatParams();
    StorageFormat storageFormat = new StorageFormat(conf);

    LOG.info("Get metadata for destination tables");
    // Go over all the destination structures and populate the related
    // metadata
    QBParseInfo qbp = qb.getParseInfo();

    for (String name : qbp.getClauseNamesForDest()) {
      ASTNode ast = qbp.getDestForClause(name);
      switch (ast.getToken().getType()) {
      case HiveParser.TOK_TAB: {
        TableSpec ts = new TableSpec(db, conf, ast);
        if (ts.tableHandle.isView() ||
            (mvRebuildMode == MaterializationRebuildMode.NONE && ts.tableHandle.isMaterializedView())) {
          throw new SemanticException(ErrorMsg.DML_AGAINST_VIEW.getMsg());
        }

        Class<?> outputFormatClass = ts.tableHandle.getOutputFormatClass();
        if (!ts.tableHandle.isNonNative() &&
            !HiveOutputFormat.class.isAssignableFrom(outputFormatClass)) {
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE.getMsg(),
              ast, "The class is " + outputFormatClass.toString()));
        }

        boolean isTableWrittenTo = qb.getParseInfo().isInsertIntoTable(ts.tableHandle.getDbName(),
            ts.tableHandle.getTableName(), ts.tableHandle.getSnapshotRef());
        isTableWrittenTo |= (qb.getParseInfo().getInsertOverwriteTables().
            get(getUnescapedName((ASTNode) ast.getChild(0), ts.tableHandle.getDbName()).toLowerCase()) != null);
        assert isTableWrittenTo :
            "Inconsistent data structure detected: we are writing to " + ts.tableHandle  + " in " +
                name + " but it's not in isInsertIntoTable() or getInsertOverwriteTables()";
        Boolean isTableTag = Optional.ofNullable(ts.tableHandle.getSnapshotRef()).map(HiveUtils::isTableTag)
            .orElse(false);
        if (isTableTag) {
          throw new UnsupportedOperationException("Don't support write (insert/delete/update/merge) to iceberg tag " +
              HiveUtils.getTableSnapshotRef(ts.tableHandle.getSnapshotRef()));
        }
        // Disallow update and delete on non-acid tables
        boolean isWriteOperation = updating(name) || deleting(name);
        boolean isFullAcid = AcidUtils.isFullAcidTable(ts.tableHandle) ||
            AcidUtils.isNonNativeAcidTable(ts.tableHandle);
        if (isWriteOperation && !isFullAcid) {
          if (!AcidUtils.isInsertOnlyTable(ts.tableHandle)) {
            // Whether we are using an acid compliant transaction manager has already been caught in
            // UpdateDeleteSemanticAnalyzer, so if we are updating or deleting and getting nonAcid
            // here, it means the table itself doesn't support it.
            throw new SemanticException(ErrorMsg.ACID_OP_ON_NONACID_TABLE, ts.getTableName().getTable());
          } else {
            throw new SemanticException(ErrorMsg.ACID_OP_ON_INSERTONLYTRAN_TABLE, ts.getTableName().getTable());
          }
        }
        // TableSpec ts is got from the query (user specified),
        // which means the user didn't specify partitions in their query,
        // but whether the table itself is partitioned is not know.
        if (ts.specType != SpecType.STATIC_PARTITION) {
          // This is a table or dynamic partition
          qb.getMetaData().setDestForAlias(name, ts.tableHandle);
          // has dynamic as well as static partitions
          if (ts.partSpec != null && ts.partSpec.size() > 0) {
            qb.getMetaData().setPartSpecForAlias(name, ts.partSpec);
          }
        } else {
          // This is a partition
          qb.getMetaData().setDestForAlias(name, ts.partHandle);
          if (ts.tableHandle.hasNonNativePartitionSupport() && ts.partSpec != null && ts.partSpec.size() > 0) {
            qb.getMetaData().setPartSpecForAlias(name, ts.partSpec);
          }
        }
        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_AUTOGATHER)) {
          // Add the table spec for the destination table.
          qb.getParseInfo().addTableSpec(ts.getTableName().getTable().toLowerCase(), ts);
        }
        break;
      }

      case HiveParser.TOK_DIR: {
        // This is a dfs file
        String fname = stripQuotes(ast.getChild(0).getText());
        if ((!qb.getParseInfo().getIsSubQ()) && (((ASTNode) ast.getChild(0)).getToken().getType()
            == HiveParser.TOK_TMP_FILE)) {

          if (qb.isCTAS() || qb.isMaterializedView()) {
            qb.setIsQuery(false);
            ctx.setResDir(null);
            ctx.setResFile(null);

            Path location;
            // If the CTAS query does specify a location, use the table location, else use the db location
            if (qb.isMaterializedView() && qb.getViewDesc() != null && qb.getViewDesc().getLocation() != null) {
              location = new Path(qb.getViewDesc().getLocation());
            } else if (qb.isCTAS() && qb.getTableDesc().getLocation() != null) {
              location = new Path(qb.getTableDesc().getLocation());
            } else {
              // allocate a temporary output dir on the location of the table
              String tableName = getUnescapedName((ASTNode) ast.getChild(0));
              String[] names = Utilities.getDbTableName(tableName);
              try {
                Warehouse wh = new Warehouse(conf);
                //Use destination table's db location.
                String destTableDb = qb.getTableDesc() != null ? qb.getTableDesc().getDatabaseName() : null;
                if (destTableDb == null) {
                  destTableDb = names[0];
                }
                boolean useExternal = false;
                if (qb.isMaterializedView()) {
                  useExternal = !AcidUtils.isTransactionalView(qb.getViewDesc()) && !makeAcid();
                } else {
                  useExternal = (qb.getTableDesc() == null || qb.getTableDesc().isTemporary()
                    || qb.getTableDesc().isExternal() || !makeAcid());
                }
                if (useExternal) {
                  location = wh.getDatabaseExternalPath(db.getDatabase(destTableDb));
                } else {
                  location = wh.getDatabaseManagedPath(db.getDatabase(destTableDb));
                }
              } catch (MetaException e) {
                throw new SemanticException(e);
              }
            }
            try {
              CreateTableDesc tblDesc = qb.getTableDesc();
              if (tblDesc != null && tblDesc.isTemporary() && AcidUtils.isInsertOnlyTable(tblDesc.getTblProps())) {
                fname = FileUtils.makeQualified(location, conf).toString();
              } else {
                fname = ctx.getExtTmpPathRelTo(FileUtils.makeQualified(location, conf)).toString();
              }
            } catch (Exception e) {
              throw new SemanticException(
                  generateErrorMessage(ast, "Error creating temporary folder on: " + location.toString()), e);
            }
            if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_AUTOGATHER)) {
              TableSpec ts = new TableSpec(db, conf, this.ast);
              // Add the table spec for the destination table.
              qb.getParseInfo().addTableSpec(ts.getTableName().getTable().toLowerCase(), ts);
            }
          } else {
            // This is the only place where isQuery is set to true; it defaults to false.
            qb.setIsQuery(true);
            Path stagingPath = getStagingDirectoryPathname(qb, conf, ctx);
            fname = stagingPath.toString();
            ctx.setResDir(stagingPath);
          }
        }

        boolean isDfsFile = true;
        if (ast.getChildCount() >= 2 && ast.getChild(1).getText().toLowerCase().equals("local")) {
          isDfsFile = false;
        }
        // Set the destination for the SELECT query inside the CTAS
        qb.getMetaData().setDestForAlias(name, fname, isDfsFile);

        CreateTableDesc directoryDesc = new CreateTableDesc();
        boolean directoryDescIsSet = false;
        int numCh = ast.getChildCount();
        for (int num = 1; num < numCh ; num++){
          ASTNode child = (ASTNode) ast.getChild(num);
          if (child != null) {
            if (storageFormat.fillStorageFormat(child)) {
              directoryDesc.setInputFormat(storageFormat.getInputFormat());
              directoryDesc.setOutputFormat(storageFormat.getOutputFormat());
              directoryDesc.setSerde(storageFormat.getSerde());
              directoryDescIsSet = true;
              continue;
            }
            switch (child.getToken().getType()) {
            case HiveParser.TOK_TABLEROWFORMAT:
              rowFormatParams.analyzeRowFormat(child);
              directoryDesc.setFieldDelim(rowFormatParams.fieldDelim);
              directoryDesc.setLineDelim(rowFormatParams.lineDelim);
              directoryDesc.setCollItemDelim(rowFormatParams.collItemDelim);
              directoryDesc.setMapKeyDelim(rowFormatParams.mapKeyDelim);
              directoryDesc.setFieldEscape(rowFormatParams.fieldEscape);
              directoryDesc.setNullFormat(rowFormatParams.nullFormat);
              directoryDescIsSet=true;
              break;
            case HiveParser.TOK_TABLESERIALIZER:
              ASTNode serdeChild = (ASTNode) child.getChild(0);
              storageFormat.setSerde(unescapeSQLString(serdeChild.getChild(0).getText()));
              directoryDesc.setSerde(storageFormat.getSerde());
              if (serdeChild.getChildCount() > 1) {
                directoryDesc.setSerdeProps(new HashMap<String, String>());
                readProps((ASTNode) serdeChild.getChild(1).getChild(0), directoryDesc.getSerdeProps());
              }
              directoryDescIsSet = true;
              break;
            }
          }
        }
        if (directoryDescIsSet){
          qb.setDirectoryDesc(directoryDesc);
        }
        break;
      }
      default:
        throw new SemanticException(generateErrorMessage(ast,
            "Unknown Token Type " + ast.getToken().getType()));
      }
    }
  }

  /**
   * Checks if a given path is encrypted (valid only for HDFS files)
   * @param path The path to check for encryption
   * @return True if the path is encrypted; False if it is not encrypted
   * @throws HiveException If an error occurs while checking for encryption
   */
  private static boolean isPathEncrypted(Path path, HiveConf conf) throws HiveException {

    try {
      HadoopShims.HdfsEncryptionShim hdfsEncryptionShim =
          SessionState.get().getHdfsEncryptionShim(path.getFileSystem(conf), conf);
      if (hdfsEncryptionShim != null) {
        if (hdfsEncryptionShim.isPathEncrypted(path)) {
          return true;
        }
      }
    } catch (Exception e) {
      throw new HiveException("Unable to determine if " + path + " is encrypted: " + e, e);
    }

    return false;
  }

  /**
   * Compares to path key encryption strenghts.
   *
   * @param p1 Path to an HDFS file system
   * @param p2 Path to an HDFS file system
   * @return -1 if strength is weak; 0 if is equals; 1 if it is stronger
   * @throws HiveException If an error occurs while comparing key strengths.
   */
  private static int comparePathKeyStrength(Path p1, Path p2, HiveConf conf) throws HiveException {
    try {
      HadoopShims.HdfsEncryptionShim hdfsEncryptionShim1 = SessionState.get().getHdfsEncryptionShim(p1.getFileSystem(conf), conf);
      HadoopShims.HdfsEncryptionShim hdfsEncryptionShim2 = SessionState.get().getHdfsEncryptionShim(p2.getFileSystem(conf), conf);

      if (hdfsEncryptionShim1 != null && hdfsEncryptionShim2 != null) {
        return hdfsEncryptionShim1.comparePathKeyStrength(p1, p2, hdfsEncryptionShim2);
      }
    } catch (Exception e) {
      throw new HiveException("Unable to compare key strength for " + p1 + " and " + p2 + " : " + e, e);
    }

    return 0; // Non-encrypted path (or equals strength)
  }

  /**
   * Checks if a given path has read-only access permissions.
   *
   * @param path The path to check for read-only permissions.
   * @return True if the path is read-only; False otherwise.
   * @throws HiveException If an error occurs while checking file permissions.
   */
  private static boolean isPathReadOnly(Path path) throws HiveException {
    HiveConf conf = SessionState.get().getConf();
    try {
      FileSystem fs = path.getFileSystem(conf);
      UserGroupInformation ugi = Utils.getUGI();
      FileStatus status = fs.getFileStatus(path);

      // We just check for writing permissions. If it fails with AccessControException, then it
      // means the location may be read-only.
      FileUtils.checkFileAccessWithImpersonation(fs, status, FsAction.WRITE, ugi.getUserName());

      // Path has writing permissions
      return false;
    } catch (AccessControlException e) {
      // An AccessControlException may be caused for other different errors,
      // but we take it as if our path is read-only
      return true;
    } catch (Exception e) {
      throw new HiveException("Unable to determine if " + path + " is read only: " + e, e);
    }
  }

  /**
   * Gets the strongest encrypted table path.
   *
   * @param qb The QB object that contains a list of all table locations.
   * @return The strongest encrypted path. It may return NULL if there are not tables encrypted, or are not HDFS tables.
   * @throws HiveException if an error occurred attempting to compare the encryption strength
   */
  private static Path getStrongestEncryptedTablePath(QB qb, HiveConf conf) throws HiveException {
    List<String> tabAliases = new ArrayList<String>(qb.getTabAliases());
    Path strongestPath = null;

    /* Walk through all found table locations to get the most encrypted table */
    for (String alias : tabAliases) {
      Table tab = qb.getMetaData().getTableForAlias(alias);
      if (tab != null) {
        Path tablePath = tab.getDataLocation();
        if (tablePath != null) {
          if ("hdfs".equalsIgnoreCase(tablePath.toUri().getScheme())) {
            if (isPathEncrypted(tablePath, conf)) {
              if (strongestPath == null) {
                strongestPath = tablePath;
              } else if (comparePathKeyStrength(tablePath, strongestPath, conf) > 0) {
                strongestPath = tablePath;
              }
            }
          }
        }
      }
    }

    return strongestPath;
  }

  /**
   * Gets the staging directory where MR files will be stored temporary.
   * It walks through the QB plan to find the correct location where save temporary files. This
   * temporary location (or staging directory) may be created inside encrypted tables locations for
   * security reasons. If the QB has read-only tables, then the older scratch directory will be used,
   * or a permission error will be thrown if the requested query table is encrypted and the old scratch
   * directory is not.
   *
   * @param qb The QB object that contains a list of all table locations.
   * @return The path to the staging directory.
   * @throws HiveException If an error occurs while identifying the correct staging location.
   */
  static Path getStagingDirectoryPathname(QB qb, HiveConf conf, Context ctx) throws HiveException {
    Path stagingPath = null, tablePath = null;

    if (DFSUtilClient.isHDFSEncryptionEnabled(conf)) {
      // Looks for the most encrypted table location
      // It may return null if there are not tables encrypted, or are not part of HDFS
      tablePath = getStrongestEncryptedTablePath(qb, conf);
    }
    if (tablePath != null) {
      // At this point, tablePath is part of HDFS and it is encrypted
      if (isPathReadOnly(tablePath)) {
        Path tmpPath = ctx.getMRTmpPath();
        if (comparePathKeyStrength(tablePath, tmpPath, conf) < 0) {
          throw new HiveException("Read-only encrypted tables cannot be read " +
              "if the scratch directory is not encrypted (or encryption is weak)");
        } else {
          stagingPath = tmpPath;
        }
      }

      if (stagingPath == null) {
        stagingPath = ctx.getMRTmpPath(tablePath.toUri());
      }
    } else {
      stagingPath = ctx.getMRTmpPath(false);
    }

    return stagingPath;
  }

  private void replaceViewReferenceWithDefinition(QB qb, Table tab,
                                                  String tab_name, String alias) throws SemanticException {

    ASTNode viewTree;
    final ASTNodeOrigin viewOrigin = new ASTNodeOrigin("VIEW", tab.getTableName(),
        tab.getViewExpandedText(), alias, qb.getParseInfo().getSrcForAlias(
        alias));
    try {
      // Reparse text, passing null for context to avoid clobbering
      // the top-level token stream.
      String viewFullyQualifiedName = tab.getCompleteName();
      String viewText = tab.getViewExpandedText();
      TableMask viewMask = new TableMask(this, conf, false);
      viewTree = ParseUtils.parse(viewText, ctx, tab.getCompleteName());
      cacheTableHelper.populateCacheForView(ctx.getParsedTables(), conf,
          getTxnMgr(), tab.getDbName(), tab.getTableName());
      if (viewMask.isEnabled() && analyzeRewrite == null) {
        ParseResult parseResult = rewriteASTWithMaskAndFilter(viewMask, viewTree,
            ctx.getViewTokenRewriteStream(viewFullyQualifiedName),
            ctx, db);
        viewTree = parseResult.getTree();
      }
      SemanticDispatcher nodeOriginDispatcher = new SemanticDispatcher() {
        @Override
        public Object dispatch(Node nd, java.util.Stack<Node> stack,
                               Object... nodeOutputs) {
          ((ASTNode) nd).setOrigin(viewOrigin);
          return null;
        }
      };
      SemanticGraphWalker nodeOriginTagger = new DefaultGraphWalker(
          nodeOriginDispatcher);
      nodeOriginTagger.startWalking(java.util.Collections
          .<Node> singleton(viewTree), null);
    } catch (ParseException e) {
      // A user could encounter this if a stored view definition contains
      // an old SQL construct which has been eliminated in a later Hive
      // version, so we need to provide full debugging info to help
      // with fixing the view definition.
      LOG.error("Failed to replaceViewReferenceWithDefinition", e);
      StringBuilder sb = new StringBuilder();
      sb.append(e.getMessage());
      ASTErrorUtils.renderOrigin(sb, viewOrigin);
      throw new SemanticException(sb.toString(), e);
    }
    QBExpr qbexpr = new QBExpr(alias);
    doPhase1QBExpr(viewTree, qbexpr, qb.getId(), alias, true, null);
    // if skip authorization, skip checking;
    // if it is inside a view, skip checking;
    // if HIVE_STATS_COLLECT_SCANCOLS is enabled, check.
    if ((!this.skipAuthorization() && !qb.isInsideView())
        || HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS)) {
      qb.rewriteViewToSubq(alias, tab_name, qbexpr, tab);
    } else {
      qb.rewriteViewToSubq(alias, tab_name, qbexpr, null);
    }
  }

  private boolean isPresent(String[] list, String elem) {
    for (String s : list) {
      if (s.toLowerCase().equals(elem)) {
        return true;
      }
    }

    return false;
  }

  /*
   * This method is invoked for unqualified column references in join conditions.
   * This is passed in the Alias to Operator mapping in the QueryBlock so far.
   * We try to resolve the unqualified column against each of the Operator Row Resolvers.
   * - if the column is present in only one RowResolver, we treat this as a reference to
   *   that Operator.
   * - if the column resolves with more than one RowResolver, we treat it as an Ambiguous
   *   reference.
   * - if the column doesn't resolve with any RowResolver, we treat this as an Invalid
   *   reference.
   */
  @SuppressWarnings("rawtypes")
  private String findAlias(ASTNode columnRef,
                           Map<String, Operator> aliasToOpInfo) throws SemanticException {
    String colName = unescapeIdentifier(columnRef.getChild(0).getText()
        .toLowerCase());
    String tabAlias = null;
    if ( aliasToOpInfo != null ) {
      for (Map.Entry<String, Operator> opEntry : aliasToOpInfo.entrySet()) {
        Operator op = opEntry.getValue();
        RowResolver rr = opParseCtx.get(op).getRowResolver();
        ColumnInfo colInfo = rr.get(null, colName);
        if (colInfo != null) {
          if (tabAlias == null) {
            tabAlias = opEntry.getKey();
          } else {
            throw new SemanticException(ASTErrorUtils.getMsg(
                ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(), columnRef.getChild(0)));
          }
        }
      }
    }
    if ( tabAlias == null ) {
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.INVALID_TABLE_ALIAS.getMsg(), columnRef.getChild(0)));
    }
    return tabAlias;
  }

  @SuppressWarnings("nls")
  void parseJoinCondPopulateAlias(QBJoinTree joinTree, ASTNode condn,
                                  List<String> leftAliases, List<String> rightAliases,
                                  List<String> fields,
                                  Map<String, Operator> aliasToOpInfo) throws SemanticException {
    // String[] allAliases = joinTree.getAllAliases();
    switch (condn.getToken().getType()) {
    case HiveParser.TOK_TABLE_OR_COL:
      String tableOrCol = unescapeIdentifier(condn.getChild(0).getText()
          .toLowerCase());
      unparseTranslator.addIdentifierTranslation((ASTNode) condn.getChild(0));
      if (isPresent(joinTree.getLeftAliases(), tableOrCol)) {
        if (!leftAliases.contains(tableOrCol)) {
          leftAliases.add(tableOrCol);
        }
      } else if (isPresent(joinTree.getRightAliases(), tableOrCol)) {
        if (!rightAliases.contains(tableOrCol)) {
          rightAliases.add(tableOrCol);
        }
      } else {
        tableOrCol = findAlias(condn, aliasToOpInfo);
        if (isPresent(joinTree.getLeftAliases(), tableOrCol)) {
          if (!leftAliases.contains(tableOrCol)) {
            leftAliases.add(tableOrCol);
          }
        } else  {
          if (!rightAliases.contains(tableOrCol)) {
            rightAliases.add(tableOrCol);
          }
          if (joinTree.getNoSemiJoin() == false) {
            // if this is a semijoin, we need to add the condition
            joinTree.addRHSSemijoinColumns(tableOrCol, condn);
          }
        }
      }
      break;

    case HiveParser.Identifier:
      // it may be a field name, return the identifier and let the caller decide
      // whether it is or not
      if (fields != null) {
        fields
            .add(unescapeIdentifier(condn.getToken().getText().toLowerCase()));
      }
      unparseTranslator.addIdentifierTranslation(condn);
      break;
    case HiveParser.TOK_NULL:
    case HiveParser.Number:
    case HiveParser.StringLiteral:
    case HiveParser.IntegralLiteral:
    case HiveParser.NumberLiteral:
    case HiveParser.TOK_STRINGLITERALSEQUENCE:
    case HiveParser.TOK_CHARSETLITERAL:
    case HiveParser.KW_TRUE:
    case HiveParser.KW_FALSE:
    case HiveParser.TOK_DATELITERAL:
    case HiveParser.TOK_TIMESTAMPLITERAL:
    case HiveParser.TOK_TIMESTAMPLOCALTZLITERAL:
    case HiveParser.TOK_INTERVAL_DAY_LITERAL:
    case HiveParser.TOK_INTERVAL_DAY_TIME:
    case HiveParser.TOK_INTERVAL_DAY_TIME_LITERAL:
    case HiveParser.TOK_INTERVAL_HOUR_LITERAL:
    case HiveParser.TOK_INTERVAL_MINUTE_LITERAL:
    case HiveParser.TOK_INTERVAL_MONTH_LITERAL:
    case HiveParser.TOK_INTERVAL_SECOND_LITERAL:
    case HiveParser.TOK_INTERVAL_YEAR_LITERAL:
    case HiveParser.TOK_INTERVAL_YEAR_MONTH:
    case HiveParser.TOK_INTERVAL_YEAR_MONTH_LITERAL:
      break;

    case HiveParser.TOK_FUNCTION:
      // check all the arguments
      for (int i = 1; i < condn.getChildCount(); i++) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(i),
            leftAliases, rightAliases, null, aliasToOpInfo);
      }
      break;

    default:
      // This is an operator - so check whether it is unary or binary operator
      if (condn.getChildCount() == 1) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
            leftAliases, rightAliases, null, aliasToOpInfo);
      } else if (condn.getChildCount() == 2) {

        List<String> fields1 = null;
        // if it is a dot operator, remember the field name of the rhs of the
        // left semijoin
        if (joinTree.getNoSemiJoin() == false
            && condn.getToken().getType() == HiveParser.DOT) {
          // get the semijoin rhs table name and field name
          fields1 = new ArrayList<String>();
          int rhssize = rightAliases.size();
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
              leftAliases, rightAliases, null, aliasToOpInfo);
          String rhsAlias = null;

          if (rightAliases.size() > rhssize) { // the new table is rhs table
            rhsAlias = rightAliases.get(rightAliases.size() - 1);
          }

          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(1),
              leftAliases, rightAliases, fields1, aliasToOpInfo);
          if (rhsAlias != null && fields1.size() > 0) {
            joinTree.addRHSSemijoinColumns(rhsAlias, condn);
          }
        } else {
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
              leftAliases, rightAliases, null, aliasToOpInfo);
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(1),
              leftAliases, rightAliases, fields1, aliasToOpInfo);
        }
      } else {
        throw new SemanticException(condn.toStringTree() + " encountered with "
            + condn.getChildCount() + " children");
      }
      break;
    }
  }

  private void populateAliases(List<String> leftAliases,
                               List<String> rightAliases, ASTNode condn, QBJoinTree joinTree,
                               List<String> leftSrc) {
    if ((leftAliases.size() != 0) && (rightAliases.size() != 0)) {
      joinTree.addPostJoinFilter(condn);
      return;
    }

    if (rightAliases.size() != 0) {
      assert rightAliases.size() == 1;
      joinTree.getExpressions().get(1).add(condn);
    } else if (leftAliases.size() != 0) {
      joinTree.getExpressions().get(0).add(condn);
      for (String s : leftAliases) {
        if (!leftSrc.contains(s)) {
          leftSrc.add(s);
        }
      }
    } else {
      joinTree.addPostJoinFilter(condn);
    }
  }

  /*
   * refactored out of the Equality case of parseJoinCondition
   * so that this can be recursively called on its left tree in the case when
   * only left sources are referenced in a Predicate
   */
  void applyEqualityPredicateToQBJoinTree(QBJoinTree joinTree,
                                          JoinType type,
                                          List<String> leftSrc,
                                          ASTNode joinCond,
                                          ASTNode leftCondn,
                                          ASTNode rightCondn,
                                          List<String> leftCondAl1,
                                          List<String> leftCondAl2,
                                          List<String> rightCondAl1,
                                          List<String> rightCondAl2) {
    if (leftCondAl1.size() != 0) {
      if ((rightCondAl1.size() != 0)
          || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {
        if (type.equals(JoinType.LEFTOUTER) ||
            type.equals(JoinType.FULLOUTER)) {
          joinTree.getFilters().get(0).add(joinCond);
        } else {
          /*
           * If the rhs references table sources and this QBJoinTree has a leftTree;
           * hand it to the leftTree and let it recursively handle it.
           * There are 3 cases of passing a condition down:
           * 1. The leftSide && rightSide don't contains references to the leftTree's rightAlias
           *    => pass the lists down as is.
           * 2. The leftSide contains refs to the leftTree's rightAlias, the rightSide doesn't
           *    => switch the leftCondAl1 and leftConAl2 lists and pass down.
           * 3. The rightSide contains refs to the leftTree's rightAlias, the leftSide doesn't
           *    => switch the rightCondAl1 and rightConAl2 lists and pass down.
           * 4. In case both contain references to the leftTree's rightAlias
           *   => we cannot push the condition down.
           * 5. If either contain references to both left & right
           *    => we cannot push forward.
           */
          if (rightCondAl1.size() != 0) {
            QBJoinTree leftTree = joinTree.getJoinSrc();
            List<String> leftTreeLeftSrc = new ArrayList<String>();
            if (leftTree != null && leftTree.getNoOuterJoin()) {
              String leftTreeRightSource = leftTree.getRightAliases() != null &&
                  leftTree.getRightAliases().length > 0 ?
                  leftTree.getRightAliases()[0] : null;

              boolean leftHasRightReference = false;
              for (String r : leftCondAl1) {
                if (r.equals(leftTreeRightSource)) {
                  leftHasRightReference = true;
                  break;
                }
              }
              boolean rightHasRightReference = false;
              for (String r : rightCondAl1) {
                if (r.equals(leftTreeRightSource)) {
                  rightHasRightReference = true;
                  break;
                }
              }

              boolean pushedDown = false;
              if ( !leftHasRightReference && !rightHasRightReference ) {
                applyEqualityPredicateToQBJoinTree(leftTree, type, leftTreeLeftSrc,
                    joinCond, leftCondn, rightCondn,
                    leftCondAl1, leftCondAl2,
                    rightCondAl1, rightCondAl2);
                pushedDown = true;
              } else if ( !leftHasRightReference && rightHasRightReference && rightCondAl1.size() == 1 ) {
                applyEqualityPredicateToQBJoinTree(leftTree, type, leftTreeLeftSrc,
                    joinCond, leftCondn, rightCondn,
                    leftCondAl1, leftCondAl2,
                    rightCondAl2, rightCondAl1);
                pushedDown = true;
              } else if (leftHasRightReference && !rightHasRightReference && leftCondAl1.size() == 1 ) {
                applyEqualityPredicateToQBJoinTree(leftTree, type, leftTreeLeftSrc,
                    joinCond, leftCondn, rightCondn,
                    leftCondAl2, leftCondAl1,
                    rightCondAl1, rightCondAl2);
                pushedDown = true;
              }

              if (leftTreeLeftSrc.size() == 1) {
                leftTree.setLeftAlias(leftTreeLeftSrc.get(0));
              }
              if ( pushedDown) {
                return;
              }
            } // leftTree != null
          }
          joinTree.getFiltersForPushing().get(0).add(joinCond);
        }
      } else if (rightCondAl2.size() != 0) {
        populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree,
            leftSrc);
        populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree,
            leftSrc);
        boolean nullsafe = joinCond.getToken().getType() == HiveParser.EQUAL_NS;
        joinTree.getNullSafes().add(nullsafe);
      }
    } else if (leftCondAl2.size() != 0) {
      if ((rightCondAl2.size() != 0)
          || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {
        if (type.equals(JoinType.RIGHTOUTER)
            || type.equals(JoinType.FULLOUTER)) {
          joinTree.getFilters().get(1).add(joinCond);
        } else {
          joinTree.getFiltersForPushing().get(1).add(joinCond);
        }
      } else if (rightCondAl1.size() != 0) {
        populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree,
            leftSrc);
        populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree,
            leftSrc);
        boolean nullsafe = joinCond.getToken().getType() == HiveParser.EQUAL_NS;
        joinTree.getNullSafes().add(nullsafe);
      }
    } else if (rightCondAl1.size() != 0) {
      if (type.equals(JoinType.LEFTOUTER)
          || type.equals(JoinType.FULLOUTER)) {
        joinTree.getFilters().get(0).add(joinCond);
      } else {
        joinTree.getFiltersForPushing().get(0).add(joinCond);
      }
    } else {
      if (type.equals(JoinType.RIGHTOUTER)
          || type.equals(JoinType.FULLOUTER)) {
        joinTree.getFilters().get(1).add(joinCond);
      } else if (type.equals(JoinType.LEFTSEMI)) {
        joinTree.getExpressions().get(0).add(leftCondn);
        joinTree.getExpressions().get(1).add(rightCondn);
        boolean nullsafe = joinCond.getToken().getType() == HiveParser.EQUAL_NS;
        joinTree.getNullSafes().add(nullsafe);
        joinTree.getFiltersForPushing().get(1).add(joinCond);
      } else {
        joinTree.getFiltersForPushing().get(1).add(joinCond);
      }
    }

  }

  @SuppressWarnings("rawtypes")
  private void parseJoinCondition(QBJoinTree joinTree, ASTNode joinCond, List<String> leftSrc,
                                  Map<String, Operator> aliasToOpInfo)
      throws SemanticException {
    if (joinCond == null) {
      return;
    }
    JoinCond cond = joinTree.getJoinCond()[0];

    JoinType type = cond.getJoinType();
    parseJoinCondition(joinTree, joinCond, leftSrc, type, aliasToOpInfo);

    List<List<ASTNode>> filters = joinTree.getFilters();
    if (type == JoinType.LEFTOUTER || type == JoinType.FULLOUTER) {
      joinTree.addFilterMapping(cond.getLeft(), cond.getRight(), filters.get(0).size());
    }
    if (type == JoinType.RIGHTOUTER || type == JoinType.FULLOUTER) {
      joinTree.addFilterMapping(cond.getRight(), cond.getLeft(), filters.get(1).size());
    }
  }

  /**
   * Parse the join condition. For equality conjuncts, break them into left and
   * right expressions and store in the join tree. For other conditions, either
   * add them to the post-conditions if they apply to more than one input, add
   * them to the filter conditions of a given input if it applies only on
   * one of them and should not be pushed, e.g., left outer join with condition
   * that applies only to left input, or push them below the join if they
   * apply only to one input and can be pushed, e.g., left outer join with
   * condition that applies only to right input.
   *
   * @param joinTree
   *          jointree to be populated
   * @param joinCond
   *          join condition
   * @param leftSrc
   *          left sources
   * @throws SemanticException
   */
  @SuppressWarnings("rawtypes")
  private void parseJoinCondition(QBJoinTree joinTree, ASTNode joinCond,
                                  List<String> leftSrc, JoinType type,
                                  Map<String, Operator> aliasToOpInfo) throws SemanticException {
    if (joinCond == null) {
      return;
    }

    switch (joinCond.getToken().getType()) {
    case HiveParser.KW_OR:
      parseJoinCondPopulateAlias(joinTree, (ASTNode) joinCond.getChild(0),
          new ArrayList<String>(), new ArrayList<String>(),
          null, aliasToOpInfo);
      parseJoinCondPopulateAlias(joinTree, (ASTNode) joinCond.getChild(1),
          new ArrayList<String>(), new ArrayList<String>(),
          null, aliasToOpInfo);
      joinTree.addPostJoinFilter(joinCond);
      break;

    case HiveParser.KW_AND:
      parseJoinCondition(joinTree, (ASTNode) joinCond.getChild(0), leftSrc, type, aliasToOpInfo);
      parseJoinCondition(joinTree, (ASTNode) joinCond.getChild(1), leftSrc, type, aliasToOpInfo);
      break;

    case HiveParser.EQUAL_NS:
    case HiveParser.EQUAL:
      ASTNode leftCondn = (ASTNode) joinCond.getChild(0);
      List<String> leftCondAl1 = new ArrayList<String>();
      List<String> leftCondAl2 = new ArrayList<String>();
      parseJoinCondPopulateAlias(joinTree, leftCondn, leftCondAl1, leftCondAl2,
          null, aliasToOpInfo);

      ASTNode rightCondn = (ASTNode) joinCond.getChild(1);
      List<String> rightCondAl1 = new ArrayList<String>();
      List<String> rightCondAl2 = new ArrayList<String>();
      parseJoinCondPopulateAlias(joinTree, rightCondn, rightCondAl1,
          rightCondAl2, null, aliasToOpInfo);

      // is it a filter or a join condition
      // if it is filter see if it can be pushed above the join
      // filter cannot be pushed if
      // * join is full outer or
      // * join is left outer and filter is on left alias or
      // * join is right outer and filter is on right alias
      if (((leftCondAl1.size() != 0) && (leftCondAl2.size() != 0))
          || ((rightCondAl1.size() != 0) && (rightCondAl2.size() != 0))) {
        joinTree.addPostJoinFilter(joinCond);
      } else {
        applyEqualityPredicateToQBJoinTree(joinTree, type, leftSrc,
            joinCond, leftCondn, rightCondn,
            leftCondAl1, leftCondAl2,
            rightCondAl1, rightCondAl2);
      }
      break;

    default:
      boolean isFunction = (joinCond.getType() == HiveParser.TOK_FUNCTION);

      // Create all children
      int childrenBegin = (isFunction ? 1 : 0);
      List<List<String>> leftAlias = new ArrayList<List<String>>(joinCond.getChildCount() - childrenBegin);
      List<List<String>> rightAlias = new ArrayList<List<String>>(joinCond.getChildCount() - childrenBegin);
      for (int ci = 0; ci < joinCond.getChildCount() - childrenBegin; ci++) {
        List<String> left = new ArrayList<String>();
        List<String> right = new ArrayList<String>();
        leftAlias.add(left);
        rightAlias.add(right);
      }

      for (int ci = childrenBegin; ci < joinCond.getChildCount(); ci++) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) joinCond.getChild(ci),
            leftAlias.get(ci - childrenBegin), rightAlias.get(ci
                - childrenBegin), null, aliasToOpInfo);
      }

      boolean leftAliasNull = true;
      for (List<String> left : leftAlias) {
        if (left.size() != 0) {
          leftAliasNull = false;
          break;
        }
      }

      boolean rightAliasNull = true;
      for (List<String> right : rightAlias) {
        if (right.size() != 0) {
          rightAliasNull = false;
          break;
        }
      }

      if (!leftAliasNull && !rightAliasNull) {
        joinTree.addPostJoinFilter(joinCond);
      } else {
        if (!leftAliasNull) {
          if (type.equals(JoinType.LEFTOUTER)
              || type.equals(JoinType.FULLOUTER)) {
            joinTree.getFilters().get(0).add(joinCond);
          } else {
            joinTree.getFiltersForPushing().get(0).add(joinCond);
          }
        } else {
          if (type.equals(JoinType.RIGHTOUTER)
              || type.equals(JoinType.FULLOUTER)) {
            joinTree.getFilters().get(1).add(joinCond);
          } else {
            joinTree.getFiltersForPushing().get(1).add(joinCond);
          }
        }
      }

      break;
    }
  }

  @SuppressWarnings("rawtypes")
  private void extractJoinCondsFromWhereClause(QBJoinTree joinTree, ASTNode predicate,
                                               Map<String, Operator> aliasToOpInfo) {

    switch (predicate.getType()) {
    case HiveParser.KW_AND:
      extractJoinCondsFromWhereClause(joinTree,
          (ASTNode) predicate.getChild(0), aliasToOpInfo);
      extractJoinCondsFromWhereClause(joinTree,
          (ASTNode) predicate.getChild(1), aliasToOpInfo);
      break;
    case HiveParser.EQUAL_NS:
    case HiveParser.EQUAL:

      ASTNode leftCondn = (ASTNode) predicate.getChild(0);
      List<String> leftCondAl1 = new ArrayList<String>();
      List<String> leftCondAl2 = new ArrayList<String>();
      try {
        parseJoinCondPopulateAlias(joinTree, leftCondn, leftCondAl1, leftCondAl2, null, aliasToOpInfo);
      } catch(SemanticException se) {
        // suppress here; if it is a real issue will get caught in where clause handling.
        return;
      }

      ASTNode rightCondn = (ASTNode) predicate.getChild(1);
      List<String> rightCondAl1 = new ArrayList<String>();
      List<String> rightCondAl2 = new ArrayList<String>();
      try {
        parseJoinCondPopulateAlias(joinTree, rightCondn, rightCondAl1,
            rightCondAl2, null, aliasToOpInfo);
      } catch(SemanticException se) {
        // suppress here; if it is a real issue will get caught in where clause handling.
        return;
      }

      if (((leftCondAl1.size() != 0) && (leftCondAl2.size() != 0))
          || ((rightCondAl1.size() != 0) && (rightCondAl2.size() != 0))) {
        // this is not a join condition.
        return;
      }

      if (((leftCondAl1.size() == 0) && (leftCondAl2.size() == 0))
          || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {
        // this is not a join condition. Will get handled by predicate pushdown.
        return;
      }

      List<String> leftSrc = new ArrayList<String>();
      JoinCond cond = joinTree.getJoinCond()[0];
      JoinType type = cond.getJoinType();
      applyEqualityPredicateToQBJoinTree(joinTree, type, leftSrc,
          predicate, leftCondn, rightCondn,
          leftCondAl1, leftCondAl2,
          rightCondAl1, rightCondAl2);
      if (leftSrc.size() == 1) {
        joinTree.setLeftAlias(leftSrc.get(0));
      }

      // todo: hold onto this predicate, so that we don't add it to the Filter Operator.

      break;
    default:
      return;
    }
  }

  @SuppressWarnings("nls")
  <T extends OperatorDesc> Operator<T> putOpInsertMap(Operator<T> op,
                                                             RowResolver rr) {
    OpParseContext ctx = new OpParseContext(rr);
    opParseCtx.put(op, ctx);
    op.augmentPlan();
    return op;
  }

  @SuppressWarnings("nls")
  private Operator genHavingPlan(String dest, QB qb, Operator input,
                                 Map<String, Operator> aliasToOpInfo)
      throws SemanticException {

    ASTNode havingExpr = qb.getParseInfo().getHavingForClause(dest);

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRowResolver();
    Map<ASTNode, String> exprToColumnAlias = qb.getParseInfo().getAllExprToColumnAlias();
    inputRR.putAll(exprToColumnAlias);
    ASTNode condn = (ASTNode) havingExpr.getChild(0);

    if (!isCBOExecuted() && !qb.getParseInfo().getDestToGroupBy().isEmpty()) {
      // If CBO did not optimize the query, we might need to replace grouping function
      final String destClauseName = qb.getParseInfo().getClauseNames().iterator().next();
      final boolean cubeRollupGrpSetPresent = (!qb.getParseInfo().getDestRollups().isEmpty()
          || !qb.getParseInfo().getDestGroupingSets().isEmpty()
          || !qb.getParseInfo().getDestCubes().isEmpty());
      // Special handling of grouping function
      condn = rewriteGroupingFunctionAST(getGroupByForClause(qb.getParseInfo(), destClauseName), condn,
          !cubeRollupGrpSetPresent);
    }

    /*
     * Now a having clause can contain a SubQuery predicate;
     * so we invoke genFilterPlan to handle SubQuery algebraic transformation,
     * just as is done for SubQuery predicates appearing in the Where Clause.
     */
    Operator output = genFilterPlan(condn, qb, input, aliasToOpInfo, true, false);
    output = putOpInsertMap(output, inputRR);
    return output;
  }

  protected ASTNode rewriteGroupingFunctionAST(final List<ASTNode> grpByAstExprs, ASTNode targetNode,
                                                      final boolean noneSet) {

    TreeVisitorAction action = new TreeVisitorAction() {

      @Override
      public Object pre(Object t) {
        return t;
      }

      @Override
      public Object post(Object t) {
        ASTNode root = (ASTNode) t;
        if (root.getType() == HiveParser.TOK_FUNCTION) {
          ASTNode func = (ASTNode) ParseDriver.adaptor.getChild(root, 0);
          if ("grouping".equalsIgnoreCase(func.getText()) && func.getChildCount() == 0) {
            int numberOperands = ParseDriver.adaptor.getChildCount(root);
            // We implement this logic using replaceChildren instead of replacing
            // the root node itself because windowing logic stores multiple
            // pointers to the AST, and replacing root might lead to some pointers
            // leading to non-rewritten version
            ASTNode newRoot = new ASTNode();
            // Rewritten grouping function
            ASTNode groupingFunc = (ASTNode) ParseDriver.adaptor.create(
                HiveParser.Identifier, "grouping");
            ParseDriver.adaptor.addChild(groupingFunc, ParseDriver.adaptor.create(
                HiveParser.Identifier, "rewritten"));
            newRoot.addChild(groupingFunc);
            // Grouping ID reference
            ASTNode childGroupingID;
            if (noneSet) {
              // Query does not contain CUBE, ROLLUP, or GROUPING SETS, and thus,
              // grouping should return 0
              childGroupingID = (ASTNode) ParseDriver.adaptor.create(HiveParser.IntegralLiteral,
                "0L");
            } else {
              // We refer to grouping_id column
              childGroupingID = (ASTNode) ParseDriver.adaptor.create(
                  HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
              ParseDriver.adaptor.addChild(childGroupingID, ParseDriver.adaptor.create(
                  HiveParser.Identifier, VirtualColumn.GROUPINGID.getName()));
            }
            newRoot.addChild(childGroupingID);
            // Indices
            for (int i = 1; i < numberOperands; i++) {
              ASTNode c = (ASTNode) ParseDriver.adaptor.getChild(root, i);
              for (int j = 0; j < grpByAstExprs.size(); j++) {
                ASTNode grpByExpr = grpByAstExprs.get(j);
                if (grpByExpr.toStringTree().equals(c.toStringTree())) {
                  // Create and add AST node with position of grouping function input
                  // in group by clause
                  ASTNode childN = (ASTNode) ParseDriver.adaptor.create(HiveParser.IntegralLiteral,
                    String.valueOf(IntMath.mod(-j-1, grpByAstExprs.size())) + "L");
                  newRoot.addChild(childN);
                  break;
                }
              }
            }
            if (numberOperands + 1 != ParseDriver.adaptor.getChildCount(newRoot)) {
              throw new RuntimeException(ErrorMsg.HIVE_GROUPING_FUNCTION_EXPR_NOT_IN_GROUPBY.getMsg());
            }
            // Replace expression
            root.replaceChildren(0, numberOperands - 1, newRoot);
          }
        }
        return t;
      }
    };
    return (ASTNode) new TreeVisitor(ParseDriver.adaptor).visit(targetNode, action);
  }

  private Operator genPlanForSubQueryPredicate(
      QB qbSQ,
      ISubQueryJoinInfo subQueryPredicate) throws SemanticException {
    qbSQ.setSubQueryDef(subQueryPredicate.getSubQuery());
    Phase1Ctx ctx_1 = initPhase1Ctx();
    doPhase1(subQueryPredicate.getSubQueryAST(), qbSQ, ctx_1, null);
    getMetaData(qbSQ);
    return genPlan(qbSQ);
  }

  @SuppressWarnings("nls")
  private Operator genFilterPlan(ASTNode searchCond, QB qb, Operator input,
                                 Map<String, Operator> aliasToOpInfo,
                                 boolean forHavingClause, boolean forGroupByClause)
      throws SemanticException {

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRowResolver();

    /*
     * Handling of SubQuery Expressions:
     * if "Where clause contains no SubQuery expressions" then
     *   -->[true] ===CONTINUE_FILTER_PROCESSING===
     * else
     *   -->[false] "extract SubQuery expressions\n from Where clause"
     *   if "this is a nested SubQuery or \nthere are more than 1 SubQuery expressions" then
     *     -->[yes] "throw Unsupported Error"
     *   else
     *     --> "Rewrite Search condition to \nremove SubQuery predicate"
     *      --> "build QBSubQuery"
     *        --> "extract correlated predicates \nfrom Where Clause"
     *        --> "add correlated Items to \nSelect List and Group By"
     *        --> "construct Join Predicate \nfrom correlation predicates"
     *     --> "Generate Plan for\n modified SubQuery"
     *     --> "Build the Join Condition\n for Parent Query to SubQuery join"
     *     --> "Build the QBJoinTree from the Join condition"
     *     --> "Update Parent Query Filter\n with any Post Join conditions"
     *     --> ===CONTINUE_FILTER_PROCESSING===
     *   endif
     * endif
     *
     * Support for Sub Queries in Having Clause:
     * - By and large this works the same way as SubQueries in the Where Clause.
     * - The one addum is the handling of aggregation expressions from the Outer Query
     *   appearing in correlation clauses.
     *   - So such correlating predicates are allowed:
     *        min(OuterQuert.x) = SubQuery.y
     *   - this requires special handling when converting to joins. See QBSubQuery.rewrite
     *     method method for detailed comments.
     */
    List<ASTNode> subQueriesInOriginalTree = SubQueryUtils.findSubQueries(searchCond);

    if ( subQueriesInOriginalTree.size() > 0 ) {

      /*
       * Restriction.9.m :: disallow nested SubQuery expressions.
       */
      if (qb.getSubQueryPredicateDef() != null  ) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(),
            subQueriesInOriginalTree.get(0), "Nested SubQuery expressions are not supported."));
      }

      /*
       * Restriction.8.m :: We allow only 1 SubQuery expression per Query.
       */
      if (subQueriesInOriginalTree.size() > 1 ) {

        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(),
            subQueriesInOriginalTree.get(1), "Only 1 SubQuery expression is supported."));
      }

      /*
       * Clone the Search AST; apply all rewrites on the clone.
       */
      ASTNode clonedSearchCond = (ASTNode) SubQueryUtils.adaptor.dupTree(searchCond);
      List<ASTNode> subQueries = SubQueryUtils.findSubQueries(clonedSearchCond);

      for(int i=0; i < subQueries.size(); i++) {
        ASTNode subQueryAST = subQueries.get(i);
        ASTNode originalSubQueryAST = subQueriesInOriginalTree.get(i);

        int sqIdx = qb.incrNumSubQueryPredicates();
        clonedSearchCond = SubQueryUtils.rewriteParentQueryWhere(clonedSearchCond, subQueryAST);

        QBSubQuery subQuery = SubQueryUtils.buildSubQuery(qb.getId(),
            sqIdx, subQueryAST, originalSubQueryAST, ctx);

        if ( !forHavingClause ) {
          qb.setWhereClauseSubQueryPredicate(subQuery);
        } else {
          qb.setHavingClauseSubQueryPredicate(subQuery);
        }
        String havingInputAlias = null;

        if ( forHavingClause ) {
          havingInputAlias = "gby_sq" + sqIdx;
          aliasToOpInfo.put(havingInputAlias, input);
        }

        subQuery.validateAndRewriteAST(inputRR, forHavingClause, havingInputAlias, aliasToOpInfo.keySet());

        QB qbSQ = new QB(subQuery.getOuterQueryId(), subQuery.getAlias(), true);
        qbSQ.setInsideView(qb.isInsideView());
        Operator sqPlanTopOp = genPlanForSubQueryPredicate(qbSQ, subQuery);
        aliasToOpInfo.put(subQuery.getAlias(), sqPlanTopOp);
        RowResolver sqRR = opParseCtx.get(sqPlanTopOp).getRowResolver();

        /*
         * Check.5.h :: For In and Not In the SubQuery must implicitly or
         * explicitly only contain one select item.
         */
        if ( subQuery.getOperator().getType() != SubQueryType.EXISTS &&
            subQuery.getOperator().getType() != SubQueryType.NOT_EXISTS &&
            sqRR.getColumnInfos().size() -
                subQuery.getNumOfCorrelationExprsAddedToSQSelect() > 1 ) {
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(),
              subQueryAST, "SubQuery can contain only 1 item in Select List."));
        }

        /*
         * If this is a Not In SubQuery Predicate then Join in the Null Check SubQuery.
         * See QBSubQuery.NotInCheck for details on why and how this is constructed.
         */
        if ( subQuery.getNotInCheck() != null ) {
          QBSubQuery.NotInCheck notInCheck = subQuery.getNotInCheck();
          notInCheck.setSQRR(sqRR);
          QB qbSQ_nic = new QB(subQuery.getOuterQueryId(), notInCheck.getAlias(), true);
          Operator sqnicPlanTopOp = genPlanForSubQueryPredicate(qbSQ_nic, notInCheck);
          aliasToOpInfo.put(notInCheck.getAlias(), sqnicPlanTopOp);
          QBJoinTree joinTree_nic = genSQJoinTree(qb, notInCheck,
              input,
              aliasToOpInfo);
          pushJoinFilters(qb, joinTree_nic, aliasToOpInfo, false);
          input = genJoinOperator(qbSQ_nic, joinTree_nic, aliasToOpInfo, input);
          inputRR = opParseCtx.get(input).getRowResolver();
          if ( forHavingClause ) {
            aliasToOpInfo.put(havingInputAlias, input);
          }
        }

        /*
         * Gen Join between outer Operator and SQ op
         */
        subQuery.buildJoinCondition(inputRR, sqRR, forHavingClause, havingInputAlias);
        QBJoinTree joinTree = genSQJoinTree(qb, subQuery,
            input,
            aliasToOpInfo);
        /*
         * push filters only for this QBJoinTree. Child QBJoinTrees have already been handled.
         */
        pushJoinFilters(qb, joinTree, aliasToOpInfo, false);

        /*
         *  Note that: in case of multi dest queries, with even one containing a notIn operator, the code is not changed yet.
         *  That needs to be worked on as a separate bug : https://issues.apache.org/jira/browse/HIVE-27844
         */
        boolean notInCheckPresent = (subQuery.getNotInCheck() != null && !qb.isMultiDestQuery());
        input = genJoinOperator(qbSQ, joinTree, aliasToOpInfo , input, notInCheckPresent);

        searchCond = subQuery.updateOuterQueryFilter(clonedSearchCond);
      }
    }

    return genFilterPlan(qb, searchCond, input, forHavingClause || forGroupByClause);
  }

  /**
   * create a filter plan. The condition and the inputs are specified.
   *
   * @param qb
   *          current query block
   * @param condn
   *          The condition to be resolved
   * @param input
   *          the input operator
   */
  @SuppressWarnings("nls")
  private Operator genFilterPlan(QB qb, ASTNode condn, Operator input, boolean useCaching)
      throws SemanticException {

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRowResolver();

    ExprNodeDesc filterCond = genExprNodeDesc(condn, inputRR, useCaching, isCBOExecuted());
    if (filterCond instanceof ExprNodeConstantDesc) {
      ExprNodeConstantDesc c = (ExprNodeConstantDesc) filterCond;
      if (Boolean.TRUE.equals(c.getValue())) {
        // If filter condition is TRUE, we ignore it
        return input;
      }
      if (ExprNodeDescUtils.isNullConstant(c)) {
        // If filter condition is NULL, transform to FALSE
        filterCond = new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, false);
      }
    }

    if (!filterCond.getTypeInfo().accept(TypeInfoFactory.booleanTypeInfo)) {
      // If the returning type of the filter condition is not boolean, try to implicitly
      // convert the result of the condition to a boolean value.
      if (filterCond.getTypeInfo().getCategory() == ObjectInspector.Category.PRIMITIVE) {
        // For primitive types like string/double/timestamp, try to cast the result of
        // the child expression to a boolean.
        filterCond = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
            .createConversionCast(filterCond, TypeInfoFactory.booleanTypeInfo);
      } else {
        // For complex types like map/list/struct, create a isnotnull function on the child expression.
        filterCond = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
            .getFuncExprNodeDesc("isnotnull", filterCond);
      }
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new FilterDesc(filterCond, false), new RowSchema(
            inputRR.getColumnInfos()), input), inputRR);

    ctx.getPlanMapper().link(condn, output);

    LOG.debug("Created Filter Plan for {} row schema: {}", qb.getId(), inputRR.toString());
    return output;
  }

  /*
   * for inner joins push a 'is not null predicate' to the join sources for
   * every non nullSafe predicate.
   */

  private Operator genNotNullFilterForJoinSourcePlan(QB qb, Operator input,
                                                        QBJoinTree joinTree, ExprNodeDesc[] joinKeys) throws SemanticException {
    return genNotNullFilterForJoinSourcePlan(qb, input, joinTree, joinKeys, false);
  }

  private Operator genNotNullFilterForJoinSourcePlan(QB qb, Operator input,
                                                     QBJoinTree joinTree, ExprNodeDesc[] joinKeys, boolean OuternotInCheck) throws SemanticException {

    /*
     * The notInCheck param is used for the purpose of adding an
     *  (outerQueryTable.outerQueryCol is not null ) predicate to the join,
     *  since it is not added naturally because of outer join
     */

    if (qb == null || joinTree == null) {
      return input;
    }

    if (!joinTree.getNoOuterJoin() && !OuternotInCheck) {
      return input;
    }

    if (joinKeys == null || joinKeys.length == 0) {
      return input;
    }
    Multimap<Integer, ExprNodeColumnDesc> hashes = ArrayListMultimap.create();
    if (input instanceof FilterOperator) {
      ExprNodeDescUtils.getExprNodeColumnDesc(Arrays.asList(((FilterDesc)input.getConf()).getPredicate()), hashes);
    }
    ExprNodeDesc filterPred = null;
    List<Boolean> nullSafes = joinTree.getNullSafes();
    for (int i = 0; i < joinKeys.length; i++) {
      if (nullSafes.get(i) || (joinKeys[i] instanceof ExprNodeColumnDesc &&
          ((ExprNodeColumnDesc)joinKeys[i]).getIsPartitionColOrVirtualCol())) {
        // no need to generate is not null predicate for partitioning or
        // virtual column, since those columns can never be null.
        continue;
      }
      boolean skip = false;
      for (ExprNodeColumnDesc node : hashes.get(joinKeys[i].hashCode())) {
        if (node.isSame(joinKeys[i])) {
          skip = true;
          break;
        }
      }
      if (skip) {
        // there is already a predicate on this src.
        continue;
      }
      List<ExprNodeDesc> args = new ArrayList<ExprNodeDesc>();
      args.add(joinKeys[i]);
      ExprNodeDesc nextExpr = ExprNodeGenericFuncDesc.newInstance(
          FunctionRegistry.getFunctionInfo("isnotnull").getGenericUDF(), args);
      filterPred = filterPred == null ? nextExpr : ExprNodeDescUtils
          .mergePredicates(filterPred, nextExpr);
    }

    if (filterPred == null) {
      return input;
    }

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRowResolver();

    if (input instanceof FilterOperator) {
      FilterOperator f = (FilterOperator) input;
      List<ExprNodeDesc> preds = new ArrayList<ExprNodeDesc>();
      preds.add(f.getConf().getPredicate());
      preds.add(filterPred);
      f.getConf().setPredicate(ExprNodeDescUtils.mergePredicates(preds));

      return input;
    }

    FilterDesc filterDesc = new FilterDesc(filterPred, false);
    filterDesc.setGenerated(true);
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(filterDesc,
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);

    LOG.debug("Created Filter Plan for {} row schema: {}", qb.getId(), inputRR);
    return output;
  }



  Integer genExprNodeDescRegex(String colRegex, String tabAlias, ASTNode sel,
      List<ExprNodeDesc> exprList, Set<ColumnInfo> excludeCols, RowResolver input,
      RowResolver colSrcRR, Integer pos, RowResolver output, List<String> aliases,
      boolean ensureUniqueCols) throws SemanticException {
    List<Pair<ColumnInfo, RowResolver>> colList = new ArrayList<>();
    Integer i = genColListRegex(colRegex, tabAlias, sel,
        colList, excludeCols, input, colSrcRR, pos, output, aliases, ensureUniqueCols);
    for (Pair<ColumnInfo, RowResolver> p : colList) {
      exprList.add(ExprNodeTypeCheck.toExprNode(p.getLeft(), p.getRight()));
    }
    return i;
  }

  @SuppressWarnings("nls")
  // TODO: make aliases unique, otherwise needless rewriting takes place
  Integer genColListRegex(String colRegex, String tabAlias, ASTNode sel,
      List<Pair<ColumnInfo, RowResolver>> colList, Set<ColumnInfo> excludeCols, RowResolver input,
      RowResolver colSrcRR, Integer pos, RowResolver output, List<String> aliases,
      boolean ensureUniqueCols) throws SemanticException {

    if (colSrcRR == null) {
      colSrcRR = input;
    }
    // The table alias should exist
    if (tabAlias != null && !colSrcRR.hasTableAlias(tabAlias)) {
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.INVALID_TABLE_ALIAS.getMsg(), sel));
    }

    // TODO: Have to put in the support for AS clause
    Pattern regex = null;
    try {
      regex = Pattern.compile(colRegex, Pattern.CASE_INSENSITIVE);
    } catch (PatternSyntaxException e) {
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.INVALID_COLUMN.getMsg(), sel, e.getMessage()));
    }

    StringBuilder replacementText = new StringBuilder();
    int matched = 0;
    // add empty string to the list of aliases. Some operators (ex. GroupBy) add
    // ColumnInfos for table alias "".
    if (!aliases.contains("")) {
      aliases.add("");
    }
    /*
     * track the input ColumnInfos that are added to the output.
     * if a columnInfo has multiple mappings; then add the column only once,
     * but carry the mappings forward.
     */
    Map<ColumnInfo, ColumnInfo> inputColsProcessed = new HashMap<ColumnInfo, ColumnInfo>();
    // For expr "*", aliases should be iterated in the order they are specified
    // in the query.

    if (colSrcRR.getNamedJoinInfo() != null) {
      // We got using() clause in previous join. Need to generate select list as
      // per standard. For * we will have joining columns first non-repeated
      // followed by other columns.
      Map<String, ColumnInfo> leftMap = colSrcRR.getFieldMap(colSrcRR.getNamedJoinInfo().getAliases().get(0));
      Map<String, ColumnInfo> rightMap = colSrcRR.getFieldMap(colSrcRR.getNamedJoinInfo().getAliases().get(1));
      Map<String, ColumnInfo> chosenMap = null;
      if (colSrcRR.getNamedJoinInfo().getHiveJoinType() != JoinType.RIGHTOUTER) {
        chosenMap = leftMap;
      } else {
        chosenMap = rightMap;
      }
      // first get the columns in named columns
      for (String columnName : colSrcRR.getNamedJoinInfo().getNamedColumns()) {
        for (Map.Entry<String, ColumnInfo> entry : chosenMap.entrySet()) {
          ColumnInfo colInfo = entry.getValue();
          if (!columnName.equals(colInfo.getAlias())) {
            continue;
          }
          String name = colInfo.getInternalName();
          String[] tmp = colSrcRR.reverseLookup(name);

          // Skip the colinfos which are not for this particular alias
          if (tabAlias != null && !tmp[0].equalsIgnoreCase(tabAlias)) {
            continue;
          }

          if (colInfo.getIsVirtualCol() && colInfo.isHiddenVirtualCol()) {
            continue;
          }
          ColumnInfo oColInfo = inputColsProcessed.get(colInfo);
          if (oColInfo == null) {
            colList.add(Pair.of(colInfo, colSrcRR));
            oColInfo = new ColumnInfo(getColumnInternalName(pos), colInfo.getType(),
                colInfo.getTabAlias(), colInfo.getIsVirtualCol(), colInfo.isHiddenVirtualCol());
            inputColsProcessed.put(colInfo, oColInfo);
          }
          if (ensureUniqueCols) {
            if (!output.putWithCheck(tmp[0], tmp[1], null, oColInfo)) {
              throw new CalciteSemanticException("Cannot add column to RR: " + tmp[0] + "."
                  + tmp[1] + " => " + oColInfo + " due to duplication, see previous warnings",
                  UnsupportedFeature.Duplicates_in_RR);
            }
          } else {
            output.put(tmp[0], tmp[1], oColInfo);
          }
          pos++;
          matched++;

          if (unparseTranslator.isEnabled() || (tableMask.isEnabled() && analyzeRewrite == null)) {
            if (replacementText.length() > 0) {
              replacementText.append(", ");
            }
            replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
            replacementText.append(".");
            replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
          }
        }
      }
    }
    for (String alias : aliases) {
      Map<String, ColumnInfo> fMap = colSrcRR.getFieldMap(alias);
      if (fMap == null) {
        continue;
      }
      // For the tab.* case, add all the columns to the fieldList
      // from the input schema
      for (Map.Entry<String, ColumnInfo> entry : fMap.entrySet()) {
        ColumnInfo colInfo = entry.getValue();
        if (colSrcRR.getNamedJoinInfo() != null && colSrcRR.getNamedJoinInfo().getNamedColumns().contains(colInfo.getAlias())) {
          // we already added this column in select list.
          continue;
        }
        if (excludeCols != null && excludeCols.contains(colInfo)) {
          continue; // This was added during plan generation.
        }
        // First, look up the column from the source against which * is to be
        // resolved.
        // We'd later translated this into the column from proper input, if
        // it's valid.
        // TODO: excludeCols may be possible to remove using the same
        // technique.
        String name = colInfo.getInternalName();
        String[] tmp = colSrcRR.reverseLookup(name);

        // Skip the colinfos which are not for this particular alias
        if (tabAlias != null && !tmp[0].equalsIgnoreCase(tabAlias)) {
          continue;
        }

        if (colInfo.getIsVirtualCol() && colInfo.isHiddenVirtualCol()) {
          continue;
        }

        // Not matching the regex?
        if (!regex.matcher(tmp[1]).matches()) {
          continue;
        }

        // If input (GBY) is different than the source of columns, find the
        // same column in input.
        // TODO: This is fraught with peril.
        if (input != colSrcRR) {
          colInfo = input.get(tabAlias, tmp[1]);
          if (colInfo == null) {
            LOG.error("Cannot find colInfo for {}.{}, derived from [{}], in [{}]", tabAlias, tmp[1], colSrcRR, input);
            throw new SemanticException(ErrorMsg.NON_KEY_EXPR_IN_GROUPBY, tmp[1]);
          }
          name = colInfo.getInternalName();
          tmp = input.reverseLookup(name);
          if (LOG.isDebugEnabled()) {
            String oldCol = name + " => " + (tmp == null ? "null" : (tmp[0] + "." + tmp[1]));
            String newCol = name + " => " + (tmp == null ? "null" : (tmp[0] + "." + tmp[1]));
            LOG.debug("Translated [" + oldCol + "] to [" + newCol + "]");
          }
        }

        ColumnInfo oColInfo = inputColsProcessed.get(colInfo);
        if (oColInfo == null) {
          colList.add(Pair.of(colInfo, input));
          oColInfo = new ColumnInfo(getColumnInternalName(pos), colInfo.getType(),
              colInfo.getTabAlias(), colInfo.getIsVirtualCol(), colInfo.isHiddenVirtualCol());
          inputColsProcessed.put(colInfo, oColInfo);
        }
        assert nonNull(tmp);
        if (ensureUniqueCols) {
          if (!output.putWithCheck(tmp[0], tmp[1], oColInfo.getInternalName(), oColInfo)) {
            throw new CalciteSemanticException("Cannot add column to RR: " + tmp[0] + "." + tmp[1]
                + " => " + oColInfo + " due to duplication, see previous warnings",
                UnsupportedFeature.Duplicates_in_RR);
          }
        } else {
          output.put(tmp[0], tmp[1], oColInfo);
        }
        pos++;
        matched++;

        if (unparseTranslator.isEnabled() || tableMask.isEnabled()) {
          if (replacementText.length() > 0) {
            replacementText.append(", ");
          }
          replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
          replacementText.append(".");
          replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
        }
      }
    }

    if (matched == 0) {
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.INVALID_COLUMN.getMsg(), sel));
    }

    unparseTranslator.addTranslation(sel, replacementText.toString());
    if (tableMask.isEnabled()) {
      tableMask.addTranslation(sel, replacementText.toString());
    }
    return pos;
  }

  public static String getColumnInternalName(int pos) {
    return HiveConf.getColumnInternalName(pos);
  }

  private String getScriptProgName(String cmd) {
    int end = cmd.indexOf(" ");
    return (end == -1) ? cmd : cmd.substring(0, end);
  }

  private String getScriptArgs(String cmd) {
    int end = cmd.indexOf(" ");
    return (end == -1) ? "" : cmd.substring(end, cmd.length());
  }

  private String fetchFilesNotInLocalFilesystem(String cmd) {
    SessionState ss = SessionState.get();
    String progName = getScriptProgName(cmd);

    if (!ResourceDownloader.isFileUri(progName)) {
      String filePath = ss.add_resource(ResourceType.FILE, progName);
      Path p = new Path(filePath);
      String fileName = p.getName();
      String scriptArgs = getScriptArgs(cmd);
      return fileName + scriptArgs;
    }

    return cmd;
  }

  private TableDesc getTableDescFromSerDe(ASTNode child, String cols,
                                          String colTypes) throws SemanticException {
    if (child.getType() == HiveParser.TOK_SERDENAME) {
      String serdeName = unescapeSQLString(child.getChild(0).getText());
      Class<? extends Deserializer> serdeClass = null;

      try {
        serdeClass = (Class<? extends Deserializer>) Class.forName(serdeName,
            true, Utilities.getSessionSpecifiedClassLoader());
      } catch (ClassNotFoundException e) {
        throw new SemanticException(e);
      }

      TableDesc tblDesc = PlanUtils.getTableDesc(serdeClass, Integer
          .toString(Utilities.tabCode), cols, colTypes, null, false);
      // copy all the properties
      if (child.getChildCount() == 2) {
        ASTNode prop = (ASTNode) ((ASTNode) child.getChild(1)).getChild(0);
        for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
          String key = unescapeSQLString(prop.getChild(propChild).getChild(0)
              .getText());
          String value = unescapeSQLString(prop.getChild(propChild).getChild(1)
              .getText());
          tblDesc.getProperties().setProperty(key, value);
        }
      }
      return tblDesc;
    } else if (child.getType() == HiveParser.TOK_SERDEPROPS) {
      TableDesc tblDesc = PlanUtils.getDefaultTableDesc(Integer
          .toString(Utilities.ctrlaCode), cols, colTypes, false);
      int numChildRowFormat = child.getChildCount();
      for (int numC = 0; numC < numChildRowFormat; numC++) {
        ASTNode rowChild = (ASTNode) child.getChild(numC);
        switch (rowChild.getToken().getType()) {
        case HiveParser.TOK_TABLEROWFORMATFIELD:
          String fieldDelim = unescapeSQLString(rowChild.getChild(0).getText());
          tblDesc.getProperties()
              .setProperty(serdeConstants.FIELD_DELIM, fieldDelim);
          tblDesc.getProperties().setProperty(serdeConstants.SERIALIZATION_FORMAT,
              fieldDelim);

          if (rowChild.getChildCount() >= 2) {
            String fieldEscape = unescapeSQLString(rowChild.getChild(1)
                .getText());
            tblDesc.getProperties().setProperty(serdeConstants.ESCAPE_CHAR,
                fieldEscape);
          }
          break;
        case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
          tblDesc.getProperties().setProperty(serdeConstants.COLLECTION_DELIM,
              unescapeSQLString(rowChild.getChild(0).getText()));
          break;
        case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
          tblDesc.getProperties().setProperty(serdeConstants.MAPKEY_DELIM,
              unescapeSQLString(rowChild.getChild(0).getText()));
          break;
        case HiveParser.TOK_TABLEROWFORMATLINES:
          String lineDelim = unescapeSQLString(rowChild.getChild(0).getText());
          tblDesc.getProperties().setProperty(serdeConstants.LINE_DELIM, lineDelim);
          if (!lineDelim.equals("\n") && !lineDelim.equals("10")) {
            throw new SemanticException(generateErrorMessage(rowChild,
                ErrorMsg.LINES_TERMINATED_BY_NON_NEWLINE.getMsg()));
          }
          break;
        case HiveParser.TOK_TABLEROWFORMATNULL:
          String nullFormat = unescapeSQLString(rowChild.getChild(0).getText());
          tblDesc.getProperties().setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT,
              nullFormat);
          break;
        default:
          assert false;
        }
      }

      return tblDesc;
    }

    // should never come here
    return null;
  }

  private void failIfColAliasExists(Set<String> nameSet, String name)
      throws SemanticException {
    if (nameSet.contains(name)) {
      throw new SemanticException(ErrorMsg.COLUMN_ALIAS_ALREADY_EXISTS
          .getMsg(name));
    }
    nameSet.add(name);
  }

  @SuppressWarnings("nls")
  private Operator genScriptPlan(ASTNode trfm, QB qb, Operator input)
      throws SemanticException {
    // If there is no "AS" clause, the output schema will be "key,value"
    List<ColumnInfo> outputCols = new ArrayList<ColumnInfo>();
    int inputSerDeNum = 1, inputRecordWriterNum = 2;
    int outputSerDeNum = 4, outputRecordReaderNum = 5;
    int outputColsNum = 6;
    boolean outputColNames = false, outputColSchemas = false;
    int execPos = 3;
    boolean defaultOutputCols = false;

    // Go over all the children
    if (trfm.getChildCount() > outputColsNum) {
      ASTNode outCols = (ASTNode) trfm.getChild(outputColsNum);
      if (outCols.getType() == HiveParser.TOK_ALIASLIST) {
        outputColNames = true;
      } else if (outCols.getType() == HiveParser.TOK_TABCOLLIST) {
        outputColSchemas = true;
      }
    }

    // If column type is not specified, use a string
    if (!outputColNames && !outputColSchemas) {
      String intName = getColumnInternalName(0);
      ColumnInfo colInfo = new ColumnInfo(intName,
          TypeInfoFactory.stringTypeInfo, null, false);
      colInfo.setAlias("key");
      outputCols.add(colInfo);
      intName = getColumnInternalName(1);
      colInfo = new ColumnInfo(intName, TypeInfoFactory.stringTypeInfo, null,
          false);
      colInfo.setAlias("value");
      outputCols.add(colInfo);
      defaultOutputCols = true;
    } else {
      ASTNode collist = (ASTNode) trfm.getChild(outputColsNum);
      int ccount = collist.getChildCount();

      Set<String> colAliasNamesDuplicateCheck = new HashSet<String>();
      if (outputColNames) {
        for (int i = 0; i < ccount; ++i) {
          String colAlias = unescapeIdentifier(((ASTNode) collist.getChild(i))
              .getText()).toLowerCase();
          failIfColAliasExists(colAliasNamesDuplicateCheck, colAlias);
          String intName = getColumnInternalName(i);
          ColumnInfo colInfo = new ColumnInfo(intName,
              TypeInfoFactory.stringTypeInfo, null, false);
          colInfo.setAlias(colAlias);
          outputCols.add(colInfo);
        }
      } else {
        for (int i = 0; i < ccount; ++i) {
          ASTNode child = (ASTNode) collist.getChild(i);
          assert child.getType() == HiveParser.TOK_TABCOL;
          String colAlias = unescapeIdentifier(((ASTNode) child.getChild(0))
              .getText()).toLowerCase();
          failIfColAliasExists(colAliasNamesDuplicateCheck, colAlias);
          String intName = getColumnInternalName(i);
          ColumnInfo colInfo = new ColumnInfo(intName, TypeInfoUtils
              .getTypeInfoFromTypeString(getTypeStringFromAST((ASTNode) child
                  .getChild(1))), null, false);
          colInfo.setAlias(colAlias);
          outputCols.add(colInfo);
        }
      }
    }

    RowResolver out_rwsch = new RowResolver();
    StringBuilder columns = new StringBuilder();
    StringBuilder columnTypes = new StringBuilder();

    for (int i = 0; i < outputCols.size(); ++i) {
      if (i != 0) {
        columns.append(",");
        columnTypes.append(",");
      }

      columns.append(outputCols.get(i).getInternalName());
      columnTypes.append(outputCols.get(i).getType().getTypeName());

      out_rwsch.put(qb.getParseInfo().getAlias(), outputCols.get(i).getAlias(),
          outputCols.get(i));
    }

    StringBuilder inpColumns = new StringBuilder();
    StringBuilder inpColumnTypes = new StringBuilder();
    List<ColumnInfo> inputSchema = opParseCtx.get(input).getRowResolver().getColumnInfos();
    for (int i = 0; i < inputSchema.size(); ++i) {
      if (i != 0) {
        inpColumns.append(",");
        inpColumnTypes.append(",");
      }

      inpColumns.append(inputSchema.get(i).getInternalName());
      inpColumnTypes.append(inputSchema.get(i).getType().getTypeName());
    }

    TableDesc outInfo;
    TableDesc errInfo;
    TableDesc inInfo;
    String defaultSerdeName = conf.getVar(HiveConf.ConfVars.HIVE_SCRIPT_SERDE);
    Class<? extends Deserializer> serde;

    try {
      serde = (Class<? extends Deserializer>) Class.forName(defaultSerdeName,
          true, Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }

    int fieldSeparator = Utilities.tabCode;
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SCRIPT_ESCAPE)) {
      fieldSeparator = Utilities.ctrlaCode;
    }

    // Input and Output Serdes
    if (trfm.getChild(inputSerDeNum).getChildCount() > 0) {
      inInfo = getTableDescFromSerDe((ASTNode) (((ASTNode) trfm
              .getChild(inputSerDeNum))).getChild(0), inpColumns.toString(),
          inpColumnTypes.toString());
    } else {
      // It is not a very clean way, and should be modified later - due to
      // compatibility reasons, user sees the results as JSON for custom
      // scripts and has no way for specifying that. Right now, it is
      // hard-coded to DelimitedJSONSerDe
      inInfo = PlanUtils.getTableDesc(DelimitedJSONSerDe.class, Integer
          .toString(fieldSeparator), inpColumns.toString(), inpColumnTypes
          .toString(), null, false);
    }

    if (trfm.getChild(outputSerDeNum).getChildCount() > 0) {
      outInfo = getTableDescFromSerDe((ASTNode) (((ASTNode) trfm
              .getChild(outputSerDeNum))).getChild(0), columns.toString(),
          columnTypes.toString());
      // This is for backward compatibility. If the user did not specify the
      // output column list, we assume that there are 2 columns: key and value.
      // However, if the script outputs: col1, col2, col3 seperated by TAB, the
      // requirement is: key is col and value is (col2 TAB col3)
    } else {
      outInfo = PlanUtils.getTableDesc(serde, Integer
          .toString(fieldSeparator), columns.toString(), columnTypes
          .toString(), null, defaultOutputCols);
    }

    // Error stream always uses the default serde with a single column
    errInfo = PlanUtils.getTableDesc(serde, Integer.toString(Utilities.tabCode), "KEY");

    // Output record readers
    Class<? extends RecordReader> outRecordReader = getRecordReader((ASTNode) trfm
        .getChild(outputRecordReaderNum));
    Class<? extends RecordWriter> inRecordWriter = getRecordWriter((ASTNode) trfm
        .getChild(inputRecordWriterNum));
    Class<? extends RecordReader> errRecordReader = getDefaultRecordReader();

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(new ScriptDesc(
            fetchFilesNotInLocalFilesystem(stripQuotes(trfm.getChild(execPos).getText())),
            inInfo, inRecordWriter, outInfo, outRecordReader, errRecordReader, errInfo),
        new RowSchema(out_rwsch.getColumnInfos()), input), out_rwsch);
    output.setColumnExprMap(new HashMap<String, ExprNodeDesc>());  // disable backtracking

    // Add URI entity for transform script. script assumed t be local unless downloadable
    if (conf.getBoolVar(ConfVars.HIVE_CAPTURE_TRANSFORM_ENTITY)) {
      String scriptCmd = getScriptProgName(stripQuotes(trfm.getChild(execPos).getText()));
      getInputs().add(new ReadEntity(new Path(scriptCmd),
          ResourceDownloader.isFileUri(scriptCmd)));
    }

    return output;
  }

  private Class<? extends RecordReader> getRecordReader(ASTNode node)
      throws SemanticException {
    String name;

    if (node.getChildCount() == 0) {
      name = conf.getVar(HiveConf.ConfVars.HIVE_SCRIPT_RECORD_READER);
    } else {
      name = unescapeSQLString(node.getChild(0).getText());
    }

    try {
      return (Class<? extends RecordReader>) Class.forName(name, true,
          Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }
  }

  private Class<? extends RecordReader> getDefaultRecordReader()
      throws SemanticException {
    String name;

    name = conf.getVar(HiveConf.ConfVars.HIVE_SCRIPT_RECORD_READER);

    try {
      return (Class<? extends RecordReader>) Class.forName(name, true,
          Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }
  }

  private Class<? extends RecordWriter> getRecordWriter(ASTNode node)
      throws SemanticException {
    String name;

    if (node.getChildCount() == 0) {
      name = conf.getVar(HiveConf.ConfVars.HIVE_SCRIPT_RECORD_WRITER);
    } else {
      name = unescapeSQLString(node.getChild(0).getText());
    }

    try {
      return (Class<? extends RecordWriter>) Class.forName(name, true,
          Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }
  }

  private List<Long> getGroupingSetsForRollup(int size) {
    List<Long> groupingSetKeys = new ArrayList<Long>();
    for (int i = 0; i <= size; i++) {
      groupingSetKeys.add((1L << i) - 1);
    }
    return groupingSetKeys;
  }

  private List<Long> getGroupingSetsForCube(int size) {
    long count = 1L << size;
    List<Long> results = new ArrayList<Long>();
    for (long i = 0; i < count; ++i) {
      results.add(i);
    }
    return results;
  }

  // This function returns the grouping sets along with the grouping expressions
  // Even if rollups and cubes are present in the query, they are converted to
  // grouping sets at this point
  Pair<List<ASTNode>, List<Long>> getGroupByGroupingSetsForClause(
    QBParseInfo parseInfo, String dest) throws SemanticException {
    List<Long> groupingSets = new ArrayList<Long>();
    List<ASTNode> groupByExprs = getGroupByForClause(parseInfo, dest);

    if (parseInfo.getDestRollups().contains(dest)) {
      groupingSets = getGroupingSetsForRollup(groupByExprs.size());
    } else if (parseInfo.getDestCubes().contains(dest)) {
      groupingSets = getGroupingSetsForCube(groupByExprs.size());
    } else if (parseInfo.getDestGroupingSets().contains(dest)) {
      groupingSets = getGroupingSets(groupByExprs, parseInfo, dest);
    }

    if (!groupingSets.isEmpty() && groupByExprs.size() > Long.SIZE) {
      throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_SIZE_LIMIT.getMsg());
    }

    return Pair.of(groupByExprs, groupingSets);
  }

  private List<Long> getGroupingSets(List<ASTNode> groupByExpr, QBParseInfo parseInfo,
      String dest) throws SemanticException {
    Map<String, Integer> exprPos = new HashMap<String, Integer>();
    for (int i = 0; i < groupByExpr.size(); ++i) {
      ASTNode node = groupByExpr.get(i);
      exprPos.put(node.toStringTree(), i);
    }

    ASTNode root = parseInfo.getGroupByForClause(dest);
    List<Long> result = new ArrayList<Long>(root == null ? 0 : root.getChildCount());
    if (root != null) {
      for (int i = 0; i < root.getChildCount(); ++i) {
        ASTNode child = (ASTNode) root.getChild(i);
        if (child.getType() != HiveParser.TOK_GROUPING_SETS_EXPRESSION) {
          continue;
        }
        long bitmap = LongMath.pow(2, groupByExpr.size()) - 1;
        for (int j = 0; j < child.getChildCount(); ++j) {
          String treeAsString = child.getChild(j).toStringTree();
          Integer pos = exprPos.get(treeAsString);
          if (pos == null) {
            throw new SemanticException(
                generateErrorMessage((ASTNode) child.getChild(j),
                    ErrorMsg.HIVE_GROUPING_SETS_EXPR_NOT_IN_GROUPBY.getErrorCodedMsg()));
          }
          bitmap = unsetBit(bitmap, groupByExpr.size() - pos - 1);

          // Add the copy translation for grouping set keys. This will make sure that same translation as
          // group by key is applied on the grouping set key. If translation is added to group by key
          // to add the table name to the column name (tbl.key), then same thing will be done for grouping
          // set keys also.
          unparseTranslator.addCopyTranslation((ASTNode)child.getChild(j), groupByExpr.get(pos));
        }
        result.add(bitmap);
      }
    }

    if (checkForEmptyGroupingSets(result, LongMath.pow(2, groupByExpr.size()) - 1)) {
      throw new SemanticException(
        ErrorMsg.HIVE_GROUPING_SETS_EMPTY.getMsg());
    }
    return result;
  }

  private boolean checkForEmptyGroupingSets(List<Long> bitmaps, long groupingIdAllSet) {
    boolean ret = true;
    for (long mask : bitmaps) {
      ret &= mask == groupingIdAllSet;
    }
    return ret;
  }

  public static long setBit(long bitmap, int bitIdx) {
    return bitmap | (1L << bitIdx);
  }

  private long unsetBit(long bitmap, int bitIdx) {
    return bitmap & ~(1L << bitIdx);
  }

  /**
   * Returns the GBY, if present;
   * DISTINCT, if present, will be handled when generating the SELECT.
   */
  List<ASTNode> getGroupByForClause(QBParseInfo parseInfo, String dest) throws SemanticException {
    ASTNode selectExpr = parseInfo.getSelForClause(dest);
    Collection<ASTNode> aggregateFunction = parseInfo.getDestToAggregationExprs().get(dest).values();
    if (!(this instanceof CalcitePlanner) && isSelectDistinct(selectExpr) && hasGroupBySibling(selectExpr)) {
      throw new SemanticException("SELECT DISTINCT with GROUP BY is only supported with CBO");
    }

    if (isSelectDistinct(selectExpr) && !hasGroupBySibling(selectExpr) &&
        !isAggregateInSelect(selectExpr, aggregateFunction)) {
      List<ASTNode> result = new ArrayList<ASTNode>(selectExpr.getChildCount());
      for (int i = 0; i < selectExpr.getChildCount(); ++i) {
        if (((ASTNode) selectExpr.getChild(i)).getToken().getType() == HiveParser.QUERY_HINT) {
          continue;
        }
        // table.column AS alias
        ASTNode grpbyExpr = (ASTNode) selectExpr.getChild(i).getChild(0);
        result.add(grpbyExpr);
      }
      return result;
    } else {
      // look for a true GBY
      ASTNode grpByExprs = parseInfo.getGroupByForClause(dest);
      List<ASTNode> result = new ArrayList<ASTNode>(grpByExprs == null ? 0 : grpByExprs.getChildCount());
      if (grpByExprs != null) {
        for (int i = 0; i < grpByExprs.getChildCount(); ++i) {
          ASTNode grpbyExpr = (ASTNode) grpByExprs.getChild(i);
          if (grpbyExpr.getType() != HiveParser.TOK_GROUPING_SETS_EXPRESSION) {
            result.add(grpbyExpr);
          }
        }
      }
      return result;
    }
  }

  protected boolean hasGroupBySibling(ASTNode selectExpr) {
    boolean isGroupBy = false;
    if (selectExpr.getParent() != null && selectExpr.getParent() instanceof Node) {
      for (Node sibling : ((Node)selectExpr.getParent()).getChildren()) {
        isGroupBy |= sibling instanceof ASTNode && ((ASTNode)sibling).getType() == HiveParser.TOK_GROUPBY;
      }
    }

    return isGroupBy;
  }

  protected boolean isSelectDistinct(ASTNode expr) {
    return expr.getType() == HiveParser.TOK_SELECTDI;
  }

  private boolean isAggregateInSelect(Node node, Collection<ASTNode> aggregateFunction) {
    if (node.getChildren() == null) {
      return false;
    }

    for (Node child : node.getChildren()) {
      if (aggregateFunction.contains(child) || isAggregateInSelect(child, aggregateFunction)) {
        return true;
      }
    }

    return false;
  }

  static String[] getColAlias(ASTNode selExpr, String defaultName,
                              RowResolver inputRR, boolean includeFuncName, int colNum) {
    String colAlias = null;
    String tabAlias = null;
    String[] colRef = new String[2];

    //for queries with a windowing expressions, the selexpr may have a third child
    if (selExpr.getChildCount() == 2 ||
        (selExpr.getChildCount() == 3 &&
            selExpr.getChild(2).getType() == HiveParser.TOK_WINDOWSPEC)) {
      // return zz for "xx + yy AS zz"
      colAlias = unescapeIdentifier(selExpr.getChild(1).getText().toLowerCase());
      colRef[0] = tabAlias;
      colRef[1] = colAlias;
      return colRef;
    }

    ASTNode root = (ASTNode) selExpr.getChild(0);
    if (root.getType() == HiveParser.TOK_TABLE_OR_COL) {
      colAlias =
          BaseSemanticAnalyzer.unescapeIdentifier(root.getChild(0).getText().toLowerCase());
      colRef[0] = tabAlias;
      colRef[1] = colAlias;
      return colRef;
    }

    if (root.getType() == HiveParser.DOT) {
      ASTNode tab = (ASTNode) root.getChild(0);
      if (tab.getType() == HiveParser.TOK_TABLE_OR_COL) {
        String t = unescapeIdentifier(tab.getChild(0).getText());
        if (inputRR.hasTableAlias(t)) {
          tabAlias = t;
        }
      }

      // Return zz for "xx.zz" and "xx.yy.zz"
      ASTNode col = (ASTNode) root.getChild(1);
      if (col.getType() == HiveParser.Identifier) {
        colAlias = unescapeIdentifier(col.getText().toLowerCase());
      }
    }

    // if specified generate alias using func name
    if (includeFuncName && (root.getType() == HiveParser.TOK_FUNCTION)) {

      String expr_flattened = root.toStringTree();

      // remove all TOK tokens
      String expr_no_tok = expr_flattened.replaceAll("tok_\\S+", "");

      // remove all non alphanumeric letters, replace whitespace spans with underscore
      String expr_formatted = expr_no_tok.replaceAll("\\W", " ").trim().replaceAll("\\s+", "_");

      // limit length to 20 chars
      if (expr_formatted.length() > AUTOGEN_COLALIAS_PRFX_MAXLENGTH) {
        expr_formatted = expr_formatted.substring(0, AUTOGEN_COLALIAS_PRFX_MAXLENGTH);
      }

      // append colnum to make it unique
      colAlias = expr_formatted.concat("_" + colNum);
    }

    if (colAlias == null) {
      // Return defaultName if selExpr is not a simple xx.yy.zz
      colAlias = defaultName + colNum;
    }

    colRef[0] = tabAlias;
    colRef[1] = colAlias;
    return colRef;
  }

  /**
   * Returns whether the pattern is a regex expression (instead of a normal
   * string). Normal string is a string with all alphabets/digits and "_".
   */
  static boolean isRegex(String pattern, HiveConf conf) {
    String qIdSupport = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT);
    if (!"none".equals(qIdSupport)) {
      return false;
    }
    for (int i = 0; i < pattern.length(); i++) {
      if (!Character.isLetterOrDigit(pattern.charAt(i))
          && pattern.charAt(i) != '_') {
        return true;
      }
    }
    return false;
  }


  private Operator<?> genSelectPlan(String dest, QB qb, Operator<?> input,
                                    Operator<?> inputForSelectStar) throws SemanticException {
    ASTNode selExprList = qb.getParseInfo().getSelForClause(dest);
    Operator<?> op = genSelectPlan(dest, selExprList, qb, input, inputForSelectStar, false);

    LOG.debug("Created Select Plan for clause: {}", dest);

    return op;
  }

  @SuppressWarnings("nls")
  private Operator<?> genSelectPlan(String dest, ASTNode selExprList, QB qb, Operator<?> input,
                                    Operator<?> inputForSelectStar, boolean outerLV) throws SemanticException {

    LOG.debug("tree: {}", selExprList.toStringTree());

    List<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    RowResolver out_rwsch = new RowResolver();
    ASTNode trfm = null;
    Integer pos = 0;
    RowResolver inputRR = opParseCtx.get(input).getRowResolver();
    RowResolver starRR = null;
    if (inputForSelectStar != null && inputForSelectStar != input) {
      starRR = opParseCtx.get(inputForSelectStar).getRowResolver();
    }
    // SELECT * or SELECT TRANSFORM(*)
    boolean selectStar = false;
    int posn = 0;
    boolean hintPresent = (selExprList.getChild(0).getType() == HiveParser.QUERY_HINT);
    if (hintPresent) {
      posn++;
    }

    boolean isInTransform = (selExprList.getChild(posn).getChild(0).getType() ==
        HiveParser.TOK_TRANSFORM);
    if (isInTransform) {
      queryProperties.setUsesScript(true);
      globalLimitCtx.setHasTransformOrUDTF(true);
      trfm = (ASTNode) selExprList.getChild(posn).getChild(0);
    }

    // Detect queries of the form SELECT udtf(col) AS ...
    // by looking for a function as the first child, and then checking to see
    // if the function is a Generic UDTF. It's not as clean as TRANSFORM due to
    // the lack of a special token.
    boolean isUDTF = false;
    String udtfTableAlias = null;
    List<String> udtfColAliases = new ArrayList<String>();
    ASTNode udtfExpr = (ASTNode) selExprList.getChild(posn).getChild(0);
    GenericUDTF genericUDTF = null;

    int udtfExprType = udtfExpr.getType();
    if (udtfExprType == HiveParser.TOK_FUNCTION
        || udtfExprType == HiveParser.TOK_FUNCTIONSTAR) {
      String funcName = TypeCheckProcFactory.getFunctionText(udtfExpr, true);
      FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcName);
      if (fi != null) {
        genericUDTF = fi.getGenericUDTF();
      }
      isUDTF = (genericUDTF != null);
      if (isUDTF) {
        globalLimitCtx.setHasTransformOrUDTF(true);
      }
      if (isUDTF && !fi.isNative()) {
        unparseTranslator.addIdentifierTranslation((ASTNode) udtfExpr
            .getChild(0));
      }
      if (isUDTF && (selectStar = udtfExprType == HiveParser.TOK_FUNCTIONSTAR)) {
        genExprNodeDescRegex(".*", null, (ASTNode) udtfExpr.getChild(0),
            colList, null, inputRR, starRR, pos, out_rwsch, qb.getAliases(), false);
      }
    }

    if (isUDTF) {
      // Only support a single expression when it's a UDTF
      if (selExprList.getChildCount() > 1) {
        throw new SemanticException(generateErrorMessage(
            (ASTNode) selExprList.getChild(1),
            ErrorMsg.UDTF_MULTIPLE_EXPR.getMsg()));
      }

      ASTNode selExpr = (ASTNode) selExprList.getChild(posn);

      // Get the column / table aliases from the expression. Start from 1 as
      // 0 is the TOK_FUNCTION
      // column names also can be inferred from result of UDTF
      for (int i = 1; i < selExpr.getChildCount(); i++) {
        ASTNode selExprChild = (ASTNode) selExpr.getChild(i);
        switch (selExprChild.getType()) {
        case HiveParser.Identifier:
          udtfColAliases.add(unescapeIdentifier(selExprChild.getText().toLowerCase()));
          unparseTranslator.addIdentifierTranslation(selExprChild);
          break;
        case HiveParser.TOK_TABALIAS:
          assert (selExprChild.getChildCount() == 1);
          udtfTableAlias = unescapeIdentifier(selExprChild.getChild(0)
              .getText());
          qb.addAlias(udtfTableAlias);
          unparseTranslator.addIdentifierTranslation((ASTNode) selExprChild
              .getChild(0));
          break;
        default:
          assert (false);
        }
      }
      LOG.debug("UDTF table alias is {}", udtfTableAlias);
      LOG.debug("UDTF col aliases are {}", udtfColAliases);
    }

    // The list of expressions after SELECT or SELECT TRANSFORM.
    ASTNode exprList;
    if (isInTransform) {
      exprList = (ASTNode) trfm.getChild(0);
    } else if (isUDTF) {
      exprList = udtfExpr;
    } else {
      exprList = selExprList;
    }

    LOG.debug("genSelectPlan: input = {} starRr = {}", inputRR, starRR);

    // For UDTF's, skip the function name to get the expressions
    int startPosn = isUDTF ? posn + 1 : posn;
    if (isInTransform) {
      startPosn = 0;
    }

    final boolean cubeRollupGrpSetPresent = (!qb.getParseInfo().getDestRollups().isEmpty()
        || !qb.getParseInfo().getDestGroupingSets().isEmpty()
        || !qb.getParseInfo().getDestCubes().isEmpty());
    Set<String> colAliases = new HashSet<String>();
    int offset = 0;
    // Iterate over all expression (either after SELECT, or in SELECT TRANSFORM)
    for (int i = startPosn; i < exprList.getChildCount(); ++i) {

      // child can be EXPR AS ALIAS, or EXPR.
      ASTNode child = (ASTNode) exprList.getChild(i);
      boolean hasAsClause = (!isInTransform) && (child.getChildCount() == 2);
      boolean isWindowSpec = child.getChildCount() == 3 &&
          child.getChild(2).getType() == HiveParser.TOK_WINDOWSPEC;

      // EXPR AS (ALIAS,...) parses, but is only allowed for UDTF's
      // This check is not needed and invalid when there is a transform b/c the
      // AST's are slightly different.
      if (!isWindowSpec && !isInTransform && !isUDTF && child.getChildCount() > 2) {
        throw new SemanticException(generateErrorMessage(
            (ASTNode) child.getChild(2),
            ErrorMsg.INVALID_AS.getMsg()));
      }

      // The real expression
      ASTNode expr;
      String tabAlias;
      String colAlias;

      if (isInTransform || isUDTF) {
        tabAlias = null;
        colAlias = autogenColAliasPrfxLbl + i;
        expr = child;
      } else {
        // Get rid of TOK_SELEXPR
        expr = (ASTNode) child.getChild(0);
        String[] colRef = getColAlias(child, autogenColAliasPrfxLbl, inputRR,
            autogenColAliasPrfxIncludeFuncName, i + offset);
        tabAlias = colRef[0];
        colAlias = colRef[1];
        if (hasAsClause) {
          unparseTranslator.addIdentifierTranslation((ASTNode) child
              .getChild(1));
        }
      }
      colAliases.add(colAlias);

      // The real expression
      if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
        int initPos = pos;
        pos = genExprNodeDescRegex(".*", expr.getChildCount() == 0 ? null
                : getUnescapedName((ASTNode) expr.getChild(0)).toLowerCase(),
            expr, colList, null, inputRR, starRR, pos, out_rwsch, qb.getAliases(), false);
        if (unparseTranslator.isEnabled()) {
          offset += pos - initPos - 1;
        }
        selectStar = true;
      } else if (expr.getType() == HiveParser.TOK_TABLE_OR_COL && !hasAsClause
          && !inputRR.getIsExprResolver()
          && isRegex(unescapeIdentifier(expr.getChild(0).getText()), conf)) {
        // In case the expression is a regex COL.
        // This can only happen without AS clause
        // We don't allow this for ExprResolver - the Group By case
        pos = genExprNodeDescRegex(unescapeIdentifier(expr.getChild(0).getText()),
            null, expr, colList, null, inputRR, starRR, pos, out_rwsch, qb.getAliases(), false);
      } else if (expr.getType() == HiveParser.DOT
          && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
          && inputRR.hasTableAlias(unescapeIdentifier(expr.getChild(0)
          .getChild(0).getText().toLowerCase())) && !hasAsClause
          && !inputRR.getIsExprResolver()
          && isRegex(unescapeIdentifier(expr.getChild(1).getText()), conf)) {
        // In case the expression is TABLE.COL (col can be regex).
        // This can only happen without AS clause
        // We don't allow this for ExprResolver - the Group By case
        pos = genExprNodeDescRegex(unescapeIdentifier(expr.getChild(1).getText()),
            unescapeIdentifier(expr.getChild(0).getChild(0).getText().toLowerCase()),
            expr, colList, null, inputRR, starRR, pos, out_rwsch, qb.getAliases(), false);
      } else {
        // Case when this is an expression
        TypeCheckCtx tcCtx = new TypeCheckCtx(inputRR, true, isCBOExecuted());
        // We allow stateful functions in the SELECT list (but nowhere else)
        tcCtx.setAllowStatefulFunctions(true);
        tcCtx.setAllowDistinctFunctions(false);
        if (!isCBOExecuted() && !qb.getParseInfo().getDestToGroupBy().isEmpty()) {
          // If CBO did not optimize the query, we might need to replace grouping function
          // Special handling of grouping function
          expr = rewriteGroupingFunctionAST(getGroupByForClause(qb.getParseInfo(), dest), expr,
              !cubeRollupGrpSetPresent);
        }
        ExprNodeDesc exp = genExprNodeDesc(expr, inputRR, tcCtx);
        String recommended = recommendName(exp, colAlias);
        if (recommended != null && !colAliases.contains(recommended) &&
            out_rwsch.get(null, recommended) == null) {
          colAlias = recommended;
        }
        colList.add(exp);

        ColumnInfo colInfo = new ColumnInfo(getColumnInternalName(pos),
            exp.getWritableObjectInspector(), tabAlias, false);
        colInfo.setSkewedCol((exp instanceof ExprNodeColumnDesc) && ((ExprNodeColumnDesc) exp)
            .isSkewedCol());
        out_rwsch.put(tabAlias, colAlias, colInfo);

        if ( exp instanceof ExprNodeColumnDesc ) {
          ExprNodeColumnDesc colExp = (ExprNodeColumnDesc) exp;
          String[] altMapping = inputRR.getAlternateMappings(colExp.getColumn());
          if ( altMapping != null ) {
            out_rwsch.put(altMapping[0], altMapping[1], colInfo);
          }
        }

        pos++;
      }
    }
    Operator output = generateSelectOperator(dest, selExprList, qb, input, exprList, out_rwsch, colList, selectStar, posn);
    if (isInTransform) {
      output = genScriptPlan(trfm, qb, output);
    }

    if (isUDTF) {
      output = genUDTFPlan(genericUDTF, udtfTableAlias, udtfColAliases, qb, output, outerLV);

      if(!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED)
              && qb.getParseInfo().getDestSchemaForClause(dest) != null) {
        List<ExprNodeDesc> expColList = explodeColListForUDTF(colList);
        output = generateSelectOperator(dest, selExprList, qb, output, exprList, opParseCtx.get(output).getRowResolver(), expColList, selectStar, posn);
      }
    }

    LOG.debug("Created Select Plan row schema: {}", out_rwsch);
    return output;
  }

  /**
   * Generates a Select operator for the given query block and destination.
   * This method processes the SELECT clause of the query, creating the necessary
   * operator to handle the selection of columns or expressions.
   *
   * @param dest The destination clause identifier.
   * @param selExprList The ASTNode representing the SELECT expression list.
   * @param qb The query block containing metadata and context for the query.
   * @param input The input operator to which the Select operator will be connected.
   * @param exprList The ASTNode representing the list of expressions in the SELECT clause.
   * @param out_rwsch The RowResolver for the output schema of the Select operator.
   * @param colList The list of column expressions to be included in the Select operator.
   * @param selectStar A boolean indicating whether the SELECT clause includes a wildcard (*).
   * @param posn The position of the current expression in the SELECT clause.
   * @return The generated Select operator.
   * @throws SemanticException If there is an error during the generation of the operator.
   */
  private Operator<?> generateSelectOperator(String dest, ASTNode selExprList, QB qb, Operator<?> input,
                                             ASTNode exprList, RowResolver out_rwsch, List<ExprNodeDesc> colList,
                                          boolean selectStar, int posn) throws SemanticException {
    selectStar = selectStar && exprList.getChildCount() == posn + 1;

    out_rwsch = handleInsertStatementSpec(colList, dest, out_rwsch, qb, selExprList);

    List<String> columnNames = new ArrayList<>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    for (int i = 0; i < colList.size(); i++) {
      String outputCol = getColumnInternalName(i);
      colExprMap.put(outputCol, colList.get(i));
      columnNames.add(outputCol);
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
            new SelectDesc(colList, columnNames, selectStar), new RowSchema(
                    out_rwsch.getColumnInfos()), input), out_rwsch);

    output.setColumnExprMap(colExprMap);

    return output;
  }

  private RowResolver getColForInsertStmtSpec(Map<String, ExprNodeDesc> targetCol2Projection, final Table target,
                                              Map<String, ColumnInfo> targetCol2ColumnInfo, int colListPos,
                                              List<TypeInfo> targetTableColTypes, List<ExprNodeDesc> newColList,
                                              List<String> targetTableColNames)
      throws SemanticException {
    RowResolver newOutputRR = new RowResolver();
    Map<String, String> colNameToDefaultVal = null;

    // see if we need to fetch default constraints from metastore
    if(targetCol2Projection.size() < targetTableColNames.size()) {
      colNameToDefaultVal = getColNameToDefaultValueMap(target);
    }
    for (int i = 0; i < targetTableColNames.size(); i++) {
      String f = targetTableColNames.get(i);
      if(targetCol2Projection.containsKey(f)) {
        //put existing column in new list to make sure it is in the right position
        newColList.add(targetCol2Projection.get(f));
        ColumnInfo ci = targetCol2ColumnInfo.get(f);
        ci.setInternalName(getColumnInternalName(colListPos));
        newOutputRR.put(ci.getTabAlias(), ci.getInternalName(), ci);
      }
      else {
        //add new 'synthetic' columns for projections not provided by Select
        assert(colNameToDefaultVal != null);
        ExprNodeDesc exp = null;
        if(colNameToDefaultVal.containsKey(f)) {
          // make an expression for default value
          String defaultValue = colNameToDefaultVal.get(f);
          ParseDriver parseDriver = new ParseDriver();
          try {
            ASTNode defValAst = parseDriver.parseExpression(defaultValue);

            exp = ExprNodeTypeCheck.genExprNode(defValAst, new TypeCheckCtx(null)).get(defValAst);
          } catch(Exception e) {
            throw new SemanticException("Error while parsing default value: " + defaultValue
              + ". Error message: " + e.getMessage());
          }
          LOG.debug("Added default value from metastore: {}", exp);
        }
        else {
          exp = new ExprNodeConstantDesc(targetTableColTypes.get(i), null);
        }
        newColList.add(exp);
        final String tableAlias = null;//this column doesn't come from any table
        ColumnInfo colInfo = new ColumnInfo(getColumnInternalName(colListPos),
                                            exp.getWritableObjectInspector(), tableAlias, false);
        newOutputRR.put(colInfo.getTabAlias(), colInfo.getInternalName(), colInfo);
      }
      colListPos++;
    }
    return newOutputRR;
  }

  /**
   * Explodes a list of columns represented by a complex datatype for a User-Defined Table Function (UDTF).
   * This method takes a list of column descriptors and explodes it into individual
   * columns based on the structure of the list's element type.
   *
   * @param colList A list of column descriptors representing multiple columns
   *                with a list type like array<struct<col1:string,col2:int>>
   * @return A list of exploded column descriptors, where each column corresponds
   *         to a field in the struct type of the list's element.
   */
  List<ExprNodeDesc> explodeColListForUDTF(List<ExprNodeDesc> colList) {
    List<ExprNodeDesc> expColList = new ArrayList<>();

    ListTypeInfo typeInfo = (ListTypeInfo) colList.get(0).getTypeInfo();
    StructTypeInfo elementTypeInfo = (StructTypeInfo) typeInfo.getListElementTypeInfo();

    List<String> fieldNames = elementTypeInfo.getAllStructFieldNames();
    List<TypeInfo> typeInfos = elementTypeInfo.getAllStructFieldTypeInfos();

    for (int i = 0; i < fieldNames.size(); i++) {
      ExprNodeColumnDesc colDesc = new ExprNodeColumnDesc();
      colDesc.setColumn(fieldNames.get(i));
      colDesc.setTypeInfo(typeInfos.get(i));
      expColList.add(colDesc);
    }

    return expColList;
  }

  /**
   * This modifies the Select projections when the Select is part of an insert statement and
   * the insert statement specifies a column list for the target table, e.g.
   * create table source (a int, b int);
   * create table target (x int, y int, z int);
   * insert into target(z,x) select * from source
   *
   * Once the * is resolved to 'a,b', this list needs to rewritten to 'b,null,a' so that it looks
   * as if the original query was written as
   * insert into target select b, null, a from source
   *
   * if target schema is not specified, this is no-op
   *
   * @see #handleInsertStatementSpecPhase1(ASTNode, QBParseInfo, org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.Phase1Ctx)
   * @throws SemanticException
   */
  RowResolver handleInsertStatementSpec(List<ExprNodeDesc> col_list, String dest,
                                               RowResolver outputRR, QB qb,
                                               ASTNode selExprList) throws SemanticException {
    //(z,x)
    List<String> targetTableSchema = qb.getParseInfo().getDestSchemaForClause(dest);//specified in the query
    if(targetTableSchema == null) {
      //no insert schema was specified
      return outputRR;
    }
    if(targetTableSchema.size() != col_list.size()) {
      if(!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED)
              && col_list.get(0).getTypeInfo() instanceof ListTypeInfo
              && targetTableSchema.size() == explodeColListForUDTF(col_list).size()){
        return outputRR;
      }
      Table target = qb.getMetaData().getDestTableForAlias(dest);
      Partition partition = target == null ? qb.getMetaData().getDestPartitionForAlias(dest) : null;
      throw new SemanticException(generateErrorMessage(selExprList,
          "Expected " + targetTableSchema.size() + " columns for " + dest +
              (target != null ? "/" + target.getCompleteName() : (partition != null ? "/" + partition.getCompleteName() : "")) +
              "; select produces " + col_list.size() + " columns"));
    }
    //e.g. map z->expr for a
    Map<String, ExprNodeDesc> targetCol2Projection = new HashMap<String, ExprNodeDesc>();
    //e.g. map z->ColumnInfo for a
    Map<String, ColumnInfo> targetCol2ColumnInfo = new HashMap<String, ColumnInfo>();
    int colListPos = 0;
    for(String targetCol : targetTableSchema) {
      targetCol2ColumnInfo.put(targetCol, outputRR.getColumnInfos().get(colListPos));
      targetCol2Projection.put(targetCol, col_list.get(colListPos++));
    }
    Table target = qb.getMetaData().getDestTableForAlias(dest);
    Partition partition = target == null ? qb.getMetaData().getDestPartitionForAlias(dest) : null;
    if(target == null && partition == null) {
      throw new SemanticException(generateErrorMessage(selExprList,
          "No table/partition found in QB metadata for dest='" + dest + "'"));
    }
    List<ExprNodeDesc> newColList = new ArrayList<ExprNodeDesc>();
    colListPos = 0;
    List<FieldSchema> targetTableCols = target != null ? target.getCols() : partition.getCols();
    List<String> targetTableColNames = new ArrayList<String>();
    List<TypeInfo> targetTableColTypes = new ArrayList<TypeInfo>();
    for(FieldSchema fs : targetTableCols) {
      targetTableColNames.add(fs.getName());
      targetTableColTypes.add(TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()));
    }
    Map<String, String> partSpec = qb.getMetaData().getPartSpecForAlias(dest);
    if(partSpec != null && QBMetaData.DEST_PARTITION != qb.getMetaData().getDestTypeForAlias(dest)) {
      //find dynamic partition columns
      //relies on consistent order via LinkedHashMap
      for(Map.Entry<String, String> partKeyVal : partSpec.entrySet()) {
        if (partKeyVal.getValue() == null) {
          targetTableColNames.add(partKeyVal.getKey());//these must be after non-partition cols
          targetTableColTypes.add(TypeInfoFactory.stringTypeInfo);
        }
      }
    }

    //now make the select produce <regular columns>,<dynamic partition columns> with
    //where missing columns are NULL-filled
    Table tbl = target == null? partition.getTable() : target;
    RowResolver newOutputRR =  getColForInsertStmtSpec(targetCol2Projection, tbl, targetCol2ColumnInfo, colListPos,
                                                       targetTableColTypes, newColList, targetTableColNames);
    col_list.clear();
    col_list.addAll(newColList);
    return newOutputRR;
  }

  String recommendName(ExprNodeDesc exp, String colAlias) {
    if (!colAlias.startsWith(autogenColAliasPrfxLbl)) {
      return null;
    }
    String column = ExprNodeDescUtils.recommendInputName(exp);
    if (column != null && !column.startsWith(autogenColAliasPrfxLbl)) {
      return column;
    }
    return null;
  }

  String getAutogenColAliasPrfxLbl() {
    return this.autogenColAliasPrfxLbl;
  }

  boolean autogenColAliasPrfxIncludeFuncName() {
    return this.autogenColAliasPrfxIncludeFuncName;
  }

  /**
   * Class to store GenericUDAF related information.
   */
  public static class GenericUDAFInfo {
    public List<ExprNodeDesc> convertedParameters;
    public GenericUDAFEvaluator genericUDAFEvaluator;
    public TypeInfo returnType;
  }

  /**
   * Convert exprNodeDesc array to ObjectInspector array.
   */
  static List<ObjectInspector> getWritableObjectInspector(List<ExprNodeDesc> exprs) {
    return exprs.stream().map(ExprNodeDesc::getWritableObjectInspector).collect(Collectors.toList());
  }

  /**
   * Returns the GenericUDAFEvaluator for the aggregation. This is called once
   * for each GroupBy aggregation.
   */
  public static GenericUDAFEvaluator getGenericUDAFEvaluator(String aggName,
      List<ExprNodeDesc> aggParameters, ASTNode aggTree,
      boolean isDistinct, boolean isAllColumns)
      throws SemanticException {
    return getGenericUDAFEvaluator2(aggName, getWritableObjectInspector(aggParameters),
        aggTree, isDistinct, isAllColumns);
  }

  public static GenericUDAFEvaluator getGenericUDAFEvaluator2(String aggName,
      List<ObjectInspector> aggParameterOIs, ASTNode aggTree,
      boolean isDistinct, boolean isAllColumns)
      throws SemanticException {
    GenericUDAFEvaluator result = FunctionRegistry.getGenericUDAFEvaluator(
        aggName, aggParameterOIs, isDistinct, isAllColumns);
    if (null == result) {
      String reason = "Looking for UDAF Evaluator\"" + aggName
          + "\" with parameters " + aggParameterOIs;
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg(),
          (ASTNode) aggTree.getChild(0), reason));
    }
    return result;
  }

  /**
   * Returns the GenericUDAFInfo struct for the aggregation.
   *
   * @param evaluator
   * @param emode
   * @param aggParameters
   *          The exprNodeDesc of the original parameters
   * @return GenericUDAFInfo
   * @throws SemanticException
   *           when the UDAF is not found or has problems.
   */
  public static GenericUDAFInfo getGenericUDAFInfo(GenericUDAFEvaluator evaluator,
      GenericUDAFEvaluator.Mode emode, List<ExprNodeDesc> aggParameters)
      throws SemanticException {
    GenericUDAFInfo udafInfo = getGenericUDAFInfo2(
        evaluator, emode, getWritableObjectInspector(aggParameters));
    udafInfo.convertedParameters = aggParameters;
    return udafInfo;
  }

  public static GenericUDAFInfo getGenericUDAFInfo2(GenericUDAFEvaluator evaluator,
      GenericUDAFEvaluator.Mode emode, List<ObjectInspector> aggOIs)
      throws SemanticException {

    GenericUDAFInfo r = new GenericUDAFInfo();

    // set r.genericUDAFEvaluator
    r.genericUDAFEvaluator = evaluator;

    // set r.returnType
    ObjectInspector returnOI = null;
    try {
      ObjectInspector[] aggOIArray = new ObjectInspector[aggOIs.size()];
      for (int ii = 0; ii < aggOIs.size(); ++ii) {
        aggOIArray[ii] = aggOIs.get(ii);
      }
      returnOI = r.genericUDAFEvaluator.init(emode, aggOIArray);
      r.returnType = TypeInfoUtils.getTypeInfoFromObjectInspector(returnOI);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }

    return r;
  }

  public static GenericUDAFEvaluator.Mode groupByDescModeToUDAFMode(
      GroupByDesc.Mode mode, boolean isDistinct) {
    switch (mode) {
    case COMPLETE:
      return GenericUDAFEvaluator.Mode.COMPLETE;
    case HASH:
    case PARTIAL1:
      return GenericUDAFEvaluator.Mode.PARTIAL1;
    case PARTIAL2:
      return GenericUDAFEvaluator.Mode.PARTIAL2;
    case PARTIALS:
      return isDistinct ? GenericUDAFEvaluator.Mode.PARTIAL1
          : GenericUDAFEvaluator.Mode.PARTIAL2;
    case FINAL:
      return GenericUDAFEvaluator.Mode.FINAL;
    case MERGEPARTIAL:
      return isDistinct ? GenericUDAFEvaluator.Mode.COMPLETE
          : GenericUDAFEvaluator.Mode.FINAL;
    default:
      throw new RuntimeException("internal error in groupByDescModeToUDAFMode");
    }
  }

  /**
   * Check if the given internalName represents a constant parameter in aggregation parameters
   * of an aggregation tree.
   * This method is only invoked when map-side aggregation is not involved. In this case,
   * every parameter in every aggregation tree should already have a corresponding ColumnInfo,
   * which is generated when the corresponding ReduceSinkOperator of the GroupByOperator being
   * generating is generated. If we find that this parameter is a constant parameter,
   * we will return the corresponding ExprNodeDesc in reduceValues, and we will not need to
   * use a new ExprNodeColumnDesc, which can not be treated as a constant parameter, for this
   * parameter (since the writableObjectInspector of a ExprNodeColumnDesc will not be
   * a instance of ConstantObjectInspector).
   *
   * @param reduceValues
   *          value columns of the corresponding ReduceSinkOperator
   * @param internalName
   *          the internal name of this parameter
   * @return the ExprNodeDesc of the constant parameter if the given internalName represents
   *         a constant parameter; otherwise, return null
   */
  public static ExprNodeDesc isConstantParameterInAggregationParameters(String internalName,
                                                                        List<ExprNodeDesc> reduceValues) {
    // only the pattern of "VALUE._col([0-9]+)" should be handled.

    String[] terms = internalName.split("\\.");
    if (terms.length != 2 || reduceValues == null) {
      return null;
    }

    if (Utilities.ReduceField.VALUE.toString().equals(terms[0])) {
      int pos = HiveConf.getPositionFromInternalName(terms[1]);
      if (pos >= 0 && pos < reduceValues.size()) {
        ExprNodeDesc reduceValue = reduceValues.get(pos);
        if (reduceValue != null) {
          if (reduceValue.getWritableObjectInspector() instanceof ConstantObjectInspector) {
            // this internalName represents a constant parameter in aggregation parameters
            return reduceValue;
          }
        }
      }
    }

    return null;
  }

  /**
   * Generate the GroupByOperator for the Query Block (parseInfo.getXXX(dest)).
   * The new GroupByOperator will be a child of the reduceSinkOperatorInfo.
   *
   * @param mode
   *          The mode of the aggregation (PARTIAL1 or COMPLETE)
   * @param genericUDAFEvaluators
   *          If not null, this function will store the mapping from Aggregation
   *          StringTree to the genericUDAFEvaluator in this parameter, so it
   *          can be used in the next-stage GroupBy aggregations.
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator(QBParseInfo parseInfo,
                                                 String dest, Operator input, ReduceSinkOperator rs, GroupByDesc.Mode mode,
                                                 Map<String, GenericUDAFEvaluator> genericUDAFEvaluators)
      throws SemanticException {
    RowResolver groupByInputRowResolver = opParseCtx
        .get(input).getRowResolver();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    List<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    List<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    List<String> outputColumnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ColumnInfo exprInfo = groupByInputRowResolver.getExpression(grpbyExpr);

      if (exprInfo == null) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_COLUMN.getMsg(), grpbyExpr));
      }

      groupByKeys.add(new ExprNodeColumnDesc(exprInfo.getType(), exprInfo
          .getInternalName(), "", false));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      ColumnInfo oColInfo = new ColumnInfo(field, exprInfo.getType(), null, false);
      groupByOutputRowResolver.putExpression(grpbyExpr,
          oColInfo);
      addAlternateGByKeyMappings(grpbyExpr, oColInfo, input, groupByOutputRowResolver);
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }
    // For each aggregation
    Map<String, ASTNode> aggregationTrees = parseInfo.getAggregationExprsForClause(dest);
    assert (aggregationTrees != null);
    // get the last colName for the reduce KEY
    // it represents the column name corresponding to distinct aggr, if any
    String lastKeyColName = null;
    List<String> inputKeyCols = rs.getConf().getOutputKeyColumnNames();
    if (inputKeyCols.size() > 0) {
      lastKeyColName = inputKeyCols.get(inputKeyCols.size() - 1);
    }
    List<ExprNodeDesc> reduceValues = rs.getConf().getValueCols();
    int numDistinctUDFs = 0;
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();

      // This is the GenericUDAF name
      String aggName = unescapeIdentifier(value.getChild(0).getText());
      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;

      // Convert children to aggParameters
      List<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      // 0 is the function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode) value.getChild(i);
        ColumnInfo paraExprInfo =
            groupByInputRowResolver.getExpression(paraExpr);
        if (paraExprInfo == null) {
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.INVALID_COLUMN.getMsg(), paraExpr));
        }

        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);
        if (isDistinct && lastKeyColName != null) {
          // if aggr is distinct, the parameter is name is constructed as
          // KEY.lastKeyColName:<tag>._colx
          paraExpression = Utilities.ReduceField.KEY.name() + "." +
              lastKeyColName + ":" + numDistinctUDFs + "." +
              getColumnInternalName(i - 1);

        }

        ExprNodeDesc expr = new ExprNodeColumnDesc(paraExprInfo.getType(),
            paraExpression, paraExprInfo.getTabAlias(),
            paraExprInfo.getIsVirtualCol());
        ExprNodeDesc reduceValue = isConstantParameterInAggregationParameters(
            paraExprInfo.getInternalName(), reduceValues);

        if (reduceValue != null) {
          // this parameter is a constant
          expr = reduceValue;
        }

        aggParameters.add(expr);
      }

      if (isDistinct) {
        numDistinctUDFs++;
      }
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = getGenericUDAFEvaluator(
          aggName, aggParameters, value, isDistinct, isAllColumns);
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode, aggParameters);
      aggregations.add(new AggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters, isDistinct,
          amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(value, new ColumnInfo(
          field, udaf.returnType, "", false));
      // Save the evaluator so that it can be used by the next-stage
      // GroupByOperators
      if (genericUDAFEvaluators != null) {
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      }
    }
    float groupByMemoryUsage = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MEMORY);
    float memoryThreshold = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_MEMORY_THRESHOLD);
    float minReductionHashAggr = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION);
    float minReductionHashAggrLowerBound = HiveConf
        .getFloatVar(conf, ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION_LOWER_BOUND);
    float hashAggrFlushPercent = HiveConf
        .getFloatVar(conf, ConfVars.HIVE_MAP_AGGR_HASH_FLUSH_SIZE_PERCENT);

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            false, groupByMemoryUsage, memoryThreshold, minReductionHashAggr, minReductionHashAggrLowerBound,
                hashAggrFlushPercent, null, false, -1, numDistinctUDFs > 0),
        new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        input), groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  // Add the grouping set key to the group by operator.
  // This is not the first group by operator, but it is a subsequent group by operator
  // which is forwarding the grouping keys introduced by the grouping sets.
  // For eg: consider: select key, value, count(1) from T group by key, value with rollup.
  // Assuming map-side aggregation and no skew, the plan would look like:
  //
  // TableScan --> Select --> GroupBy1 --> ReduceSink --> GroupBy2 --> Select --> FileSink
  //
  // This function is called for GroupBy2 to pass the additional grouping keys introduced by
  // GroupBy1 for the grouping set (corresponding to the rollup).
  private void addGroupingSetKey(List<ExprNodeDesc> groupByKeys,
                                 RowResolver groupByInputRowResolver,
                                 RowResolver groupByOutputRowResolver,
                                 List<String> outputColumnNames,
                                 Map<String, ExprNodeDesc> colExprMap) throws SemanticException {
    // For grouping sets, add a dummy grouping key
    String groupingSetColumnName =
        groupByInputRowResolver.get(null, VirtualColumn.GROUPINGID.getName()).getInternalName();
    ExprNodeDesc inputExpr = new ExprNodeColumnDesc(VirtualColumn.GROUPINGID.getTypeInfo(),
        groupingSetColumnName, null, false);
    groupByKeys.add(inputExpr);

    String field = getColumnInternalName(groupByKeys.size() - 1);
    outputColumnNames.add(field);
    groupByOutputRowResolver.put(null, VirtualColumn.GROUPINGID.getName(),
        new ColumnInfo(
            field,
            VirtualColumn.GROUPINGID.getTypeInfo(),
            null,
            true));
    colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
  }

  // Process grouping set for the reduce sink operator
  // For eg: consider: select key, value, count(1) from T group by key, value with rollup.
  // Assuming map-side aggregation and no skew, the plan would look like:
  //
  // TableScan --> Select --> GroupBy1 --> ReduceSink --> GroupBy2 --> Select --> FileSink
  //
  // This function is called for ReduceSink to add the additional grouping keys introduced by
  // GroupBy1 into the reduce keys.
  private void processGroupingSetReduceSinkOperator(RowResolver reduceSinkInputRowResolver,
                                                    RowResolver reduceSinkOutputRowResolver,
                                                    List<ExprNodeDesc> reduceKeys,
                                                    List<String> outputKeyColumnNames,
                                                    Map<String, ExprNodeDesc> colExprMap) throws SemanticException {
    // add a key for reduce sink
    String groupingSetColumnName =
        reduceSinkInputRowResolver.get(null, VirtualColumn.GROUPINGID.getName()).getInternalName();
    ExprNodeDesc inputExpr = new ExprNodeColumnDesc(VirtualColumn.GROUPINGID.getTypeInfo(),
        groupingSetColumnName, null, false);
    reduceKeys.add(inputExpr);

    outputKeyColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
    String field = Utilities.ReduceField.KEY.toString() + "."
        + getColumnInternalName(reduceKeys.size() - 1);
    ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
        reduceKeys.size() - 1).getTypeInfo(), null, true);
    reduceSinkOutputRowResolver.put(null, VirtualColumn.GROUPINGID.getName(), colInfo);
    colExprMap.put(colInfo.getInternalName(), inputExpr);
  }


  /**
   * Generate the GroupByOperator for the Query Block (parseInfo.getXXX(dest)).
   * The new GroupByOperator will be a child of the reduceSinkOperatorInfo.
   *
   * @param parseInfo
   * @param dest
   * @param reduceSinkOperatorInfo
   * @param mode
   *          The mode of the aggregation (MERGEPARTIAL, PARTIAL2)
   * @param genericUDAFEvaluators
   *          The mapping from Aggregation StringTree to the
   *          genericUDAFEvaluator.
   * @param groupingSets
   *          list of grouping sets
   * @param groupingSetsPresent
   *          whether grouping sets are present in this query
   * @param groupingSetsNeedAdditionalMRJob
   *          whether grouping sets are consumed by this group by
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator1(QBParseInfo parseInfo,
      String dest, Operator reduceSinkOperatorInfo, GroupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators,
      List<Long> groupingSets,
      boolean groupingSetsPresent,
      boolean groupingSetsNeedAdditionalMRJob) throws SemanticException {
    List<String> outputColumnNames = new ArrayList<String>();
    RowResolver groupByInputRowResolver = opParseCtx
        .get(reduceSinkOperatorInfo).getRowResolver();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    List<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    List<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ColumnInfo exprInfo = groupByInputRowResolver.getExpression(grpbyExpr);

      if (exprInfo == null) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_COLUMN.getMsg(), grpbyExpr));
      }

      groupByKeys.add(new ExprNodeColumnDesc(exprInfo));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      ColumnInfo oColInfo = new ColumnInfo(field, exprInfo.getType(), "", false);
      groupByOutputRowResolver.putExpression(grpbyExpr,
          oColInfo);
      addAlternateGByKeyMappings(grpbyExpr, oColInfo, reduceSinkOperatorInfo, groupByOutputRowResolver);
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }

    // This is only needed if a new grouping set key is being created
    int groupingSetsPosition = -1;

    // For grouping sets, add a dummy grouping key
    if (groupingSetsPresent) {
      groupingSetsPosition = groupByKeys.size();
      // Consider the query: select a,b, count(1) from T group by a,b with cube;
      // where it is being executed in a single map-reduce job
      // The plan is TableScan -> GroupBy1 -> ReduceSink -> GroupBy2 -> FileSink
      // GroupBy1 already added the grouping id as part of the row
      // This function is called for GroupBy2 to add grouping id as part of the groupby keys
      if (!groupingSetsNeedAdditionalMRJob) {
        addGroupingSetKey(
            groupByKeys,
            groupByInputRowResolver,
            groupByOutputRowResolver,
            outputColumnNames,
            colExprMap);
      }
      else {
        // The grouping set has not yet been processed. Create a new grouping key
        // Consider the query: select a,b, count(1) from T group by a,b with cube;
        // where it is being executed in 2 map-reduce jobs
        // The plan for 1st MR is TableScan -> GroupBy1 -> ReduceSink -> GroupBy2 -> FileSink
        // GroupBy1/ReduceSink worked as if grouping sets were not present
        // This function is called for GroupBy2 to create new rows for grouping sets
        // For each input row (a,b), 4 rows are created for the example above:
        // (a,b), (a,null), (null, b), (null, null)
        createNewGroupingKey(groupByKeys,
            outputColumnNames,
            groupByOutputRowResolver,
            colExprMap);
      }
    }

    Map<String, ASTNode> aggregationTrees = parseInfo.getAggregationExprsForClause(dest);
    // get the last colName for the reduce KEY
    // it represents the column name corresponding to distinct aggr, if any
    String lastKeyColName = null;
    List<ExprNodeDesc> reduceValues = null;
    if (reduceSinkOperatorInfo.getConf() instanceof ReduceSinkDesc) {
      List<String> inputKeyCols = ((ReduceSinkDesc)
          reduceSinkOperatorInfo.getConf()).getOutputKeyColumnNames();
      if (inputKeyCols.size() > 0) {
        lastKeyColName = inputKeyCols.get(inputKeyCols.size() - 1);
      }
      reduceValues = ((ReduceSinkDesc) reduceSinkOperatorInfo.getConf()).getValueCols();
    }
    int numDistinctUDFs = 0;
    boolean containsDistinctAggr = false;
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      String aggName = unescapeIdentifier(value.getChild(0).getText());
      List<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      boolean isDistinct = (value.getType() == HiveParser.TOK_FUNCTIONDI);
      containsDistinctAggr = containsDistinctAggr || isDistinct;

      // If the function is distinct, partial aggregation has not been done on
      // the client side.
      // If distPartAgg is set, the client is letting us know that partial
      // aggregation has not been done.
      // For eg: select a, count(b+c), count(distinct d+e) group by a
      // For count(b+c), if partial aggregation has been performed, then we
      // directly look for count(b+c).
      // Otherwise, we look for b+c.
      // For distincts, partial aggregation is never performed on the client
      // side, so always look for the parameters: d+e
      if (isDistinct) {
        // 0 is the function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode paraExpr = (ASTNode) value.getChild(i);
          ColumnInfo paraExprInfo =
              groupByInputRowResolver.getExpression(paraExpr);
          if (paraExprInfo == null) {
            throw new SemanticException(ASTErrorUtils.getMsg(
                ErrorMsg.INVALID_COLUMN.getMsg(),
                paraExpr));
          }

          String paraExpression = paraExprInfo.getInternalName();
          assert (paraExpression != null);
          if (lastKeyColName != null) {
            // if aggr is distinct, the parameter is name is constructed as
            // KEY.lastKeyColName:<tag>._colx
            paraExpression = Utilities.ReduceField.KEY.name() + "." +
                lastKeyColName + ":" + numDistinctUDFs + "."
                + getColumnInternalName(i - 1);
          }

          ExprNodeDesc expr = new ExprNodeColumnDesc(paraExprInfo.getType(),
              paraExpression, paraExprInfo.getTabAlias(),
              paraExprInfo.getIsVirtualCol());
          ExprNodeDesc reduceValue = isConstantParameterInAggregationParameters(
              paraExprInfo.getInternalName(), reduceValues);

          if (reduceValue != null) {
            // this parameter is a constant
            expr = reduceValue;
          }
          aggParameters.add(expr);
        }
      } else {
        ColumnInfo paraExprInfo = groupByInputRowResolver.getExpression(value);
        if (paraExprInfo == null) {
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.INVALID_COLUMN.getMsg(), value));
        }
        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);
        aggParameters.add(new ExprNodeColumnDesc(paraExprInfo.getType(),
            paraExpression, paraExprInfo.getTabAlias(), paraExprInfo
            .getIsVirtualCol()));
      }
      if (isDistinct) {
        numDistinctUDFs++;
      }

      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = null;
      genericUDAFEvaluator = genericUDAFEvaluators.get(entry.getKey());
      assert (genericUDAFEvaluator != null);

      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations.add(new AggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters,
          (mode != GroupByDesc.Mode.FINAL && isDistinct), amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(value, new ColumnInfo(
          field, udaf.returnType, "", false));
    }
    float groupByMemoryUsage = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MEMORY);
    float memoryThreshold = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_MEMORY_THRESHOLD);
    float minReductionHashAggr = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION);
    float minReductionHashAggrLowerBound = HiveConf
            .getFloatVar(conf, ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION_LOWER_BOUND);
    float hashAggrFlushPercent = HiveConf
        .getFloatVar(conf, ConfVars.HIVE_MAP_AGGR_HASH_FLUSH_SIZE_PERCENT);

    // Nothing special needs to be done for grouping sets if
    // this is the final group by operator, and multiple rows corresponding to the
    // grouping sets have been generated upstream.
    // However, if an addition MR job has been created to handle grouping sets,
    // additional rows corresponding to grouping sets need to be created here.
    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            groupByMemoryUsage, memoryThreshold, minReductionHashAggr, minReductionHashAggrLowerBound,
            hashAggrFlushPercent,
            groupingSets,
            groupingSetsPresent && groupingSetsNeedAdditionalMRJob,
            groupingSetsPosition, containsDistinctAggr),
        new RowSchema(groupByOutputRowResolver.getColumnInfos()), reduceSinkOperatorInfo),
        groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  /*
   * Create a new grouping key for grouping id.
   * A dummy grouping id. is added. At runtime, the group by operator
   * creates 'n' rows per input row, where 'n' is the number of grouping sets.
   */
  private void createNewGroupingKey(List<ExprNodeDesc> groupByKeys,
                                    List<String> outputColumnNames,
                                    RowResolver groupByOutputRowResolver,
                                    Map<String, ExprNodeDesc> colExprMap) {
    // The value for the constant does not matter. It is replaced by the grouping set
    // value for the actual implementation
    ExprNodeConstantDesc constant = new ExprNodeConstantDesc(VirtualColumn.GROUPINGID.getTypeInfo(), 0L);
    groupByKeys.add(constant);
    String field = getColumnInternalName(groupByKeys.size() - 1);
    outputColumnNames.add(field);
    groupByOutputRowResolver.put(null, VirtualColumn.GROUPINGID.getName(),
        new ColumnInfo(
            field,
            VirtualColumn.GROUPINGID.getTypeInfo(),
            null,
            true));
    colExprMap.put(field, constant);
  }

  /**
   * Generate the map-side GroupByOperator for the Query Block
   * (qb.getParseInfo().getXXX(dest)). The new GroupByOperator will be a child
   * of the inputOperatorInfo.
   *
   * @param genericUDAFEvaluators
   *          If not null, this function will store the mapping from Aggregation
   *          StringTree to the genericUDAFEvaluator in this parameter, so it
   *          can be used in the next-stage GroupBy aggregations.
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapGroupByOperator(QB qb,
      String dest,
      List<ASTNode> grpByExprs,
      Operator inputOperatorInfo,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators,
      List<Long> groupingSetKeys,
      boolean groupingSetsPresent) throws SemanticException {

    RowResolver groupByInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRowResolver();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    List<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    List<String> outputColumnNames = new ArrayList<String>();
    List<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ExprNodeDesc grpByExprNode = genExprNodeDesc(grpbyExpr,
          groupByInputRowResolver);

      if ((grpByExprNode instanceof ExprNodeColumnDesc) && ExprNodeDescUtils.indexOf(grpByExprNode, groupByKeys) >= 0) {
        // Skip duplicated grouping keys, it happens when define column alias.
        grpByExprs.remove(i--);
        continue;
      }
      groupByKeys.add(grpByExprNode);
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(grpbyExpr,
          new ColumnInfo(field, grpByExprNode.getTypeInfo(), "", false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }

    // The grouping set key is present after the grouping keys, before the distinct keys
    int groupingSetsPosition = -1;

    // For grouping sets, add a dummy grouping key
    // This dummy key needs to be added as a reduce key
    // For eg: consider: select key, value, count(1) from T group by key, value with rollup.
    // Assuming map-side aggregation and no skew, the plan would look like:
    //
    // TableScan --> Select --> GroupBy1 --> ReduceSink --> GroupBy2 --> Select --> FileSink
    //
    // This function is called for GroupBy1 to create an additional grouping key
    // for the grouping set (corresponding to the rollup).
    if (groupingSetsPresent) {
      groupingSetsPosition = groupByKeys.size();
      createNewGroupingKey(groupByKeys,
          outputColumnNames,
          groupByOutputRowResolver,
          colExprMap);
    }

    // If there is a distinctFuncExp, add all parameters to the reduceKeys.
    if (!parseInfo.getDistinctFuncExprsForClause(dest).isEmpty()) {
      List<ASTNode> list = parseInfo.getDistinctFuncExprsForClause(dest);
      for (ASTNode value : list) {
        // 0 is function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode parameter = (ASTNode) value.getChild(i);
          if (groupByOutputRowResolver.getExpression(parameter) == null) {
            ExprNodeDesc distExprNode = genExprNodeDesc(parameter,
                groupByInputRowResolver);
            groupByKeys.add(distExprNode);
            String field = getColumnInternalName(groupByKeys.size() - 1);
            outputColumnNames.add(field);
            groupByOutputRowResolver.putExpression(parameter, new ColumnInfo(
                field, distExprNode.getTypeInfo(), "", false));
            colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
          }
        }
      }
    }

    // For each aggregation
    Map<String, ASTNode> aggregationTrees = parseInfo.getAggregationExprsForClause(dest);
    assert (aggregationTrees != null);

    boolean containsDistinctAggr = false;
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      String aggName = unescapeIdentifier(value.getChild(0).getText());
      List<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      // 0 is the function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode) value.getChild(i);
        ExprNodeDesc paraExprNode = genExprNodeDesc(paraExpr,
            groupByInputRowResolver);

        aggParameters.add(paraExprNode);
      }

      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      containsDistinctAggr = containsDistinctAggr || isDistinct;
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;
      Mode amode = groupByDescModeToUDAFMode(GroupByDesc.Mode.HASH, isDistinct);

      GenericUDAFEvaluator genericUDAFEvaluator = getGenericUDAFEvaluator(
          aggName, aggParameters, value, isDistinct, isAllColumns);
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations.add(new AggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters, isDistinct,
          amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      if (groupByOutputRowResolver.getExpression(value) == null) {
        groupByOutputRowResolver.putExpression(value, new ColumnInfo(
            field, udaf.returnType, "", false));
      }
      // Save the evaluator so that it can be used by the next-stage
      // GroupByOperators
      if (genericUDAFEvaluators != null) {
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      }
    }
    float groupByMemoryUsage = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MEMORY);
    float memoryThreshold = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_MEMORY_THRESHOLD);
    float minReductionHashAggr = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION);
    float minReductionHashAggrLowerBound = HiveConf
            .getFloatVar(conf, ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION_LOWER_BOUND);
    float hashAggrFlushPercent = HiveConf
        .getFloatVar(conf, ConfVars.HIVE_MAP_AGGR_HASH_FLUSH_SIZE_PERCENT);
    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(GroupByDesc.Mode.HASH, outputColumnNames, groupByKeys, aggregations,
            false, groupByMemoryUsage, memoryThreshold, minReductionHashAggr, minReductionHashAggrLowerBound,
            hashAggrFlushPercent, groupingSetKeys, groupingSetsPresent, groupingSetsPosition, containsDistinctAggr),
        new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        inputOperatorInfo), groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  /**
   * Generate the ReduceSinkOperator for the Group By Query Block
   * (qb.getPartInfo().getXXX(dest)). The new ReduceSinkOperator will be a child
   * of inputOperatorInfo.
   *
   * It will put all Group By keys and the distinct field (if any) in the
   * map-reduce sort key, and all other fields in the map-reduce value.
   *
   * @param numPartitionFields
   *          the number of fields for map-reduce partitioning. This is usually
   *          the number of fields in the Group By keys.
   * @return the new ReduceSinkOperator.
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private ReduceSinkOperator genGroupByPlanReduceSinkOperator(QB qb,
                                                              String dest,
                                                              Operator inputOperatorInfo,
                                                              List<ASTNode> grpByExprs,
                                                              int numPartitionFields,
                                                              boolean changeNumPartitionFields,
                                                              int numReducers,
                                                              boolean mapAggrDone,
                                                              boolean groupingSetsPresent) throws SemanticException {

    RowResolver reduceSinkInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRowResolver();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    // Pre-compute group-by keys and store in reduceKeys

    List<String> outputKeyColumnNames = new ArrayList<String>();
    List<String> outputValueColumnNames = new ArrayList<String>();

    List<ExprNodeDesc> reduceKeys = getReduceKeysForReduceSink(grpByExprs,
        reduceSinkInputRowResolver, reduceSinkOutputRowResolver, outputKeyColumnNames,
        colExprMap);

    int keyLength = reduceKeys.size();
    int numOfColsRmedFromkey = grpByExprs.size() - keyLength;

    // add a key for reduce sink
    if (groupingSetsPresent) {
      // Process grouping set for the reduce sink operator
      processGroupingSetReduceSinkOperator(
          reduceSinkInputRowResolver,
          reduceSinkOutputRowResolver,
          reduceKeys,
          outputKeyColumnNames,
          colExprMap);

      if (changeNumPartitionFields) {
        numPartitionFields++;
      }
    }

    List<List<Integer>> distinctColIndices = getDistinctColIndicesForReduceSink(parseInfo, dest,
        reduceKeys, reduceSinkInputRowResolver, reduceSinkOutputRowResolver, outputKeyColumnNames,
        colExprMap);

    List<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    Map<String, ASTNode> aggregationTrees = parseInfo.getAggregationExprsForClause(dest);

    if (!mapAggrDone) {
      getReduceValuesForReduceSinkNoMapAgg(parseInfo, dest, reduceSinkInputRowResolver,
          reduceSinkOutputRowResolver, outputValueColumnNames, reduceValues, colExprMap);
    } else {
      // Put partial aggregation results in reduceValues
      int inputField = reduceKeys.size() + numOfColsRmedFromkey;

      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {

        TypeInfo type = reduceSinkInputRowResolver.getColumnInfos().get(
            inputField).getType();
        ExprNodeColumnDesc exprDesc = new ExprNodeColumnDesc(type,
            getColumnInternalName(inputField), "", false);
        reduceValues.add(exprDesc);
        inputField++;
        String outputColName = getColumnInternalName(reduceValues.size() - 1);
        outputValueColumnNames.add(outputColName);
        String internalName = Utilities.ReduceField.VALUE.toString() + "."
            + outputColName;
        reduceSinkOutputRowResolver.putExpression(entry.getValue(),
            new ColumnInfo(internalName, type, null, false));
        colExprMap.put(internalName, exprDesc);
      }
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(
            PlanUtils.getReduceSinkDesc(reduceKeys,
                groupingSetsPresent ? keyLength + 1 : keyLength,
                reduceValues, distinctColIndices,
                outputKeyColumnNames, outputValueColumnNames, true, -1, numPartitionFields,
                numReducers, AcidUtils.Operation.NOT_ACID, defaultNullOrder),
            new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()), inputOperatorInfo),
        reduceSinkOutputRowResolver);
    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  private List<ExprNodeDesc> getReduceKeysForReduceSink(List<ASTNode> grpByExprs,
                                                             RowResolver reduceSinkInputRowResolver, RowResolver reduceSinkOutputRowResolver,
                                                             List<String> outputKeyColumnNames, Map<String, ExprNodeDesc> colExprMap)
      throws SemanticException {

    List<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();

    for (ASTNode grpbyExpr : grpByExprs) {
      ExprNodeDesc inputExpr = genExprNodeDesc(grpbyExpr,
          reduceSinkInputRowResolver);
      ColumnInfo prev = reduceSinkOutputRowResolver.getExpression(grpbyExpr);
      if (prev != null && isConsistentWithinQuery(inputExpr)) {
        colExprMap.put(prev.getInternalName(), inputExpr);
        continue;
      }
      reduceKeys.add(inputExpr);
      outputKeyColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
      String field = ReduceField.KEY.toString() + "."
          + getColumnInternalName(reduceKeys.size() - 1);
      ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
          reduceKeys.size() - 1).getTypeInfo(), null, false);
      reduceSinkOutputRowResolver.putExpression(grpbyExpr, colInfo);
      colExprMap.put(colInfo.getInternalName(), inputExpr);
    }

    return reduceKeys;
  }

  private boolean isConsistentWithinQuery(ExprNodeDesc expr) throws SemanticException {
    try {
      return ExprNodeEvaluatorFactory.get(expr).isConsistentWithinQuery();
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private List<List<Integer>> getDistinctColIndicesForReduceSink(QBParseInfo parseInfo,
                                                                 String dest,
                                                                 List<ExprNodeDesc> reduceKeys, RowResolver reduceSinkInputRowResolver,
                                                                 RowResolver reduceSinkOutputRowResolver, List<String> outputKeyColumnNames,
                                                                 Map<String, ExprNodeDesc> colExprMap)
      throws SemanticException {

    List<List<Integer>> distinctColIndices = new ArrayList<List<Integer>>();

    // If there is a distinctFuncExp, add all parameters to the reduceKeys.
    if (!parseInfo.getDistinctFuncExprsForClause(dest).isEmpty()) {
      List<ASTNode> distFuncs = parseInfo.getDistinctFuncExprsForClause(dest);
      String colName = getColumnInternalName(reduceKeys.size());
      outputKeyColumnNames.add(colName);
      for (int i = 0; i < distFuncs.size(); i++) {
        ASTNode value = distFuncs.get(i);
        int numExprs = 0;
        List<Integer> distinctIndices = new ArrayList<Integer>();
        // 0 is function name
        for (int j = 1; j < value.getChildCount(); j++) {
          ASTNode parameter = (ASTNode) value.getChild(j);
          ExprNodeDesc expr = genExprNodeDesc(parameter, reduceSinkInputRowResolver);
          // see if expr is already present in reduceKeys.
          // get index of expr in reduceKeys
          int ri;
          for (ri = 0; ri < reduceKeys.size(); ri++) {
            if (reduceKeys.get(ri).getExprString().equals(expr.getExprString())) {
              break;
            }
          }
          // add the expr to reduceKeys if it is not present
          if (ri == reduceKeys.size()) {
            String name = getColumnInternalName(numExprs);
            String field = Utilities.ReduceField.KEY.toString() + "." + colName
                + ":" + i
                + "." + name;
            ColumnInfo colInfo = new ColumnInfo(field, expr.getTypeInfo(), null, false);
            reduceSinkOutputRowResolver.putExpression(parameter, colInfo);
            colExprMap.put(field, expr);
            reduceKeys.add(expr);
          }
          // add the index of expr in reduceKeys to distinctIndices
          distinctIndices.add(ri);
          numExprs++;
        }
        distinctColIndices.add(distinctIndices);
      }
    }

    return distinctColIndices;
  }

  private void getReduceValuesForReduceSinkNoMapAgg(QBParseInfo parseInfo, String dest,
                                                    RowResolver reduceSinkInputRowResolver,
                                                    RowResolver reduceSinkOutputRowResolver,
                                                    List<String> outputValueColumnNames,
                                                    List<ExprNodeDesc> reduceValues,
                                                    Map<String, ExprNodeDesc> colExprMap) throws SemanticException {
    Map<String, ASTNode> aggregationTrees = parseInfo.getAggregationExprsForClause(dest);

    // Put parameters to aggregations in reduceValues
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      // 0 is function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode parameter = (ASTNode) value.getChild(i);
        if (reduceSinkOutputRowResolver.getExpression(parameter) == null) {
          ExprNodeDesc exprDesc = genExprNodeDesc(parameter, reduceSinkInputRowResolver);
          reduceValues.add(exprDesc);
          outputValueColumnNames
              .add(getColumnInternalName(reduceValues.size() - 1));
          String field = Utilities.ReduceField.VALUE.toString() + "."
              + getColumnInternalName(reduceValues.size() - 1);
          reduceSinkOutputRowResolver.putExpression(parameter, new ColumnInfo(field,
              reduceValues.get(reduceValues.size() - 1).getTypeInfo(), null,
              false));
          colExprMap.put(field, exprDesc);
        }
      }
    }
  }

  @SuppressWarnings("nls")
  private ReduceSinkOperator genCommonGroupByPlanReduceSinkOperator(QB qb, List<String> dests,
                                                                    Operator inputOperatorInfo) throws SemanticException {

    RowResolver reduceSinkInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRowResolver();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    // The group by keys and distinct keys should be the same for all dests, so using the first
    // one to produce these will be the same as using any other.
    String dest = dests.get(0);

    // Pre-compute group-by keys and store in reduceKeys
    List<String> outputKeyColumnNames = new ArrayList<String>();
    List<String> outputValueColumnNames = new ArrayList<String>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);

    List<ExprNodeDesc> reduceKeys = getReduceKeysForReduceSink(grpByExprs,
        reduceSinkInputRowResolver, reduceSinkOutputRowResolver, outputKeyColumnNames,
        colExprMap);

    int keyLength = reduceKeys.size();

    List<List<Integer>> distinctColIndices = getDistinctColIndicesForReduceSink(parseInfo, dest,
        reduceKeys, reduceSinkInputRowResolver, reduceSinkOutputRowResolver, outputKeyColumnNames,
        colExprMap);

    List<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();

    // The dests can have different non-distinct aggregations, so we have to iterate over all of
    // them
    for (String destination : dests) {

      getReduceValuesForReduceSinkNoMapAgg(parseInfo, destination, reduceSinkInputRowResolver,
          reduceSinkOutputRowResolver, outputValueColumnNames, reduceValues, colExprMap);

      // Need to pass all of the columns used in the where clauses as reduce values
      ASTNode whereClause = parseInfo.getWhrForClause(destination);
      if (whereClause != null) {
        assert whereClause.getChildCount() == 1;
        ASTNode predicates = (ASTNode) whereClause.getChild(0);

        Map<ASTNode, ExprNodeDesc> nodeOutputs =
            genAllExprNodeDesc(predicates, reduceSinkInputRowResolver);
        removeMappingForKeys(predicates, nodeOutputs, reduceKeys);

        // extract columns missing in current RS key/value
        for (Map.Entry<ASTNode, ExprNodeDesc> entry : nodeOutputs.entrySet()) {
          ASTNode parameter = entry.getKey();
          ExprNodeDesc expression = entry.getValue();
          if (!(expression instanceof ExprNodeColumnDesc) && !ExprNodeConstantDesc.isFoldedFromCol(expression)) {
            continue;
          }
          if (ExprNodeDescUtils.indexOf(expression, reduceValues) >= 0) {
            continue;
          }
          String internalName = getColumnInternalName(reduceValues.size());
          String field = Utilities.ReduceField.VALUE.toString() + "." + internalName;

          reduceValues.add(expression);
          outputValueColumnNames.add(internalName);
          reduceSinkOutputRowResolver.putExpression(parameter,
              new ColumnInfo(field, expression.getTypeInfo(), null, false));
          colExprMap.put(field, expression);
        }
      }
    }

    // Optimize the scenario when there are no grouping keys - only 1 reducer is needed
    int numReducers = -1;
    if (grpByExprs.isEmpty()) {
      numReducers = 1;
    }
    ReduceSinkDesc rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys, keyLength, reduceValues,
        distinctColIndices, outputKeyColumnNames, outputValueColumnNames,
        true, -1, keyLength, numReducers, AcidUtils.Operation.NOT_ACID, defaultNullOrder);

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(rsDesc, new RowSchema(reduceSinkOutputRowResolver
            .getColumnInfos()), inputOperatorInfo), reduceSinkOutputRowResolver);
    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  // Remove expression node descriptor and children of it for a given predicate
  // from mapping if it's already on RS keys.
  // Remaining column expressions would be a candidate for an RS value
  private void removeMappingForKeys(ASTNode predicate, Map<ASTNode, ExprNodeDesc> mapping,
                                    List<ExprNodeDesc> keys) {
    ExprNodeDesc expr = mapping.get(predicate);
    if (expr != null && ExprNodeDescUtils.indexOf(expr, keys) >= 0) {
      removeRecursively(predicate, mapping);
    } else {
      for (int i = 0; i < predicate.getChildCount(); i++) {
        removeMappingForKeys((ASTNode) predicate.getChild(i), mapping, keys);
      }
    }
  }

  // Remove expression node desc and all children of it from mapping
  private void removeRecursively(ASTNode current, Map<ASTNode, ExprNodeDesc> mapping) {
    mapping.remove(current);
    for (int i = 0; i < current.getChildCount(); i++) {
      removeRecursively((ASTNode) current.getChild(i), mapping);
    }
  }

  /**
   * Generate the second ReduceSinkOperator for the Group By Plan
   * (parseInfo.getXXX(dest)). The new ReduceSinkOperator will be a child of
   * groupByOperatorInfo.
   *
   * The second ReduceSinkOperator will put the group by keys in the map-reduce
   * sort key, and put the partial aggregation results in the map-reduce value.
   *
   * @param numPartitionFields
   *          the number of fields in the map-reduce partition key. This should
   *          always be the same as the number of Group By keys. We should be
   *          able to remove this parameter since in this phase there is no
   *          distinct any more.
   * @return the new ReduceSinkOperator.
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanReduceSinkOperator2MR(QBParseInfo parseInfo,
                                                       String dest,
                                                       Operator groupByOperatorInfo,
                                                       int numPartitionFields,
                                                       int numReducers,
                                                       boolean groupingSetsPresent) throws SemanticException {
    RowResolver reduceSinkInputRowResolver2 = opParseCtx.get(
        groupByOperatorInfo).getRowResolver();
    RowResolver reduceSinkOutputRowResolver2 = new RowResolver();
    reduceSinkOutputRowResolver2.setIsExprResolver(true);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();
    List<String> outputColumnNames = new ArrayList<String>();
    // Get group-by keys and store in reduceKeys
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      TypeInfo typeInfo = reduceSinkInputRowResolver2.getExpression(
          grpbyExpr).getType();
      ExprNodeColumnDesc inputExpr = new ExprNodeColumnDesc(typeInfo, field,
          "", false);
      reduceKeys.add(inputExpr);
      ColumnInfo colInfo = new ColumnInfo(Utilities.ReduceField.KEY.toString()
          + "." + field, typeInfo, "", false);
      reduceSinkOutputRowResolver2.putExpression(grpbyExpr, colInfo);
      colExprMap.put(colInfo.getInternalName(), inputExpr);
    }

    // add a key for reduce sink
    if (groupingSetsPresent) {
      // Note that partitioning fields dont need to change, since it is either
      // partitioned randomly, or by all grouping keys + distinct keys
      processGroupingSetReduceSinkOperator(
          reduceSinkInputRowResolver2,
          reduceSinkOutputRowResolver2,
          reduceKeys,
          outputColumnNames,
          colExprMap);
    }

    // Get partial aggregation results and store in reduceValues
    List<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    int inputField = reduceKeys.size();
    Map<String, ASTNode> aggregationTrees = parseInfo.getAggregationExprsForClause(dest);
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      String field = getColumnInternalName(inputField);
      ASTNode t = entry.getValue();
      TypeInfo typeInfo = reduceSinkInputRowResolver2.getExpression(t)
          .getType();
      ExprNodeColumnDesc exprDesc = new ExprNodeColumnDesc(typeInfo, field, "", false);
      reduceValues.add(exprDesc);
      inputField++;
      String col = getColumnInternalName(reduceValues.size() - 1);
      outputColumnNames.add(col);
      ColumnInfo colInfo = new ColumnInfo(
          Utilities.ReduceField.VALUE.toString() + "." + col, typeInfo, "",
          false);
      reduceSinkOutputRowResolver2.putExpression(t, colInfo);
      colExprMap.put(colInfo.getInternalName(), exprDesc);
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
            reduceValues, outputColumnNames, true, -1, numPartitionFields,
            numReducers, AcidUtils.Operation.NOT_ACID, defaultNullOrder),
            new RowSchema(reduceSinkOutputRowResolver2.getColumnInfos()), groupByOperatorInfo),
        reduceSinkOutputRowResolver2);

    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  /**
   * Generate the second GroupByOperator for the Group By Plan
   * (parseInfo.getXXX(dest)). The new GroupByOperator will do the second
   * aggregation based on the partial aggregation results.
   *
   * @param genericUDAFEvaluators
   *          The mapping from Aggregation StringTree to the
   *          genericUDAFEvaluator.
   * @return the new GroupByOperator
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator2MR(QBParseInfo parseInfo,
                                                    String dest,
                                                    Operator reduceSinkOperatorInfo2,
                                                    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators,
                                                    boolean groupingSetsPresent) throws SemanticException {

    RowResolver groupByInputRowResolver2 = opParseCtx.get(
        reduceSinkOperatorInfo2).getRowResolver();
    RowResolver groupByOutputRowResolver2 = new RowResolver();
    groupByOutputRowResolver2.setIsExprResolver(true);
    List<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    List<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    List<String> outputColumnNames = new ArrayList<String>();
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ColumnInfo exprInfo = groupByInputRowResolver2.getExpression(grpbyExpr);
      if (exprInfo == null) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_COLUMN.getMsg(), grpbyExpr));
      }

      String expression = exprInfo.getInternalName();
      groupByKeys.add(new ExprNodeColumnDesc(exprInfo.getType(), expression,
          exprInfo.getTabAlias(), exprInfo.getIsVirtualCol()));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      ColumnInfo oColInfo = new ColumnInfo(field, exprInfo.getType(), "", false);
      groupByOutputRowResolver2.putExpression(grpbyExpr,
          oColInfo);
      addAlternateGByKeyMappings(grpbyExpr, oColInfo, reduceSinkOperatorInfo2, groupByOutputRowResolver2);
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }

    int groupingSetsPosition = -1;
    // For grouping sets, add a dummy grouping key
    if (groupingSetsPresent) {
      groupingSetsPosition = groupByKeys.size();
      addGroupingSetKey(
          groupByKeys,
          groupByInputRowResolver2,
          groupByOutputRowResolver2,
          outputColumnNames,
          colExprMap);
    }

    Map<String, ASTNode> aggregationTrees = parseInfo.getAggregationExprsForClause(dest);
    boolean containsDistinctAggr = false;
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      List<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      ASTNode value = entry.getValue();
      ColumnInfo paraExprInfo = groupByInputRowResolver2.getExpression(value);
      if (paraExprInfo == null) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_COLUMN.getMsg(), value));
      }
      String paraExpression = paraExprInfo.getInternalName();
      assert (paraExpression != null);
      aggParameters.add(new ExprNodeColumnDesc(paraExprInfo.getType(),
          paraExpression, paraExprInfo.getTabAlias(), paraExprInfo
          .getIsVirtualCol()));

      String aggName = unescapeIdentifier(value.getChild(0).getText());

      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      containsDistinctAggr = containsDistinctAggr || isDistinct;
      Mode amode = groupByDescModeToUDAFMode(GroupByDesc.Mode.FINAL, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = genericUDAFEvaluators
          .get(entry.getKey());
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations
          .add(new AggregationDesc(
              aggName.toLowerCase(),
              udaf.genericUDAFEvaluator,
              udaf.convertedParameters,
              false,
              amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver2.putExpression(value, new ColumnInfo(
          field, udaf.returnType, "", false));
    }
    float groupByMemoryUsage = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MEMORY);
    float memoryThreshold = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_MEMORY_THRESHOLD);
    float minReductionHashAggr = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION);
    float minReductionHashAggrLowerBound = HiveConf
            .getFloatVar(conf, ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION_LOWER_BOUND);
    float hashAggrFlushPercent = HiveConf
        .getFloatVar(conf, ConfVars.HIVE_MAP_AGGR_HASH_FLUSH_SIZE_PERCENT);

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(GroupByDesc.Mode.FINAL, outputColumnNames, groupByKeys, aggregations,
            false, groupByMemoryUsage, memoryThreshold, minReductionHashAggr, minReductionHashAggrLowerBound,
                hashAggrFlushPercent, null, false,
            groupingSetsPosition, containsDistinctAggr),
        new RowSchema(groupByOutputRowResolver2.getColumnInfos()),
        reduceSinkOperatorInfo2), groupByOutputRowResolver2);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  /**
   * Generate a Group-By plan using a single map-reduce job (3 operators will be
   * inserted):
   *
   * ReduceSink ( keys = (K1_EXP, K2_EXP, DISTINCT_EXP), values = (A1_EXP,
   * A2_EXP) ) SortGroupBy (keys = (KEY.0,KEY.1), aggregations =
   * (count_distinct(KEY.2), sum(VALUE.0), count(VALUE.1))) Select (final
   * selects).
   *
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   *
   *           Generate a Group-By plan using 1 map-reduce job. Spray by the
   *           group by key, and sort by the distinct key (if any), and compute
   *           aggregates * The aggregation evaluation functions are as
   *           follows: Partitioning Key: grouping key
   *
   *           Sorting Key: grouping key if no DISTINCT grouping + distinct key
   *           if DISTINCT
   *
   *           Reducer: iterate/merge (mode = COMPLETE)
   **/
  @SuppressWarnings({"nls"})
  private Operator genGroupByPlan1MR(String dest, QB qb, Operator input)
      throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    int numReducers = -1;
    Pair<List<ASTNode>, List<Long>> grpByExprsGroupingSets = getGroupByGroupingSetsForClause(parseInfo, dest);

    List<ASTNode> grpByExprs = grpByExprsGroupingSets.getLeft();
    List<Long> groupingSets = grpByExprsGroupingSets.getRight();

    if (grpByExprs.isEmpty()) {
      numReducers = 1;
    }

    // Grouping sets are not allowed
    if (!groupingSets.isEmpty()) {
      throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_AGGR_NOMAPAGGR.getMsg());
    }

    // ////// 1. Generate ReduceSinkOperator
    ReduceSinkOperator reduceSinkOperatorInfo =
        genGroupByPlanReduceSinkOperator(qb,
            dest,
            input,
            grpByExprs,
            grpByExprs.size(),
            false,
            numReducers,
            false,
            false);

    // ////// 2. Generate GroupbyOperator
    Operator groupByOperatorInfo = genGroupByPlanGroupByOperator(parseInfo,
        dest, reduceSinkOperatorInfo, reduceSinkOperatorInfo, GroupByDesc.Mode.COMPLETE, null);

    return groupByOperatorInfo;
  }

  @SuppressWarnings({"nls"})
  private Operator genGroupByPlan1ReduceMultiGBY(List<String> dests, QB qb, Operator input,
                                                 Map<String, Operator> aliasToOpInfo)
      throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    ExprNodeDesc previous = null;
    Operator selectInput = input;

    // In order to facilitate partition pruning, or the where clauses together and put them at the
    // top of the operator tree, this could also reduce the amount of data going to the reducer
    List<ExprNodeDesc.ExprNodeDescEqualityWrapper> whereExpressions =
        new ArrayList<ExprNodeDesc.ExprNodeDescEqualityWrapper>();
    for (String dest : dests) {
      Pair<List<ASTNode>, List<Long>> grpByExprsGroupingSets =
          getGroupByGroupingSetsForClause(parseInfo, dest);

      List<Long> groupingSets = grpByExprsGroupingSets.getRight();
      if (!groupingSets.isEmpty()) {
        throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_AGGR_NOMAPAGGR_MULTIGBY.getMsg());
      }

      ASTNode whereExpr = parseInfo.getWhrForClause(dest);

      if (whereExpr != null) {
        OpParseContext inputCtx = opParseCtx.get(input);
        RowResolver inputRR = inputCtx.getRowResolver();
        ExprNodeDesc current = genExprNodeDesc((ASTNode) whereExpr.getChild(0), inputRR);

        // Check the list of where expressions already added so they aren't duplicated
        ExprNodeDesc.ExprNodeDescEqualityWrapper currentWrapped =
            new ExprNodeDesc.ExprNodeDescEqualityWrapper(current);
        if (!whereExpressions.contains(currentWrapped)) {
          whereExpressions.add(currentWrapped);
        } else {
          continue;
        }

        if (previous == null) {
          // If this is the first expression
          previous = current;
          continue;
        }

        GenericUDFOPOr or = new GenericUDFOPOr();
        List<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(2);
        expressions.add(current);
        expressions.add(previous);
        previous = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, or, expressions);
      } else {
        // If an expression does not have a where clause, there can be no common filter
        previous = null;
        break;
      }
    }

    if (previous != null) {
      OpParseContext inputCtx = opParseCtx.get(input);
      RowResolver inputRR = inputCtx.getRowResolver();
      FilterDesc orFilterDesc = new FilterDesc(previous, false);
      orFilterDesc.setGenerated(true);

      selectInput = putOpInsertMap(OperatorFactory.getAndMakeChild(orFilterDesc, new RowSchema(
          inputRR.getColumnInfos()), input), inputRR);
    }

    // insert a select operator here used by the ColumnPruner to reduce
    // the data to shuffle
    Operator select = genSelectAllDesc(selectInput);

    // Generate ReduceSinkOperator
    ReduceSinkOperator reduceSinkOperatorInfo =
        genCommonGroupByPlanReduceSinkOperator(qb, dests, select);

    // It is assumed throughout the code that a reducer has a single child, add a
    // ForwardOperator so that we can add multiple filter/group by operators as children
    RowResolver reduceSinkOperatorInfoRR = opParseCtx.get(reduceSinkOperatorInfo).getRowResolver();
    Operator forwardOp = putOpInsertMap(OperatorFactory.getAndMakeChild(new ForwardDesc(),
        new RowSchema(reduceSinkOperatorInfoRR.getColumnInfos()), reduceSinkOperatorInfo),
        reduceSinkOperatorInfoRR);

    Operator curr = forwardOp;

    for (String dest : dests) {
      curr = forwardOp;

      if (parseInfo.getWhrForClause(dest) != null) {
        ASTNode whereExpr = qb.getParseInfo().getWhrForClause(dest);
        curr = genFilterPlan((ASTNode) whereExpr.getChild(0), qb, forwardOp, aliasToOpInfo, false, true);
      }

      // Generate GroupbyOperator
      Operator groupByOperatorInfo = genGroupByPlanGroupByOperator(parseInfo,
          dest, curr, reduceSinkOperatorInfo, GroupByDesc.Mode.COMPLETE, null);

      // TODO: should we pass curr instead of null?
      curr = genPostGroupByBodyPlan(groupByOperatorInfo, dest, qb, aliasToOpInfo, null);
    }

    return curr;
  }

  /**
   * Generate a Group-By plan using a 2 map-reduce jobs (5 operators will be
   * inserted):
   *
   * ReduceSink ( keys = (K1_EXP, K2_EXP, DISTINCT_EXP), values = (A1_EXP,
   * A2_EXP) ) NOTE: If DISTINCT_EXP is null, partition by rand() SortGroupBy
   * (keys = (KEY.0,KEY.1), aggregations = (count_distinct(KEY.2), sum(VALUE.0),
   * count(VALUE.1))) ReduceSink ( keys = (0,1), values=(2,3,4)) SortGroupBy
   * (keys = (KEY.0,KEY.1), aggregations = (sum(VALUE.0), sum(VALUE.1),
   * sum(VALUE.2))) Select (final selects).
   *
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   *
   *           Generate a Group-By plan using a 2 map-reduce jobs. Spray by the
   *           grouping key and distinct key (or a random number, if no distinct
   *           is present) in hope of getting a uniform distribution, and
   *           compute partial aggregates grouped by the reduction key (grouping
   *           key + distinct key). Evaluate partial aggregates first, and spray
   *           by the grouping key to compute actual aggregates in the second
   *           phase. The aggregation evaluation functions are as follows:
   *           Partitioning Key: random() if no DISTINCT grouping + distinct key
   *           if DISTINCT
   *
   *           Sorting Key: grouping key if no DISTINCT grouping + distinct key
   *           if DISTINCT
   *
   *           Reducer: iterate/terminatePartial (mode = PARTIAL1)
   *
   *           STAGE 2
   *
   *           Partitioning Key: grouping key
   *
   *           Sorting Key: grouping key if no DISTINCT grouping + distinct key
   *           if DISTINCT
   *
   *           Reducer: merge/terminate (mode = FINAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlan2MR(String dest, QB qb, Operator input)
      throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    Pair<List<ASTNode>, List<Long>> grpByExprsGroupingSets = getGroupByGroupingSetsForClause(parseInfo, dest);

    List<ASTNode> grpByExprs = grpByExprsGroupingSets.getLeft();
    List<Long> groupingSets = grpByExprsGroupingSets.getRight();

    // Grouping sets are not allowed
    // This restriction can be lifted in future.
    // HIVE-3508 has been filed for this
    if (!groupingSets.isEmpty()) {
      throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_AGGR_NOMAPAGGR.getMsg());
    }

    // ////// 1. Generate ReduceSinkOperator
    // There is a special case when we want the rows to be randomly distributed
    // to
    // reducers for load balancing problem. That happens when there is no
    // DISTINCT
    // operator. We set the numPartitionColumns to -1 for this purpose. This is
    // captured by WritableComparableHiveObject.hashCode() function.
    ReduceSinkOperator reduceSinkOperatorInfo =
        genGroupByPlanReduceSinkOperator(qb,
            dest,
            input,
            grpByExprs,
            (parseInfo.getDistinctFuncExprsForClause(dest).isEmpty() ? -1 : Integer.MAX_VALUE),
            false,
            -1,
            false,
            false);

    // ////// 2. Generate GroupbyOperator
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators =
        new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo = (GroupByOperator) genGroupByPlanGroupByOperator(
        parseInfo, dest, reduceSinkOperatorInfo, reduceSinkOperatorInfo, GroupByDesc.Mode.PARTIAL1,
        genericUDAFEvaluators);

    int numReducers = -1;
    if (grpByExprs.isEmpty()) {
      numReducers = 1;
    }

    // ////// 3. Generate ReduceSinkOperator2
    Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
        parseInfo, dest, groupByOperatorInfo, grpByExprs.size(), numReducers, false);

    // ////// 4. Generate GroupbyOperator2
    Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator2MR(parseInfo,
        dest, reduceSinkOperatorInfo2,
        genericUDAFEvaluators, false);

    return groupByOperatorInfo2;
  }

  private boolean optimizeMapAggrGroupBy(String dest, QB qb) throws SemanticException {
    List<ASTNode> grpByExprs = getGroupByForClause(qb.getParseInfo(), dest);
    if ((grpByExprs != null) && !grpByExprs.isEmpty()) {
      return false;
    }

    return qb.getParseInfo().getDistinctFuncExprsForClause(dest).isEmpty();
  }

  /**
   * Generate a Group-By plan using 1 map-reduce job. First perform a map-side
   * partial aggregation (to reduce the amount of data), at this point of time,
   * we may turn off map-side partial aggregation based on its performance. Then
   * spray by the group by key, and sort by the distinct key (if any), and
   * compute aggregates based on actual aggregates
   *
   * The aggregation evaluation functions are as follows:
   *
   * No grouping sets:
   * Group By Operator:
   * grouping keys: group by expressions if no DISTINCT
   * grouping keys: group by expressions + distinct keys if DISTINCT
   * Mapper: iterate/terminatePartial (mode = HASH)
   * Partitioning Key: grouping key
   * Sorting Key: grouping key if no DISTINCT
   * grouping + distinct key if DISTINCT
   * Reducer: iterate/terminate if DISTINCT
   * merge/terminate if NO DISTINCT (mode MERGEPARTIAL)
   *
   * Grouping Sets:
   * Group By Operator:
   * grouping keys: group by expressions + grouping id. if no DISTINCT
   * grouping keys: group by expressions + grouping id. + distinct keys if DISTINCT
   * Mapper: iterate/terminatePartial (mode = HASH)
   * Partitioning Key: grouping key + grouping id.
   * Sorting Key: grouping key + grouping id. if no DISTINCT
   * grouping + grouping id. + distinct key if DISTINCT
   * Reducer: iterate/terminate if DISTINCT
   * merge/terminate if NO DISTINCT (mode MERGEPARTIAL)
   *
   * Grouping Sets with an additional MR job introduced (distincts are not allowed):
   * Group By Operator:
   * grouping keys: group by expressions
   * Mapper: iterate/terminatePartial (mode = HASH)
   * Partitioning Key: grouping key
   * Sorting Key: grouping key
   * Reducer: merge/terminate (mode MERGEPARTIAL)
   * Group by Operator:
   * grouping keys: group by expressions + add a new grouping id. key
   *
   * STAGE 2
   * Partitioning Key: grouping key + grouping id.
   * Sorting Key: grouping key + grouping id.
   * Reducer: merge/terminate (mode = FINAL)
   * Group by Operator:
   * grouping keys: group by expressions + grouping id.
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapAggrNoSkew(String dest, QB qb,
                                               Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();
    Pair<List<ASTNode>, List<Long>> grpByExprsGroupingSets = getGroupByGroupingSetsForClause(parseInfo, dest);

    List<ASTNode> grpByExprs = grpByExprsGroupingSets.getLeft();
    List<Long> groupingSets = grpByExprsGroupingSets.getRight();
    boolean groupingSetsPresent = !groupingSets.isEmpty();

    int newMRJobGroupingSetsThreshold =
        conf.getIntVar(HiveConf.ConfVars.HIVE_NEW_JOB_GROUPING_SET_CARDINALITY);

    // ////// Generate GroupbyOperator for a map-side partial aggregation
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators =
        new LinkedHashMap<String, GenericUDAFEvaluator>();

    // Is the grouping sets data consumed in the current in MR job, or
    // does it need an additional MR job
    boolean groupingSetsNeedAdditionalMRJob = groupingSetsPresent &&
        groupingSets.size() > newMRJobGroupingSetsThreshold;

    GroupByOperator groupByOperatorInfo =
        (GroupByOperator) genGroupByPlanMapGroupByOperator(
            qb,
            dest,
            grpByExprs,
            inputOperatorInfo,
            genericUDAFEvaluators,
            groupingSets,
            groupingSetsPresent && !groupingSetsNeedAdditionalMRJob);

    groupOpToInputTables.put(groupByOperatorInfo, opParseCtx.get(
        inputOperatorInfo).getRowResolver().getTableNames());
    int numReducers = -1;

    // Optimize the scenario when there are no grouping keys - only 1 reducer is
    // needed
    if (grpByExprs.isEmpty()) {
      numReducers = 1;
    }

    // ////// Generate ReduceSink Operator
    boolean isDistinct = !qb.getParseInfo().getDistinctFuncExprsForClause(dest).isEmpty();

    // Distincts are not allowed with an additional mr job
    if (groupingSetsNeedAdditionalMRJob && isDistinct) {
      String errorMsg = "The number of rows per input row due to grouping sets is "
          + groupingSets.size();
      throw new SemanticException(
          ErrorMsg.HIVE_GROUPING_SETS_THRESHOLD_NOT_ALLOWED_WITH_DISTINCTS.getMsg(errorMsg));
    }

    Operator reduceSinkOperatorInfo =
        genGroupByPlanReduceSinkOperator(qb,
            dest,
            groupByOperatorInfo,
            grpByExprs,
            grpByExprs.size(),
            true,
            numReducers,
            true,
            groupingSetsPresent && !groupingSetsNeedAdditionalMRJob);

    // Does it require a new MR job for grouping sets
    if (!groupingSetsPresent || !groupingSetsNeedAdditionalMRJob) {
      // This is a 1-stage map-reduce processing of the groupby. Tha map-side
      // aggregates was just used to
      // reduce output data. In case of distincts, partial results are not used,
      // and so iterate is again
      // invoked on the reducer. In case of non-distincts, partial results are
      // used, and merge is invoked
      // on the reducer.
      return genGroupByPlanGroupByOperator1(parseInfo, dest,
          reduceSinkOperatorInfo, GroupByDesc.Mode.MERGEPARTIAL,
          genericUDAFEvaluators,
          groupingSets, groupingSetsPresent, groupingSetsNeedAdditionalMRJob);
    }
    else
    {
      // Add 'n' rows corresponding to the grouping sets. For each row, create 'n' rows,
      // one for each grouping set key. Since map-side aggregation has already been performed,
      // the number of rows would have been reduced. Moreover, the rows corresponding to the
      // grouping keys come together, so there is a higher chance of finding the rows in the hash
      // table.
      Operator groupByOperatorInfo2 =
          genGroupByPlanGroupByOperator1(parseInfo, dest,
              reduceSinkOperatorInfo, GroupByDesc.Mode.PARTIALS,
              genericUDAFEvaluators,
              groupingSets, groupingSetsPresent, groupingSetsNeedAdditionalMRJob);

      // ////// Generate ReduceSinkOperator2
      Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
          parseInfo, dest, groupByOperatorInfo2, grpByExprs.size() + 1, numReducers,
          groupingSetsPresent);

      // ////// Generate GroupbyOperator3
      return genGroupByPlanGroupByOperator2MR(parseInfo, dest,
          reduceSinkOperatorInfo2,
          genericUDAFEvaluators, groupingSetsPresent);
    }
  }

  /**
   * Generate a Group-By plan using a 2 map-reduce jobs. However, only 1
   * group-by plan is generated if the query involves no grouping key and no
   * distincts. In that case, the plan is same as generated by
   * genGroupByPlanMapAggr1MR. Otherwise, the following plan is generated: First
   * perform a map side partial aggregation (to reduce the amount of data). Then
   * spray by the grouping key and distinct key (or a random number, if no
   * distinct is present) in hope of getting a uniform distribution, and compute
   * partial aggregates grouped by the reduction key (grouping key + distinct
   * key). Evaluate partial aggregates first, and spray by the grouping key to
   * compute actual aggregates in the second phase.
   *
   * The aggregation evaluation functions are as follows:
   *
   * No grouping sets:
   * STAGE 1
   * Group by Operator:
   * grouping keys: group by expressions if no DISTINCT
   * grouping keys: group by expressions + distinct keys if DISTINCT
   * Mapper: iterate/terminatePartial (mode = HASH)
   * Partitioning Key: random() if no DISTINCT
   * grouping + distinct key if DISTINCT
   * Sorting Key: grouping key if no DISTINCT
   * grouping + distinct key if DISTINCT
   * Reducer: iterate/terminatePartial if DISTINCT
   * merge/terminatePartial if NO DISTINCT (mode = MERGEPARTIAL)
   * Group by Operator:
   * grouping keys: group by expressions
   *
   * STAGE 2
   * Partitioning Key: grouping key
   * Sorting Key: grouping key
   * Reducer: merge/terminate (mode = FINAL)
   *
   * In the presence of grouping sets, the aggregation evaluation functions are as follows:
   * STAGE 1
   * Group by Operator:
   * grouping keys: group by expressions + grouping id. if no DISTINCT
   * grouping keys: group by expressions + + grouping id. + distinct keys if DISTINCT
   * Mapper: iterate/terminatePartial (mode = HASH)
   * Partitioning Key: random() if no DISTINCT
   * grouping + grouping id. + distinct key if DISTINCT
   * Sorting Key: grouping key + grouping id. if no DISTINCT
   * grouping + grouping id. + distinct key if DISTINCT
   * Reducer: iterate/terminatePartial if DISTINCT
   * merge/terminatePartial if NO DISTINCT (mode = MERGEPARTIAL)
   * Group by Operator:
   * grouping keys: group by expressions + grouping id.
   *
   * STAGE 2
   * Partitioning Key: grouping key
   * Sorting Key: grouping key + grouping id.
   * Reducer: merge/terminate (mode = FINAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapAggr2MR(String dest, QB qb,
                                            Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    Pair<List<ASTNode>, List<Long>> grpByExprsGroupingSets = getGroupByGroupingSetsForClause(parseInfo, dest);

    List<ASTNode> grpByExprs = grpByExprsGroupingSets.getLeft();
    List<Long> groupingSets = grpByExprsGroupingSets.getRight();
    boolean groupingSetsPresent = !groupingSets.isEmpty();

    if (groupingSetsPresent) {

      int newMRJobGroupingSetsThreshold =
          conf.getIntVar(HiveConf.ConfVars.HIVE_NEW_JOB_GROUPING_SET_CARDINALITY);

      // Turn off skew if an additional MR job is required anyway for grouping sets.
      if (groupingSets.size() > newMRJobGroupingSetsThreshold) {
        String errorMsg = "The number of rows per input row due to grouping sets is "
            + groupingSets.size();
        throw new SemanticException(
            ErrorMsg.HIVE_GROUPING_SETS_THRESHOLD_NOT_ALLOWED_WITH_SKEW.getMsg(errorMsg));
      }
    }

    // ////// Generate GroupbyOperator for a map-side partial aggregation
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators =
        new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo =
        (GroupByOperator) genGroupByPlanMapGroupByOperator(
            qb, dest, grpByExprs, inputOperatorInfo,
            genericUDAFEvaluators, groupingSets, groupingSetsPresent);

    groupOpToInputTables.put(groupByOperatorInfo, opParseCtx.get(
        inputOperatorInfo).getRowResolver().getTableNames());
    // Optimize the scenario when there are no grouping keys and no distinct - 2
    // map-reduce jobs are not needed
    // For eg: select count(1) from T where t.ds = ....
    if (!optimizeMapAggrGroupBy(dest, qb)) {
      List<ASTNode> distinctFuncExprs = parseInfo.getDistinctFuncExprsForClause(dest);

      // ////// Generate ReduceSink Operator
      Operator reduceSinkOperatorInfo =
          genGroupByPlanReduceSinkOperator(qb,
              dest,
              groupByOperatorInfo,
              grpByExprs,
              distinctFuncExprs.isEmpty() ? -1 : Integer.MAX_VALUE,
              false,
              -1,
              true,
              groupingSetsPresent);

      // ////// Generate GroupbyOperator for a partial aggregation
      Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator1(parseInfo,
          dest, reduceSinkOperatorInfo, GroupByDesc.Mode.PARTIALS,
          genericUDAFEvaluators,
          groupingSets, groupingSetsPresent, false);

      int numReducers = -1;
      if (grpByExprs.isEmpty()) {
        numReducers = 1;
      }

      // ////// Generate ReduceSinkOperator2
      Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
          parseInfo, dest, groupByOperatorInfo2, grpByExprs.size(), numReducers,
          groupingSetsPresent);

      // ////// Generate GroupbyOperator3
      return genGroupByPlanGroupByOperator2MR(parseInfo, dest,
          reduceSinkOperatorInfo2,
          genericUDAFEvaluators, groupingSetsPresent);
    } else {
      // If there are no grouping keys, grouping sets cannot be present
      assert !groupingSetsPresent;

      // ////// Generate ReduceSink Operator
      Operator reduceSinkOperatorInfo =
          genGroupByPlanReduceSinkOperator(qb,
              dest,
              groupByOperatorInfo,
              grpByExprs,
              grpByExprs.size(),
              false,
              1,
              true,
              groupingSetsPresent);

      return genGroupByPlanGroupByOperator2MR(parseInfo, dest,
          reduceSinkOperatorInfo, genericUDAFEvaluators, false);
    }
  }

  private int getReducersBucketing(int totalFiles, int maxReducers) {
    int numFiles = (int)Math.ceil((double)totalFiles / (double)maxReducers);
    while (true) {
      if (totalFiles % numFiles == 0) {
        return totalFiles / numFiles;
      }
      numFiles++;
    }
  }

  private static class SortBucketRSCtx {
    List<ExprNodeDesc> partnCols;
    boolean multiFileSpray;
    int numFiles;
    int totalFiles;

    public SortBucketRSCtx() {
      partnCols = null;
      multiFileSpray = false;
      numFiles = 1;
      totalFiles = 1;
    }

    /**
     * @return the partnCols
     */
    public List<ExprNodeDesc> getPartnCols() {
      return partnCols;
    }

    /**
     * @param partnCols
     *          the partnCols to set
     */
    public void setPartnCols(List<ExprNodeDesc> partnCols) {
      this.partnCols = partnCols;
    }

    /**
     * @return the multiFileSpray
     */
    public boolean isMultiFileSpray() {
      return multiFileSpray;
    }

    /**
     * @param multiFileSpray
     *          the multiFileSpray to set
     */
    public void setMultiFileSpray(boolean multiFileSpray) {
      this.multiFileSpray = multiFileSpray;
    }

    /**
     * @return the numFiles
     */
    public int getNumFiles() {
      return numFiles;
    }

    /**
     * @param numFiles
     *          the numFiles to set
     */
    public void setNumFiles(int numFiles) {
      this.numFiles = numFiles;
    }

    /**
     * @return the totalFiles
     */
    public int getTotalFiles() {
      return totalFiles;
    }

    /**
     * @param totalFiles
     *          the totalFiles to set
     */
    public void setTotalFiles(int totalFiles) {
      this.totalFiles = totalFiles;
    }
  }

  @SuppressWarnings("nls")
  private Operator genBucketingSortingDest(String dest, Operator input, QB qb,
      TableDesc table_desc, Table dest_tab, SortBucketRSCtx ctx) throws SemanticException {

    // If the table is bucketed, and bucketing is enforced, do the following:
    // If the number of buckets is smaller than the number of maximum reducers,
    // create those many reducers.
    // If not, create a multiFileSink instead of FileSink - the multiFileSink will
    // spray the data into multiple buckets. That way, we can support a very large
    // number of buckets without needing a very large number of reducers.
    boolean enforceBucketing = false;
    List<ExprNodeDesc> partnCols = new ArrayList<>();
    List<ExprNodeDesc> sortCols = new ArrayList<>();
    boolean multiFileSpray = false;
    int numFiles = 1;
    int totalFiles = 1;
    boolean isCompaction = false;
    if (dest_tab != null && dest_tab.getParameters() != null) {
      isCompaction = AcidUtils.isCompactionTable(dest_tab.getParameters());
    }

    StringBuilder order = new StringBuilder();
    StringBuilder nullOrder = new StringBuilder();
    if (dest_tab.getNumBuckets() > 0 && !dest_tab.getBucketCols().isEmpty()) {
      enforceBucketing = true;
      if (updating(dest) || deleting(dest)) {
        partnCols = getPartitionColsFromBucketColsForUpdateDelete(input, true);
        sortCols = getPartitionColsFromBucketColsForUpdateDelete(input, false);
        createSortOrderForUpdateDelete(sortCols, order, nullOrder);
      } else {
        partnCols = getPartitionColsFromBucketCols(dest, qb, dest_tab, table_desc, input, false);
      }
    } else {
      // Non-native acid tables should handle their own bucketing for updates/deletes
      if ((updating(dest) || deleting(dest)) && !AcidUtils.isNonNativeAcidTable(dest_tab)) {
        partnCols = getPartitionColsFromBucketColsForUpdateDelete(input, true);
        sortCols = getPartitionColsFromBucketColsForUpdateDelete(input, false);
        createSortOrderForUpdateDelete(sortCols, order, nullOrder);
        enforceBucketing = true;
      }
    }

    if ((dest_tab.getSortCols() != null) &&
        (dest_tab.getSortCols().size() > 0)) {
      sortCols = getSortCols(dest, qb, dest_tab, table_desc, input);
      getSortOrders(dest_tab, order, nullOrder);
      if (!enforceBucketing) {
        throw new SemanticException(ErrorMsg.TBL_SORTED_NOT_BUCKETED.getErrorCodedMsg(dest_tab.getCompleteName()));
      }
    } else if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SORT_WHEN_BUCKETING) &&
        enforceBucketing && !updating(dest) && !deleting(dest)) {
      sortCols = new ArrayList<>();
      for (ExprNodeDesc expr : partnCols) {
        sortCols.add(expr.clone());
        order.append(DirectionUtils.codeToSign(DirectionUtils.ASCENDING_CODE));
        nullOrder.append(NullOrdering.NULLS_FIRST.getSign());
      }
    }

    if (enforceBucketing) {
      Operation acidOp = AcidUtils.isFullAcidTable(dest_tab) ? getAcidType(table_desc.getOutputFileFormatClass(),
              dest, AcidUtils.isInsertOnlyTable(dest_tab)) : Operation.NOT_ACID;
      int maxReducers = conf.getIntVar(HiveConf.ConfVars.MAX_REDUCERS);
      if (conf.getIntVar(HiveConf.ConfVars.HADOOP_NUM_REDUCERS) > 0) {
        maxReducers = conf.getIntVar(HiveConf.ConfVars.HADOOP_NUM_REDUCERS);
      }
      int numBuckets = dest_tab.getNumBuckets();
      if (numBuckets > maxReducers) {
        LOG.debug("numBuckets is {} and maxReducers is {}", numBuckets, maxReducers);
        multiFileSpray = true;
        totalFiles = numBuckets;
        if (totalFiles % maxReducers == 0) {
          numFiles = totalFiles / maxReducers;
        }
        else {
          // find the number of reducers such that it is a divisor of totalFiles
          maxReducers = getReducersBucketing(totalFiles, maxReducers);
          numFiles = totalFiles / maxReducers;
        }
      }
      else {
        maxReducers = numBuckets;
      }

      input = genReduceSinkPlan(input, partnCols, sortCols, order.toString(), nullOrder.toString(),
          maxReducers, acidOp, isCompaction);
      reduceSinkOperatorsAddedByEnforceBucketingSorting.add((ReduceSinkOperator)input.getParentOperators().get(0));
      ctx.setMultiFileSpray(multiFileSpray);
      ctx.setNumFiles(numFiles);
      ctx.setTotalFiles(totalFiles);
    }
    return input;
  }

  // SORT BY ROW__ID ASC
  private void createSortOrderForUpdateDelete(List<ExprNodeDesc> sortCols,
                                              StringBuilder sortOrder, StringBuilder nullSortOrder) {
    NullOrdering defaultNullOrder = NullOrdering.defaultNullOrder(conf);
    for (int i = 0; i < sortCols.size(); i++) {
      sortOrder.append(DirectionUtils.codeToSign(DirectionUtils.ASCENDING_CODE));
      nullSortOrder.append(defaultNullOrder.getSign());
    }
  }

  private void genPartnCols(String dest, Operator input, QB qb,
      TableDesc table_desc, Table dest_tab, SortBucketRSCtx ctx) throws SemanticException {
    boolean enforceBucketing = false;
    List<ExprNodeDesc> partnColsNoConvert = new ArrayList<ExprNodeDesc>();

    if ((dest_tab.getNumBuckets() > 0)) {
      enforceBucketing = true;
      if (updating(dest) || deleting(dest)) {
        partnColsNoConvert = getPartitionColsFromBucketColsForUpdateDelete(input, false);
      } else {
        partnColsNoConvert = getPartitionColsFromBucketCols(dest, qb, dest_tab, table_desc, input,
            false);
      }
    }

    if ((dest_tab.getSortCols() != null) &&
        (dest_tab.getSortCols().size() > 0)) {
      if (!enforceBucketing) {
        throw new SemanticException(ErrorMsg.TBL_SORTED_NOT_BUCKETED.getErrorCodedMsg(dest_tab.getCompleteName()));
      }
      enforceBucketing = true;
    }

    if (enforceBucketing) {
      ctx.setPartnCols(partnColsNoConvert);
    }
  }

  private Operator genMaterializedViewDataOrgPlan(Table destinationTable, String sortColsStr, String distributeColsStr,
      RowResolver inputRR, Operator input) throws SemanticException {
    Map<String, Integer> colNameToIdx = new HashMap<>();
    for (int i = 0; i < destinationTable.getCols().size(); i++) {
      colNameToIdx.put(destinationTable.getCols().get(i).getName(), i);
    }
    List<ColumnInfo> colInfos = inputRR.getColumnInfos();
    List<ColumnInfo> sortColInfos = new ArrayList<>();
    if (sortColsStr != null) {
      Utilities.decodeColumnNames(sortColsStr)
          .forEach(s -> sortColInfos.add(colInfos.get(colNameToIdx.get(s))));
    }
    List<ColumnInfo> distributeColInfos = new ArrayList<>();
    if (distributeColsStr != null) {
      Utilities.decodeColumnNames(distributeColsStr)
          .forEach(s -> distributeColInfos.add(colInfos.get(colNameToIdx.get(s))));
    }
    return genMaterializedViewDataOrgPlan(sortColInfos, distributeColInfos, inputRR, input);
  }

  private Operator genMaterializedViewDataOrgPlan(List<ColumnInfo> sortColInfos, List<ColumnInfo> distributeColInfos,
      RowResolver inputRR, Operator input) {
    // In this case, we will introduce a RS and immediately after a SEL that restores
    // the row schema to what follow-up operations are expecting
    Set<String> keys = sortColInfos.stream()
        .map(ColumnInfo::getInternalName)
        .collect(Collectors.toSet());
    Set<String> distributeKeys = distributeColInfos.stream()
        .map(ColumnInfo::getInternalName)
        .collect(Collectors.toSet());
    List<ExprNodeDesc> keyCols = new ArrayList<>();
    List<String> keyColNames = new ArrayList<>();
    StringBuilder order = new StringBuilder();
    StringBuilder nullOrder = new StringBuilder();
    List<ExprNodeDesc> valCols = new ArrayList<>();
    List<String> valColNames = new ArrayList<>();
    List<ExprNodeDesc> partCols = new ArrayList<>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    Map<String, String> nameMapping = new HashMap<>();
    // map _col0 to KEY._col0, etc
    for (ColumnInfo ci : inputRR.getRowSchema().getSignature()) {
      ExprNodeColumnDesc e = new ExprNodeColumnDesc(ci);
      String columnName = ci.getInternalName();
      if (keys.contains(columnName)) {
        // key (sort column)
        keyColNames.add(columnName);
        keyCols.add(e);
        colExprMap.put(Utilities.ReduceField.KEY + "." + columnName, e);
        nameMapping.put(columnName, Utilities.ReduceField.KEY + "." + columnName);
        order.append("+");
        nullOrder.append("a");
      } else {
        // value
        valColNames.add(columnName);
        valCols.add(e);
        colExprMap.put(Utilities.ReduceField.VALUE + "." + columnName, e);
        nameMapping.put(columnName, Utilities.ReduceField.VALUE + "." + columnName);
      }
      if (distributeKeys.contains(columnName)) {
        // distribute column
        partCols.add(e.clone());
      }
    }
    // Create Key/Value TableDesc. When the operator plan is split into MR tasks,
    // the reduce operator will initialize Extract operator with information
    // from Key and Value TableDesc
    List<FieldSchema> fields = PlanUtils.getFieldSchemasFromColumnList(keyCols,
        keyColNames, 0, "");
    TableDesc keyTable = PlanUtils.getReduceKeyTableDesc(fields, order.toString(), nullOrder.toString());
    List<FieldSchema> valFields = PlanUtils.getFieldSchemasFromColumnList(valCols,
        valColNames, 0, "");
    TableDesc valueTable = PlanUtils.getReduceValueTableDesc(valFields);
    List<List<Integer>> distinctColumnIndices = new ArrayList<>();
    // Number of reducers is set to default (-1)
    ReduceSinkDesc rsConf = new ReduceSinkDesc(keyCols, keyCols.size(), valCols,
        keyColNames, distinctColumnIndices, valColNames, -1, partCols, -1, keyTable,
        valueTable, Operation.NOT_ACID);
    RowResolver rsRR = new RowResolver();
    List<ColumnInfo> rsSignature = new ArrayList<>();
    for (int index = 0; index < input.getSchema().getSignature().size(); index++) {
      ColumnInfo colInfo = new ColumnInfo(input.getSchema().getSignature().get(index));
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(colInfo.getInternalName());
      colInfo.setInternalName(nameMapping.get(colInfo.getInternalName()));
      rsSignature.add(colInfo);
      rsRR.put(nm[0], nm[1], colInfo);
      if (nm2 != null) {
        rsRR.addMappingOnly(nm2[0], nm2[1], colInfo);
      }
    }
    Operator<?> result = putOpInsertMap(OperatorFactory.getAndMakeChild(
        rsConf, new RowSchema(rsSignature), input), rsRR);
    result.setColumnExprMap(colExprMap);

    // Create SEL operator
    RowResolver selRR = new RowResolver();
    List<ColumnInfo> selSignature = new ArrayList<>();
    List<ExprNodeDesc> columnExprs = new ArrayList<>();
    List<String> colNames = new ArrayList<>();
    Map<String, ExprNodeDesc> selColExprMap = new HashMap<>();
    for (int index = 0; index < input.getSchema().getSignature().size(); index++) {
      ColumnInfo colInfo = new ColumnInfo(input.getSchema().getSignature().get(index));
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(colInfo.getInternalName());
      selSignature.add(colInfo);
      selRR.put(nm[0], nm[1], colInfo);
      if (nm2 != null) {
        selRR.addMappingOnly(nm2[0], nm2[1], colInfo);
      }
      String colName = colInfo.getInternalName();
      ExprNodeDesc exprNodeDesc;
      if (keys.contains(colName)) {
        exprNodeDesc = new ExprNodeColumnDesc(colInfo.getType(), ReduceField.KEY.toString() + "." + colName, null, false);
        columnExprs.add(exprNodeDesc);
      } else {
        exprNodeDesc = new ExprNodeColumnDesc(colInfo.getType(), ReduceField.VALUE.toString() + "." + colName, null, false);
        columnExprs.add(exprNodeDesc);
      }
      colNames.add(colName);
      selColExprMap.put(colName, exprNodeDesc);
    }
    SelectDesc selConf = new SelectDesc(columnExprs, colNames);
    result = putOpInsertMap(OperatorFactory.getAndMakeChild(selConf, new RowSchema(selSignature), result), selRR);
    result.setColumnExprMap(selColExprMap);

    return result;
  }

  private void setStatsForNonNativeTable(String dbName, String tableName) throws SemanticException {
    TableName qTableName = HiveTableName.ofNullable(tableName, dbName);
    Map<String, String> mapProp = new HashMap<>();
    mapProp.put(StatsSetupConst.COLUMN_STATS_ACCURATE, null);
    AlterTableUnsetPropertiesDesc alterTblDesc = new AlterTableUnsetPropertiesDesc(qTableName, null, null, false,
        mapProp, false, null);
    this.rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTblDesc)));
  }

  private boolean mergeCardinalityViolationBranch(final Operator input) {
    if(input instanceof SelectOperator) {
      SelectOperator selectOp = (SelectOperator)input;
      if(selectOp.getConf().getColList().size() == 1) {
        ExprNodeDesc colExpr = selectOp.getConf().getColList().get(0);
        if(colExpr instanceof ExprNodeGenericFuncDesc) {
          ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc)colExpr ;
          return func.getGenericUDF() instanceof GenericUDFCardinalityViolation;
        }
      }
    }
    return false;
  }

  private Operator genConstraintsPlan(String dest, QB qb, Operator input) throws SemanticException {
    if (deleting(dest)) {
      // for DELETE statements NOT NULL constraint need not be checked
      return input;
    }

      if (updating(dest) && isCBOExecuted() && this.ctx.getOperation() != Context.Operation.MERGE) {
      // for UPDATE statements CBO already added and pushed down the constraints
      return input;
    }

    //MERGE statements could have inserted a cardinality violation branch, we need to avoid that
    if (mergeCardinalityViolationBranch(input)) {
      return input;
    }

    // if this is an insert into statement we might need to add constraint check
    assert (input.getParentOperators().size() == 1);
    RowResolver inputRR = opParseCtx.get(input).getRowResolver();
    Table targetTable = getTargetTable(qb, dest);
    ExprNodeDesc combinedConstraintExpr =
        ExprNodeTypeCheck.genConstraintsExpr(conf, targetTable, updating(dest), inputRR);

    if (combinedConstraintExpr != null) {
      return putOpInsertMap(OperatorFactory.getAndMakeChild(
          new FilterDesc(combinedConstraintExpr, false), new RowSchema(
              inputRR.getColumnInfos()), input), inputRR);
    }
    return input;
  }

  protected Table getTargetTable(QB qb, String dest) throws SemanticException {
    Integer dest_type = qb.getMetaData().getDestTypeForAlias(dest);
    if (dest_type == QBMetaData.DEST_TABLE) {
      return qb.getMetaData().getDestTableForAlias(dest);

    } else if (dest_type == QBMetaData.DEST_PARTITION) {
      Partition dest_part = qb.getMetaData().getDestPartitionForAlias(dest);
      return dest_part.getTable();

    } else {
      throw new SemanticException("Generating constraint check plan: Invalid target type: " + dest);
    }
  }

  private Path getDestinationFilePath(QB qb, final String destinationFile, boolean isMmTable) {
    if (this.isResultsCacheEnabled() && this.queryTypeCanUseCache(qb)) {
      assert (!isMmTable);
      QueryResultsCache instance = QueryResultsCache.getInstance();
      // QueryResultsCache should have been initialized by now
      if (instance != null) {
        Path resultCacheTopDir = instance.getCacheDirPath();
        String dirName = UUID.randomUUID().toString();
        Path resultDir = new Path(resultCacheTopDir, dirName);
        this.ctx.setFsResultCacheDirs(resultDir);
        return resultDir;
      }
    }
    return new Path(destinationFile);
  }

  @SuppressWarnings("nls")
  protected Operator genFileSinkPlan(String dest, QB qb, Operator input)
      throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRowResolver();
    QBMetaData qbm = qb.getMetaData();
    Integer destType = qbm.getDestTypeForAlias(dest);

    Table destinationTable = null; // destination table if any
    boolean destTableIsTransactional;     // true for full ACID table and MM table
    boolean destTableIsFullAcid; // should the destination table be written to using ACID
    boolean isDirectInsert = false; // should we add files directly to the final path
    AcidUtils.Operation acidOperation = null;
    boolean destTableIsTemporary = false;
    boolean destTableIsMaterialization = false;
    Partition destinationPartition = null;// destination partition if any
    Path queryTmpdir = null; // the intermediate destination directory
    String moveTaskId = null;
    Path destinationPath = null; // the final destination directory
    TableDesc tableDescriptor = null;
    StructObjectInspector specificRowObjectInspector = null;
    int currentTableId = 0;
    boolean isLocal = false;
    SortBucketRSCtx rsCtx = new SortBucketRSCtx();
    DynamicPartitionCtx dpCtx = null;
    LoadTableDesc ltd = null;
    ListBucketingCtx lbCtx = null;
    Map<String, String> partSpec = null;
    boolean isMmTable = false, isMmCreate = false, isNonNativeTable = false, isAlreadyContainsPartCols = false;
    Long writeId = null;
    HiveTxnManager txnMgr = getTxnMgr();

    switch (destType.intValue()) {
    case QBMetaData.DEST_TABLE: {

      destinationTable = qbm.getDestTableForAlias(dest);
      destTableIsTransactional = AcidUtils.isTransactionalTable(destinationTable);
      destTableIsFullAcid = AcidUtils.isFullAcidTable(destinationTable);
      destTableIsTemporary = destinationTable.isTemporary();

      // Is the user trying to insert into a external tables
      checkExternalTable(destinationTable);

      partSpec = qbm.getPartSpecForAlias(dest);
      destinationPath = destinationTable.getPath();

      checkImmutableTable(qb, destinationTable, destinationPath, false);

      // Check for dynamic partitions.
      dpCtx = checkDynPart(qb, qbm, destinationTable, partSpec, dest);

      isNonNativeTable = destinationTable.isNonNative();
      isMmTable = AcidUtils.isInsertOnlyTable(destinationTable.getParameters());
      AcidUtils.Operation acidOp = AcidUtils.Operation.NOT_ACID;
      // this table_desc does not contain the partitioning columns
      tableDescriptor = Utilities.getTableDesc(destinationTable);

      if (!isNonNativeTable) {
        if (destTableIsTransactional) {
          acidOp = getAcidType(tableDescriptor.getOutputFileFormatClass(), dest, isMmTable);
        }
      }
      isDirectInsert = isDirectInsert(destTableIsFullAcid, acidOp);
      acidOperation = acidOp;
      queryTmpdir = getTmpDir(isNonNativeTable, isMmTable, isDirectInsert, destinationPath, dpCtx);
      moveTaskId = getMoveTaskId();
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("create filesink w/DEST_TABLE specifying " + queryTmpdir
            + " from " + destinationPath);
      }
      if (dpCtx != null) {
        // set the root of the temporary path where dynamic partition columns will populate
        dpCtx.setRootPath(queryTmpdir);
      }

      if (!qb.getIsQuery()) {
        isAlreadyContainsPartCols = Optional.ofNullable(destinationTable)
            .map(Table::hasNonNativePartitionSupport)
            .orElse(Boolean.FALSE);
        if (!updating(dest) && !deleting(dest) && isAlreadyContainsPartCols
            && destinationTable != null && partSpec != null) {
          // Do not specify partition spec again since its already added in the earlier.
          input = genConversionSelectOperatorByAddPartition(dest, qb, input, destinationTable.getDeserializer(),
                  destinationTable, partSpec);
        } else {
          input = genConversionSelectOperator(dest, qb, input, destinationTable.getDeserializer(),
                  dpCtx, destinationTable.getPartitionKeys(), destinationTable);
        }
      }

      // Add NOT NULL constraint check
      input = genConstraintsPlan(dest, qb, input);

      if (destinationTable.isMaterializedView() &&
          mvRebuildMode == MaterializationRebuildMode.INSERT_OVERWRITE_REBUILD) {
        // Data organization (DISTRIBUTED, SORTED, CLUSTERED) for materialized view
        // TODO: We only do this for a full rebuild
        String sortColsStr = destinationTable.getProperty(Constants.MATERIALIZED_VIEW_SORT_COLUMNS);
        String distributeColsStr = destinationTable.getProperty(Constants.MATERIALIZED_VIEW_DISTRIBUTE_COLUMNS);
        if (sortColsStr != null || distributeColsStr != null) {
          input = genMaterializedViewDataOrgPlan(destinationTable, sortColsStr, distributeColsStr, inputRR, input);
        }
      } else {
        // Add sorting/bucketing if needed
        input = genBucketingSortingDest(dest, input, qb, tableDescriptor, destinationTable, rsCtx);
      }

      idToTableNameMap.put(String.valueOf(destTableId), destinationTable.getTableName());
      currentTableId = destTableId;
      destTableId++;
      // Create the work for moving the table
      // NOTE: specify Dynamic partitions in dest_tab for WriteEntity
      if (!isNonNativeTable || destinationTable.getStorageHandler().commitInMoveTask()) {
        if (destTableIsTransactional) {
          acidOp = getAcidType(tableDescriptor.getOutputFileFormatClass(), dest, isMmTable);
          checkAcidConstraints();
        } else {
          lbCtx = constructListBucketingCtx(destinationTable.getSkewedColNames(),
              destinationTable.getSkewedColValues(), destinationTable.getSkewedColValueLocationMaps(),
              destinationTable.isStoredAsSubDirectories());
        }
        writeId = allocateTableWriteId(destinationTable.getFullTableName(), isMmTable || acidOp != Operation.NOT_ACID);
        
        boolean isReplace = !qb.getParseInfo().isInsertIntoTable(
            destinationTable.getDbName(), destinationTable.getTableName(), destinationTable.getSnapshotRef());
        ltd = new LoadTableDesc(queryTmpdir, tableDescriptor, dpCtx, acidOp, isReplace, writeId);
        if (writeId != null) {
          ltd.setStmtId(txnMgr.getCurrentStmtId());
        }
        ltd.setMoveTaskId(moveTaskId);
        // For Acid table, Insert Overwrite shouldn't replace the table content. We keep the old
        // deltas and base and leave them up to the cleaner to clean up
        boolean isInsertInto = qb.getParseInfo().isInsertIntoTable(
            destinationTable.getDbName(), destinationTable.getTableName(), destinationTable.getSnapshotRef());
        LoadFileType loadType;
        if (isDirectInsert) {
          loadType = LoadFileType.IGNORE;
        } else if (!isInsertInto && !destTableIsTransactional) {
          loadType = LoadFileType.REPLACE_ALL;
        } else {
          loadType = LoadFileType.KEEP_EXISTING;
        }
        ltd.setLoadFileType(loadType);
        ltd.setInsertOverwrite(!isInsertInto);
        ltd.setIsDirectInsert(isDirectInsert);
        ltd.setLbCtx(lbCtx);
        loadTableWork.add(ltd);
      } else {
        // This is a non-native table.
        // We need to set stats as inaccurate.
        setStatsForNonNativeTable(destinationTable.getDbName(), destinationTable.getTableName());
        // true if it is insert overwrite.
        boolean overwrite = !qb.getParseInfo().isInsertIntoTable(destinationTable.getDbName(), destinationTable.getTableName(),
            destinationTable.getSnapshotRef());
        createPreInsertDesc(destinationTable, overwrite);

        ltd = new LoadTableDesc(queryTmpdir, tableDescriptor, partSpec == null ? ImmutableMap.of() : partSpec);
        ltd.setInsertOverwrite(overwrite);
        ltd.setLoadFileType(overwrite ? LoadFileType.REPLACE_ALL : LoadFileType.KEEP_EXISTING);
      }

      if (destinationTable.isMaterializedView()) {
        materializedViewUpdateDesc = new MaterializedViewUpdateDesc(
            destinationTable.getFullyQualifiedName(), false, false, true);
      }

      WriteEntity output = generateTableWriteEntity(dest, destinationTable, partSpec, ltd, dpCtx);
      ctx.getLoadTableOutputMap().put(ltd, output);
      break;
    }
    case QBMetaData.DEST_PARTITION: {

      destinationPartition = qbm.getDestPartitionForAlias(dest);
      destinationTable = destinationPartition.getTable();
      destTableIsTransactional = AcidUtils.isTransactionalTable(destinationTable);
      destTableIsFullAcid = AcidUtils.isFullAcidTable(destinationTable);

      checkExternalTable(destinationTable);

      Path partPath = destinationPartition.getDataLocation();

      checkImmutableTable(qb, destinationTable, partPath, true);

      // Previous behavior (HIVE-1707) used to replace the partition's dfs with the table's dfs.
      // The changes in HIVE-19891 appears to no longer support that behavior.
      destinationPath = partPath;

      if (MetaStoreUtils.isArchived(destinationPartition.getTPartition())) {
        try {
          String conflictingArchive = ArchiveUtils.conflictingArchiveNameOrNull(
                  db, destinationTable, destinationPartition.getSpec());
          String message = String.format("Insert conflict with existing archive: %s",
                  conflictingArchive);
          throw new SemanticException(message);
        } catch (SemanticException err) {
          throw err;
        } catch (HiveException err) {
          throw new SemanticException(err);
        }
      }

      isNonNativeTable = destinationTable.isNonNative();
      isMmTable = AcidUtils.isInsertOnlyTable(destinationTable.getParameters());
      AcidUtils.Operation acidOp = AcidUtils.Operation.NOT_ACID;
      // this table_desc does not contain the partitioning columns
      tableDescriptor = Utilities.getTableDesc(destinationTable);

      if (!isNonNativeTable) {
        if (destTableIsTransactional) {
          acidOp = getAcidType(tableDescriptor.getOutputFileFormatClass(), dest, isMmTable);
        }
      }
      isDirectInsert = isDirectInsert(destTableIsFullAcid, acidOp);
      acidOperation = acidOp;
      queryTmpdir = getTmpDir(isNonNativeTable, isMmTable, isDirectInsert, destinationPath, null);
      moveTaskId = getMoveTaskId();
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("create filesink w/DEST_PARTITION specifying "
            + queryTmpdir + " from " + destinationPath);
      }

      if (destinationTable.hasNonNativePartitionSupport()) {
        partSpec = qbm.getPartSpecForAlias(dest);
      }

      if (!qb.getIsQuery()) {
        isAlreadyContainsPartCols = Optional.ofNullable(destinationTable)
                .map(Table::hasNonNativePartitionSupport)
                .orElse(Boolean.FALSE);
        if (!updating(dest) && !deleting(dest) && isAlreadyContainsPartCols
                && destinationTable != null && partSpec != null) {
          input = genConversionSelectOperatorByAddPartition(dest, qb, input, destinationTable.getDeserializer(),
                  destinationTable, partSpec);
        } else {
          input = genConversionSelectOperator(dest, qb, input, destinationTable.getDeserializer(),
                  dpCtx, null, destinationTable);
        }
      }

      // Add NOT NULL constraint check
      input = genConstraintsPlan(dest, qb, input);

      if (destinationTable.isMaterializedView() &&
          mvRebuildMode == MaterializationRebuildMode.INSERT_OVERWRITE_REBUILD) {
        // Data organization (DISTRIBUTED, SORTED, CLUSTERED) for materialized view
        // TODO: We only do this for a full rebuild
        String sortColsStr = destinationTable.getProperty(Constants.MATERIALIZED_VIEW_SORT_COLUMNS);
        String distributeColsStr = destinationTable.getProperty(Constants.MATERIALIZED_VIEW_DISTRIBUTE_COLUMNS);
        if (sortColsStr != null || distributeColsStr != null) {
          input = genMaterializedViewDataOrgPlan(destinationTable, sortColsStr, distributeColsStr, inputRR, input);
        }
      } else {
        // Add sorting/bucketing if needed
        input = genBucketingSortingDest(dest, input, qb, tableDescriptor, destinationTable, rsCtx);
      }

      idToTableNameMap.put(String.valueOf(destTableId), destinationTable.getTableName());
      currentTableId = destTableId;
      destTableId++;

      if (destTableIsTransactional) {
        acidOp = getAcidType(tableDescriptor.getOutputFileFormatClass(), dest, isMmTable);
        checkAcidConstraints();
      } else {
        // Transactional tables can't be list bucketed or have skewed cols
        lbCtx = constructListBucketingCtx(destinationPartition.getSkewedColNames(),
            destinationPartition.getSkewedColValues(), destinationPartition.getSkewedColValueLocationMaps(),
            destinationPartition.isStoredAsSubDirectories());
      }
      writeId = allocateTableWriteId(destinationTable.getFullTableName(), isMmTable || acidOp != Operation.NOT_ACID);
      
      ltd = new LoadTableDesc(queryTmpdir, tableDescriptor, destinationPartition.getSpec(), acidOp, writeId);
      if (writeId != null) {
        ltd.setStmtId(txnMgr.getCurrentStmtId());
      }
      // For the current context for generating File Sink Operator, it is either INSERT INTO or INSERT OVERWRITE.
      // So the next line works.
      boolean isInsertInto = !qb.getParseInfo().isDestToOpTypeInsertOverwrite(dest);
      // For Acid table, Insert Overwrite shouldn't replace the table content. We keep the old
      // deltas and base and leave them up to the cleaner to clean up
      LoadFileType loadType;
      if (isDirectInsert) {
        loadType = LoadFileType.IGNORE;
      } else if (!isInsertInto && !destTableIsTransactional) {
        loadType = LoadFileType.REPLACE_ALL;
      } else {
        loadType = LoadFileType.KEEP_EXISTING;
      }
      ltd.setLoadFileType(loadType);
      ltd.setInsertOverwrite(!isInsertInto);
      ltd.setIsDirectInsert(isDirectInsert);
      ltd.setLbCtx(lbCtx);
      ltd.setMoveTaskId(moveTaskId);

      loadTableWork.add(ltd);

      if (destinationTable.hasNonNativePartitionSupport()) {
        // HMS does not know about this partition
        // but the underlying storage format knows about it.
        DummyPartition dummyPartition;
        try {
          String partName = Warehouse.makePartName(partSpec, false);
          dummyPartition = new DummyPartition(destinationTable, partName, partSpec);
        } catch (MetaException e) {
          throw new SemanticException("Unable to construct name for dummy partition due to: ", e);
        }
        if (!outputs.add(new WriteEntity(dummyPartition, determineWriteType(ltd, dest)))) {
          throw new SemanticException(ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
                  .getMsg(destinationTable.getTableName() + "@" + dummyPartition.getName()));
        }
      } else {
        if (!outputs.add(new WriteEntity(destinationPartition, determineWriteType(ltd, dest)))) {
          throw new SemanticException(ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
                  .getMsg(destinationTable.getTableName() + "@" + destinationPartition.getName()));
        }
      }
     break;
    }
    case QBMetaData.DEST_LOCAL_FILE:
      isLocal = true;
      // fall through
    case QBMetaData.DEST_DFS_FILE: {
      destinationPath = getDestinationFilePath(qb, qbm.getDestFileForAlias(dest), isMmTable);

      // CTAS case: the file output format and serde are defined by the create
      // table command rather than taking the default value
      List<FieldSchema> fieldSchemas = null;
      List<FieldSchema> partitionColumns = null;
      List<String> partitionColumnNames = null;
      List<FieldSchema> sortColumns = null;
      List<String> sortColumnNames = null;
      List<FieldSchema> distributeColumns = null;
      List<String> distributeColumnNames = null;
      List<ColumnInfo> fileSinkColInfos = null;
      List<ColumnInfo> sortColInfos = null;
      List<ColumnInfo> distributeColInfos = null;
      Map<String, String> tblProps = null;
      CreateTableDesc tblDesc = qb.getTableDesc();
      CreateMaterializedViewDesc viewDesc = qb.getViewDesc();
      boolean createTableUseSuffix = false;
      
      DDLDescWithTableProperties ddlDesc = (tblDesc != null) ? tblDesc : viewDesc;
      if (ddlDesc != null) {
        fieldSchemas = new ArrayList<>();
        partitionColumns = new ArrayList<>();
        partitionColumnNames = ddlDesc.getPartColNames();
        fileSinkColInfos = new ArrayList<>();
        destTableIsTemporary = ddlDesc.isTemporary();
        destTableIsMaterialization = ddlDesc.isMaterialization();
        tblProps = ddlDesc.getTblProps();

        // Add suffix only when required confs are present
        // and user has not specified a location to the table.
        createTableUseSuffix = (HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX)
            || HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED))
          && ddlDesc.getLocation() == null;
      }
      if (viewDesc != null) {
        sortColumns = new ArrayList<>();
        sortColumnNames = viewDesc.getSortColNames();
        distributeColumns = new ArrayList<>();
        distributeColumnNames = viewDesc.getDistributeColNames();
        sortColInfos = new ArrayList<>();
        distributeColInfos = new ArrayList<>();
      }

      destTableIsTransactional = tblProps != null && AcidUtils.isTablePropertyTransactional(tblProps);
      if (destTableIsTransactional) {
        isNonNativeTable = MetaStoreUtils.isNonNativeTable(tblProps);
        isMmTable = isMmCreate = AcidUtils.isInsertOnlyTable(tblProps);
        
        writeId = allocateTableWriteId(ddlDesc.getFullTableName(), 0L);
        
        if (!isNonNativeTable && !destTableIsTemporary && (qb.isCTAS() || qb.isMaterializedView())) {
          destTableIsFullAcid = AcidUtils.isFullAcidTable(tblProps);
          acidOperation = getAcidType(dest);
          isDirectInsert = isDirectInsert(destTableIsFullAcid, acidOperation);
          
          // Set the location in context for possible rollback.
          ctx.setLocation(getCtasOrCMVLocation(tblDesc, viewDesc, createTableUseSuffix));
          
          if (isDirectInsert || isMmTable) {
            destinationPath = ctx.getLocation();
            if (createTableUseSuffix) {
              ddlDesc.getTblProps().put(SOFT_DELETE_TABLE, Boolean.TRUE.toString());
            }
            // Setting the location so that metadata transformers
            // does not change the location later while creating the table.
            ddlDesc.setLocation(destinationPath.toString());
          }
        }
        if (isMmTable || isDirectInsert) {
          ddlDesc.setInitialWriteId(writeId);
        }
      }

      // Check for dynamic partitions.
      final String cols, colTypes;
      final boolean isPartitioned;
      if (dpCtx != null) {
        throw new SemanticException("Dynamic partition context has already been created, this should not happen");
      }
      if (!CollectionUtils.isEmpty(partitionColumnNames)) {
        ColsAndTypes ct = deriveFileSinkColTypes(
            inputRR, partitionColumnNames, sortColumnNames, distributeColumnNames, fieldSchemas, partitionColumns,
            sortColumns, distributeColumns, fileSinkColInfos, sortColInfos, distributeColInfos);
        cols = ct.cols;
        colTypes = ct.colTypes;
        dpCtx = new DynamicPartitionCtx(partitionColumnNames,
            conf.getVar(HiveConf.ConfVars.DEFAULT_PARTITION_NAME),
            conf.getIntVar(HiveConf.ConfVars.DYNAMIC_PARTITION_MAX_PARTS_PER_NODE));
        qbm.setDPCtx(dest, dpCtx);
        isPartitioned = true;
      } else {
        ColsAndTypes ct = deriveFileSinkColTypes(
            inputRR, sortColumnNames, distributeColumnNames, fieldSchemas, sortColumns, distributeColumns,
            sortColInfos, distributeColInfos);
        cols = ct.cols;
        colTypes = ct.colTypes;
        isPartitioned = false;
      }

      if (isLocal) {
        assert !isMmTable;
        // for local directory - we always write to map-red intermediate
        // store and then copy to local fs
        queryTmpdir = ctx.getMRTmpPath();
        if (dpCtx != null && dpCtx.getSPPath() != null) {
          queryTmpdir = new Path(queryTmpdir, dpCtx.getSPPath());
        }
      } else {
        // otherwise write to the file system implied by the directory
        // no copy is required. we may want to revisit this policy in future
        try {
          Path qPath = FileUtils.makeQualified(destinationPath, conf);
          queryTmpdir = getTmpDir(false, isMmTable, isDirectInsert, qPath, dpCtx);
        } catch (Exception e) {
          throw new SemanticException("Error creating "
              + destinationPath, e);
        }
      }
      // set the root of the temporary path where dynamic partition columns will populate
      if (dpCtx != null) {
        dpCtx.setRootPath(queryTmpdir);
      }

      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("Setting query directory " + queryTmpdir
            + " from " + destinationPath + " (" + isMmTable + ")");
      }

      // update the create table descriptor with the resulting schema.
      if (tblDesc != null) {
        tblDesc.setCols(new ArrayList<>(fieldSchemas));
        tblDesc.setPartCols(new ArrayList<>(partitionColumns));
      } else if (viewDesc != null) {
        viewDesc.setCols(new ArrayList<>(fieldSchemas));
        viewDesc.setPartCols(new ArrayList<>(partitionColumns));
        if (viewDesc.isOrganized()) {
          viewDesc.setSortCols(new ArrayList<>(sortColumns));
          viewDesc.setDistributeCols(new ArrayList<>(distributeColumns));
        }
      }

      boolean isDestTempFile = true;
      if (!ctx.isMRTmpFileURI(destinationPath.toUri().toString())
          && !ctx.isResultCacheDir(destinationPath)) {
        // not a temp dir and not a result cache dir
        idToTableNameMap.put(String.valueOf(destTableId), destinationPath.toUri().toString());
        currentTableId = destTableId;
        destTableId++;
        isDestTempFile = false;
      }

      try {
        if (tblDesc == null) {
          if (viewDesc != null) {
            if (viewDesc.getStorageHandler() != null) {
              viewDesc.setLocation(getCtasOrCMVLocation(tblDesc, viewDesc, createTableUseSuffix).toString());
            }
            tableDescriptor = PlanUtils.getTableDesc(viewDesc, cols, colTypes);
          } else if (qb.getIsQuery()) {
            Class<? extends Deserializer> serdeClass = LazySimpleSerDe.class;
            String fileFormat = conf.getResultFileFormat().toString();
            if (SessionState.get().getIsUsingThriftJDBCBinarySerDe()) {
              serdeClass = ThriftJDBCBinarySerDe.class;
              fileFormat = ResultFileFormat.SEQUENCEFILE.toString();
              // Set the fetch formatter to be a no-op for the ListSinkOperator, since we'll
              // write out formatted thrift objects to SequenceFile
              conf.set(SerDeUtils.LIST_SINK_OUTPUT_FORMATTER, NoOpFetchFormatter.class.getName());
            } else if (fileFormat.equals(PlanUtils.LLAP_OUTPUT_FORMAT_KEY)) {
              serdeClass = LazyBinarySerDe2.class;
            }
            tableDescriptor = PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, fileFormat,
                serdeClass);
          } else {
            tableDescriptor = PlanUtils.getDefaultTableDesc(qb.getDirectoryDesc(), cols, colTypes);
          }
        } else {
          if (tblDesc.isCTAS() && tblDesc.getStorageHandler() != null) {
            tblDesc.toTable(conf).getStorageHandler().setTableLocationForCTAS(
                tblDesc, getCtasOrCMVLocation(tblDesc, viewDesc, false).toString());
          }
          tableDescriptor = PlanUtils.getTableDesc(tblDesc, cols, colTypes);
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }

      // We need a specific rowObjectInspector in this case
      try {
        specificRowObjectInspector =
            (StructObjectInspector) tableDescriptor.getDeserializer(conf).getObjectInspector();
      } catch (Exception e) {
        throw new SemanticException(e.getMessage(), e);
      }

      boolean isDfsDir = (destType == QBMetaData.DEST_DFS_FILE);

      try {
        if (tblDesc != null) {
          Table t = tblDesc.toTable(conf);
          destinationTable = tblDesc.isMaterialization() ? t : db.getTranslateTableDryrun(t.getTTable());
        } else {
          destinationTable = viewDesc != null ? viewDesc.toTable(conf) : null;
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }

      destTableIsFullAcid = AcidUtils.isFullAcidTable(destinationTable);

      // Data organization (DISTRIBUTED, SORTED, CLUSTERED) for materialized view
      if (viewDesc != null && viewDesc.isOrganized()) {
        input = genMaterializedViewDataOrgPlan(sortColInfos, distributeColInfos, inputRR, input);
      }

      moveTaskId = getMoveTaskId();

      if (isPartitioned) {
        // Create a SELECT that may reorder the columns if needed
        RowResolver rowResolver = new RowResolver();
        List<ExprNodeDesc> columnExprs = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
        for (int i = 0; i < fileSinkColInfos.size(); i++) {
          ColumnInfo ci = fileSinkColInfos.get(i);
          ExprNodeDesc columnExpr = new ExprNodeColumnDesc(ci);
          String name = getColumnInternalName(i);
          rowResolver.put("", name, new ColumnInfo(name, columnExpr.getTypeInfo(), "", false));
          columnExprs.add(columnExpr);
          colNames.add(name);
          colExprMap.put(name, columnExpr);
        }
        input = putOpInsertMap(OperatorFactory.getAndMakeChild(
            new SelectDesc(columnExprs, colNames), new RowSchema(rowResolver
                .getColumnInfos()), input), rowResolver);
        input.setColumnExprMap(colExprMap);

        // If this is a partitioned CTAS or MV statement, we are going to create a LoadTableDesc
        // object. Although the table does not exist in metastore, we will swap the CreateTableTask
        // and MoveTask resulting from this LoadTable so in this specific case, first we create
        // the metastore table, then we move and commit the partitions. At least for the time being,
        // this order needs to be enforced because metastore expects a table to exist before we can
        // add any partitions to it.
        isNonNativeTable = tableDescriptor.isNonNative();
        if (!isNonNativeTable || destinationTable.getStorageHandler().commitInMoveTask()) {
          AcidUtils.Operation acidOp = AcidUtils.Operation.NOT_ACID;
          if (destTableIsTransactional) {
            acidOp = getAcidType(tableDescriptor.getOutputFileFormatClass(), dest, isMmTable);
            checkAcidConstraints();
          }
          // isReplace = false in case concurrent operation is executed
          ltd = new LoadTableDesc(queryTmpdir, tableDescriptor, dpCtx, acidOp, false, writeId);
          if (writeId != null) {
            ltd.setStmtId(txnMgr.getCurrentStmtId());
          }
          ltd.setLoadFileType(LoadFileType.KEEP_EXISTING);
          ltd.setInsertOverwrite(false);
          ltd.setIsDirectInsert(isDirectInsert);
          loadTableWork.add(ltd);
        } else {
          // This is a non-native table.
          // We need to set stats as inaccurate.
          setStatsForNonNativeTable(tableDescriptor.getDbName(), tableDescriptor.getTableName());
          ltd = new LoadTableDesc(queryTmpdir, tableDescriptor, dpCtx.getPartSpec());
          ltd.setInsertOverwrite(false);
          ltd.setLoadFileType(LoadFileType.KEEP_EXISTING);
        }
        ltd.setMoveTaskId(moveTaskId);
        ltd.setMdTable(destinationTable);
        WriteEntity output = generateTableWriteEntity(dest, destinationTable, dpCtx.getPartSpec(), ltd, dpCtx);
        ctx.getLoadTableOutputMap().put(ltd, output);
      } else {
        // Create LFD even for MM CTAS - it's a no-op move, but it still seems to be used for stats.
        LoadFileDesc loadFileDesc = new LoadFileDesc(tblDesc, viewDesc, queryTmpdir, destinationPath, isDfsDir, cols,
            colTypes,
            destTableIsFullAcid ?//there is a change here - prev version had 'transactional', one before 'acid'
                Operation.INSERT : Operation.NOT_ACID,
            isMmCreate);
        loadFileDesc.setMoveTaskId(moveTaskId);
        loadFileWork.add(loadFileDesc);
        try {
          FileSystem fs = isDfsDir ?  destinationPath.getFileSystem(conf) : FileSystem.getLocal(conf);
          Path qualifiedPath = conf.getBoolVar(ConfVars.HIVE_RANGER_USE_FULLY_QUALIFIED_URL) ?
              fs.makeQualified(destinationPath) : destinationPath;
          if (!outputs.add(new WriteEntity(qualifiedPath, !isDfsDir, isDestTempFile))) {
            throw new SemanticException(ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
                    .getMsg(destinationPath.toUri().toString()));
          }
        } catch (IOException ex) {
          throw new SemanticException("Error while getting the full qualified path for the given directory: " + ex.getMessage());
        }
      }
      break;
    }
    default:
      throw new SemanticException("Unknown destination type: " + destType);
    }

    if (!(destType == QBMetaData.DEST_DFS_FILE && qb.getIsQuery())
            && destinationTable != null && destinationTable.getStorageHandler() != null) {
      try {
        if (!updating(dest) && !deleting(dest) && isAlreadyContainsPartCols && MapUtils.isNotEmpty(partSpec)) {
          input = genConversionSelectOperatorByAddPartition(dest, qb, input, destinationTable.getDeserializer(),
                  destinationTable, null);
        } else {
          input = genConversionSelectOperator(dest, qb, input, destinationTable.getDeserializer(),
                  dpCtx, null, destinationTable);
        }
      } catch (Exception e) {
        throw new SemanticException(e);
      }
    }

    inputRR = opParseCtx.get(input).getRowResolver();

    List<ColumnInfo> vecCol = new ArrayList<ColumnInfo>();
    
    if (updating(dest) || deleting(dest) || merging(dest)) {
      if (AcidUtils.isNonNativeAcidTable(destinationTable)) {
        destinationTable.getStorageHandler().acidVirtualColumns().stream()
            .map(col -> new ColumnInfo(col.getName(), col.getTypeInfo(), "", true))
            .forEach(vecCol::add);
      } else {
        vecCol.add(new ColumnInfo(VirtualColumn.ROWID.getName(), VirtualColumn.ROWID.getTypeInfo(),
            "", true));
      }
    } else {
      try {
        // If we already have a specific inspector (view or directory as a target) use that
        // Otherwise use the table deserializer to get the inspector
        StructObjectInspector rowObjectInspector = specificRowObjectInspector != null ? specificRowObjectInspector :
            (StructObjectInspector) destinationTable.getDeserializer().getObjectInspector();
        List<? extends StructField> fields = rowObjectInspector
            .getAllStructFieldRefs();
        for (StructField field : fields) {
          vecCol.add(new ColumnInfo(field.getFieldName(), TypeInfoUtils
              .getTypeInfoFromObjectInspector(field
                  .getFieldObjectInspector()), "", false));
        }
      } catch (Exception e) {
        throw new SemanticException(e.getMessage(), e);
      }
    }

    RowSchema fsRS = new RowSchema(vecCol);

    // The output files of a FileSink can be merged if they are either not being written to a table
    // or are being written to a table which is not bucketed
    // and table the table is not sorted
    boolean canBeMerged = (destinationTable == null || !((destinationTable.getNumBuckets() > 0) ||
        (destinationTable.getSortCols() != null && destinationTable.getSortCols().size() > 0)));

    // If this table is working with ACID semantics or
    // if its a delete, update, merge operation that supports merge task, turn off merging
    canBeMerged &= !destTableIsFullAcid;
    if (destinationTable != null && destinationTable.getStorageHandler() != null) {
      canBeMerged &= destinationTable.getStorageHandler().supportsMergeFiles();
      // TODO: Support for merge task for update and merge queries
      //  when storage handler supports it.
      if (Context.Operation.DELETE.equals(ctx.getOperation()) && !deleting(dest)) {
        canBeMerged = true;
      }
      if (Context.Operation.UPDATE.equals(ctx.getOperation())
              || Context.Operation.MERGE.equals(ctx.getOperation())) {
        canBeMerged = false;
      }
    }

    // Generate the partition columns from the parent input
    if (destType == QBMetaData.DEST_TABLE || destType == QBMetaData.DEST_PARTITION) {
      genPartnCols(dest, input, qb, tableDescriptor, destinationTable, rsCtx);
    }

    FileSinkDesc fileSinkDesc = createFileSinkDesc(dest, tableDescriptor, destinationPartition,
        destinationPath, currentTableId, destTableIsFullAcid, destTableIsTemporary,//this was 1/4 acid
        destTableIsMaterialization, queryTmpdir, rsCtx, dpCtx, lbCtx, fsRS,
        canBeMerged, destinationTable, isMmCreate, destType, qb, isDirectInsert, acidOperation, moveTaskId);
    if (isMmCreate || (qb.isCTAS() || qb.isMaterializedView()) && isDirectInsert) {
      // Add FSD so that the LoadTask compilation could fix up its path to avoid the move.
      if (tableDesc != null) {
        tableDesc.setWriter(fileSinkDesc);
      } else {
        createVwDesc.setWriter(fileSinkDesc);
      }
    }

    if (fileSinkDesc.getInsertOverwrite()) {
      if (ltd != null) {
        ltd.setInsertOverwrite(true);
      }
    }
    if (null != tableDescriptor && useBatchingSerializer(tableDescriptor.getSerdeClassName())) {
      fileSinkDesc.setIsUsingBatchingSerDe(true);
    } else {
      fileSinkDesc.setIsUsingBatchingSerDe(false);
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        fileSinkDesc, fsRS, input), inputRR);

    // In the MoveTask the lineage information is not set in case of delete and update as the
    // columns are not matching. So we only need the lineage information for insert.
    // If directInsert=false, adding the lineage info here for the other operations is
    // ok, as the paths are different. But if directInsert=true, the path for all
    // operation is the same (the table location) and this can lead to invalid lineage information
    // in case of a merge statement.
    if (!isDirectInsert || acidOperation == AcidUtils.Operation.INSERT) {
      handleLineage(destinationTable, ltd, output);
    }
    setWriteIdForSurrogateKeys(ltd, input);

    LOG.debug("Created FileSink Plan for clause: {}dest_path: {} row schema: {}", dest, destinationPath, inputRR);

    FileSinkOperator fso = (FileSinkOperator) output;
    fso.getConf().setTable(destinationTable);
    // the following code is used to collect column stats when
    // hive.stats.autogather=true
    // and it is an insert overwrite or insert into table
    if (conf.getBoolVar(ConfVars.HIVE_STATS_AUTOGATHER)
        && conf.getBoolVar(ConfVars.HIVE_STATS_COL_AUTOGATHER)
        && enableColumnStatsCollecting()
        && destinationTable != null
        && (!destinationTable.isNonNative() || destinationTable.getStorageHandler().commitInMoveTask())
        && !destTableIsTemporary && !destTableIsMaterialization
        && ColumnStatsAutoGatherContext.canRunAutogatherStats(fso)) {
      if (destType == QBMetaData.DEST_TABLE) {
        genAutoColumnStatsGatheringPipeline(destinationTable, partSpec, input,
            qb.getParseInfo().isInsertIntoTable(destinationTable.getDbName(), destinationTable.getTableName(),
                destinationTable.getSnapshotRef()), false);
      } else if (destType == QBMetaData.DEST_PARTITION) {
        genAutoColumnStatsGatheringPipeline(destinationTable, destinationPartition.getSpec(), input,
            qb.getParseInfo().isInsertIntoTable(destinationTable.getDbName(), destinationTable.getTableName(),
                destinationTable.getSnapshotRef()), false);
      } else if (destType == QBMetaData.DEST_LOCAL_FILE || destType == QBMetaData.DEST_DFS_FILE) {
        // CTAS or CMV statement
        genAutoColumnStatsGatheringPipeline(destinationTable, null, input,
            false, true);
      }
    }
    return output;
  }

  private Long allocateTableWriteId(TableName tableName, boolean isAcid) throws SemanticException {
    return isAcid ? allocateTableWriteId(tableName, null) : null;
  }

  private Long allocateTableWriteId(TableName tableName, Long defaultValue) throws SemanticException {
    if (ctx.getExplainConfig() != null) {
      return defaultValue; // For explain plan, txn won't be opened and doesn't make sense to allocate write id
    }
    queryState.getValidTxnList();
    try {
      return getTxnMgr().getTableWriteId(tableName.getDb(), tableName.getTable());
    } catch (LockException ex) {
      throw new SemanticException("Failed to allocate write Id", ex);
    }
  }

  protected boolean enableColumnStatsCollecting() {
    return true;
  }

  private Path getCtasOrCMVLocation(CreateTableDesc tblDesc, CreateMaterializedViewDesc viewDesc, 
        boolean createTableWithSuffix) throws SemanticException {
    Path location;
    String protoName;
    Table tbl;
    try {
      if (tblDesc != null) {
        protoName = tblDesc.getDbTableName();
        // Handle table translation initially and if not present
        // use default table path.
        // Property modifications of the table is handled later.
        // We are interested in the location if it has changed
        // due to table translation.
        tbl = tblDesc.toTable(conf);
        tbl = db.getTranslateTableDryrun(tbl.getTTable());
      } else {
        protoName = viewDesc.getViewName();
        tbl = viewDesc.toTable(conf);
      }
      
      String[] names = Utilities.getDbTableName(protoName);
      Warehouse wh = new Warehouse(conf);
      if (tbl.getSd() == null || tbl.getSd().getLocation() == null) {
        location = wh.getDefaultTablePath(db.getDatabase(names[0]), names[1], false);
      } else {
        location = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
      }
      return location.suffix(
          Utilities.getTableOrMVSuffix(ctx, createTableWithSuffix));
    } catch (HiveException | MetaException e) {
      throw new SemanticException(e);
    }
  }

  private boolean isDirectInsert(boolean destTableIsFullAcid, AcidUtils.Operation acidOp) {
    // In case of an EXPLAIN ANALYZE query, the direct insert has to be turned off. HIVE-24336
    if (ctx.getExplainAnalyze() == AnalyzeState.RUNNING) {
      return false;
    }
    boolean directInsertEnabled = conf.getBoolVar(HiveConf.ConfVars.HIVE_ACID_DIRECT_INSERT_ENABLED);
    boolean directInsert = directInsertEnabled && destTableIsFullAcid && acidOp != AcidUtils.Operation.NOT_ACID;
    if (LOG.isDebugEnabled() && directInsert) {
      LOG.debug("Direct insert for ACID tables is enabled.");
    }
    return directInsert;
  }

  private Path getTmpDir(boolean isNonNativeTable, boolean isMmTable, boolean isDirectInsert,
      Path destinationPath, DynamicPartitionCtx dpCtx) {
    /**
     * We will directly insert to the final destination in the following cases:
     * 1. Non native table
     * 2. Micro-managed (insert only table)
     * 3. Full ACID table and operation type is INSERT
     */
    Path destPath = null;
    if (isNonNativeTable || isMmTable || isDirectInsert) {
      destPath = destinationPath;
    } else if (HiveConf.getBoolVar(conf, ConfVars.HIVE_USE_SCRATCHDIR_FOR_STAGING)) {
      destPath = ctx.getTempDirForInterimJobPath(destinationPath);
    } else {
      destPath = ctx.getTempDirForFinalJobPath(destinationPath);
    }
    if (dpCtx != null && dpCtx.getSPPath() != null) {
      return new Path(destPath, dpCtx.getSPPath());
    }
    return destPath;
  }

  private String getMoveTaskId() {
    return ctx.getMoveTaskId();
  }

  private boolean useBatchingSerializer(String serdeClassName) {
    return SessionState.get().isHiveServerQuery() &&
      hasSetBatchSerializer(serdeClassName);
  }

  private boolean hasSetBatchSerializer(String serdeClassName) {
    return (serdeClassName.equalsIgnoreCase(ThriftJDBCBinarySerDe.class.getName()) &&
      HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_SERIALIZE_IN_TASKS));
  }

  private ColsAndTypes deriveFileSinkColTypes(RowResolver inputRR, List<String> sortColumnNames, List<String> distributeColumnNames,
      List<FieldSchema> fieldSchemas, List<FieldSchema> sortColumns, List<FieldSchema> distributeColumns,
      List<ColumnInfo> sortColInfos, List<ColumnInfo> distributeColInfos) throws SemanticException {
    return deriveFileSinkColTypes(inputRR, new ArrayList<>(), sortColumnNames, distributeColumnNames,
        fieldSchemas, new ArrayList<>(), sortColumns, distributeColumns, new ArrayList<>(),
        sortColInfos, distributeColInfos);
  }

  private ColsAndTypes deriveFileSinkColTypes(
      RowResolver inputRR, List<String> partitionColumnNames, List<String> sortColumnNames, List<String> distributeColumnNames,
      List<FieldSchema> columns, List<FieldSchema> partitionColumns, List<FieldSchema> sortColumns, List<FieldSchema> distributeColumns,
      List<ColumnInfo> fileSinkColInfos, List<ColumnInfo> sortColInfos, List<ColumnInfo> distributeColInfos) throws SemanticException {
    ColsAndTypes result = new ColsAndTypes("", "");
    List<String> allColumns = new ArrayList<>();
    List<ColumnInfo> colInfos = inputRR.getColumnInfos();
    List<ColumnInfo> nonPartColInfos = new ArrayList<>();
    SortedMap<Integer, Pair<FieldSchema, ColumnInfo>> partColInfos = new TreeMap<>();
    SortedMap<Integer, Pair<FieldSchema, ColumnInfo>> sColInfos = new TreeMap<>();
    SortedMap<Integer, Pair<FieldSchema, ColumnInfo>> dColInfos = new TreeMap<>();
    boolean first = true;
    int numNonPartitionedCols = colInfos.size() - partitionColumnNames.size();
    if (numNonPartitionedCols <= 0) {
      throw new SemanticException("Too many partition columns declared");
    }
    for (ColumnInfo colInfo : colInfos) {
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());

      if (nm[1] != null) { // non-null column alias
        colInfo.setAlias(nm[1]);
      }

      boolean isPartitionCol = false;
      String colName = colInfo.getInternalName();  //default column name
      if (columns != null) {
        FieldSchema col = new FieldSchema();
        if (!("".equals(nm[0])) && nm[1] != null) {
          colName = unescapeIdentifier(colInfo.getAlias()).toLowerCase(); // remove ``
        }
        colName = fixCtasColumnName(colName);
        col.setName(colName);
        allColumns.add(colName);
        String typeName = colInfo.getType().getTypeName();
        // CTAS should NOT create a VOID type
        if (typeName.equals(serdeConstants.VOID_TYPE_NAME)) {
          throw new SemanticException(ErrorMsg.CTAS_CREATES_VOID_TYPE.getMsg(colName));
        }
        col.setType(typeName);
        int idx = partitionColumnNames.indexOf(colName);
        if (idx >= 0) {
          partColInfos.put(idx, Pair.of(col, colInfo));
          isPartitionCol = true;
        } else {
          if (sortColumnNames != null) {
            idx = sortColumnNames.indexOf(colName);
            if (idx >= 0) {
              sColInfos.put(idx, Pair.of(col, colInfo));
            }
          }
          if (distributeColumnNames != null) {
            idx = distributeColumnNames.indexOf(colName);
            if (idx >= 0) {
              dColInfos.put(idx, Pair.of(col, colInfo));
            }
          }
          columns.add(col);
          nonPartColInfos.add(colInfo);
        }
      }

      if (!isPartitionCol) {
        if (!first) {
          result.cols = result.cols.concat(",");
          result.colTypes = result.colTypes.concat(":");
        }

        first = false;
        result.cols = result.cols.concat(colName);

        // Replace VOID type with string when the output is a temp table or
        // local files.
        // A VOID type can be generated under the query:
        //
        // select NULL from tt;
        // or
        // insert overwrite local directory "abc" select NULL from tt;
        //
        // where there is no column type to which the NULL value should be
        // converted.
        //
        String tName = colInfo.getType().getTypeName();
        if (tName.equals(serdeConstants.VOID_TYPE_NAME)) {
          result.colTypes = result.colTypes.concat(serdeConstants.STRING_TYPE_NAME);
        } else {
          result.colTypes = result.colTypes.concat(tName);
        }
      }

    }

    if (partColInfos.size() != partitionColumnNames.size()) {
      throw new SemanticException("Table declaration contains partition columns that are not present " +
        "in query result schema. " +
        "Query columns: " + allColumns + ". " +
        "Partition columns: " + partitionColumnNames);
    }

    if (sortColumnNames != null && sColInfos.size() != sortColumnNames.size()) {
      throw new SemanticException("Table declaration contains cluster/sort columns that are not present " +
          "in query result schema. " +
          "Query columns: " + allColumns + ". " +
          "Organization columns: " + sortColumnNames);
    }

    if (distributeColumnNames != null && dColInfos.size() != distributeColumnNames.size()) {
      throw new SemanticException("Table declaration contains cluster/distribute columns that are not present " +
          "in query result schema. " +
          "Query columns: " + allColumns + ". " +
          "Organization columns: " + distributeColumnNames);
    }

    // FileSinkColInfos comprise nonPartCols followed by partCols
    fileSinkColInfos.addAll(nonPartColInfos);
    partitionColumns.addAll(partColInfos.values().stream().map(Pair::getLeft).collect(Collectors.toList()));
    fileSinkColInfos.addAll(partColInfos.values().stream().map(Pair::getRight).collect(Collectors.toList()));
    // data org columns
    if (sortColumnNames != null) {
      sortColumns.addAll(sColInfos.values().stream().map(Pair::getLeft).collect(Collectors.toList()));
      sortColInfos.addAll(sColInfos.values().stream().map(Pair::getRight).collect(Collectors.toList()));
    }
    if (distributeColumnNames != null) {
      distributeColumns.addAll(dColInfos.values().stream().map(Pair::getLeft).collect(Collectors.toList()));
      distributeColInfos.addAll(dColInfos.values().stream().map(Pair::getRight).collect(Collectors.toList()));
    }

    return result;
  }

  private FileSinkDesc createFileSinkDesc(String dest, TableDesc table_desc,
                                          Partition dest_part, Path dest_path, int currentTableId,
                                          boolean destTableIsAcid, boolean destTableIsTemporary,
                                          boolean destTableIsMaterialization, Path queryTmpdir,
                                          SortBucketRSCtx rsCtx, DynamicPartitionCtx dpCtx, ListBucketingCtx lbCtx,
                                          RowSchema fsRS, boolean canBeMerged, Table dest_tab, boolean isMmCtas,
                                          Integer dest_type, QB qb, boolean isDirectInsert, AcidUtils.Operation acidOperation, String moveTaskId) throws SemanticException {
    boolean isInsertOverwrite = false;
    boolean isLocal = false;
    Context.Operation writeOperation = getWriteOperation(dest);
    switch (dest_type) {
    case QBMetaData.DEST_PARTITION:
      //fall through
    case QBMetaData.DEST_TABLE:
      //INSERT [OVERWRITE] path
      String destTableFullName = dest_tab.getCompleteName().replace('@', '.');
      Map<String, ASTNode> iowMap = qb.getParseInfo().getInsertOverwriteTables();
      if (iowMap.containsKey(destTableFullName) &&
          qb.getParseInfo().isDestToOpTypeInsertOverwrite(dest)) {
        isInsertOverwrite = true;
      }

      // Some non-native tables might be partitioned without partition spec information being present in the Table object
      HiveStorageHandler storageHandler = dest_tab.getStorageHandler();
      if (dest_tab.hasNonNativePartitionSupport()) {
        DynamicPartitionCtx nonNativeDpCtx = storageHandler.createDPContext(conf, dest_tab, writeOperation);
        if (dpCtx == null && nonNativeDpCtx != null) {
          dpCtx = nonNativeDpCtx;
        }
      }

      break;
    case QBMetaData.DEST_LOCAL_FILE:
      isLocal = true;
    case QBMetaData.DEST_DFS_FILE:
      //CTAS path or insert into file/directory
      break;
    default:
      throw new IllegalStateException("Unexpected dest_type=" + dest_tab);
    }
    FileSinkDesc fileSinkDesc = new FileSinkDesc(queryTmpdir, table_desc,
        conf.getBoolVar(HiveConf.ConfVars.COMPRESS_RESULT), currentTableId, rsCtx.isMultiFileSpray(),
        canBeMerged, rsCtx.getNumFiles(), rsCtx.getTotalFiles(), rsCtx.getPartnCols(), dpCtx,
        dest_path, isMmCtas, isInsertOverwrite, qb.getIsQuery(),
        qb.isCTAS() || qb.isMaterializedView(), isDirectInsert, acidOperation,
            ctx.isDeleteBranchOfUpdate(dest));

    fileSinkDesc.setMoveTaskId(moveTaskId);
    boolean isHiveServerQuery = SessionState.get().isHiveServerQuery();
    fileSinkDesc.setHiveServerQuery(isHiveServerQuery);
    // If this is an insert, update, or delete on an ACID table then mark that so the
    // FileSinkOperator knows how to properly write to it.
    boolean isDestInsertOnly = (dest_part != null && dest_part.getTable() != null &&
        AcidUtils.isInsertOnlyTable(dest_part.getTable().getParameters()))
        || (table_desc != null && AcidUtils.isInsertOnlyTable(table_desc.getProperties()));

    if (isDestInsertOnly) {
      fileSinkDesc.setWriteType(Operation.INSERT);
      acidFileSinks.add(fileSinkDesc);
    }

    if (destTableIsAcid) {
      AcidUtils.Operation wt = updating(dest) ? AcidUtils.Operation.UPDATE :
          (deleting(dest) ? AcidUtils.Operation.DELETE : AcidUtils.Operation.INSERT);
      fileSinkDesc.setWriteType(wt);
      acidFileSinks.add(fileSinkDesc);
    }

    fileSinkDesc.setWriteOperation(writeOperation);

    fileSinkDesc.setTemporary(destTableIsTemporary);
    fileSinkDesc.setMaterialization(destTableIsMaterialization);

    /* Set List Bucketing context. */
    if (lbCtx != null) {
      lbCtx.processRowSkewedIndex(fsRS);
      lbCtx.calculateSkewedValueSubDirList();
    }
    fileSinkDesc.setLbCtx(lbCtx);

    // set the stats publishing/aggregating key prefix
    // the same as directory name. The directory name
    // can be changed in the optimizer but the key should not be changed
    // it should be the same as the MoveWork's sourceDir.
    fileSinkDesc.setStatsAggPrefix(fileSinkDesc.getDirName().toString());
    if (!destTableIsMaterialization &&
        HiveConf.getVar(conf, HIVE_STATS_DBCLASS).equalsIgnoreCase(StatDB.fs.name())) {
      String statsTmpLoc;
      if (isLocal){
        statsTmpLoc = ctx.getMRTmpPath().toString();
      } else {
        statsTmpLoc = ctx.getTempDirForInterimJobPath(dest_path).toString();
      }
      fileSinkDesc.setStatsTmpDir(statsTmpLoc);
      LOG.debug("Set stats collection dir : " + statsTmpLoc);
    }

    if (dest_part != null) {
      try {
        String staticSpec = Warehouse.makePartPath(dest_part.getSpec());
        fileSinkDesc.setStaticSpec(staticSpec);
      } catch (MetaException e) {
        throw new SemanticException(e);
      }
    } else if (dpCtx != null) {
      fileSinkDesc.setStaticSpec(dpCtx.getSPPath());
    }
    return fileSinkDesc;
  }

  private void handleLineage(Table destinationTable, LoadTableDesc ltd, Operator output)
      throws SemanticException {
    if (ltd != null) {
      queryState.getLineageState().mapDirToOp(ltd.getSourcePath(), output);
    }
    if (HiveOperation.CREATETABLE_AS_SELECT.equals(queryState.getHiveOperation())) {

      Path tlocation = null;
      String tName = Utilities.getDbTableName(tableDesc.getDbTableName())[1];
      try {
        String suffix = Utilities.getTableOrMVSuffix(ctx,
                AcidUtils.isTableSoftDeleteEnabled(destinationTable, conf));
        Warehouse wh = new Warehouse(conf);
        tlocation = wh.getDefaultTablePath(db.getDatabase(tableDesc.getDatabaseName()),
            tName + suffix, tableDesc.isExternal());

        if (destinationTable != null && destinationTable.getSd() != null
                && destinationTable.getPath() != null) {
          tlocation = destinationTable.getPath();
        }
      } catch (MetaException|HiveException e) {
        throw new SemanticException(e);
      }

      queryState.getLineageState()
          .mapDirToOp(tlocation, output);
    } else if (HiveOperation.CREATE_MATERIALIZED_VIEW.equals(queryState.getHiveOperation())) {
      Path tlocation;
      String [] dbTable = Utilities.getDbTableName(createVwDesc.getViewName());
      try {
        Warehouse wh = new Warehouse(conf);
        Map<String, String> tblProps = createVwDesc.getTblProps();
        tlocation = wh.getDefaultTablePath(db.getDatabase(dbTable[0]), dbTable[1],
          tblProps == null || !AcidUtils.isTablePropertyTransactional(tblProps));
      } catch (MetaException|HiveException e) {
        throw new SemanticException(e);
      }

      queryState.getLineageState()
        .mapDirToOp(tlocation, output);
    }
  }

  private void setWriteIdForSurrogateKeys(LoadTableDesc ltd, Operator input) {
    if (ltd == null) {
      return;
    }

    Map<String, ExprNodeDesc> columnExprMap = input.getConf().getColumnExprMap();
    if (columnExprMap != null) {
      for (ExprNodeDesc desc : columnExprMap.values()) {
        if (desc instanceof ExprNodeGenericFuncDesc) {
          GenericUDF genericUDF = ((ExprNodeGenericFuncDesc)desc).getGenericUDF();
          if (genericUDF instanceof GenericUDFSurrogateKey) {
            ((GenericUDFSurrogateKey)genericUDF).setWriteId(ltd.getWriteId());
          }
        }
      }
    }

    for (Operator<? extends OperatorDesc> parent : (List<Operator<? extends OperatorDesc>>)input.getParentOperators()) {
      setWriteIdForSurrogateKeys(ltd, parent);
    }
  }

  private WriteEntity generateTableWriteEntity(String dest, Table dest_tab,
                                               Map<String, String> partSpec, LoadTableDesc ltd,
                                               DynamicPartitionCtx dpCtx)
      throws SemanticException {
    WriteEntity output = null;

    // Here only register the whole table for post-exec hook if no DP present
    // in the case of DP, we will register WriteEntity in MoveTask when the
    // list of dynamically created partitions are known.
    if ((dpCtx == null || dpCtx.getNumDPCols() == 0)) {
      output = new WriteEntity(dest_tab, determineWriteType(ltd, dest));
      if (!outputs.add(output)) {
        if(!allowOutputMultipleTimes()) {
          /**
           * Merge stmt with early split update may create several (2) writes to the same
           * table with the same {@link WriteType}, e.g. if original Merge stmt has both update and
           * delete clauses, and update is split into insert + delete, in which case it's not an
           * error*/
          throw new SemanticException(ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
              .getMsg(dest_tab.getTableName()));
        }
      }
    }

    if ((dpCtx != null) && (dpCtx.getNumDPCols() >= 0)) {
      // No static partition specified
      if (dpCtx.getNumSPCols() == 0) {
        output = new WriteEntity(dest_tab, determineWriteType(ltd, dest), true);
        outputs.add(output);
        output.setDynamicPartitionWrite(true);
      }
      // part of the partition specified
      // Create a DummyPartition in this case. Since, the metastore does not store partial
      // partitions currently, we need to store dummy partitions
      else {
        String ppath = dpCtx.getSPPath();
        ppath = ppath.substring(0, ppath.length() - 1);
        DummyPartition p = new DummyPartition(dest_tab, 
            dest_tab.getDbName() + "@" + dest_tab.getTableName() + "@" + ppath, 
          partSpec);
        WriteEntity.WriteType writeType;
        if (ltd.isInsertOverwrite()) {
          writeType = WriteEntity.WriteType.INSERT_OVERWRITE;
        } else {
          writeType = getWriteType(dest);
        }
        output = new WriteEntity(p, writeType, false);
        output.setDynamicPartitionWrite(true);
        outputs.add(output);
      }
    }
    return output;
  }

  protected boolean allowOutputMultipleTimes() {
    return false;
  }

  private void checkExternalTable(Table dest_tab) throws SemanticException {
    if ((!conf.getBoolVar(HiveConf.ConfVars.HIVE_INSERT_INTO_EXTERNAL_TABLES)) &&
        (dest_tab.getTableType().equals(TableType.EXTERNAL_TABLE))) {
      throw new SemanticException(
          ErrorMsg.INSERT_EXTERNAL_TABLE.getMsg(dest_tab.getTableName()));
    }
  }

  private void checkImmutableTable(QB qb, Table dest_tab, Path dest_path, boolean isPart)
      throws SemanticException {
    // If the query here is an INSERT_INTO and the target is an immutable table,
    // verify that our destination is empty before proceeding
    if (!dest_tab.isImmutable() || !qb.getParseInfo().isInsertIntoTable(
        dest_tab.getDbName(), dest_tab.getTableName(), dest_tab.getSnapshotRef())) {
      return;
    }
    try {
      FileSystem fs = dest_path.getFileSystem(conf);
      if (! org.apache.hadoop.hive.metastore.utils.FileUtils.isDirEmpty(fs,dest_path)){
        LOG.warn("Attempted write into an immutable table : "
            + dest_tab.getTableName() + " : " + dest_path);
        throw new SemanticException(
            ErrorMsg.INSERT_INTO_IMMUTABLE_TABLE.getMsg(dest_tab.getTableName()));
      }
    } catch (IOException ioe) {
      LOG.warn("Error while trying to determine if immutable table "
          + (isPart ? "partition " : "") + "has any data : "  + dest_tab.getTableName()
          + " : " + dest_path);
      throw new SemanticException(ErrorMsg.INSERT_INTO_IMMUTABLE_TABLE.getMsg(ioe.getMessage()));
    }
  }

  private DynamicPartitionCtx checkDynPart(QB qb, QBMetaData qbm, Table dest_tab,
                                           Map<String, String> partSpec, String dest) throws SemanticException {
    List<FieldSchema> parts = dest_tab.getPartitionKeys();
    if (parts == null || parts.isEmpty()) {
      return null; // table is not partitioned
    }
    if (partSpec == null || partSpec.isEmpty()) { // user did NOT specify partition
      throw new SemanticException(generateErrorMessage(qb.getParseInfo().getDestForClause(dest),
          ErrorMsg.NEED_PARTITION_ERROR.getMsg()));
    }
    DynamicPartitionCtx dpCtx = qbm.getDPCtx(dest);
    if (dpCtx == null) {
      dest_tab.validatePartColumnNames(partSpec, false);
      dpCtx = new DynamicPartitionCtx(partSpec,
          conf.getVar(HiveConf.ConfVars.DEFAULT_PARTITION_NAME),
          conf.getIntVar(HiveConf.ConfVars.DYNAMIC_PARTITION_MAX_PARTS_PER_NODE));
      qbm.setDPCtx(dest, dpCtx);
    }

    verifyDynamicPartitionEnabled(conf, qb, dest);

    if ((dest_tab.getNumBuckets() > 0)) {
      dpCtx.setNumBuckets(dest_tab.getNumBuckets());
    }
    return dpCtx;
  }

  private static void verifyDynamicPartitionEnabled(HiveConf conf, QB qb, String dest) throws SemanticException {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMIC_PARTITIONING)) { // allow DP
      throw new SemanticException(generateErrorMessage(qb.getParseInfo().getDestForClause(dest),
          ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg()));
    }
  }

  private void createPreInsertDesc(Table table, boolean overwrite) {
    PreInsertTableDesc preInsertTableDesc = new PreInsertTableDesc(table, overwrite);
    this.rootTasks
        .add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), preInsertTableDesc)));
  }


  private void genAutoColumnStatsGatheringPipeline(Table table, Map<String, String> partSpec, Operator curr,
                                                   boolean isInsertInto, boolean useTableValueConstructor)
      throws SemanticException {
    LOG.info("Generate an operator pipeline to autogather column stats for table " + table.getTableName()
        + " in query " + ctx.getCmd());
    ColumnStatsAutoGatherContext columnStatsAutoGatherContext = null;
    columnStatsAutoGatherContext = new ColumnStatsAutoGatherContext(this, conf, curr, table, partSpec, isInsertInto, ctx);
    if (useTableValueConstructor) {
      // Table does not exist, use table value constructor to simulate
      columnStatsAutoGatherContext.insertTableValuesAnalyzePipeline();
    } else {
      // Table already exists
      columnStatsAutoGatherContext.insertAnalyzePipeline();
    }
    columnStatsAutoGatherContexts.add(columnStatsAutoGatherContext);
  }

  String fixCtasColumnName(String colName) {
    return colName;
  }

  private void checkAcidConstraints() {
    /*
    LOG.info("Modifying config values for ACID write");
    conf.setBoolVar(ConfVars.HIVE_OPT_REDUCE_DEDUPLICATION, true);
    conf.setIntVar(ConfVars.HIVE_OPT_REDUCE_DEDUPLICATION_MIN_REDUCER, 1);
    These props are now enabled elsewhere (see commit diffs).  It would be better instead to throw
    if they are not set.  For exmaple, if user has set hive.optimize.reducededuplication=false for
    some reason, we'll run a query contrary to what they wanted...  But throwing now would be
    backwards incompatible.
    */
    conf.set(AcidUtils.CONF_ACID_KEY, "true");
    SessionState.get().getConf().set(AcidUtils.CONF_ACID_KEY, "true");
  }

  private Operator genConversionSelectOperatorByAddPartition(String dest, QB qb, Operator input,
      Deserializer deserializer, Table table, Map<String, String> partitionSpec) throws SemanticException {
    StructObjectInspector oi = null;
    try {
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    // Check column number
    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    List<ColumnInfo> rowFields = opParseCtx.get(input).getRowResolver().getColumnInfos();
    int inColumnCnt = rowFields.size();
    int outColumnCnt = tableFields.size();
    long staticPartitionColumnCnt = partitionSpec != null ?
        partitionSpec.values().stream().filter(Objects::nonNull).count() : 0;
    List<ExprNodeDesc> expressions = new ArrayList<>(outColumnCnt);

    AtomicBoolean convert = new AtomicBoolean(false);
    if (inColumnCnt + staticPartitionColumnCnt != outColumnCnt) {
      String reason = "Table " + dest + " has " + outColumnCnt
              + " columns, but query has " + (inColumnCnt + staticPartitionColumnCnt) + " columns.";
      throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(),
              qb.getParseInfo().getDestForClause(dest), reason));
    }

    int rowNum = 0;
    for (StructField tableField : tableFields) {
      ExprNodeDesc column;
      // Static partition column case
      if (partitionSpec != null && partitionSpec.containsKey(tableField.getFieldName())
          && partitionSpec.get(tableField.getFieldName()) != null) {
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(
                tableField.getFieldObjectInspector().getTypeName());
        if (tableField.getFieldObjectInspector().getCategory() == Category.PRIMITIVE) {
          AbstractPrimitiveJavaObjectInspector convertOI = PrimitiveObjectInspectorFactory
              .getPrimitiveJavaObjectInspector(((AbstractPrimitiveObjectInspector) tableField.getFieldObjectInspector())
              .getPrimitiveCategory());
          Object value = ObjectInspectorConverters.getConverter(
              new JavaConstantStringObjectInspector(partitionSpec.get(tableField.getFieldName())),
              convertOI).convert(partitionSpec.get(tableField.getFieldName()));
          column = new ExprNodeConstantDesc(typeInfo, value);
        } else {
          throw new SemanticException("Unable to use complex type as a partition type");
        }
      } else if (partitionSpec != null && partitionSpec.containsKey(tableField.getFieldName())
          && partitionSpec.get(tableField.getFieldName()) == null) {
        // Dynamic partition column case
        ColumnInfo inputColumn = rowFields.get(rowNum);
        TypeInfo inputTypeInfo = inputColumn.getType();
        column = new ExprNodeColumnDesc(inputTypeInfo, inputColumn.getInternalName(),
                "", true);
        rowNum++;
      } else {
        // Non-partitioned column case
        column = handleConversion(tableField, rowFields.get(rowNum), convert, dest, rowNum);
        rowNum++;
      }
      expressions.add(column);
    }

    if (expressions.size() != outColumnCnt) {
      String reason = "Table " + dest + " has " + outColumnCnt
              + " columns, but query has " + expressions.size() + " columns.";
      throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(),
              qb.getParseInfo().getDestForClause(dest), reason));
    } else {
      // add the select operator
      RowResolver rowResolver = new RowResolver();
      List<String> colNames = new ArrayList<String>();
      Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
      for (int i = 0; i < expressions.size(); i++) {
        String name = getColumnInternalName(i);
        rowResolver.put("", name, new ColumnInfo(name, expressions.get(i)
                .getTypeInfo(), "", false));
        colNames.add(name);
        colExprMap.put(name, expressions.get(i));
      }
      input = putOpInsertMap(OperatorFactory.getAndMakeChild(
              new SelectDesc(expressions, colNames), new RowSchema(rowResolver
              .getColumnInfos()), input), rowResolver);
      input.setColumnExprMap(colExprMap);
    }
    return input;
  }

  /**
   * Generate the conversion SelectOperator that converts the columns into the
   * types that are expected by the table_desc.
   */
  private Operator genConversionSelectOperator(String dest, QB qb, Operator input,
      Deserializer deserializer, DynamicPartitionCtx dpCtx, List<FieldSchema> parts, Table table)
      throws SemanticException {
    StructObjectInspector oi = null;
    try {
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    // Check column number
    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    boolean dynPart = HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMIC_PARTITIONING);
    List<ColumnInfo> rowFields = opParseCtx.get(input).getRowResolver().getColumnInfos();
    int inColumnCnt = rowFields.size();
    int outColumnCnt = tableFields.size();

    // if target table is always unpartitioned, then the output object inspector will already contain the partition cols
    // too, therefore we shouldn't add the partition col num to the output col num
    boolean alreadyContainsPartCols = Optional.ofNullable(table)
            .map(Table::hasNonNativePartitionSupport)
            .orElse(Boolean.FALSE);

    if (dynPart && dpCtx != null && !alreadyContainsPartCols) {
      outColumnCnt += dpCtx.getNumDPCols();
    }
    
    // The numbers of input columns and output columns should match for regular query
    if (!updating(dest) && !deleting(dest) && !merging(dest) && inColumnCnt != outColumnCnt) {
      String reason = "Table " + dest + " has " + outColumnCnt
          + " columns, but query has " + inColumnCnt + " columns.";
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(),
          qb.getParseInfo().getDestForClause(dest), reason));
    }

    // Check column types
    AtomicBoolean converted = new AtomicBoolean(false);
    int columnNumber = tableFields.size();
    List<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(columnNumber);

    // MetadataTypedColumnsetSerDe does not need type conversions because it
    // does the conversion to String by itself.
    if (!(deserializer instanceof MetadataTypedColumnsetSerDe) && !deleting(dest)) {

      // If we're updating, add the required virtual columns.
      int virtualColumnSize = updating(dest) || merging(dest) ? AcidUtils.getAcidVirtualColumns(table).size() : 0;
      for (int i = 0; i < virtualColumnSize; i++) {
        expressions.add(new ExprNodeColumnDesc(rowFields.get(i).getType(),
            rowFields.get(i).getInternalName(), "", true));
      }

      // here only deals with non-partition columns. We deal with partition columns next
      int rowFieldsOffset = expressions.size();
      for (int i = 0; i < columnNumber; i++) {
        ExprNodeDesc column = handleConversion(tableFields.get(i), rowFields.get(rowFieldsOffset + i), converted, dest, i);
        expressions.add(column);
      }

      // For Non-Native ACID tables we should convert the new values as well
      rowFieldsOffset = expressions.size();
      if (updating(dest) && AcidUtils.isNonNativeAcidTable(table)
          && rowFields.size() >= rowFieldsOffset + columnNumber) {
        for (int i = 0; i < columnNumber; i++) {
          ExprNodeDesc column = handleConversion(tableFields.get(i), rowFields.get(rowFieldsOffset-columnNumber + i), converted, dest, i);
          expressions.add(column);
        }
      }

      // deal with dynamic partition columns
      rowFieldsOffset = expressions.size();
      if (dynPart && dpCtx != null && dpCtx.getNumDPCols() > 0) {
        // rowFields contains non-partitioned columns (tableFields) followed by DP columns
        for (int dpColIdx = 0; dpColIdx < rowFields.size() - rowFieldsOffset; ++dpColIdx) {

          // create ExprNodeDesc
          ColumnInfo inputColumn = rowFields.get(dpColIdx + rowFieldsOffset);
          TypeInfo inputTypeInfo = inputColumn.getType();
          ExprNodeDesc column =
              new ExprNodeColumnDesc(inputTypeInfo, inputColumn.getInternalName(), "", true);

          // Cast input column to destination column type if necessary.
          if (conf.getBoolVar(DYNAMIC_PARTITION_CONVERT)) {
            if (parts != null && !parts.isEmpty()) {
              String destPartitionName = dpCtx.getDPColNames().get(dpColIdx);
              FieldSchema destPartitionFieldSchema = parts.stream()
                  .filter(dynamicPartition -> dynamicPartition.getName().equals(destPartitionName))
                  .findFirst().orElse(null);
              if (destPartitionFieldSchema == null) {
                throw new IllegalStateException("Partition schema for dynamic partition " +
                    destPartitionName + " not found in DynamicPartitionCtx.");
              }
              String partitionType = destPartitionFieldSchema.getType();
              if (partitionType == null) {
                throw new IllegalStateException("Couldn't get FieldSchema for partition" +
                    destPartitionFieldSchema.getName());
              }
              PrimitiveTypeInfo partitionTypeInfo =
                  TypeInfoFactory.getPrimitiveTypeInfo(partitionType);
              if (!partitionTypeInfo.equals(inputTypeInfo)) {
                column = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
                    .createConversionCast(column, partitionTypeInfo);
                converted.set(true);
              }
            } else {
              LOG.warn("Partition schema for dynamic partition " + inputColumn.getAlias() + " ("
                  + inputColumn.getInternalName() + ") not found in DynamicPartitionCtx. "
                  + "This is expected with a CTAS.");
            }
          }
          expressions.add(column);
        }
      }
    }

    if (converted.get()) {
      // add the select operator
      RowResolver rowResolver = new RowResolver();
      List<String> colNames = new ArrayList<String>();
      Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
      for (int i = 0; i < expressions.size(); i++) {
        String name = getColumnInternalName(i);
        rowResolver.put("", name, new ColumnInfo(name, expressions.get(i)
            .getTypeInfo(), "", false));
        colNames.add(name);
        colExprMap.put(name, expressions.get(i));
      }
      input = putOpInsertMap(OperatorFactory.getAndMakeChild(
          new SelectDesc(expressions, colNames), new RowSchema(rowResolver
              .getColumnInfos()), input), rowResolver);
      input.setColumnExprMap(colExprMap);
    }
    return input;
  }

  /**
   * Creates an expression for converting from a table column to a row column. For example:
   * The table column is int but the query provides a string in the row, then we need to cast automatically.
   * @param tableField The target table column
   * @param rowField The source row column
   * @param conversion The value of this boolean is set to true if we detect that a conversion is needed. This is a
   *                   hidden return value hidden here, to notify the caller that a cast was needed.
   * @param dest The destination table for the error message
   * @param columnNum The destination column id for the error message
   * @return The Expression describing the selected column. Note that `conversion` can be considered as a return value
   *         as well
   * @throws SemanticException If conversion were needed, but automatic conversion is not available
   */
  private ExprNodeDesc handleConversion(StructField tableField, ColumnInfo rowField, AtomicBoolean conversion, String dest, int columnNum)
      throws SemanticException {
    ObjectInspector tableFieldOI = tableField
        .getFieldObjectInspector();
    TypeInfo tableFieldTypeInfo = TypeInfoUtils
        .getTypeInfoFromObjectInspector(tableFieldOI);
    TypeInfo rowFieldTypeInfo = rowField.getType();
    ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo,
        rowField.getInternalName(), "", false,
        rowField.isSkewedCol());
    // LazySimpleSerDe can convert any types to String type using
    // JSON-format. However, we may add more operators.
    // Thus, we still keep the conversion.
    if (!tableFieldTypeInfo.equals(rowFieldTypeInfo)) {
      // need to do some conversions here
      conversion.set(true);
      if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
        // cannot convert to complex types
        column = null;
      } else {
        column = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
            .createConversionCast(column, (PrimitiveTypeInfo)tableFieldTypeInfo);
      }
      if (column == null) {
        String reason = "Cannot convert column " + columnNum + " from "
            + rowFieldTypeInfo + " to " + tableFieldTypeInfo + ".";
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(),
            qb.getParseInfo().getDestForClause(dest), reason));
      }
    }
    return column;
  }

  @SuppressWarnings("nls")
  private Operator genLimitPlan(String dest, Operator input, int offset, int limit) {
    // A map-only job can be optimized - instead of converting it to a
    // map-reduce job, we can have another map
    // job to do the same to avoid the cost of sorting in the map-reduce phase.
    // A better approach would be to
    // write into a local file and then have a map-only job.
    // Add the limit operator to get the value fields

    RowResolver inputRR = opParseCtx.get(input).getRowResolver();

    LimitDesc limitDesc = new LimitDesc(offset, limit);
    globalLimitCtx.setLastReduceLimitDesc(limitDesc);

    Operator limitMap = putOpInsertMap(OperatorFactory.getAndMakeChild(
        limitDesc, new RowSchema(inputRR.getColumnInfos()), input),
        inputRR);

    LOG.debug("Created LimitOperator Plan for clause: {} row schema: {}", dest, inputRR);

    return limitMap;
  }

  private Operator genUDTFPlan(GenericUDTF genericUDTF, String outputTableAlias, List<String> colAliases, QB qb,
      Operator input, boolean outerLV) throws SemanticException {

    // No GROUP BY / DISTRIBUTE BY / SORT BY / CLUSTER BY
    QBParseInfo qbp = qb.getParseInfo();
    if (!qbp.getDestToGroupBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_GROUP_BY.getMsg());
    }
    if (!qbp.getDestToDistributeBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_DISTRIBUTE_BY.getMsg());
    }
    if (!qbp.getDestToSortBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_SORT_BY.getMsg());
    }
    if (!qbp.getDestToClusterBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_CLUSTER_BY.getMsg());
    }
    if (!qbp.getAliasToLateralViews().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_LATERAL_VIEW.getMsg());
    }

    LOG.debug("Table alias: {} Col aliases: {}", outputTableAlias, colAliases);

    // Use the RowResolver from the input operator to generate a input
    // ObjectInspector that can be used to initialize the UDTF. Then, the

    // resulting output object inspector can be used to make the RowResolver
    // for the UDTF operator
    RowResolver selectRR = opParseCtx.get(input).getRowResolver();
    List<ColumnInfo> inputCols = selectRR.getColumnInfos();

    // Create the object inspector for the input columns and initialize the UDTF
    List<String> colNames = new ArrayList<String>();
    ObjectInspector[] colOIs = new ObjectInspector[inputCols.size()];
    for (int i = 0; i < inputCols.size(); i++) {
      colNames.add(inputCols.get(i).getInternalName());
      colOIs[i] = inputCols.get(i).getObjectInspector();
    }
    StandardStructObjectInspector rowOI =
        ObjectInspectorFactory.getStandardStructObjectInspector(colNames, Arrays.asList(colOIs));
    StructObjectInspector outputOI = genericUDTF.initialize(rowOI);

    int numUdtfCols = outputOI.getAllStructFieldRefs().size();
    if (colAliases.isEmpty()) {
      // user did not specfied alias names, infer names from outputOI
      for (StructField field : outputOI.getAllStructFieldRefs()) {
        colAliases.add(field.getFieldName());
      }
    }
    // Make sure that the number of column aliases in the AS clause matches
    // the number of columns output by the UDTF
    int numSuppliedAliases = colAliases.size();
    if (numUdtfCols != numSuppliedAliases) {
      throw new SemanticException(ErrorMsg.UDTF_ALIAS_MISMATCH
          .getMsg("expected " + numUdtfCols + " aliases " + "but got "
              + numSuppliedAliases));
    }

    // Generate the output column info's / row resolver using internal names.
    List<ColumnInfo> udtfCols = new ArrayList<ColumnInfo>();

    Iterator<String> colAliasesIter = colAliases.iterator();
    for (StructField sf : outputOI.getAllStructFieldRefs()) {

      String colAlias = colAliasesIter.next();
      assert (colAlias != null);

      // Since the UDTF operator feeds into a LVJ operator that will rename
      // all the internal names, we can just use field name from the UDTF's OI
      // as the internal name
      ColumnInfo col = new ColumnInfo(sf.getFieldName(), TypeInfoUtils
          .getTypeInfoFromObjectInspector(sf.getFieldObjectInspector()),
          outputTableAlias, false);
      udtfCols.add(col);
    }

    // Create the row resolver for this operator from the output columns
    RowResolver out_rwsch = new RowResolver();
    for (int i = 0; i < udtfCols.size(); i++) {
      out_rwsch.put(outputTableAlias, colAliases.get(i), udtfCols.get(i));
    }

    // Add the UDTFOperator to the operator DAG
    return putOpInsertMap(OperatorFactory.getAndMakeChild(
        new UDTFDesc(genericUDTF, outerLV), new RowSchema(out_rwsch.getColumnInfos()),
        input), out_rwsch);
  }

  @SuppressWarnings("nls")
  private Operator genLimitMapRedPlan(String dest, QB qb, Operator input,
      int offset, int limit, boolean extraMRStep) throws SemanticException {
    // A map-only job can be optimized - instead of converting it to a
    // map-reduce job, we can have another map
    // job to do the same to avoid the cost of sorting in the map-reduce phase.
    // A better approach would be to
    // write into a local file and then have a map-only job.
    // Add the limit operator to get the value fields
    Operator curr = genLimitPlan(dest, input, offset, limit);

    // the client requested that an extra map-reduce step be performed
    if (!extraMRStep  || !HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_GROUPBY_LIMIT_EXTRASTEP)){
      return curr;
    }

    // Create a reduceSink operator followed by another limit
    curr = genReduceSinkPlan(dest, qb, curr, 1, false);
    return genLimitPlan(dest, curr, offset, limit);
  }

  private List<ExprNodeDesc> getPartitionColsFromBucketCols(String dest, QB qb, Table tab, TableDesc table_desc,
      Operator input, boolean convert)
      throws SemanticException {
    List<String> tabBucketCols = tab.getBucketCols();
    List<FieldSchema> tabCols = tab.getCols();

    // Partition by the bucketing column
    List<Integer> posns = new ArrayList<Integer>();

    for (String bucketCol : tabBucketCols) {
      int pos = 0;
      for (FieldSchema tabCol : tabCols) {
        if (bucketCol.equals(tabCol.getName())) {
          posns.add(pos);
          break;
        }
        pos++;
      }
    }

    return genConvertCol(dest, qb, table_desc, input, posns, convert);
  }

  // We have to set up the bucketing columns differently for update and deletes,
  // as it is always using the ROW__ID column.
  private List<ExprNodeDesc> getPartitionColsFromBucketColsForUpdateDelete(
      Operator input, boolean convert) throws SemanticException {
    //return genConvertCol(dest, qb, tab, table_desc, input, Arrays.asList(0), convert);
    // In the case of update and delete the bucketing column is always the first column,
    // and it isn't in the table info.  So rather than asking the table for it,
    // we'll construct it ourself and send it back.  This is based on the work done in
    // genConvertCol below.
    ColumnInfo rowField = opParseCtx.get(input).getRowResolver().getColumnInfos().get(0);
    TypeInfo rowFieldTypeInfo = rowField.getType();
    ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo, rowField.getInternalName(),
        rowField.getTabAlias(), true);
    if (convert) {
      column = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .createConversionCast(column, TypeInfoFactory.intTypeInfo);
    }
    return Collections.singletonList(column);
  }

  private List<ExprNodeDesc> genConvertCol(String dest, QB qb, TableDesc tableDesc, Operator input,
                                           List<Integer> posns, boolean convert)
      throws SemanticException {
    StructObjectInspector oi = null;
    try {
      AbstractSerDe deserializer = tableDesc.getSerDeClass()
          .newInstance();
      deserializer.initialize(conf, tableDesc.getProperties(), null);
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    List<ColumnInfo> rowFields = opParseCtx.get(input).getRowResolver().getColumnInfos();

    // Check column type
    int columnNumber = posns.size();
    List<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(columnNumber);
    for (Integer posn : posns) {
      ObjectInspector tableFieldOI = tableFields.get(posn).getFieldObjectInspector();
      TypeInfo tableFieldTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(tableFieldOI);
      TypeInfo rowFieldTypeInfo = rowFields.get(posn).getType();
      ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo,
          rowFields.get(posn).getInternalName(), rowFields.get(posn).getTabAlias(),
          rowFields.get(posn).getIsVirtualCol());

      if (convert && !tableFieldTypeInfo.equals(rowFieldTypeInfo)) {
        // need to do some conversions here
        if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
          // cannot convert to complex types
          column = null;
        } else {
          column = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
              .createConversionCast(column, (PrimitiveTypeInfo)tableFieldTypeInfo);
        }
        if (column == null) {
          String reason = "Cannot convert column " + posn + " from "
              + rowFieldTypeInfo + " to " + tableFieldTypeInfo + ".";
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(),
              qb.getParseInfo().getDestForClause(dest), reason));
        }
      }
      expressions.add(column);
    }

    return expressions;
  }

  private List<ExprNodeDesc> getSortCols(String dest, QB qb, Table tab, TableDesc tableDesc, Operator input)
      throws SemanticException {
    List<Order> tabSortCols = tab.getSortCols();
    List<FieldSchema> tabCols = tab.getCols();

    // Partition by the bucketing column
    List<Integer> posns = new ArrayList<Integer>();
    for (Order sortCol : tabSortCols) {
      int pos = 0;
      for (FieldSchema tabCol : tabCols) {
        if (sortCol.getCol().equals(tabCol.getName())) {
          posns.add(pos);
          break;
        }
        pos++;
      }
    }

    return genConvertCol(dest, qb, tableDesc, input, posns, false);
  }

  private void getSortOrders(Table tab, StringBuilder order, StringBuilder nullOrder) {
    List<Order> tabSortCols = tab.getSortCols();
    List<FieldSchema> tabCols = tab.getCols();

    for (Order sortCol : tabSortCols) {
      for (FieldSchema tabCol : tabCols) {
        if (sortCol.getCol().equals(tabCol.getName())) {
          order.append(DirectionUtils.codeToSign(sortCol.getOrder()));
          nullOrder.append(sortCol.getOrder() == DirectionUtils.ASCENDING_CODE ? 'a' : 'z');
          break;
        }
      }
    }
  }

  private Operator genReduceSinkPlan(String dest, QB qb, Operator<?> input,
                                     int numReducers, boolean hasOrderBy) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRowResolver();

    // First generate the expression for the partition and sort keys
    // The cluster by clause / distribute by clause has the aliases for
    // partition function
    ASTNode partitionExprs = qb.getParseInfo().getClusterByForClause(dest);
    if (partitionExprs == null) {
      partitionExprs = qb.getParseInfo().getDistributeByForClause(dest);
    }
    List<ExprNodeDesc> partCols = new ArrayList<ExprNodeDesc>();
    if (partitionExprs != null) {
      int ccount = partitionExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) partitionExprs.getChild(i);
        partCols.add(genExprNodeDesc(cl, inputRR));
      }
    }
    ASTNode sortExprs = qb.getParseInfo().getClusterByForClause(dest);
    if (sortExprs == null) {
      sortExprs = qb.getParseInfo().getSortByForClause(dest);
    }

    if (sortExprs == null) {
      sortExprs = qb.getParseInfo().getOrderByForClause(dest);
      if (sortExprs != null) {
        assert numReducers == 1;
        // in strict mode, in the presence of order by, limit must be specified
        if (qb.getParseInfo().getDestLimit(dest) == null) {
          String error = StrictChecks.checkNoLimit(conf);
          if (error != null) {
            throw new SemanticException(generateErrorMessage(sortExprs, error));
          }
        }
      }
    }
    List<ExprNodeDesc> sortCols = new ArrayList<ExprNodeDesc>();
    StringBuilder order = new StringBuilder();
    StringBuilder nullOrder = new StringBuilder();
    if (sortExprs != null) {
      int ccount = sortExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) sortExprs.getChild(i);

        if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
          // SortBy ASC
          order.append("+");
          cl = (ASTNode) cl.getChild(0);
          if (cl.getType() == HiveParser.TOK_NULLS_FIRST) {
            nullOrder.append("a");
          } else if (cl.getType() == HiveParser.TOK_NULLS_LAST) {
            nullOrder.append("z");
          } else {
            throw new SemanticException(
                "Unexpected null ordering option: " + cl.getType());
          }
          cl = (ASTNode) cl.getChild(0);
        } else if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC) {
          // SortBy DESC
          order.append("-");
          cl = (ASTNode) cl.getChild(0);
          if (cl.getType() == HiveParser.TOK_NULLS_FIRST) {
            nullOrder.append("a");
          } else if (cl.getType() == HiveParser.TOK_NULLS_LAST) {
            nullOrder.append("z");
          } else {
            throw new SemanticException(
                "Unexpected null ordering option: " + cl.getType());
          }
          cl = (ASTNode) cl.getChild(0);
        } else {
          // ClusterBy
          order.append("+");
          nullOrder.append("a");
        }
        ExprNodeDesc exprNode = genExprNodeDesc(cl, inputRR);
        sortCols.add(exprNode);
      }
    }

    Table dest_tab = qb.getMetaData().getDestTableForAlias(dest);
    AcidUtils.Operation acidOp = Operation.NOT_ACID;
    if (AcidUtils.isTransactionalTable(dest_tab)) {
      acidOp = getAcidType(Utilities.getTableDesc(dest_tab).getOutputFileFormatClass(), dest,
          AcidUtils.isInsertOnlyTable(dest_tab));
    }
    boolean isCompaction = false;
    if (dest_tab != null && dest_tab.getParameters() != null) {
      isCompaction = AcidUtils.isCompactionTable(dest_tab.getParameters());
    }
    Operator result = genReduceSinkPlan(
        input, partCols, sortCols, order.toString(), nullOrder.toString(),
        numReducers, acidOp, true, isCompaction);
    if (result.getParentOperators().size() == 1 &&
        result.getParentOperators().get(0) instanceof ReduceSinkOperator) {
      ((ReduceSinkOperator) result.getParentOperators().get(0))
          .getConf().setHasOrderBy(hasOrderBy);
    }
    return result;
  }

  private Operator genReduceSinkPlan(Operator<?> input,
                                     List<ExprNodeDesc> partitionCols, List<ExprNodeDesc> sortCols,
                                     String sortOrder, String nullOrder, int numReducers, AcidUtils.Operation acidOp, boolean isCompaction)
      throws SemanticException {
    return genReduceSinkPlan(input, partitionCols, sortCols, sortOrder, nullOrder, numReducers,
        acidOp, false, isCompaction);
  }

  @SuppressWarnings("nls")
  private Operator genReduceSinkPlan(Operator<?> input, List<ExprNodeDesc> partitionCols, List<ExprNodeDesc> sortCols,
                                     String sortOrder, String nullOrder, int numReducers, AcidUtils.Operation acidOp,
                                     boolean pullConstants, boolean isCompaction) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRowResolver();

    Operator dummy = Operator.createDummy();
    dummy.setParentOperators(Arrays.asList(input));

    List<ExprNodeDesc> newSortCols = new ArrayList<ExprNodeDesc>();
    StringBuilder newSortOrder = new StringBuilder();
    StringBuilder newNullOrder = new StringBuilder();
    List<ExprNodeDesc> sortColsBack = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < sortCols.size(); i++) {
      ExprNodeDesc sortCol = sortCols.get(i);
      // If we are not pulling constants, OR
      // we are pulling constants but this is not a constant
      if (!pullConstants || !(sortCol instanceof ExprNodeConstantDesc)) {
        newSortCols.add(sortCol);
        newSortOrder.append(sortOrder.charAt(i));
        newNullOrder.append(nullOrder.charAt(i));
        sortColsBack.add(ExprNodeDescUtils.backtrack(sortCol, dummy, input));
      }
    }

    // For the generation of the values expression just get the inputs
    // signature and generate field expressions for those
    RowResolver rsRR = new RowResolver();
    List<String> outputColumns = new ArrayList<String>();
    List<ExprNodeDesc> valueCols = new ArrayList<ExprNodeDesc>();
    List<ExprNodeDesc> valueColsBack = new ArrayList<ExprNodeDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<ExprNodeDesc> constantCols = new ArrayList<ExprNodeDesc>();

    List<ColumnInfo> columnInfos = inputRR.getColumnInfos();

    int[] index = new int[columnInfos.size()];
    for (int i = 0; i < index.length; i++) {
      ColumnInfo colInfo = columnInfos.get(i);
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(colInfo.getInternalName());
      ExprNodeColumnDesc value = new ExprNodeColumnDesc(colInfo);

      // backtrack can be null when input is script operator
      ExprNodeDesc valueBack = ExprNodeDescUtils.backtrack(value, dummy, input);
      if (pullConstants && valueBack instanceof ExprNodeConstantDesc) {
        // ignore, it will be generated by SEL op
        index[i] = Integer.MAX_VALUE;
        constantCols.add(valueBack);
        continue;
      }
      int kindex = valueBack == null ? -1 : ExprNodeDescUtils.indexOf(valueBack, sortColsBack);
      if (kindex >= 0) {
        index[i] = kindex;
        ColumnInfo newColInfo = new ColumnInfo(colInfo);
        newColInfo.setInternalName(Utilities.ReduceField.KEY + ".reducesinkkey" + kindex);
        newColInfo.setTabAlias(nm[0]);
        rsRR.put(nm[0], nm[1], newColInfo);
        if (nm2 != null) {
          rsRR.addMappingOnly(nm2[0], nm2[1], newColInfo);
        }
        continue;
      }
      int vindex = valueBack == null ? -1 : ExprNodeDescUtils.indexOf(valueBack, valueColsBack);
      if (vindex >= 0) {
        index[i] = -vindex - 1;
        continue;
      }
      index[i] = -valueCols.size() - 1;
      String outputColName = getColumnInternalName(valueCols.size());

      valueCols.add(value);
      valueColsBack.add(valueBack);

      ColumnInfo newColInfo = new ColumnInfo(colInfo);
      newColInfo.setInternalName(Utilities.ReduceField.VALUE + "." + outputColName);
      newColInfo.setTabAlias(nm[0]);

      rsRR.put(nm[0], nm[1], newColInfo);
      if (nm2 != null) {
        rsRR.addMappingOnly(nm2[0], nm2[1], newColInfo);
      }
      outputColumns.add(outputColName);
    }

    dummy.setParentOperators(null);

    ReduceSinkDesc rsdesc = PlanUtils.getReduceSinkDesc(newSortCols, valueCols, outputColumns,
        false, -1, partitionCols, newSortOrder.toString(), newNullOrder.toString(), defaultNullOrder,
        numReducers, acidOp, isCompaction);
    Operator interim = putOpInsertMap(OperatorFactory.getAndMakeChild(rsdesc,
        new RowSchema(rsRR.getColumnInfos()), input), rsRR);

    List<String> keyColNames = rsdesc.getOutputKeyColumnNames();
    for (int i = 0 ; i < keyColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.KEY + "." + keyColNames.get(i), newSortCols.get(i));
    }
    List<String> valueColNames = rsdesc.getOutputValueColumnNames();
    for (int i = 0 ; i < valueColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.VALUE + "." + valueColNames.get(i), valueCols.get(i));
    }
    interim.setColumnExprMap(colExprMap);

    RowResolver selectRR = new RowResolver();
    List<ExprNodeDesc> selCols = new ArrayList<ExprNodeDesc>();
    List<String> selOutputCols = new ArrayList<String>();
    Map<String, ExprNodeDesc> selColExprMap = new HashMap<String, ExprNodeDesc>();

    Iterator<ExprNodeDesc> constants = constantCols.iterator();
    for (int i = 0; i < index.length; i++) {
      ColumnInfo prev = columnInfos.get(i);
      String[] nm = inputRR.reverseLookup(prev.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(prev.getInternalName());
      ColumnInfo info = new ColumnInfo(prev);

      ExprNodeDesc desc;
      if (index[i] == Integer.MAX_VALUE) {
        desc = constants.next();
      } else {
        String field;
        if (index[i] >= 0) {
          field = Utilities.ReduceField.KEY + "." + keyColNames.get(index[i]);
        } else {
          field = Utilities.ReduceField.VALUE + "." + valueColNames.get(-index[i] - 1);
        }
        desc = new ExprNodeColumnDesc(info.getType(),
            field, info.getTabAlias(), info.getIsVirtualCol());
      }
      selCols.add(desc);

      String internalName = getColumnInternalName(i);
      info.setInternalName(internalName);
      selectRR.put(nm[0], nm[1], info);
      if (nm2 != null) {
        selectRR.addMappingOnly(nm2[0], nm2[1], info);
      }
      selOutputCols.add(internalName);
      selColExprMap.put(internalName, desc);
    }
    SelectDesc select = new SelectDesc(selCols, selOutputCols);
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(select,
        new RowSchema(selectRR.getColumnInfos()), interim), selectRR);
    output.setColumnExprMap(selColExprMap);
    return output;
  }

  private Operator genJoinOperatorChildren(QBJoinTree join, Operator left,
                                           Operator[] right, Set<Integer> omitOpts, ExprNodeDesc[][] joinKeys) throws SemanticException {

    RowResolver outputRR = new RowResolver();
    List<String> outputColumnNames = new ArrayList<String>();
    // all children are base classes
    Operator<?>[] rightOps = new Operator[right.length];

    Map<String, Byte> reversedExprs = new HashMap<String, Byte>();
    Map<Byte, List<ExprNodeDesc>> exprMap = new HashMap<Byte, List<ExprNodeDesc>>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    Map<Integer, Set<String>> posToAliasMap = new HashMap<Integer, Set<String>>();
    Map<Byte, List<ExprNodeDesc>> filterMap = new HashMap<Byte, List<ExprNodeDesc>>();

    // Only used for semijoin with residual predicates
    List<ColumnInfo> topSelectInputColumns = new ArrayList<>();

    for (int pos = 0; pos < right.length; ++pos) {
      Operator<?> input = right[pos] == null ? left : right[pos];
      if (input == null) {
        input = left;
      }
      ReduceSinkOperator rs = (ReduceSinkOperator) input;
      if (rs.getNumParent() != 1) {
        throw new SemanticException("RS should have single parent");
      }
      Operator<?> parent = rs.getParentOperators().get(0);
      ReduceSinkDesc rsDesc = (ReduceSinkDesc) (input.getConf());

      int[] index = rs.getValueIndex();

      List<ExprNodeDesc> valueDesc = new ArrayList<ExprNodeDesc>();
      List<ExprNodeDesc> filterDesc = new ArrayList<ExprNodeDesc>();
      Byte tag = (byte) rsDesc.getTag();

      // check whether this input operator produces output
      // If it has residual, we do not skip this output,
      // we will add a Select on top of the join
      if (omitOpts != null && omitOpts.contains(pos)
          && join.getPostJoinFilters().size() == 0) {
        exprMap.put(tag, valueDesc);
        filterMap.put(tag, filterDesc);
        rightOps[pos] = input;
        continue;
      }

      List<String> keyColNames = rsDesc.getOutputKeyColumnNames();
      List<String> valColNames = rsDesc.getOutputValueColumnNames();

      // prepare output descriptors for the input opt
      RowResolver inputRR = opParseCtx.get(input).getRowResolver();
      RowResolver parentRR = opParseCtx.get(parent).getRowResolver();
      posToAliasMap.put(pos, new HashSet<String>(inputRR.getTableNames()));

      List<ColumnInfo> columns = parentRR.getColumnInfos();
      for (int i = 0; i < index.length; i++) {
        ColumnInfo prev = columns.get(i);
        String[] nm = parentRR.reverseLookup(prev.getInternalName());
        String[] nm2 = parentRR.getAlternateMappings(prev.getInternalName());
        if (outputRR.get(nm[0], nm[1]) != null) {
          continue;
        }
        ColumnInfo info = new ColumnInfo(prev);
        String field;
        if (index[i] >= 0) {
          field = Utilities.ReduceField.KEY + "." + keyColNames.get(index[i]);
        } else {
          field = Utilities.ReduceField.VALUE + "." + valColNames.get(-index[i] - 1);
        }
        String internalName = getColumnInternalName(outputColumnNames.size());
        ExprNodeColumnDesc desc = new ExprNodeColumnDesc(info.getType(),
            field, info.getTabAlias(), info.getIsVirtualCol());

        info.setInternalName(internalName);
        colExprMap.put(internalName, desc);
        outputRR.put(nm[0], nm[1], info);
        if (nm2 != null) {
          outputRR.addMappingOnly(nm2[0], nm2[1], info);
        }

        valueDesc.add(desc);
        outputColumnNames.add(internalName);
        reversedExprs.put(internalName, tag);

        // Populate semijoin select if needed
        if (omitOpts == null || !omitOpts.contains(pos)) {
          topSelectInputColumns.add(info);
        }
      }
      for (ASTNode cond : join.getFilters().get(tag)) {
        filterDesc.add(genExprNodeDesc(cond, inputRR));
      }
      exprMap.put(tag, valueDesc);
      filterMap.put(tag, filterDesc);
      rightOps[pos] = input;
    }

    JoinCondDesc[] joinCondns = new JoinCondDesc[join.getJoinCond().length];
    for (int i = 0; i < join.getJoinCond().length; i++) {
      JoinCond condn = join.getJoinCond()[i];
      joinCondns[i] = new JoinCondDesc(condn);
    }

    JoinDesc desc = new JoinDesc(exprMap, outputColumnNames,
        join.getNoOuterJoin(), joinCondns, filterMap, joinKeys, null);
    desc.setReversedExprs(reversedExprs);
    desc.setFilterMap(join.getFilterMap());
    // Add filters that apply to more than one input
    if (join.getPostJoinFilters().size() != 0 &&
        (!join.getNoOuterJoin() || !join.getNoSemiJoin()
            || HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_PUSH_RESIDUAL_INNER))) {
      LOG.debug("Generate JOIN with post-filtering conditions");
      List<ExprNodeDesc> residualFilterExprs = new ArrayList<ExprNodeDesc>();
      for (ASTNode cond : join.getPostJoinFilters()) {
        residualFilterExprs.add(genExprNodeDesc(cond, outputRR, false, isCBOExecuted()));
      }
      desc.setResidualFilterExprs(residualFilterExprs);
      // Clean post-conditions
      join.getPostJoinFilters().clear();
    }

    JoinOperator joinOp = (JoinOperator) OperatorFactory.getAndMakeChild(getOpContext(), desc,
        new RowSchema(outputRR.getColumnInfos()), rightOps);
    joinOp.setColumnExprMap(colExprMap);
    joinOp.setPosToAliasMap(posToAliasMap);

    if (join.getNullSafes() != null) {
      boolean[] nullsafes = new boolean[join.getNullSafes().size()];
      for (int i = 0; i < nullsafes.length; i++) {
        nullsafes[i] = join.getNullSafes().get(i);
      }
      desc.setNullSafes(nullsafes);
    }

    Operator<?> topOp = putOpInsertMap(joinOp, outputRR);
    if (omitOpts != null && !omitOpts.isEmpty()
        && desc.getResidualFilterExprs() != null && !desc.getResidualFilterExprs().isEmpty()) {
      // Adding a select operator to top of semijoin to ensure projection of only correct columns
      final List<ExprNodeDesc> topSelectExprs = new ArrayList<>();
      final List<String> topSelectOutputColNames = new ArrayList<>();
      final RowResolver topSelectRR = new RowResolver();
      final Map<String, ExprNodeDesc> topSelectColExprMap = new HashMap<String, ExprNodeDesc>();
      for (ColumnInfo colInfo : topSelectInputColumns) {
        ExprNodeColumnDesc columnExpr = new ExprNodeColumnDesc(colInfo);
        topSelectExprs.add(columnExpr);
        topSelectOutputColNames.add(colInfo.getInternalName());
        topSelectColExprMap.put(colInfo.getInternalName(), columnExpr);
        String[] nm = outputRR.reverseLookup(columnExpr.getColumn());
        String[] nm2 = outputRR.getAlternateMappings(columnExpr.getColumn());
        topSelectRR.put(nm[0], nm[1], colInfo);
        if (nm2 != null) {
          topSelectRR.addMappingOnly(nm2[0], nm2[1], colInfo);
        }
      }
      final SelectDesc topSelect = new SelectDesc(topSelectExprs, topSelectOutputColNames);
      topOp = putOpInsertMap(OperatorFactory.getAndMakeChild(topSelect,
          new RowSchema(topSelectRR.getColumnInfos()), topOp), topSelectRR);
      topOp.setColumnExprMap(topSelectColExprMap);
    }

    return topOp;
  }

  private ExprNodeDesc[][] genJoinKeys(QBJoinTree joinTree, Operator[] inputs)
      throws SemanticException {
    ExprNodeDesc[][] joinKeys = new ExprNodeDesc[inputs.length][];
    for (int i = 0; i < inputs.length; i++) {
      RowResolver inputRR = opParseCtx.get(inputs[i]).getRowResolver();
      List<ASTNode> expressions = joinTree.getExpressions().get(i);
      joinKeys[i] = new ExprNodeDesc[expressions.size()];
      for (int j = 0; j < joinKeys[i].length; j++) {
        joinKeys[i][j] = genExprNodeDesc(expressions.get(j), inputRR, true, isCBOExecuted());
      }
    }
    // Type checking and implicit type conversion for join keys
    return genJoinOperatorTypeCheck(joinKeys);
  }

  @SuppressWarnings("nls")
  private Operator genJoinReduceSinkChild(ExprNodeDesc[] joinKeys,
                                          Operator<?> parent, String[] srcs, int tag) throws SemanticException {

    Operator dummy = Operator.createDummy();  // dummy for backtracking
    dummy.setParentOperators(Arrays.asList(parent));

    RowResolver inputRR = opParseCtx.get(parent).getRowResolver();
    RowResolver outputRR = new RowResolver();
    List<String> outputColumns = new ArrayList<String>();
    List<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();
    List<ExprNodeDesc> reduceKeysBack = new ArrayList<ExprNodeDesc>();

    // Compute join keys and store in reduceKeys
    for (ExprNodeDesc joinKey : joinKeys) {
      reduceKeys.add(joinKey);
      reduceKeysBack.add(ExprNodeDescUtils.backtrack(joinKey, dummy, parent));
    }

    // Walk over the input row resolver and copy in the output
    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();

    List<ColumnInfo> columns = inputRR.getColumnInfos();
    int[] index = new int[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      ColumnInfo colInfo = columns.get(i);
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(colInfo.getInternalName());
      ExprNodeDesc expr = new ExprNodeColumnDesc(colInfo);

      // backtrack can be null when input is script operator
      ExprNodeDesc exprBack = ExprNodeDescUtils.backtrack(expr, dummy, parent);
      if (exprBack != null) {
        if (ExprNodeDescUtils.isConstant(exprBack)) {
          int kindex = reduceKeysBack.indexOf(exprBack);
          if (kindex >= 0) {
            addJoinKeyToRowSchema(outputRR, index, i, colInfo, nm, nm2, kindex);
            continue;
          }
        } else {
          int startIdx = 0;
          int kindex;
          // joinKey may present multiple times, add the duplicates to the schema with different internal name.
          // example: KEY.reducesinkkey0, KEY.reducesinkkey1
          //      join        LU_CUSTOMER        a16
          //      on         (a15.CUSTOMER_ID = a16.CUSTOMER_ID and pa11.CUSTOMER_ID = a16.CUSTOMER_ID)
          while ((kindex = ExprNodeDescUtils.indexOf(exprBack, reduceKeysBack, startIdx)) >= 0) {
            addJoinKeyToRowSchema(outputRR, index, i, colInfo, nm, nm2, kindex);
            startIdx = kindex + 1;
          }
          if (startIdx > 0) {
            // at least one instance found
            continue;
          }
        }
      }
      index[i] = -reduceValues.size() - 1;
      String outputColName = getColumnInternalName(reduceValues.size());

      reduceValues.add(expr);

      ColumnInfo newColInfo = new ColumnInfo(colInfo);
      String internalColName = Utilities.ReduceField.VALUE + "." + outputColName;
      newColInfo.setInternalName(internalColName);
      newColInfo.setTabAlias(nm[0]);

      outputRR.put(nm[0], nm[1], newColInfo);
      if (nm2 != null) {
        outputRR.addMappingOnly(nm2[0], nm2[1], newColInfo);
      }
      outputColumns.add(outputColName);
    }
    dummy.setParentOperators(null);

    int numReds = -1;

    // Use only 1 reducer in case of cartesian product
    if (reduceKeys.size() == 0) {
      numReds = 1;
      String error = StrictChecks.checkCartesian(conf);
      if (error != null) {
        throw new SemanticException(error);
      }
    }

    ReduceSinkDesc rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys,
        reduceValues, outputColumns, false, tag,
        reduceKeys.size(), numReds, AcidUtils.Operation.NOT_ACID, defaultNullOrder);

    Map<String, String> translatorMap = new HashMap<String, String>();

    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    List<String> keyColNames = rsDesc.getOutputKeyColumnNames();
    for (int i = 0 ; i < keyColNames.size(); i++) {
      String oldName = keyColNames.get(i);
      String newName = Utilities.ReduceField.KEY + "." + oldName;
      colExprMap.put(newName, reduceKeys.get(i));
      translatorMap.put(oldName, newName);
    }
    List<String> valColNames = rsDesc.getOutputValueColumnNames();
    for (int i = 0 ; i < valColNames.size(); i++) {
      String oldName = valColNames.get(i);
      String newName = Utilities.ReduceField.VALUE + "." + oldName;
      colExprMap.put(newName, reduceValues.get(i));
      translatorMap.put(oldName, newName);
    }

    RowSchema defaultRs = new RowSchema(outputRR.getColumnInfos());

    List<ColumnInfo> newColumnInfos = new ArrayList<ColumnInfo>();
    for (ColumnInfo ci : outputRR.getColumnInfos()) {
      if (translatorMap.containsKey(ci.getInternalName())) {
        ci = new ColumnInfo(ci);
        ci.setInternalName(translatorMap.get(ci.getInternalName()));
      }
      newColumnInfos.add(ci);
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(rsDesc, new RowSchema(newColumnInfos), parent), outputRR);

    rsOp.setValueIndex(index);
    rsOp.setColumnExprMap(colExprMap);
    rsOp.setInputAliases(srcs);
    return rsOp;
  }

  private void addJoinKeyToRowSchema(
      RowResolver outputRR, int[] index, int i, ColumnInfo colInfo, String[] nm, String[] nm2, int kindex) {
    ColumnInfo newColInfo = new ColumnInfo(colInfo);
    String internalColName = ReduceField.KEY + ".reducesinkkey" + kindex;
    newColInfo.setInternalName(internalColName);
    newColInfo.setTabAlias(nm[0]);
    outputRR.put(nm[0], nm[1], newColInfo);
    if (nm2 != null) {
      outputRR.addMappingOnly(nm2[0], nm2[1], newColInfo);
    }
    index[i] = kindex;
  }

  private Operator genJoinOperator(QB qb, QBJoinTree joinTree,
                                   Map<String, Operator> map,
                                   Operator joiningOp) throws SemanticException {
    return genJoinOperator(qb, joinTree, map, joiningOp, false);
  }

  private Operator genJoinOperator(QB qb, QBJoinTree joinTree,
                                   Map<String, Operator> map,
                                   Operator joiningOp, boolean notInCheckPresent) throws SemanticException {
    QBJoinTree leftChild = joinTree.getJoinSrc();
    Operator joinSrcOp = joiningOp instanceof JoinOperator ? joiningOp : null;
    Operator OuterSrcOp = joiningOp;

    if (joinSrcOp == null && leftChild != null) {
      joinSrcOp = genJoinOperator(qb, leftChild, map, null);
    }

    if ( joinSrcOp != null ) {
      List<ASTNode> filter = joinTree.getFiltersForPushing().get(0);
      for (ASTNode cond : filter) {
        joinSrcOp = genFilterPlan(qb, cond, joinSrcOp, false);
      }
    }

    String[] baseSrc = joinTree.getBaseSrc();
    Operator[] srcOps = new Operator[baseSrc.length];

    Set<Integer> omitOpts = null; // set of input to the join that should be
    // omitted by the output
    int pos = 0;
    for (String src : baseSrc) {
      if (src != null) {
        Operator srcOp = map.get(src.toLowerCase());

        // for left-semi join, generate an additional selection & group-by
        // operator before ReduceSink
        List<ASTNode> fields = joinTree.getRHSSemijoinColumns(src);
        if (fields != null) {
          // the RHS table columns should be not be output from the join
          if (omitOpts == null) {
            omitOpts = new HashSet<Integer>();
          }
          omitOpts.add(pos);

          // generate a selection operator for group-by keys only
          srcOp = insertSelectForSemijoin(fields, srcOp);

          // generate a groupby operator (HASH mode) for a map-side partial
          // aggregation for semijoin
          srcOps[pos++] = genMapGroupByForSemijoin(fields, srcOp);
        } else {
          srcOps[pos++] = srcOp;
        }
      } else {
        assert pos == 0;
        srcOps[pos++] = joinSrcOp;
      }
    }

    ExprNodeDesc[][] joinKeys = genJoinKeys(joinTree, srcOps);

    for (int i = 0; i < srcOps.length; i++) {
      // generate a ReduceSink operator for the join
      String[] srcs = baseSrc[i] != null ? new String[] {baseSrc[i]} : joinTree.getLeftAliases();
      if (!isCBOExecuted()) {
        /*
         * The condition srcOps[i] == OuterSrcOp is used to make sure that the predicate for notnull check
         * is added only for the outer query table.outerqueryCol
         * even after the outer join condition
         */
        boolean outerNotInCheck = (notInCheckPresent && (srcOps[i] == OuterSrcOp));
        srcOps[i] = genNotNullFilterForJoinSourcePlan(qb, srcOps[i], joinTree, joinKeys[i], outerNotInCheck);
      }
      srcOps[i] = genJoinReduceSinkChild(joinKeys[i], srcOps[i], srcs, joinTree.getNextTag());
    }

    Operator<?> topOp = genJoinOperatorChildren(joinTree, joinSrcOp, srcOps, omitOpts, joinKeys);
    JoinOperator joinOp;
    if (topOp instanceof JoinOperator) {
      joinOp = (JoinOperator) topOp;
    } else {
      // We might generate a Select operator on top of the join operator for
      // semijoin
      joinOp = (JoinOperator) topOp.getParentOperators().get(0);
    }
    joinOp.getConf().setQBJoinTreeProps(joinTree);
    joinContext.put(joinOp, joinTree);

    if (joinTree.getPostJoinFilters().size() != 0) {
      assert joinTree.getNoOuterJoin();
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_PUSH_RESIDUAL_INNER)) {
        // Safety check for postconditions
        throw new SemanticException("Post-filtering conditions should have been added to the JOIN operator");
      }
      for(ASTNode condn : joinTree.getPostJoinFilters()) {
        topOp = genFilterPlan(qb, condn, topOp, false);
      }
    }

    return topOp;
  }

  /**
   * Construct a selection operator for semijoin that filter out all fields
   * other than the group by keys.
   *
   * @param fields
   *          list of fields need to be output
   * @param input
   *          input operator
   * @return the selection operator.
   * @throws SemanticException
   */
  private Operator insertSelectForSemijoin(List<ASTNode> fields,
                                           Operator<?> input) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRowResolver();
    List<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    List<String> outputColumnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    RowResolver outputRR = new RowResolver();

    // construct the list of columns that need to be projected
    for (int i = 0; i < fields.size(); ++i) {
      ASTNode field = fields.get(i);
      String[] nm;
      String[] nm2;
      ExprNodeDesc expr = genExprNodeDesc(field, inputRR);
      if (expr instanceof ExprNodeColumnDesc) {
        // In most of the cases, this is a column reference
        ExprNodeColumnDesc columnExpr = (ExprNodeColumnDesc) expr;
        nm = inputRR.reverseLookup(columnExpr.getColumn());
        nm2 = inputRR.getAlternateMappings(columnExpr.getColumn());
      } else if (expr instanceof ExprNodeConstantDesc) {
        // However, it can be a constant too. In that case, we need to track
        // the column that it originated from in the input operator so we can
        // propagate the aliases.
        ExprNodeConstantDesc constantExpr = (ExprNodeConstantDesc) expr;
        String inputCol = constantExpr.getFoldedFromCol();
        nm = inputRR.reverseLookup(inputCol);
        nm2 = inputRR.getAlternateMappings(inputCol);
      } else {
        // We might generate other types that are not recognized, e.g., a field reference
        // if it is a nested field, but since this is just an additional optimization,
        // we bail out without introducing the Select + GroupBy below the right input
        // of the left semijoin
        return input;
      }
      String colName = getColumnInternalName(i);
      outputColumnNames.add(colName);
      ColumnInfo colInfo = new ColumnInfo(colName, expr.getTypeInfo(), "", false);
      outputRR.put(nm[0], nm[1], colInfo);
      if (nm2 != null) {
        outputRR.addMappingOnly(nm2[0], nm2[1], colInfo);
      }
      colList.add(expr);
      colExprMap.put(colName, expr);
    }

    // create selection operator
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new SelectDesc(colList, outputColumnNames, false),
        new RowSchema(outputRR.getColumnInfos()), input), outputRR);

    output.setColumnExprMap(colExprMap);
    return output;
  }

  private Operator genMapGroupByForSemijoin(List<ASTNode> fields, Operator<?> input)
      throws SemanticException {

    RowResolver groupByInputRowResolver = opParseCtx.get(input).getRowResolver();
    RowResolver groupByOutputRowResolver = new RowResolver();
    List<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    List<String> outputColumnNames = new ArrayList<String>();
    List<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    for (int i = 0; i < fields.size(); ++i) {
      // get the group by keys to ColumnInfo
      ASTNode colName = fields.get(i);
      String[] nm;
      String[] nm2;
      ExprNodeDesc grpByExprNode = genExprNodeDesc(colName, groupByInputRowResolver);
      if (grpByExprNode instanceof ExprNodeColumnDesc) {
        // In most of the cases, this is a column reference
        ExprNodeColumnDesc columnExpr = (ExprNodeColumnDesc) grpByExprNode;
        nm = groupByInputRowResolver.reverseLookup(columnExpr.getColumn());
        nm2 = groupByInputRowResolver.getAlternateMappings(columnExpr.getColumn());
      } else if (grpByExprNode instanceof ExprNodeConstantDesc) {
        // However, it can be a constant too. In that case, we need to track
        // the column that it originated from in the input operator so we can
        // propagate the aliases.
        ExprNodeConstantDesc constantExpr = (ExprNodeConstantDesc) grpByExprNode;
        String inputCol = constantExpr.getFoldedFromCol();
        nm = groupByInputRowResolver.reverseLookup(inputCol);
        nm2 = groupByInputRowResolver.getAlternateMappings(inputCol);
      } else {
        // We might generate other types that are not recognized, e.g., a field reference
        // if it is a nested field, but since this is just an additional optimization,
        // we bail out without introducing the Select + GroupBy below the right input
        // of the left semijoin
        return input;
      }
      groupByKeys.add(grpByExprNode);
      // generate output column names
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      ColumnInfo colInfo2 = new ColumnInfo(field, grpByExprNode.getTypeInfo(),
          "", false);
      groupByOutputRowResolver.put(nm[0], nm[1], colInfo2);
      if (nm2 != null) {
        groupByOutputRowResolver.addMappingOnly(nm2[0], nm2[1], colInfo2);
      }
      groupByOutputRowResolver.putExpression(colName, colInfo2);
      // establish mapping from the output column to the input column
      colExprMap.put(field, grpByExprNode);
    }

    // Generate group-by operator
    float groupByMemoryUsage = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MEMORY);
    float memoryThreshold = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_MEMORY_THRESHOLD);
    float minReductionHashAggr = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION);
    float minReductionHashAggrLowerBound = HiveConf
            .getFloatVar(conf, ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION_LOWER_BOUND);
    float hashAggrFlushPercent = HiveConf
        .getFloatVar(conf, ConfVars.HIVE_MAP_AGGR_HASH_FLUSH_SIZE_PERCENT);
    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(GroupByDesc.Mode.HASH, outputColumnNames, groupByKeys, aggregations,
            false, groupByMemoryUsage, memoryThreshold, minReductionHashAggr, minReductionHashAggrLowerBound,
                hashAggrFlushPercent, null, false, -1, false),
        new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        input), groupByOutputRowResolver);

    op.setColumnExprMap(colExprMap);
    return op;
  }

  private ExprNodeDesc[][] genJoinOperatorTypeCheck(ExprNodeDesc[][] keys)
      throws SemanticException {
    // keys[i] -> ArrayList<exprNodeDesc> for the i-th join operator key list
    int keyLength = 0;
    for (int i = 0; i < keys.length; i++) {
      if (i == 0) {
        keyLength = keys[i].length;
      } else {
        assert keyLength == keys[i].length;
      }
    }
    // implicit type conversion hierarchy
    for (int k = 0; k < keyLength; k++) {
      // Find the common class for type conversion
      TypeInfo commonType = keys[0][k].getTypeInfo();
      for (int i = 1; i < keys.length; i++) {
        TypeInfo a = commonType;
        TypeInfo b = keys[i][k].getTypeInfo();
        commonType = FunctionRegistry.getCommonClassForComparison(a, b);
        if (commonType == null) {
          throw new SemanticException(
              "Cannot do equality join on different types: " + a.getTypeName()
                  + " and " + b.getTypeName());
        }
      }
      // Add implicit type conversion if necessary
      for (int i = 0; i < keys.length; i++) {
        if (TypeInfoUtils.isConversionRequiredForComparison(
            keys[i][k].getTypeInfo(), commonType)) {
          keys[i][k] = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
              .createConversionCast(keys[i][k], (PrimitiveTypeInfo)commonType);
        } else {
          // For the case no implicit type conversion, e.g., varchar(5) and varchar(10),
          // pick the common type for all the keys since during run-time, same key type is assumed.
          keys[i][k].setTypeInfo(commonType);
        }
      }
    }
    return keys;
  }

  private Operator genJoinPlan(QB qb, Map<String, Operator> map)
      throws SemanticException {
    QBJoinTree joinTree = qb.getQbJoinTree();
    return genJoinOperator(qb, joinTree, map, null);
  }

  /**
   * Extract the filters from the join condition and push them on top of the
   * source operators. This procedure traverses the query tree recursively,
   */
  private void pushJoinFilters(QB qb, QBJoinTree joinTree,
                               Map<String, Operator> map) throws SemanticException {
    pushJoinFilters(qb, joinTree, map, true);
  }

  /**
   * Extract the filters from the join condition and push them on top of the
   * source operators. This procedure traverses the query tree recursively,
   */
  private void pushJoinFilters(QB qb, QBJoinTree joinTree,
                               Map<String, Operator> map,
                               boolean recursively) throws SemanticException {
    if ( recursively ) {
      if (joinTree.getJoinSrc() != null) {
        pushJoinFilters(qb, joinTree.getJoinSrc(), map);
      }
    }
    List<List<ASTNode>> filters = joinTree.getFiltersForPushing();
    int pos = 0;
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        Operator srcOp = map.get(src);
        List<ASTNode> filter = filters.get(pos);
        for (ASTNode cond : filter) {
          srcOp = genFilterPlan(qb, cond, srcOp, false);
        }
        map.put(src, srcOp);
      }
      pos++;
    }
  }

  private List<String> getMapSideJoinTables(QB qb) {
    List<String> cols = new ArrayList<String>();


    ASTNode hints = qb.getParseInfo().getHints();
    for (int pos = 0; pos < hints.getChildCount(); pos++) {
      ASTNode hint = (ASTNode) hints.getChild(pos);
      if (((ASTNode) hint.getChild(0)).getToken().getType() == HintParser.TOK_MAPJOIN) {
        // the user has specified to ignore mapjoin hint
        if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_IGNORE_MAPJOIN_HINT)
            && !conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
          ASTNode hintTblNames = (ASTNode) hint.getChild(1);
          int numCh = hintTblNames.getChildCount();
          for (int tblPos = 0; tblPos < numCh; tblPos++) {
            String tblName = ((ASTNode) hintTblNames.getChild(tblPos)).getText()
                .toLowerCase();
            if (!cols.contains(tblName)) {
              cols.add(tblName);
            }
          }
        }
        else {
          queryProperties.setMapJoinRemoved(true);
        }
      }
    }

    return cols;
  }

  // The join alias is modified before being inserted for consumption by sort-merge
  // join queries. If the join is part of a sub-query the alias is modified to include
  // the sub-query alias.
  private String getModifiedAlias(QB qb, String alias) {
    return QB.getAppendedAliasFromId(qb.getId(), alias);
  }

  private QBJoinTree genUniqueJoinTree(QB qb, ASTNode joinParseTree,
                                       Map<String, Operator> aliasToOpInfo)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    joinTree.setNoOuterJoin(false);

    joinTree.setExpressions(new ArrayList<List<ASTNode>>());
    joinTree.setFilters(new ArrayList<List<ASTNode>>());
    joinTree.setFiltersForPushing(new ArrayList<List<ASTNode>>());

    // Create joinTree structures to fill them up later
    List<String> rightAliases = new ArrayList<String>();
    List<String> leftAliases = new ArrayList<String>();
    List<String> baseSrc = new ArrayList<String>();
    List<Boolean> preserved = new ArrayList<Boolean>();

    boolean lastPreserved = false;
    int cols = -1;

    for (int i = 0; i < joinParseTree.getChildCount(); i++) {
      ASTNode child = (ASTNode) joinParseTree.getChild(i);

      switch (child.getToken().getType()) {
      case HiveParser.TOK_TABREF:
        // Handle a table - populate aliases appropriately:
        // leftAliases should contain the first table, rightAliases should
        // contain all other tables and baseSrc should contain all tables

        String tableName = getUnescapedUnqualifiedTableName((ASTNode) child.getChild(0));

        String alias = child.getChildCount() == 1 ? tableName
            : unescapeIdentifier(child.getChild(child.getChildCount() - 1)
            .getText().toLowerCase());

        if (i == 0) {
          leftAliases.add(alias);
          joinTree.setLeftAlias(alias);
        } else {
          rightAliases.add(alias);
        }
        joinTree.getAliasToOpInfo().put(getModifiedAlias(qb, alias), aliasToOpInfo.get(alias));
        joinTree.setId(qb.getId());
        baseSrc.add(alias);

        preserved.add(lastPreserved);
        lastPreserved = false;
        break;

      case HiveParser.TOK_EXPLIST:
        if (cols == -1 && child.getChildCount() != 0) {
          cols = child.getChildCount();
        } else if (child.getChildCount() != cols) {
          throw new SemanticException("Tables with different or invalid "
              + "number of keys in UNIQUEJOIN");
        }

        List<ASTNode> expressions = new ArrayList<ASTNode>();
        List<ASTNode> filt = new ArrayList<ASTNode>();
        List<ASTNode> filters = new ArrayList<ASTNode>();

        for (Node exp : child.getChildren()) {
          expressions.add((ASTNode) exp);
        }

        joinTree.getExpressions().add(expressions);
        joinTree.getFilters().add(filt);
        joinTree.getFiltersForPushing().add(filters);
        break;

      case HiveParser.KW_PRESERVE:
        lastPreserved = true;
        break;

      case HiveParser.TOK_SUBQUERY:
        throw new SemanticException(
            "Subqueries are not supported in UNIQUEJOIN");

      default:
        throw new SemanticException("Unexpected UNIQUEJOIN structure");
      }
    }

    joinTree.setBaseSrc(baseSrc.toArray(new String[0]));
    joinTree.setLeftAliases(leftAliases.toArray(new String[0]));
    joinTree.setRightAliases(rightAliases.toArray(new String[0]));

    JoinCond[] condn = new JoinCond[preserved.size()];
    for (int i = 0; i < condn.length; i++) {
      condn[i] = new JoinCond(preserved.get(i));
    }
    joinTree.setJoinCond(condn);

    if ((qb.getParseInfo().getHints() != null)
        && !(conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez"))) {
      LOG.info("STREAMTABLE hint honored.");
      parseStreamTables(joinTree, qb);
    }
    return joinTree;
  }

  /*
   * Setup a QBJoinTree between a SubQuery and its Parent Query. The Parent Query
   * is the lhs of the Join.
   *
   * The Parent Query is represented by the last Operator needed to process its From Clause.
   * In case of a single table Query this will be a TableScan, but it can be a Join Operator
   * if the Parent Query contains Join clauses, or in case of a single source from clause,
   * the source could be a SubQuery or a PTF invocation.
   *
   * We setup the QBJoinTree with the above constrains in place. So:
   * - the lhs of the QBJoinTree can be a another QBJoinTree if the Parent Query operator
   *   is a JoinOperator. In this case we get its QBJoinTree from the 'joinContext'
   * - the rhs is always a reference to the SubQuery. Its alias is obtained from the
   *   QBSubQuery object.
   *
   * The QBSubQuery also provides the Joining Condition AST. The Joining Condition has been
   * transformed in QBSubQuery setup, before this call. The Joining condition has any correlated
   * predicates and a predicate for joining the Parent Query expression with the SubQuery.
   *
   * The QBSubQuery also specifies what kind of Join to construct.
   *
   * Given this information, once we initialize the QBJoinTree, we call the 'parseJoinCondition'
   * method to validate and parse Join conditions.
   */
  private QBJoinTree genSQJoinTree(QB qb, ISubQueryJoinInfo subQuery,
                                   Operator joiningOp,
                                   Map<String, Operator> aliasToOpInfo)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    JoinCond[] condn = new JoinCond[1];

    switch (subQuery.getJoinType()) {
    case LEFTOUTER:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTOUTER);
      break;
    case RIGHTOUTER:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.RIGHTOUTER);
      break;
    case FULLOUTER:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.FULLOUTER);
      break;
    case LEFTSEMI:
      joinTree.setNoSemiJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTSEMI);
      break;
    case ANTI:
      joinTree.setNoSemiJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.ANTI);
      break;
    default:
      condn[0] = new JoinCond(0, 1, JoinType.INNER);
      joinTree.setNoOuterJoin(true);
      break;
    }
    joinTree.setJoinCond(condn);

    if ( joiningOp instanceof JoinOperator ) {
      QBJoinTree leftTree = joinContext.get(joiningOp);
      joinTree.setJoinSrc(leftTree);
      String[] leftChildAliases = leftTree.getLeftAliases();
      String leftAliases[] = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++) {
        leftAliases[i] = leftChildAliases[i];
      }
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      joinTree.setLeftAliases(leftAliases);
    } else {
      String alias = unescapeIdentifier(
          SubQueryUtils.getAlias(joiningOp, aliasToOpInfo).toLowerCase());
      joinTree.setLeftAlias(alias);
      String[] leftAliases = new String[1];
      leftAliases[0] = alias;
      joinTree.setLeftAliases(leftAliases);
      String[] children = new String[2];
      children[0] = alias;
      joinTree.setBaseSrc(children);
      joinTree.setId(qb.getId());
      joinTree.getAliasToOpInfo().put(
          getModifiedAlias(qb, alias), aliasToOpInfo.get(alias));
    }

    String rightalias = unescapeIdentifier(subQuery.getAlias().toLowerCase());
    String[] rightAliases = new String[1];
    rightAliases[0] = rightalias;
    joinTree.setRightAliases(rightAliases);
    String[] children = joinTree.getBaseSrc();
    if (children == null) {
      children = new String[2];
    }
    children[1] = rightalias;
    joinTree.setBaseSrc(children);
    joinTree.setId(qb.getId());
    joinTree.getAliasToOpInfo().put(
        getModifiedAlias(qb, rightalias), aliasToOpInfo.get(rightalias));
    // remember rhs table for semijoin
    if (!joinTree.getNoSemiJoin()) {
      joinTree.addRHSSemijoin(rightalias);
    }

    List<List<ASTNode>> expressions = new ArrayList<List<ASTNode>>();
    expressions.add(new ArrayList<ASTNode>());
    expressions.add(new ArrayList<ASTNode>());
    joinTree.setExpressions(expressions);

    List<Boolean> nullsafes = new ArrayList<Boolean>();
    joinTree.setNullSafes(nullsafes);

    List<List<ASTNode>> filters = new ArrayList<List<ASTNode>>();
    filters.add(new ArrayList<ASTNode>());
    filters.add(new ArrayList<ASTNode>());
    joinTree.setFilters(filters);
    joinTree.setFilterMap(new int[2][]);

    List<List<ASTNode>> filtersForPushing = new ArrayList<List<ASTNode>>();
    filtersForPushing.add(new ArrayList<ASTNode>());
    filtersForPushing.add(new ArrayList<ASTNode>());
    joinTree.setFiltersForPushing(filtersForPushing);

    ASTNode joinCond = subQuery.getJoinConditionAST();
    List<String> leftSrc = new ArrayList<String>();
    parseJoinCondition(joinTree, joinCond, leftSrc, aliasToOpInfo);
    if (leftSrc.size() == 1) {
      joinTree.setLeftAlias(leftSrc.get(0));
    }

    return joinTree;
  }

  private QBJoinTree genJoinTree(QB qb, ASTNode joinParseTree,
                                 Map<String, Operator> aliasToOpInfo)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    JoinCond[] condn = new JoinCond[1];

    switch (joinParseTree.getToken().getType()) {
    case HiveParser.TOK_LEFTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTOUTER);
      break;
    case HiveParser.TOK_RIGHTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.RIGHTOUTER);
      break;
    case HiveParser.TOK_FULLOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.FULLOUTER);
      break;
    case HiveParser.TOK_LEFTSEMIJOIN:
      joinTree.setNoSemiJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTSEMI);
      break;
    case HiveParser.TOK_LEFTANTISEMIJOIN:
      joinTree.setNoSemiJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.ANTI);
      break;
    default:
      condn[0] = new JoinCond(0, 1, JoinType.INNER);
      joinTree.setNoOuterJoin(true);
      break;
    }

    joinTree.setJoinCond(condn);

    ASTNode left = (ASTNode) joinParseTree.getChild(0);
    ASTNode right = (ASTNode) joinParseTree.getChild(1);
    if (joinParseTree.getChildren().size() >= 4) {
      addPkFkInfo(joinTree, (ASTNode) joinParseTree.getChild(3));
    }

    boolean isValidLeftToken = isValidJoinSide(left);
    boolean isJoinLeftToken = !isValidLeftToken && isJoinToken(left);
    boolean isValidRightToken = isValidJoinSide(right);
    boolean isJoinRightToken = !isValidRightToken && isJoinToken(right);
    // TODO: if we didn't care about the column order, we could switch join sides here
    //       for TOK_JOIN and TOK_FULLOUTERJOIN.
    if (!isValidLeftToken && !isJoinLeftToken) {
      throw new SemanticException("Invalid token on the left side of the join: "
          + left.getToken().getText() + "; please rewrite your query");
    } else if (!isValidRightToken) {
      String advice= "";
      if (isJoinRightToken && !isJoinLeftToken) {
        advice = "; for example, put the nested join on the left side, or nest joins differently";
      } else if (isJoinRightToken) {
        advice = "; for example, nest joins differently";
      }
      throw new SemanticException("Invalid token on the right side of the join: "
          + right.getToken().getText() + "; please rewrite your query" + advice);
    }

    if (isValidLeftToken) {
      String alias = extractJoinAlias(left);
      joinTree.setLeftAlias(alias);
      String[] leftAliases = new String[1];
      leftAliases[0] = alias;
      joinTree.setLeftAliases(leftAliases);
      String[] children = new String[2];
      children[0] = alias;
      joinTree.setBaseSrc(children);
      joinTree.setId(qb.getId());
      joinTree.getAliasToOpInfo().put(
          getModifiedAlias(qb, alias), aliasToOpInfo.get(alias));
    } else if (isJoinLeftToken) {
      QBJoinTree leftTree = genJoinTree(qb, left, aliasToOpInfo);
      joinTree.setJoinSrc(leftTree);
      String[] leftChildAliases = leftTree.getLeftAliases();
      String leftAliases[] = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++) {
        leftAliases[i] = leftChildAliases[i];
      }
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      joinTree.setLeftAliases(leftAliases);
    } else {
      assert (false);
    }

    if (isValidRightToken) {
      String alias = extractJoinAlias(right);
      String[] rightAliases = new String[1];
      rightAliases[0] = alias;
      joinTree.setRightAliases(rightAliases);
      String[] children = joinTree.getBaseSrc();
      if (children == null) {
        children = new String[2];
      }
      children[1] = alias;
      joinTree.setBaseSrc(children);
      joinTree.setId(qb.getId());
      joinTree.getAliasToOpInfo().put(
          getModifiedAlias(qb, alias), aliasToOpInfo.get(alias));
      // remember rhs table for semijoin
      if (!joinTree.getNoSemiJoin()) {
        joinTree.addRHSSemijoin(alias);
      }
    } else {
      assert false;
    }

    List<List<ASTNode>> expressions = new ArrayList<List<ASTNode>>();
    expressions.add(new ArrayList<ASTNode>());
    expressions.add(new ArrayList<ASTNode>());
    joinTree.setExpressions(expressions);

    List<Boolean> nullsafes = new ArrayList<Boolean>();
    joinTree.setNullSafes(nullsafes);

    List<List<ASTNode>> filters = new ArrayList<List<ASTNode>>();
    filters.add(new ArrayList<ASTNode>());
    filters.add(new ArrayList<ASTNode>());
    joinTree.setFilters(filters);
    joinTree.setFilterMap(new int[2][]);

    List<List<ASTNode>> filtersForPushing = new ArrayList<List<ASTNode>>();
    filtersForPushing.add(new ArrayList<ASTNode>());
    filtersForPushing.add(new ArrayList<ASTNode>());
    joinTree.setFiltersForPushing(filtersForPushing);

    ASTNode joinCond = (ASTNode) joinParseTree.getChild(2);
    List<String> leftSrc = new ArrayList<String>();
    parseJoinCondition(joinTree, joinCond, leftSrc, aliasToOpInfo);
    if (leftSrc.size() == 1) {
      joinTree.setLeftAlias(leftSrc.get(0));
    }

    // check the hints to see if the user has specified a map-side join. This
    // will be removed later on, once the cost-based
    // infrastructure is in place
    if (qb.getParseInfo().getHints() != null) {
      List<String> mapSideTables = getMapSideJoinTables(qb);
      List<String> mapAliases = joinTree.getMapAliases();

      for (String mapTbl : mapSideTables) {
        boolean mapTable = false;
        for (String leftAlias : joinTree.getLeftAliases()) {
          if (mapTbl.equalsIgnoreCase(leftAlias)) {
            mapTable = true;
          }
        }
        for (String rightAlias : joinTree.getRightAliases()) {
          if (mapTbl.equalsIgnoreCase(rightAlias)) {
            mapTable = true;
          }
        }

        if (mapTable) {
          if (mapAliases == null) {
            mapAliases = new ArrayList<String>();
          }
          mapAliases.add(mapTbl);
          joinTree.setMapSideJoin(true);
        }
      }

      joinTree.setMapAliases(mapAliases);

      if (!(conf.getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("tez"))) {
        parseStreamTables(joinTree, qb);
      }
    }

    return joinTree;
  }

  private void addPkFkInfo(QBJoinTree joinTree, ASTNode hints) {
    if (hints.getToken().getType() == HintParser.TOK_HINTLIST) {
      Tree hint = hints.getChild(0);
      if (hint.getType() == HintParser.TOK_HINT && hint.getChild(0).getType() == HintParser.TOK_PKFK_JOIN) {
        Tree args = hint.getChild(1);
        joinTree.setFkJoinTableIndex(Integer.parseInt(args.getChild(0).getText()));
        joinTree.setNonFkSideIsFiltered(NON_FK_FILTERED.equals(args.getChild(1).getText()));
      }
    }
  }

  private boolean isValidJoinSide(ASTNode right) {
    return (right.getToken().getType() == HiveParser.TOK_TABREF)
        || (right.getToken().getType() == HiveParser.TOK_SUBQUERY)
        || (right.getToken().getType() == HiveParser.TOK_PTBLFUNCTION);
  }

  private String extractJoinAlias(ASTNode node) throws SemanticException {
    // ptf node form is:
    // ^(TOK_PTBLFUNCTION $name $alias? partitionTableFunctionSource partitioningSpec? expression*)
    // guaranteed to have an alias here: check done in processJoin
    if (node.getType() == HiveParser.TOK_PTBLFUNCTION) {
      return unescapeIdentifier(node.getChild(1).getText().toLowerCase());
    }
    if (node.getChildCount() == 1) {
      return getUnescapedUnqualifiedTableName((ASTNode) node.getChild(0)).toLowerCase();
    }
    for (int i = node.getChildCount() - 1; i >= 1; i--) {
      if (node.getChild(i).getType() == HiveParser.Identifier) {
        return unescapeIdentifier(node.getChild(i).getText().toLowerCase());
      }
    }
    throw new SemanticException("Unable to get join alias.");
  }

  private void parseStreamTables(QBJoinTree joinTree, QB qb) {
    List<String> streamAliases = joinTree.getStreamAliases();

    for (Node hintNode : qb.getParseInfo().getHints().getChildren()) {
      ASTNode hint = (ASTNode) hintNode;
      if (hint.getChild(0).getType() == HintParser.TOK_STREAMTABLE) {
        for (int i = 0; i < hint.getChild(1).getChildCount(); i++) {
          if (streamAliases == null) {
            streamAliases = new ArrayList<String>();
          }
          streamAliases.add(hint.getChild(1).getChild(i).getText());
        }
      }
    }

    joinTree.setStreamAliases(streamAliases);
  }

  /** Parses semjoin hints in the query and returns the table names mapped to filter size, or -1 if not specified.
   *  Hints can be in 2 formats
   *  1. TableName, ColumnName, Target-TableName, bloom filter entries
   *  2. TableName, ColumnName, Target-TableName
   *  */
  private Map<String, List<SemiJoinHint>> parseSemiJoinHint(List<ASTNode> hints) throws SemanticException {
    if (hints == null || hints.size() == 0) {
      return null;
    }
    Map<String, List<SemiJoinHint>> result = null;
    for (ASTNode hintNode : hints) {
      for (Node node : hintNode.getChildren()) {
        ASTNode hint = (ASTNode) node;
        if (hint.getChild(0).getType() != HintParser.TOK_LEFTSEMIJOIN &&
                hint.getChild(0).getType() != HintParser.TOK_LEFTANTISEMIJOIN) {
          continue;
        }
        if (result == null) {
          result = new HashMap<>();
        }
        Tree args = hint.getChild(1);
        if (args.getChildCount() == 1) {
          String text = args.getChild(0).getText();
          if (text.equalsIgnoreCase("None")) {
            // Hint to disable runtime filtering.
            return result;
          }
        }
        int curIdx = 0;
        while(curIdx < args.getChildCount()) {
          curIdx = parseSingleSemiJoinHint(args, curIdx, result);
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Semijoin hint parsed: " + result);
    }
    return result;
  }

  private int parseSingleSemiJoinHint(Tree args, int curIdx, Map<String, List<SemiJoinHint>> result)
      throws SemanticException {
    // Check if there are enough entries in the tree to constitute a hint.
    int numEntriesLeft = args.getChildCount() - curIdx;
    if (numEntriesLeft < 3) {
      throw new SemanticException("User provided only 1 entry for the hint with alias "
          + args.getChild(curIdx).getText());
    }

    String source = args.getChild(curIdx++).getText();
    // validate
    if (StringUtils.isNumeric(source)) {
      throw new SemanticException("User provided bloom filter entries when source alias is "
          + "expected. source:" + source);
    }

    String colName = args.getChild(curIdx++).getText();
    // validate
    if (StringUtils.isNumeric(colName)) {
      throw new SemanticException("User provided bloom filter entries when column name is "
          + "expected. colName:" + colName);
    }

    String target = args.getChild(curIdx++).getText();
    // validate
    if (StringUtils.isNumeric(target)) {
      throw new SemanticException("User provided bloom filter entries when target alias is "
          + "expected. target: " + target);
    }

    Integer number = null;
    if (numEntriesLeft > 3) {
      // Check if there exists bloom filter size entry
      try {
        number = Integer.parseInt(args.getChild(curIdx).getText());
        curIdx++;
      } catch (NumberFormatException e) { // Ignore
        LOG.warn("Number format exception when parsing " + number, e);
      }
    }
    result.computeIfAbsent(source, value -> new ArrayList<>()).add(new SemiJoinHint(colName, target, number));
    return curIdx;
  }

  /**
   * disableMapJoinWithHint
   * @param hints
   * @return true if hint to disable hint is provided, else false
   */
  private boolean disableMapJoinWithHint(List<ASTNode> hints) {
    if (hints == null || hints.size() == 0) {
      return false;
    }
    for (ASTNode hintNode : hints) {
      for (Node node : hintNode.getChildren()) {
        ASTNode hint = (ASTNode) node;
        if (hint.getChild(0).getType() != HintParser.TOK_MAPJOIN) {
          continue;
        }
        Tree args = hint.getChild(1);
        if (args.getChildCount() == 1) {
          String text = args.getChild(0).getText();
          if (text.equalsIgnoreCase("None")) {
            // Hint to disable mapjoin.
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Merges node to target
   */
  private void mergeJoins(QBJoinTree node, QBJoinTree target, int pos, int[] tgtToNodeExprMap) {
    String[] nodeRightAliases = node.getRightAliases();
    String[] trgtRightAliases = target.getRightAliases();
    String[] rightAliases = new String[nodeRightAliases.length + trgtRightAliases.length];

    for (int i = 0; i < trgtRightAliases.length; i++) {
      rightAliases[i] = trgtRightAliases[i];
    }
    for (int i = 0; i < nodeRightAliases.length; i++) {
      rightAliases[i + trgtRightAliases.length] = nodeRightAliases[i];
    }
    target.setRightAliases(rightAliases);
    target.getAliasToOpInfo().putAll(node.getAliasToOpInfo());

    String[] nodeBaseSrc = node.getBaseSrc();
    String[] trgtBaseSrc = target.getBaseSrc();
    String[] baseSrc = new String[nodeBaseSrc.length + trgtBaseSrc.length - 1];

    for (int i = 0; i < trgtBaseSrc.length; i++) {
      baseSrc[i] = trgtBaseSrc[i];
    }
    for (int i = 1; i < nodeBaseSrc.length; i++) {
      baseSrc[i + trgtBaseSrc.length - 1] = nodeBaseSrc[i];
    }
    target.setBaseSrc(baseSrc);

    List<List<ASTNode>> expr = target.getExpressions();
    for (int i = 0; i < nodeRightAliases.length; i++) {
      List<ASTNode> nodeConds = node.getExpressions().get(i + 1);
      List<ASTNode> reordereNodeConds = new ArrayList<ASTNode>();
      for(int k=0; k < tgtToNodeExprMap.length; k++) {
        reordereNodeConds.add(nodeConds.get(tgtToNodeExprMap[k]));
      }
      expr.add(reordereNodeConds);
    }

    List<Boolean> nns = node.getNullSafes();
    List<Boolean> tns = target.getNullSafes();
    for (int i = 0; i < tns.size(); i++) {
      tns.set(i, tns.get(i) & nns.get(i)); // any of condition contains non-NS, non-NS
    }

    List<List<ASTNode>> filters = target.getFilters();
    for (int i = 0; i < nodeRightAliases.length; i++) {
      filters.add(node.getFilters().get(i + 1));
    }

    if (node.getFilters().get(0).size() != 0) {
      List<ASTNode> filterPos = filters.get(pos);
      filterPos.addAll(node.getFilters().get(0));
    }

    int[][] nmap = node.getFilterMap();
    int[][] tmap = target.getFilterMap();
    int[][] newmap = new int[tmap.length + nmap.length - 1][];

    for (int[] mapping : nmap) {
      if (mapping != null) {
        for (int i = 0; i < mapping.length; i += 2) {
          if (pos > 0 || mapping[i] > 0) {
            mapping[i] += trgtRightAliases.length;
          }
        }
      }
    }
    if (nmap[0] != null) {
      if (tmap[pos] == null) {
        tmap[pos] = nmap[0];
      } else {
        int[] appended = new int[tmap[pos].length + nmap[0].length];
        System.arraycopy(tmap[pos], 0, appended, 0, tmap[pos].length);
        System.arraycopy(nmap[0], 0, appended, tmap[pos].length, nmap[0].length);
        tmap[pos] = appended;
      }
    }
    System.arraycopy(tmap, 0, newmap, 0, tmap.length);
    System.arraycopy(nmap, 1, newmap, tmap.length, nmap.length - 1);
    target.setFilterMap(newmap);

    List<List<ASTNode>> filter = target.getFiltersForPushing();
    for (int i = 0; i < nodeRightAliases.length; i++) {
      filter.add(node.getFiltersForPushing().get(i + 1));
    }

    if (node.getFiltersForPushing().get(0).size() != 0) {
      /*
       * for each predicate:
       * - does it refer to one or many aliases
       * - if one: add it to the filterForPushing list of that alias
       * - if many: add as a filter from merging trees.
       */

      for(ASTNode nodeFilter : node.getFiltersForPushing().get(0) ) {
        int fPos = ParseUtils.checkJoinFilterRefersOneAlias(target.getBaseSrc(), nodeFilter);

        if ( fPos != - 1 ) {
          filter.get(fPos).add(nodeFilter);
        } else {
          target.addPostJoinFilter(nodeFilter);
        }
      }
    }

    target.setNoOuterJoin(node.getNoOuterJoin() && target.getNoOuterJoin());
    target.setNoSemiJoin(node.getNoSemiJoin() && target.getNoSemiJoin());

    target.mergeRHSSemijoin(node);

    JoinCond[] nodeCondns = node.getJoinCond();
    int nodeCondnsSize = nodeCondns.length;
    JoinCond[] targetCondns = target.getJoinCond();
    int targetCondnsSize = targetCondns.length;
    JoinCond[] newCondns = new JoinCond[nodeCondnsSize + targetCondnsSize];
    for (int i = 0; i < targetCondnsSize; i++) {
      newCondns[i] = targetCondns[i];
    }

    for (int i = 0; i < nodeCondnsSize; i++) {
      JoinCond nodeCondn = nodeCondns[i];
      if (nodeCondn.getLeft() == 0) {
        nodeCondn.setLeft(pos);
      } else {
        nodeCondn.setLeft(nodeCondn.getLeft() + targetCondnsSize);
      }
      nodeCondn.setRight(nodeCondn.getRight() + targetCondnsSize);
      newCondns[targetCondnsSize + i] = nodeCondn;
    }

    target.setJoinCond(newCondns);
    if (target.isMapSideJoin()) {
      assert node.isMapSideJoin();
      List<String> mapAliases = target.getMapAliases();
      for (String mapTbl : node.getMapAliases()) {
        if (!mapAliases.contains(mapTbl)) {
          mapAliases.add(mapTbl);
        }
      }
      target.setMapAliases(mapAliases);
    }

    if (node.getPostJoinFilters().size() != 0) {
      // Safety check: if we are merging join operators and there are post-filtering
      // conditions, they cannot be outer joins
      assert node.getNoOuterJoin() ;
      assert target.getPostJoinFilters().size() == 0 || target.getNoOuterJoin();
      for (ASTNode exprPostFilter : node.getPostJoinFilters()) {
        target.addPostJoinFilter(exprPostFilter);
      }
    }
  }

  private Pair<Integer, int[]> findMergePos(QBJoinTree node, QBJoinTree target) {
    int res = -1;
    String leftAlias = node.getLeftAlias();
    if (leftAlias == null && (!node.getNoOuterJoin() || !target.getNoOuterJoin())) {
      // Cross with outer join: currently we do not merge
      return Pair.of(-1, null);
    }

    List<ASTNode> nodeCondn = node.getExpressions().get(0);
    List<ASTNode> targetCondn = null;

    if (leftAlias == null || leftAlias.equals(target.getLeftAlias())) {
      targetCondn = target.getExpressions().get(0);
      res = 0;
    } else {
      for (int i = 0; i < target.getRightAliases().length; i++) {
        if (leftAlias.equals(target.getRightAliases()[i])) {
          targetCondn = target.getExpressions().get(i + 1);
          res = i + 1;
          break;
        }
      }
    }

    if ( targetCondn == null || (nodeCondn.size() != targetCondn.size())) {
      return Pair.of(-1, null);
    }

    /*
     * The order of the join condition expressions don't matter.
     * A merge can happen:
     * - if every target condition is present in some position of the node condition list.
     * - there is no node condition, which is not equal to any target condition.
     */

    int[] tgtToNodeExprMap = new int[targetCondn.size()];
    boolean[] nodeFiltersMapped = new boolean[nodeCondn.size()];
    int i, j;
    for(i=0; i<targetCondn.size(); i++) {
      String tgtExprTree = targetCondn.get(i).toStringTree();
      tgtToNodeExprMap[i] = -1;
      for(j=0; j < nodeCondn.size(); j++) {
        if ( nodeCondn.get(j).toStringTree().equals(tgtExprTree)) {
          tgtToNodeExprMap[i] = j;
          nodeFiltersMapped[j] = true;
        }
      }
      if ( tgtToNodeExprMap[i] == -1) {
        return Pair.of(-1, null);
      }
    }

    for(j=0; j < nodeCondn.size(); j++) {
      if ( !nodeFiltersMapped[j]) {
        return Pair.of(-1, null);
      }
    }

    return Pair.of(res, tgtToNodeExprMap);
  }

  boolean isCBOExecuted() {
    return false;
  }

  boolean isCBOSupportedLateralView(ASTNode lateralView) {
    return false;
  }

  boolean continueJoinMerge() {
    return true;
  }

  private boolean shouldMerge(final QBJoinTree node, final QBJoinTree target) {
    boolean isNodeOuterJoin=false, isNodeSemiJoin=false, hasNodePostJoinFilters=false;
    boolean isTargetOuterJoin=false, isTargetSemiJoin=false, hasTargetPostJoinFilters=false;

    isNodeOuterJoin = !node.getNoOuterJoin();
    isNodeSemiJoin= !node.getNoSemiJoin();
    hasNodePostJoinFilters = node.getPostJoinFilters().size() !=0;

    isTargetOuterJoin = !target.getNoOuterJoin();
    isTargetSemiJoin= !target.getNoSemiJoin();
    hasTargetPostJoinFilters = target.getPostJoinFilters().size() !=0;

    if (hasNodePostJoinFilters || hasTargetPostJoinFilters) {
      if (isNodeOuterJoin || isNodeSemiJoin  || isTargetOuterJoin || isTargetSemiJoin) {
        return false;
      }
    }
    return true;
  }

  // try merge join tree from inner most source
  // (it was merged from outer most to inner, which could be invalid)
  //
  // in a join tree ((A-B)-C)-D where C is not mergeable with A-B,
  // D can be merged with A-B into single join If and only if C and D has same join type
  // In this case, A-B-D join will be executed first and ABD-C join will be executed in next
  private void mergeJoinTree(QB qb) {
    QBJoinTree tree = qb.getQbJoinTree();
    if (tree.getJoinSrc() == null) {
      return;
    }

    // make array with QBJoinTree : outer most(0) --> inner most(n)
    List<QBJoinTree> trees = new ArrayList<QBJoinTree>();
    for (;tree != null; tree = tree.getJoinSrc()) {
      trees.add(tree);
    }

    // merging from 'target'(inner) to 'node'(outer)
    boolean mergedQBJTree = false;
    for (int i = trees.size() - 1; i >= 0; i--) {
      QBJoinTree target = trees.get(i);
      if (target == null) {
        continue;
      }
      JoinType prevType = null;   // save join type
      boolean continueScanning = true;
      for (int j = i - 1; j >= 0 && continueScanning; j--) {
        QBJoinTree node = trees.get(j);
        if (node == null) {
          continue;
        }
        JoinType currType = getType(node.getJoinCond());
        if (prevType != null && prevType != currType) {
          break;
        }
        if(!shouldMerge(node, target)) {
          // Outer joins or outer and not outer  with post-filtering conditions cannot be merged
          break;
        }
        Pair<Integer, int[]> mergeDetails = findMergePos(node, target);
        int pos = mergeDetails.getLeft();
        if (pos >= 0) {
          // for outer joins, it should not exceed 16 aliases (short type)
          if (!node.getNoOuterJoin() || !target.getNoOuterJoin()) {
            if (node.getRightAliases().length + target.getRightAliases().length + 1 > 16) {
              LOG.info(ErrorMsg.JOINNODE_OUTERJOIN_MORETHAN_16.getErrorCodedMsg());
              continueScanning = continueJoinMerge();
              continue;
            }
          }
          mergeJoins(node, target, pos, mergeDetails.getRight());
          trees.set(j, null);
          mergedQBJTree = true;
          continue; // continue merging with next alias
        }
        /*
         * for CBO provided orderings, don't attempt to reorder joins.
         * only convert consecutive joins into n-way joins.
         */
        continueScanning = continueJoinMerge();
        if (prevType == null) {
          prevType = currType;
        }
      }
    }

    // Now that we reordered QBJoinTrees, update leftaliases of all
    // QBJoinTree from innermost to outer
    if ((trees.size() > 1) && mergedQBJTree) {
      QBJoinTree curQBJTree = null;
      QBJoinTree prevQBJTree = null;
      for (int i = trees.size() - 1; i >= 0; i--) {
        curQBJTree = trees.get(i);
        if (curQBJTree != null) {
          if (prevQBJTree != null) {
            List<String> newCurLeftAliases = new ArrayList<String>();
            newCurLeftAliases.addAll(Arrays.asList(prevQBJTree.getLeftAliases()));
            newCurLeftAliases.addAll(Arrays.asList(prevQBJTree.getRightAliases()));
            curQBJTree
                .setLeftAliases(newCurLeftAliases.toArray(new String[newCurLeftAliases.size()]));
          }
          prevQBJTree = curQBJTree;
        }
      }
    }

    // reconstruct join tree
    QBJoinTree current = null;
    for (QBJoinTree target : trees) {
      if (target == null) {
        continue;
      }
      if (current == null) {
        qb.setQbJoinTree(current = target);
      } else {
        current.setJoinSrc(target);
        current = target;
      }
    }
  }

  // Join types should be all the same for merging (or returns null)
  private JoinType getType(JoinCond[] conds) {
    JoinType type = conds[0].getJoinType();
    return Arrays.stream(conds).allMatch(cond -> cond.getJoinType() == type) ? type : null;
  }

  private Operator genSelectAllDesc(Operator input) {
    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRowResolver();
    List<ColumnInfo> columns = inputRR.getColumnInfos();
    List<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    List<String> columnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> columnExprMap = new HashMap<String, ExprNodeDesc>();
    for (ColumnInfo col : columns) {
      colList.add(new ExprNodeColumnDesc(col, true));
      columnNames.add(col.getInternalName());
      columnExprMap.put(col.getInternalName(), new ExprNodeColumnDesc(col, true));
    }
    RowResolver outputRR = inputRR.duplicate();
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new SelectDesc(colList, columnNames, true),
        outputRR.getRowSchema(), input), outputRR);
    output.setColumnExprMap(columnExprMap);
    return output;
  }

  // Groups the clause names into lists so that any two clauses in the same list has the same
  // group by and distinct keys and no clause appears in more than one list. Returns a list of the
  // lists of clauses.
  private List<List<String>> getCommonGroupByDestGroups(QB qb,
                                                        Map<String, Operator<? extends OperatorDesc>> inputs) throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();

    Set<String> ks = new TreeSet<>(qbp.getClauseNames());
    List<List<String>> commonGroupByDestGroups = new ArrayList<>();

    // If this is a trivial query block return
    if (ks.isEmpty()) {
      commonGroupByDestGroups.add(Collections.emptyList());
      return commonGroupByDestGroups;
    }
    if (ks.size() == 1) {
      commonGroupByDestGroups.add(Collections.singletonList(ks.iterator().next()));
      return commonGroupByDestGroups;
    }

    List<Operator<? extends OperatorDesc>> inputOperators =
        new ArrayList<Operator<? extends OperatorDesc>>(ks.size());
    // We will try to combine multiple clauses into a smaller number with compatible keys.
    List<List<ExprNodeDesc>> newSprayKeyLists = new ArrayList<List<ExprNodeDesc>>(ks.size());
    List<List<ExprNodeDesc>> newDistinctKeyLists = new ArrayList<List<ExprNodeDesc>>(ks.size());

    // Iterate over each clause
    for (String dest : ks) {
      Operator input = inputs.get(dest);
      RowResolver inputRR = opParseCtx.get(input).getRowResolver();

      // Determine the keys for the current clause.
      List<ExprNodeDesc> currentDistinctKeys = getDistinctExprs(qbp, dest, inputRR);
      List<ExprNodeDesc> currentSprayKeys = determineSprayKeys(qbp, dest, inputRR);

      // Loop through each of the lists of exprs, looking for a match.
      boolean found = false;
      for (int i = 0; i < newSprayKeyLists.size(); i++) {
        if (!input.equals(inputOperators.get(i))) {
          continue;
        }
        // We will try to merge this clause into one of the previously added ones.
        List<ExprNodeDesc> targetSprayKeys = newSprayKeyLists.get(i);
        List<ExprNodeDesc> targetDistinctKeys = newDistinctKeyLists.get(i);
        if (currentDistinctKeys.isEmpty() != targetDistinctKeys.isEmpty()) {
          // GBY without distinct keys is not prepared to process distinct key structured rows.
          continue;
        }

        if (currentDistinctKeys.isEmpty()) {
          // current dest has no distinct keys.
          List<ExprNodeDesc> combinedList = combineExprNodeLists(targetSprayKeys, targetDistinctKeys);
          if (!matchExprLists(combinedList, currentSprayKeys)) {
            continue;
          } // else do the common code at the end.
        } else {
          if (targetDistinctKeys.isEmpty()) {
            List<ExprNodeDesc> combinedList = combineExprNodeLists(currentSprayKeys, currentDistinctKeys);
            if (!matchExprLists(combinedList, targetSprayKeys)) {
              continue;
            } else {
              // we have found a match. insert this distinct clause to head.
              newDistinctKeyLists.remove(i);
              newSprayKeyLists.remove(i);
              newDistinctKeyLists.add(i, currentDistinctKeys);
              newSprayKeyLists.add(i, currentSprayKeys);
              commonGroupByDestGroups.get(i).add(0, dest);
              found = true;
              break;
            }
          } else {
            if (!matchExprLists(targetDistinctKeys, currentDistinctKeys)) {
              continue;
            }

            if (!matchExprLists(targetSprayKeys, currentSprayKeys)) {
              continue;
            }
            // else do common code
          }
        }

        // common code
        // A match was found, so add the clause to the corresponding list
        commonGroupByDestGroups.get(i).add(dest);
        found = true;
        break;
      }

      // No match was found, so create new entries
      if (!found) {
        inputOperators.add(input);
        newSprayKeyLists.add(currentSprayKeys);
        newDistinctKeyLists.add(currentDistinctKeys);
        List<String> destGroup = new ArrayList<String>();
        destGroup.add(dest);
        commonGroupByDestGroups.add(destGroup);
      }
    }

    return commonGroupByDestGroups;
  }

  private List<ExprNodeDesc> determineSprayKeys(QBParseInfo qbp, String dest,
      RowResolver inputRR) throws SemanticException {
    List<ExprNodeDesc> sprayKeys = new ArrayList<ExprNodeDesc>();

    // Add the group by expressions
    List<ASTNode> grpByExprs = getGroupByForClause(qbp, dest);
    for (ASTNode grpByExpr : grpByExprs) {
      ExprNodeDesc exprDesc = genExprNodeDesc(grpByExpr, inputRR);
      if (ExprNodeDescUtils.indexOf(exprDesc, sprayKeys) < 0) {
        sprayKeys.add(exprDesc);
      }
    }
    return sprayKeys;
  }

  private List<ExprNodeDesc> combineExprNodeLists(List<ExprNodeDesc> list, List<ExprNodeDesc> list2) {
    ArrayList<ExprNodeDesc> result = new ArrayList<>(list);
    for (ExprNodeDesc elem : list2) {
      if (!result.contains(elem)) {
        result.add(elem);
      }
    }
    return result;
  }

  // Returns whether or not two lists contain the same elements independent of order
  private boolean matchExprLists(List<ExprNodeDesc> list1, List<ExprNodeDesc> list2) {

    if (list1.size() != list2.size()) {
      return false;
    }
    for (ExprNodeDesc exprNodeDesc : list1) {
      if (ExprNodeDescUtils.indexOf(exprNodeDesc, list2) < 0) {
        return false;
      }
    }

    return true;
  }

  // Returns a list of the distinct exprs without duplicates for a given clause name
  private List<ExprNodeDesc> getDistinctExprs(QBParseInfo qbp, String dest, RowResolver inputRR)
      throws SemanticException {

    List<ASTNode> distinctAggExprs = qbp.getDistinctFuncExprsForClause(dest);
    List<ExprNodeDesc> distinctExprs = new ArrayList<ExprNodeDesc>();

    for (ASTNode distinctAggExpr : distinctAggExprs) {
      // 0 is function name
      for (int i = 1; i < distinctAggExpr.getChildCount(); i++) {
        ASTNode parameter = (ASTNode) distinctAggExpr.getChild(i);
        ExprNodeDesc expr = genExprNodeDesc(parameter, inputRR);
        if (ExprNodeDescUtils.indexOf(expr, distinctExprs) < 0) {
          distinctExprs.add(expr);
        }
      }
    }

    return distinctExprs;
  }

  @SuppressWarnings("nls")
  private Operator genBodyPlan(QB qb, Operator input, Map<String, Operator> aliasToOpInfo)
      throws SemanticException {
    QBParseInfo qbp = qb.getParseInfo();

    SortedSet<String> ks = new TreeSet<String>(qbp.getClauseNames());
    Map<String, Operator<? extends OperatorDesc>> inputs = createInputForDests(qb, input, ks);

    Operator curr = input;

    List<List<String>> commonGroupByDestGroups = null;

    // If we can put multiple group bys in a single reducer, determine suitable groups of
    // expressions, otherwise treat all the expressions as a single group
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_MULTI_GROUPBY_SINGLE_REDUCER)) {
      try {
        commonGroupByDestGroups = getCommonGroupByDestGroups(qb, inputs);
      } catch (SemanticException e) {
        LOG.error("Failed to group clauses by common spray keys.", e);
      }
    }

    if (commonGroupByDestGroups == null) {
      commonGroupByDestGroups = Collections.singletonList(new ArrayList<>(ks));
    }

    if (!commonGroupByDestGroups.isEmpty()) {

      // Iterate over each group of subqueries with the same group by/distinct keys
      for (List<String> commonGroupByDestGroup : commonGroupByDestGroups) {
        if (commonGroupByDestGroup.isEmpty()) {
          continue;
        }

        String firstDest = commonGroupByDestGroup.get(0);
        input = inputs.get(firstDest);

        // Constructs a standard group by plan if:
        // There is no other subquery with the same group by/distinct keys or
        // (There are no aggregations in a representative query for the group and
        // There is no group by in that representative query) or
        // The data is skewed or
        // The conf variable used to control combining group bys into a single reducer is false
        if (commonGroupByDestGroup.size() == 1 ||
            (qbp.getAggregationExprsForClause(firstDest).size() == 0 &&
                getGroupByForClause(qbp, firstDest).size() == 0) ||
            conf.getBoolVar(HiveConf.ConfVars.HIVE_GROUPBY_SKEW) ||
            !conf.getBoolVar(HiveConf.ConfVars.HIVE_MULTI_GROUPBY_SINGLE_REDUCER)) {

          // Go over all the destination tables
          for (String dest : commonGroupByDestGroup) {
            curr = inputs.get(dest);

            if (qbp.getWhrForClause(dest) != null) {
              ASTNode whereExpr = qb.getParseInfo().getWhrForClause(dest);
              curr = genFilterPlan((ASTNode) whereExpr.getChild(0), qb, curr, aliasToOpInfo, false, false);
            }
            // Preserve operator before the GBY - we'll use it to resolve '*'
            Operator<?> gbySource = curr;

            if ((qbp.getAggregationExprsForClause(dest).size() != 0
                || getGroupByForClause(qbp, dest).size() > 0)
                && (qbp.getSelForClause(dest).getToken().getType() != HiveParser.TOK_SELECTDI
                || qbp.getWindowingExprsForClause(dest) == null)) {
              // multiple distincts is not supported with skew in data
              if (conf.getBoolVar(HiveConf.ConfVars.HIVE_GROUPBY_SKEW) &&
                  qbp.getDistinctFuncExprsForClause(dest).size() > 1) {
                throw new SemanticException(ErrorMsg.UNSUPPORTED_MULTIPLE_DISTINCTS.
                    getMsg());
              }
              // insert a select operator here used by the ColumnPruner to reduce
              // the data to shuffle
              curr = genSelectAllDesc(curr);
              // Check and transform group by *. This will only happen for select distinct *.
              // Here the "genSelectPlan" is being leveraged.
              // The main benefits are (1) remove virtual columns that should
              // not be included in the group by; (2) add the fully qualified column names to unParseTranslator
              // so that view is supported. The drawback is that an additional SEL op is added. If it is
              // not necessary, it will be removed by NonBlockingOpDeDupProc Optimizer because it will match
              // SEL%SEL% rule.
              ASTNode selExprList = qbp.getSelForClause(dest);
              if (selExprList.getToken().getType() == HiveParser.TOK_SELECTDI
                  && selExprList.getChildCount() == 1 && selExprList.getChild(0).getChildCount() == 1) {
                ASTNode node = (ASTNode) selExprList.getChild(0).getChild(0);
                if (node.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
                  curr = genSelectPlan(dest, qb, curr, curr);
                  RowResolver rr = opParseCtx.get(curr).getRowResolver();
                  qbp.setSelExprForClause(dest, genSelectDIAST(rr));
                }
              }
              if (conf.getBoolVar(HiveConf.ConfVars.HIVE_MAPSIDE_AGGREGATE)) {
                if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_GROUPBY_SKEW)) {
                  curr = genGroupByPlanMapAggrNoSkew(dest, qb, curr);
                } else {
                  curr = genGroupByPlanMapAggr2MR(dest, qb, curr);
                }
              } else if (conf.getBoolVar(HiveConf.ConfVars.HIVE_GROUPBY_SKEW)) {
                curr = genGroupByPlan2MR(dest, qb, curr);
              } else {
                curr = genGroupByPlan1MR(dest, qb, curr);
              }
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("RR before GB " + opParseCtx.get(gbySource).getRowResolver()
                  + " after GB " + opParseCtx.get(curr).getRowResolver());
            }

            curr = genPostGroupByBodyPlan(curr, dest, qb, aliasToOpInfo, gbySource);
          }
        } else {
          curr = genGroupByPlan1ReduceMultiGBY(commonGroupByDestGroup, qb, input, aliasToOpInfo);
        }
      }
    }

    LOG.debug("Created Body Plan for Query Block {}", qb.getId());

    return curr;
  }

  private Map<String, Operator<? extends OperatorDesc>> createInputForDests(QB qb,
                                                                            Operator<? extends OperatorDesc> input, Set<String> dests) throws SemanticException {
    Map<String, Operator<? extends OperatorDesc>> inputs =
        new HashMap<String, Operator<? extends OperatorDesc>>();
    for (String dest : dests) {
      inputs.put(dest, genLateralViewPlanForDest(dest, qb, input));
    }
    return inputs;
  }

  private Operator genPostGroupByBodyPlan(Operator curr, String dest, QB qb,
                                          Map<String, Operator> aliasToOpInfo, Operator gbySource)
      throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();

    // Insert HAVING plan here
    if (qbp.getHavingForClause(dest) != null) {
      if (getGroupByForClause(qbp, dest).size() == 0) {
        throw new SemanticException("HAVING specified without GROUP BY");
      }
      curr = genHavingPlan(dest, qb, curr, aliasToOpInfo);
    }

    if(queryProperties.hasWindowing() && qb.getWindowingSpec(dest) != null) {
      curr = genWindowingPlan(qb, qb.getWindowingSpec(dest), curr);
      // GBy for DISTINCT after windowing
      if ((qbp.getAggregationExprsForClause(dest).size() != 0
          || getGroupByForClause(qbp, dest).size() > 0)
          && qbp.getSelForClause(dest).getToken().getType() == HiveParser.TOK_SELECTDI
          && qbp.getWindowingExprsForClause(dest) != null) {
        if (conf.getBoolVar(HiveConf.ConfVars.HIVE_MAPSIDE_AGGREGATE)) {
          if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_GROUPBY_SKEW)) {
            curr = genGroupByPlanMapAggrNoSkew(dest, qb, curr);
          } else {
            curr = genGroupByPlanMapAggr2MR(dest, qb, curr);
          }
        } else if (conf.getBoolVar(HiveConf.ConfVars.HIVE_GROUPBY_SKEW)) {
          curr = genGroupByPlan2MR(dest, qb, curr);
        } else {
          curr = genGroupByPlan1MR(dest, qb, curr);
        }
      }
    }

    curr = genSelectPlan(dest, qb, curr, gbySource);

    Integer limit = qbp.getDestLimit(dest);
    int offset = (qbp.getDestLimitOffset(dest) == null) ? 0 : qbp.getDestLimitOffset(dest);

    // Expressions are not supported currently without an alias.

    // Reduce sink is needed if the query contains a cluster by, distribute by,
    // order by or a sort by clause.
    boolean genReduceSink = false;
    int numReducers = -1;
    boolean hasOrderBy = false;

    // Currently, expressions are not allowed in cluster by, distribute by,
    // order by or a sort by clause. For each of the above clause types, check
    // if the clause contains any expression.
    if (qbp.getClusterByForClause(dest) != null) {
      genReduceSink = true;
    }

    if (qbp.getDistributeByForClause(dest) != null) {
      genReduceSink = true;
    }

    if (qbp.getOrderByForClause(dest) != null) {
      genReduceSink = true;
      numReducers = 1;
      hasOrderBy = true;
    }

    if (offset > 0 && !hasOrderBy) {
      HiveConf.StrictChecks.checkOffsetWithoutOrderBy(conf);
      warn("OFFSET without ORDER BY is mostly non-deterministic and meaningless. "
          + "Please make sure that you really don't need ORDER BY");

      assert limit != null : "OFFSET is always paired with LIMIT";
      genReduceSink = true;
      // # of reducers must be one in order to compute a global offset
      numReducers = 1;
      final long offsetPlusLimit = (long) offset + (long) limit;
      if (offsetPlusLimit <= Integer.MAX_VALUE) {
        // This pushes `offset` + `limit` because the parallelism of `curr` can be greater than 1
        curr = genLimitPlan(dest, curr, 0, (int) offsetPlusLimit);
      }
    }

    if (qbp.getSortByForClause(dest) != null) {
      genReduceSink = true;
    }

    if (genReduceSink) {
      curr = genReduceSinkPlan(dest, qb, curr, numReducers, hasOrderBy);
    }


    if (qbp.getIsSubQ()) {
      if (limit != null) {
        // In the following cases, no further step is required
        // - When limit is zero, `curr` generates empty rows
        // - When `curr` is already a single reduce task, it can generate the exactly correct result
        final boolean extraMRStep = limit != 0 && numReducers != 1;
        curr = genLimitMapRedPlan(dest, qb, curr, offset, limit, extraMRStep);
      }
    } else {
      // exact limit can be taken care of by the fetch operator
      if (limit != null) {
        boolean extraMRStep = true;

        if (limit == 0 || numReducers == 1 ||
            qb.getIsQuery() && qbp.getClusterByForClause(dest) == null &&
                qbp.getSortByForClause(dest) == null) {
          extraMRStep = false;
        }

        curr = genLimitMapRedPlan(dest, qb, curr, offset,
            limit, extraMRStep);
        qb.getParseInfo().setOuterQueryLimit(limit);
      }
      if (!HiveOperation.CREATEVIEW.equals(queryState.getHiveOperation())) {
        curr = genFileSinkPlan(dest, qb, curr);
      }
    }

    return curr;
  }

  @SuppressWarnings("nls")
  private Operator genUnionPlan(String unionalias, String leftalias,
                                Operator leftOp, String rightalias, Operator rightOp)
      throws SemanticException {

    // Currently, the unions are not merged - each union has only 2 parents. So,
    // a n-way union will lead to (n-1) union operators.
    // This can be easily merged into 1 union
    RowResolver leftRR = opParseCtx.get(leftOp).getRowResolver();
    RowResolver rightRR = opParseCtx.get(rightOp).getRowResolver();
    Map<String, ColumnInfo> leftmap = leftRR.getFieldMap(leftalias);
    Map<String, ColumnInfo> rightmap = rightRR.getFieldMap(rightalias);
    // make sure the schemas of both sides are the same
    ASTNode tabref = qb.getAliases().isEmpty() ? null :
        qb.getParseInfo().getSrcForAlias(qb.getAliases().get(0));
    if (leftmap.size() != rightmap.size()) {
      throw new SemanticException("Schema of both sides of union should match.");
    }

    RowResolver unionoutRR = new RowResolver();

    Iterator<Map.Entry<String, ColumnInfo>> lIter = leftmap.entrySet().iterator();
    Iterator<Map.Entry<String, ColumnInfo>> rIter = rightmap.entrySet().iterator();
    while (lIter.hasNext()) {
      Map.Entry<String, ColumnInfo> lEntry = lIter.next();
      Map.Entry<String, ColumnInfo> rEntry = rIter.next();
      ColumnInfo lInfo = lEntry.getValue();
      ColumnInfo rInfo = rEntry.getValue();

      String field = lEntry.getKey(); // use left alias (~mysql, postgresql)
      // try widening conversion, otherwise fail union
      TypeInfo commonTypeInfo = FunctionRegistry.getCommonClassForUnionAll(lInfo.getType(),
          rInfo.getType());
      if (commonTypeInfo == null) {
        throw new SemanticException(generateErrorMessage(tabref,
            "Schema of both sides of union should match: Column " + field
                + " is of type " + lInfo.getType().getTypeName()
                + " on first table and type " + rInfo.getType().getTypeName()
                + " on second table"));
      }
      ColumnInfo unionColInfo = new ColumnInfo(lInfo);
      unionColInfo.setType(commonTypeInfo);
      unionoutRR.put(unionalias, field, unionColInfo);
    }

    // For TEZ we rely on the generated SelectOperator to do the type casting.
    // Consider:
    //    SEL_1 (int)   SEL_2 (int)    SEL_3 (double)
    // If we first merge SEL_1 and SEL_2 into a UNION_1, and then merge UNION_1
    // with SEL_3 to get UNION_2, then no SelectOperator will be inserted. Hence error
    // will happen afterwards. The solution here is to insert one after UNION_1, which
    // cast int to double.
    boolean isMR = HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("mr");

    if (!isMR || !(leftOp instanceof UnionOperator)) {
      leftOp = genInputSelectForUnion(leftOp, leftmap, leftalias, unionoutRR, unionalias);
    }

    if (!isMR || !(rightOp instanceof UnionOperator)) {
      rightOp = genInputSelectForUnion(rightOp, rightmap, rightalias, unionoutRR, unionalias);
    }

    // If one of the children (left or right) is:
    // (i) a union, or
    // (ii) an identity projection followed by a union,
    // merge with it
    // else create a new one
    if (leftOp instanceof UnionOperator ||
        (leftOp instanceof SelectOperator &&
            leftOp.getParentOperators() != null &&
            !leftOp.getParentOperators().isEmpty() &&
            leftOp.getParentOperators().get(0) instanceof UnionOperator &&
            ((SelectOperator)leftOp).isIdentitySelect()) ) {

      if(!(leftOp instanceof UnionOperator)) {
        Operator oldChild = leftOp;
        leftOp = (Operator) leftOp.getParentOperators().get(0);
        leftOp.removeChildAndAdoptItsChildren(oldChild);
      }

      // make left a child of right
      List<Operator<? extends OperatorDesc>> child =
          new ArrayList<Operator<? extends OperatorDesc>>();
      child.add(leftOp);
      rightOp.setChildOperators(child);

      List<Operator<? extends OperatorDesc>> parent = leftOp
          .getParentOperators();
      parent.add(rightOp);

      UnionDesc uDesc = ((UnionOperator) leftOp).getConf();
      uDesc.setNumInputs(uDesc.getNumInputs() + 1);
      return putOpInsertMap(leftOp, unionoutRR);
    }

    if (rightOp instanceof UnionOperator ||
        (rightOp instanceof SelectOperator &&
            rightOp.getParentOperators() != null &&
            !rightOp.getParentOperators().isEmpty() &&
            rightOp.getParentOperators().get(0) instanceof UnionOperator &&
            ((SelectOperator)rightOp).isIdentitySelect()) ) {

      if(!(rightOp instanceof UnionOperator)) {
        Operator oldChild = rightOp;
        rightOp = (Operator) rightOp.getParentOperators().get(0);
        rightOp.removeChildAndAdoptItsChildren(oldChild);
      }

      // make right a child of left
      List<Operator<? extends OperatorDesc>> child =
          new ArrayList<Operator<? extends OperatorDesc>>();
      child.add(rightOp);
      leftOp.setChildOperators(child);

      List<Operator<? extends OperatorDesc>> parent = rightOp
          .getParentOperators();
      parent.add(leftOp);
      UnionDesc uDesc = ((UnionOperator) rightOp).getConf();
      uDesc.setNumInputs(uDesc.getNumInputs() + 1);

      return putOpInsertMap(rightOp, unionoutRR);
    }

    // Create a new union operator
    Operator<? extends OperatorDesc> unionforward = OperatorFactory
        .getAndMakeChild(getOpContext(), new UnionDesc(), new RowSchema(unionoutRR
            .getColumnInfos()));

    // set union operator as child of each of leftOp and rightOp
    rightOp.setChildOperators(Lists.newArrayList(unionforward));
    leftOp.setChildOperators(Lists.newArrayList(unionforward));

    unionforward.setParentOperators(Lists.newArrayList(leftOp, rightOp));

    // create operator info list to return
    return putOpInsertMap(unionforward, unionoutRR);
  }

  /**
   * Generates a select operator which can go between the original input operator and the union
   * operator. This select casts columns to match the type of the associated column in the union,
   * other columns pass through unchanged. The new operator's only parent is the original input
   * operator to the union, and it's only child is the union. If the input does not need to be
   * cast, the original operator is returned, and no new select operator is added.
   *
   * @param origInputOp
   *          The original input operator to the union.
   * @param origInputFieldMap
   *          A map from field name to ColumnInfo for the original input operator.
   * @param origInputAlias
   *          The alias associated with the original input operator.
   * @param unionoutRR
   *          The union's output row resolver.
   * @param unionalias
   *          The alias of the union.
   * @return
   * @throws SemanticException
   */
  private Operator<? extends OperatorDesc> genInputSelectForUnion(
      Operator<? extends OperatorDesc> origInputOp, Map<String, ColumnInfo> origInputFieldMap,
      String origInputAlias, RowResolver unionoutRR, String unionalias)
      throws SemanticException {

    Map<String, ColumnInfo> fieldMap = unionoutRR.getFieldMap(unionalias);

    Iterator<ColumnInfo> oIter = origInputFieldMap.values().iterator();
    Iterator<ColumnInfo> uIter = fieldMap.values().iterator();

    List<ExprNodeDesc> columns = new ArrayList<>();
    boolean needsCast = false;
    while (oIter.hasNext()) {
      ColumnInfo oInfo = oIter.next();
      ColumnInfo uInfo = uIter.next();
      ExprNodeDesc column = new ExprNodeColumnDesc(oInfo.getType(), oInfo.getInternalName(),
          oInfo.getTabAlias(), oInfo.getIsVirtualCol(), oInfo.isSkewedCol());
      if (!oInfo.getType().equals(uInfo.getType())) {
        needsCast = true;
        column = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
            .createConversionCast(column, (PrimitiveTypeInfo)uInfo.getType());
      }
      columns.add(column);
    }

    // If none of the columns need to be cast there's no need for an additional select operator
    if (!needsCast) {
      return origInputOp;
    }

    RowResolver rowResolver = new RowResolver();
    Map<String, ExprNodeDesc> columnExprMap = new HashMap<String, ExprNodeDesc>();

    List<String> colName = new ArrayList<String>();
    for (int i = 0; i < columns.size(); i++) {
      String name = getColumnInternalName(i);
      ColumnInfo col = new ColumnInfo(name, columns.get(i)
          .getTypeInfo(), "", false);
      rowResolver.put(origInputAlias, name, col);
      colName.add(name);
      columnExprMap.put(name, columns.get(i));
    }

    Operator<SelectDesc> newInputOp = OperatorFactory.getAndMakeChild(
        new SelectDesc(columns, colName), new RowSchema(rowResolver.getColumnInfos()),
        columnExprMap, origInputOp);
    return putOpInsertMap(newInputOp, rowResolver);
  }

  /**
   * Generates the sampling predicate from the TABLESAMPLE clause information.
   * This function uses the bucket column list to decide the expression inputs
   * to the predicate hash function in case useBucketCols is set to true,
   * otherwise the expression list stored in the TableSample is used. The bucket
   * columns of the table are used to generate this predicate in case no
   * expressions are provided on the TABLESAMPLE clause and the table has
   * clustering columns defined in it's metadata. The predicate created has the
   * following structure:
   *
   * ((hash(expressions) & Integer.MAX_VALUE) % denominator) == numerator
   *
   * @param ts
   *          TABLESAMPLE clause information
   * @param bucketCols
   *          The clustering columns of the table
   * @param useBucketCols
   *          Flag to indicate whether the bucketCols should be used as input to
   *          the hash function
   * @param alias
   *          The alias used for the table in the row resolver
   * @param rwsch
   *          The row resolver used to resolve column references
   * @param planExpr
   *          The plan tree for the expression. If the user specified this, the
   *          parse expressions are not used
   * @return exprNodeDesc
   * @exception SemanticException
   */
  private ExprNodeDesc genSamplePredicate(TableSample ts,
                                          List<String> bucketCols, boolean useBucketCols, String alias,
                                          RowResolver rwsch, ExprNodeDesc planExpr, int bucketingVersion)
      throws SemanticException {

    ExprNodeDesc numeratorExpr = new ExprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, Integer.valueOf(ts.getNumerator() - 1));

    ExprNodeDesc denominatorExpr = new ExprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, Integer.valueOf(ts.getDenominator()));

    ExprNodeDesc intMaxExpr = new ExprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, Integer.valueOf(Integer.MAX_VALUE));

    List<ExprNodeDesc> args = new ArrayList<ExprNodeDesc>();
    if (planExpr != null) {
      args.add(planExpr);
    } else if (useBucketCols) {
      for (String col : bucketCols) {
        ColumnInfo ci = rwsch.get(alias, col);
        // TODO: change type to the one in the table schema
        args.add(new ExprNodeColumnDesc(ci));
      }
    } else {
      for (ASTNode expr : ts.getExprs()) {
        args.add(genExprNodeDesc(expr, rwsch));
      }
    }

    ExprNodeDesc equalsExpr = null;
    {
      ExprNodeDesc hashfnExpr = new ExprNodeGenericFuncDesc(
          TypeInfoFactory.intTypeInfo,
              bucketingVersion == 2 ? new GenericUDFMurmurHash() : new GenericUDFHash(), args);
      LOG.info("hashfnExpr = " + hashfnExpr);
      ExprNodeDesc andExpr = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("&", hashfnExpr, intMaxExpr);
      LOG.info("andExpr = " + andExpr);
      ExprNodeDesc modExpr = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("%", andExpr, denominatorExpr);
      LOG.info("modExpr = " + modExpr);
      LOG.info("numeratorExpr = " + numeratorExpr);
      equalsExpr = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("==", modExpr, numeratorExpr);
      LOG.info("equalsExpr = " + equalsExpr);
    }
    return equalsExpr;
  }

  protected String getAliasId(String alias, QB qb) {
    return (qb.getId() == null ? alias : qb.getId() + ":" + alias).toLowerCase();
  }

  @SuppressWarnings("nls")
  private Operator genTablePlan(String alias, QB qb) throws SemanticException {

    String alias_id = getAliasId(alias, qb);
    Table tab = qb.getMetaData().getSrcForAlias(alias);
    RowResolver rwsch;

    // is the table already present
    TableScanOperator top = topOps.get(alias_id);

    // Obtain table props in query
    Map<String, String> properties = qb.getTabPropsForAlias(alias);

    if (top == null) {
      // Determine row schema for TSOP.
      // Include column names from SerDe, the partition and virtual columns.
      rwsch = new RowResolver();
      try {
        // Including parameters passed in the query
        if (properties != null) {
          for (Entry<String, String> prop : properties.entrySet()) {
            if (tab.getSerdeParam(prop.getKey()) != null) {
              LOG.warn("SerDe property in input query overrides stored SerDe property");
            }
            tab.setSerdeParam(prop.getKey(), prop.getValue());
          }
        }
        // Obtain inspector for schema
        final Deserializer deserializer = tab.getDeserializer();
        StructObjectInspector rowObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();

        deserializer.handleJobLevelConfiguration(conf);
        List<? extends StructField> fields = rowObjectInspector
            .getAllStructFieldRefs();
        Set<String> partCols = tab.hasNonNativePartitionSupport() ?
            Sets.newHashSet(tab.getPartColNames()) : Collections.emptySet();
        for (int i = 0; i < fields.size(); i++) {
          /**
           * if the column is a skewed column, use ColumnInfo accordingly
           */
          ColumnInfo colInfo = new ColumnInfo(fields.get(i).getFieldName(),
              TypeInfoUtils.getTypeInfoFromObjectInspector(fields.get(i)
                  .getFieldObjectInspector()), alias, false);
          if (partCols.contains(colInfo.getInternalName())) {
            colInfo.setHiddenPartitionCol(true);
          }
          colInfo.setSkewedCol(isSkewedCol(alias, qb, fields.get(i).getFieldName()));
          rwsch.put(alias, fields.get(i).getFieldName(), colInfo);
        }
      } catch (SerDeException e) {
        throw new RuntimeException(e);
      }
      // Hack!! - refactor once the metadata APIs with types are ready
      // Finally add the partitioning columns
      for (FieldSchema part_col : tab.getPartCols()) {
        LOG.trace("Adding partition col: " + part_col);
        rwsch.put(alias, part_col.getName(), new ColumnInfo(part_col.getName(),
            TypeInfoFactory.getPrimitiveTypeInfo(part_col.getType()), alias, true));
      }

      // put virtual columns into RowResolver.
      List<VirtualColumn> vcList = tab.getVirtualColumns();

      vcList.forEach(vc -> rwsch.put(alias, vc.getName().toLowerCase(), new ColumnInfo(vc.getName(),
              vc.getTypeInfo(), alias, true, vc.getIsHidden()
      )));

      // Create the root of the operator tree
      TableScanDesc tsDesc = new TableScanDesc(alias, vcList, tab);
      setupStats(tsDesc, qb.getParseInfo(), tab, alias, rwsch);

      Map<String, String> tblProperties = tab.getParameters();
      Map<String, String> tblPropertiesFromQuery = qb.getTabPropsForAlias(alias);

      AcidUtils.AcidOperationalProperties acidOperationalProperties = tsDesc.getAcidOperationalProperties();
      if (acidOperationalProperties != null) {
        tsDesc.getAcidOperationalProperties().setInsertOnlyFetchBucketId(
            (tblProperties != null && Boolean.parseBoolean(tblProperties.get(Constants.INSERT_ONLY_FETCH_BUCKET_ID))) ||
                (tblPropertiesFromQuery != null &&
                    Boolean.parseBoolean(tblPropertiesFromQuery.get(Constants.INSERT_ONLY_FETCH_BUCKET_ID))));

        tsDesc.getAcidOperationalProperties().setFetchDeletedRows(
            (tblProperties != null && Boolean.parseBoolean(tblProperties.get(Constants.ACID_FETCH_DELETED_ROWS))) ||
                (tblPropertiesFromQuery != null &&
                    Boolean.parseBoolean(tblPropertiesFromQuery.get(Constants.ACID_FETCH_DELETED_ROWS))));
      }

      SplitSample sample = nameToSplitSample.get(alias_id);
      if (sample != null && sample.getRowCount() != null) {
        tsDesc.setRowLimit(sample.getRowCount());
        nameToSplitSample.remove(alias_id);
      }

      top = (TableScanOperator) putOpInsertMap(OperatorFactory.get(getOpContext(), tsDesc,
          new RowSchema(rwsch.getColumnInfos())), rwsch);

      // Set insiderView so that we can skip the column authorization for this.
      top.setInsideView(qb.isInsideView() || qb.getAliasInsideView().contains(alias.toLowerCase()));

      // Add this to the list of top operators - we always start from a table
      // scan
      topOps.put(alias_id, top);

      if (properties != null) {
        tsDesc.setOpProps(properties);
      }
    } else {
      rwsch = opParseCtx.get(top).getRowResolver();
      top.setChildOperators(null);
    }

    // check if this table is sampled and needs more than input pruning
    Operator<? extends OperatorDesc> op = top;
    TableSample ts = qb.getParseInfo().getTabSample(alias);
    if (ts != null) {
      TableScanOperator tableScanOp = top;
      tableScanOp.getConf().setTableSample(ts);
      int num = ts.getNumerator();
      int den = ts.getDenominator();
      List<ASTNode> sampleExprs = ts.getExprs();

      // TODO: Do the type checking of the expressions
      List<String> tabBucketCols = tab.getBucketCols();
      int numBuckets = tab.getNumBuckets();

      // If there are no sample cols and no bucket cols then throw an error
      if (tabBucketCols.size() == 0 && sampleExprs.size() == 0) {
        throw new SemanticException(ErrorMsg.NON_BUCKETED_TABLE.getMsg() + " "
            + tab.getTableName());
      }

      if (num > den) {
        throw new SemanticException(
            ErrorMsg.BUCKETED_NUMERATOR_BIGGER_DENOMINATOR.getMsg() + " "
                + tab.getTableName());
      }

      // check if a predicate is needed
      // predicate is needed if either input pruning is not enough
      // or if input pruning is not possible

      // check if the sample columns are the same as the table bucket columns
      boolean colsEqual = true;
      if ((sampleExprs.size() != tabBucketCols.size())
          && (sampleExprs.size() != 0)) {
        colsEqual = false;
      }

      for (int i = 0; i < sampleExprs.size() && colsEqual; i++) {
        boolean colFound = false;
        for (int j = 0; j < tabBucketCols.size() && !colFound; j++) {
          if (sampleExprs.get(i).getToken().getType() != HiveParser.TOK_TABLE_OR_COL) {
            break;
          }

          if ((sampleExprs.get(i).getChild(0)).getText().equalsIgnoreCase(tabBucketCols.get(j))) {
            colFound = true;
          }
        }
        colsEqual = colFound;
      }

      // Check if input can be pruned
      ts.setInputPruning((sampleExprs.size() == 0 || colsEqual));

      // check if input pruning is enough
      if ((sampleExprs.size() == 0 || colsEqual)
          && (num == den || (den % numBuckets == 0 || numBuckets % den == 0))) {

        // input pruning is enough; add the filter for the optimizer to use it
        // later
        LOG.info("No need for sample filter");
        ExprNodeDesc samplePredicate = genSamplePredicate(ts, tabBucketCols,
            colsEqual, alias, rwsch, null,
                tab.getBucketingVersion());
        FilterDesc filterDesc = new FilterDesc(
            samplePredicate, true, new SampleDesc(ts.getNumerator(),
            ts.getDenominator(), tabBucketCols, true));
        filterDesc.setGenerated(true);
        op = OperatorFactory.getAndMakeChild(filterDesc,
            new RowSchema(rwsch.getColumnInfos()), top);
      } else {
        // need to add filter
        // create tableOp to be filterDesc and set as child to 'top'
        LOG.info("Need sample filter");
        ExprNodeDesc samplePredicate = genSamplePredicate(ts, tabBucketCols,
            colsEqual, alias, rwsch, null,
                tab.getBucketingVersion());
        FilterDesc filterDesc = new FilterDesc(samplePredicate, true);
        filterDesc.setGenerated(true);
        op = OperatorFactory.getAndMakeChild(filterDesc,
            new RowSchema(rwsch.getColumnInfos()), top);
      }
    } else {
      boolean testMode = conf.getBoolVar(ConfVars.HIVE_TEST_MODE);
      if (testMode) {
        String tabName = tab.getTableName();

        // has the user explicitly asked not to sample this table
        String unSampleTblList = conf
            .getVar(ConfVars.HIVE_TEST_MODE_NOSAMPLE);
        String[] unSampleTbls = unSampleTblList.split(",");
        boolean unsample = false;
        for (String unSampleTbl : unSampleTbls) {
          if (tabName.equalsIgnoreCase(unSampleTbl)) {
            unsample = true;
          }
        }

        if (!unsample) {
          int numBuckets = tab.getNumBuckets();

          // If the input table is bucketed, choose the first bucket
          if (numBuckets > 0) {
            TableSample tsSample = new TableSample(1, numBuckets);
            tsSample.setInputPruning(true);
            qb.getParseInfo().setTabSample(alias, tsSample);
            ExprNodeDesc samplePred = genSamplePredicate(tsSample, tab
                .getBucketCols(), true, alias, rwsch, null,
                    tab.getBucketingVersion());
            FilterDesc filterDesc = new FilterDesc(samplePred, true,
                new SampleDesc(tsSample.getNumerator(), tsSample
                    .getDenominator(), tab.getBucketCols(), true));
            filterDesc.setGenerated(true);
            op = OperatorFactory.getAndMakeChild(filterDesc,
                new RowSchema(rwsch.getColumnInfos()), top);
            LOG.info("No need for sample filter");
          } else {
            // The table is not bucketed, add a dummy filter :: rand()
            int freq = conf.getIntVar(ConfVars.HIVE_TEST_MODE_SAMPLE_FREQ);
            TableSample tsSample = new TableSample(1, freq);
            tsSample.setInputPruning(false);
            qb.getParseInfo().setTabSample(alias, tsSample);
            LOG.info("Need sample filter");
            ExprNodeDesc randFunc = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
                .getFuncExprNodeDesc("rand",
                    new ExprNodeConstantDesc(Integer.valueOf(460476415)));
            ExprNodeDesc samplePred = genSamplePredicate(tsSample, null, false,
                alias, rwsch, randFunc, tab.getBucketingVersion());
            FilterDesc filterDesc = new FilterDesc(samplePred, true);
            filterDesc.setGenerated(true);
            op = OperatorFactory.getAndMakeChild(filterDesc,
                new RowSchema(rwsch.getColumnInfos()), top);
          }
        }
      }
    }

    Operator output = putOpInsertMap(op, rwsch);

    if (tab.isMaterializedTable()) {
      // Clone Statistics just in case because multiple TableScanOperator can access the same CTE
      top.setStatistics(ctx.getMaterializedTableStats(tab.getFullTableName()).clone());
    }

    LOG.debug("Created Table Plan for {} {}", alias, op);

    return output;
  }

  boolean isSkewedCol(String alias, QB qb, String colName) {
    return qb.getSkewedColumnNames(alias).stream()
        .anyMatch(skewedCol -> skewedCol.equalsIgnoreCase(colName));
  }

  private void setupStats(TableScanDesc tsDesc, QBParseInfo qbp, Table tab, String alias,
                          RowResolver rwsch)
      throws SemanticException {

    // if it is not analyze command and not column stats, then do not gatherstats
    if (!qbp.isAnalyzeCommand() && qbp.getAnalyzeRewrite() == null) {
      tsDesc.setGatherStats(false);
      return;
    }

    if (HiveConf.getVar(conf, HIVE_STATS_DBCLASS).equalsIgnoreCase(StatDB.fs.name())) {
      String statsTmpLoc = ctx.getTempDirForInterimJobPath(tab.getPath()).toString();
      LOG.debug("Set stats collection dir : " + statsTmpLoc);
      tsDesc.setTmpStatsDir(statsTmpLoc);
    }
    tsDesc.setGatherStats(true);
    tsDesc.setStatsReliable(conf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_RELIABLE));

    // append additional virtual columns for storing statistics
    Iterator<VirtualColumn> vcs = VirtualColumn.getStatsRegistry(conf).iterator();
    List<VirtualColumn> vcList = new ArrayList<VirtualColumn>();
    while (vcs.hasNext()) {
      VirtualColumn vc = vcs.next();
      rwsch.put(alias, vc.getName(), new ColumnInfo(vc.getName(),
          vc.getTypeInfo(), alias, true, vc.getIsHidden()));
      vcList.add(vc);
    }
    tsDesc.addVirtualCols(vcList);

    String tblName = tab.getTableName();
    // Theoretically the key prefix could be any unique string shared
    // between TableScanOperator (when publishing) and StatsTask (when aggregating).
    // Here we use
    // db_name.table_name + partitionSec
    // as the prefix for easy of read during explain and debugging.
    // Currently, partition spec can only be static partition.
    String k = FileUtils.escapePathName(tblName).toLowerCase() + Path.SEPARATOR;
    tsDesc.setStatsAggPrefix(FileUtils.escapePathName(tab.getDbName()).toLowerCase() + "." + k);

    // set up WriteEntity for replication and txn stats
    WriteEntity we = new WriteEntity(tab, WriteEntity.WriteType.DDL_SHARED);
    we.setTxnAnalyze(true);
    outputs.add(we);
    if (AcidUtils.isTransactionalTable(tab)) {
      if (acidAnalyzeTable != null) {
        throw new IllegalStateException("Multiple ACID tables in analyze: "
            + we + ", " + acidAnalyzeTable);
      }
      acidAnalyzeTable = we;
    }

    // add WriteEntity for each matching partition
    if (tab.isPartitioned() && !tab.hasNonNativePartitionSupport()) {
      List<String> cols = new ArrayList<String>();
      if (qbp.getAnalyzeRewrite() != null) {
        List<FieldSchema> partitionCols = tab.getPartCols();
        for (FieldSchema fs : partitionCols) {
          cols.add(fs.getName());
        }
        tsDesc.setPartColumns(cols);
        return;
      }
      TableSpec tblSpec = qbp.getTableSpec(alias);
      Map<String, String> partSpec = tblSpec.getPartSpec();
      if (partSpec != null) {
        cols.addAll(partSpec.keySet());
        tsDesc.setPartColumns(cols);
      } else {
        throw new SemanticException(ErrorMsg.NEED_PARTITION_SPECIFICATION.getMsg());
      }
      List<Partition> partitions = qbp.getTableSpec().partitions;
      if (partitions != null) {
        for (Partition partn : partitions) {
          WriteEntity pwe = new WriteEntity(partn, WriteEntity.WriteType.DDL_NO_LOCK);
          pwe.setTxnAnalyze(true);
          outputs.add(pwe);
        }
      }
    }
  }

  private Operator genPlan(QB parent, QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.EXCEPT || qbexpr.getOpcode() == QBExpr.Opcode.EXCEPTALL
        || qbexpr.getOpcode() == QBExpr.Opcode.INTERSECT || qbexpr.getOpcode() == QBExpr.Opcode.INTERSECTALL) {
      throw new SemanticException(
          "EXCEPT and INTERSECT operations are only supported with Cost Based Optimizations enabled. Please set 'hive.cbo.enable' to true!");
    }
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      boolean skipAmbiguityCheck = viewSelect == null && parent.isTopLevelSelectStarQuery();
      return genPlan(qbexpr.getQB(), skipAmbiguityCheck);
    }
    if (qbexpr.getOpcode() == QBExpr.Opcode.UNION) {
      Operator qbexpr1Ops = genPlan(parent, qbexpr.getQBExpr1());
      Operator qbexpr2Ops = genPlan(parent, qbexpr.getQBExpr2());

      return genUnionPlan(qbexpr.getAlias(), qbexpr.getQBExpr1().getAlias(),
          qbexpr1Ops, qbexpr.getQBExpr2().getAlias(), qbexpr2Ops);
    }
    return null;
  }

  Operator genPlan(QB qb) throws SemanticException {
    return genPlan(qb, false);
  }

  @SuppressWarnings("nls")
  private Operator genPlan(QB qb, boolean skipAmbiguityCheck)
      throws SemanticException {

    if (!ctx.isCboSucceeded() && qb.getParseInfo().hasQualifyClause()) {
      throw new SemanticException(ErrorMsg.CBO_IS_REQUIRED.getErrorCodedMsg("Qualify clause"));
    }

    // First generate all the opInfos for the elements in the from clause
    // Must be deterministic order map - see HIVE-8707
    Map<String, Operator> aliasToOpInfo = new LinkedHashMap<String, Operator>();

    // Recurse over the subqueries to fill the subquery part of the plan
    for (String alias : qb.getSubqAliases()) {
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      Operator<?> operator = genPlan(qb, qbexpr);
      aliasToOpInfo.put(alias, operator);
      if (qb.getViewToTabSchema().containsKey(alias)) {
        // we set viewProjectToTableSchema so that we can leverage ColumnPruner.
        if (operator instanceof LimitOperator) {
          // If create view has LIMIT operator, this can happen
          // Fetch parent operator
          operator = operator.getParentOperators().get(0);
        }
        if (operator instanceof SelectOperator) {
          if (this.viewProjectToTableSchema == null) {
            this.viewProjectToTableSchema = new LinkedHashMap<>();
          }
          viewProjectToTableSchema.put((SelectOperator) operator, qb.getViewToTabSchema()
              .get(alias));
        } else {
          throw new SemanticException("View " + alias + " is corresponding to "
              + operator.getType().name() + ", rather than a SelectOperator.");
        }
      }
    }

    // Recurse over all the source tables
    for (String alias : qb.getTabAliases()) {
      if(alias.equals(DUMMY_TABLE)) {
        continue;
      }
      Operator op = genTablePlan(alias, qb);
      aliasToOpInfo.put(alias, op);
    }

    if (aliasToOpInfo.isEmpty()) {
      qb.getMetaData().setSrcForAlias(DUMMY_TABLE, getDummyTable());
      TableScanOperator op = (TableScanOperator) genTablePlan(DUMMY_TABLE, qb);
      op.getConf().setRowLimit(1);
      qb.addAlias(DUMMY_TABLE);
      qb.setTabAlias(DUMMY_TABLE, DUMMY_TABLE);
      aliasToOpInfo.put(DUMMY_TABLE, op);
    }

    Operator srcOpInfo = null;
    Operator lastPTFOp = null;

    if(queryProperties.hasPTF()){
      //After processing subqueries and source tables, process
      // partitioned table functions

      Map<ASTNode, PTFInvocationSpec> ptfNodeToSpec = qb.getPTFNodeToSpec();
      if ( ptfNodeToSpec != null ) {
        for(Entry<ASTNode, PTFInvocationSpec> entry : ptfNodeToSpec.entrySet()) {
          ASTNode ast = entry.getKey();
          PTFInvocationSpec spec = entry.getValue();
          String inputAlias = spec.getQueryInputName();
          Operator inOp = aliasToOpInfo.get(inputAlias);
          if ( inOp == null ) {
            throw new SemanticException(generateErrorMessage(ast,
                "Cannot resolve input Operator for PTF invocation"));
          }
          lastPTFOp = genPTFPlan(spec, inOp);
          String ptfAlias = spec.getFunction().getAlias();
          if ( ptfAlias != null ) {
            aliasToOpInfo.put(ptfAlias, lastPTFOp);
          }
        }
      }

    }

    // For all the source tables that have a lateral view, attach the
    // appropriate operators to the TS
    genLateralViewPlans(aliasToOpInfo, qb);


    // process join
    if (qb.getParseInfo().getJoinExpr() != null) {
      ASTNode joinExpr = qb.getParseInfo().getJoinExpr();

      if (joinExpr.getToken().getType() == HiveParser.TOK_UNIQUEJOIN) {
        QBJoinTree joinTree = genUniqueJoinTree(qb, joinExpr, aliasToOpInfo);
        qb.setQbJoinTree(joinTree);
      } else {
        QBJoinTree joinTree = genJoinTree(qb, joinExpr, aliasToOpInfo);
        qb.setQbJoinTree(joinTree);
        /*
         * if there is only one destination in Query try to push where predicates
         * as Join conditions
         */
        Set<String> dests = qb.getParseInfo().getClauseNames();
        if ( dests.size() == 1 && joinTree.getNoOuterJoin()) {
          String dest = dests.iterator().next();
          ASTNode whereClause = qb.getParseInfo().getWhrForClause(dest);
          if ( whereClause != null ) {
            extractJoinCondsFromWhereClause(joinTree,
                (ASTNode) whereClause.getChild(0),
                aliasToOpInfo );
          }
        }

        if (!disableJoinMerge) {
          mergeJoinTree(qb);
        }
      }

      // if any filters are present in the join tree, push them on top of the
      // table
      pushJoinFilters(qb, qb.getQbJoinTree(), aliasToOpInfo);
      srcOpInfo = genJoinPlan(qb, aliasToOpInfo);
    } else {
      // Now if there are more than 1 sources then we have a join case
      // later we can extend this to the union all case as well
      srcOpInfo = aliasToOpInfo.values().iterator().next();
      // with ptfs, there maybe more (note for PTFChains:
      // 1 ptf invocation may entail multiple PTF operators)
      srcOpInfo = lastPTFOp != null ? lastPTFOp : srcOpInfo;
    }

    Operator bodyOpInfo = genBodyPlan(qb, srcOpInfo, aliasToOpInfo);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created Plan for Query Block " + qb.getId());
    }

    if (qb.getAlias() != null) {
      rewriteRRForSubQ(qb.getAlias(), bodyOpInfo, skipAmbiguityCheck);
    }

    setQB(qb);
    return bodyOpInfo;
  }

  // change curr ops row resolver's tab aliases to subq alias
  private void rewriteRRForSubQ(String alias, Operator operator, boolean skipAmbiguityCheck)
      throws SemanticException {
    RowResolver rr = opParseCtx.get(operator).getRowResolver();
    RowResolver newRR = new RowResolver();
    for (ColumnInfo colInfo : rr.getColumnInfos()) {
      String name = colInfo.getInternalName();
      String[] tmp = rr.reverseLookup(name);
      if ("".equals(tmp[0]) || tmp[1] == null) {
        // ast expression is not a valid column name for table
        tmp[1] = colInfo.getInternalName();
      } else if (newRR.get(alias, tmp[1]) != null) {
        // enforce uniqueness of column names
        if (!skipAmbiguityCheck) {
          throw new SemanticException(ErrorMsg.AMBIGUOUS_COLUMN.getMsg(tmp[1] + " in " + alias));
        }
        // if it's wrapped by top-level select star query, skip ambiguity check (for backward compatibility)
        tmp[1] = colInfo.getInternalName();
      }
      newRR.put(alias, tmp[1], colInfo);
    }
    opParseCtx.get(operator).setRowResolver(newRR);
  }

  Path dummyPath;
  public Table getDummyTable() throws SemanticException {
    if (dummyPath == null) {
      dummyPath = createDummyFile();
    }

    Table desc = new Table(DUMMY_DATABASE, DUMMY_TABLE);
    desc.getTTable().getSd().setLocation(dummyPath.toString());
    desc.getTTable().getSd().getSerdeInfo().setSerializationLib(NullStructSerDe.class.getName());
    desc.setInputFormatClass(NullRowsInputFormat.class);
    desc.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);
    return desc;
  }

  // add dummy data for not removed by CombineHiveInputFormat, etc.
  private Path createDummyFile() throws SemanticException {
    Path dummyPath = new Path(ctx.getMRScratchDir(), "dummy_path");
    Path dummyFile = new Path(dummyPath, "dummy_file");
    FSDataOutputStream fout = null;
    try {
      FileSystem fs = dummyFile.getFileSystem(conf);
      if (fs.exists(dummyFile)) {
        return dummyPath;
      }
      fout = fs.create(dummyFile);
      fout.write(1);
      fout.close();
    } catch (IOException e) {
      throw new SemanticException(e);
    } finally {
      IOUtils.closeStream(fout);
    }
    return dummyPath;
  }

  /**
   * Generates the operator DAG needed to implement lateral views and attaches
   * it to the TS operator.
   *
   * @param aliasToOpInfo
   *          A mapping from a table alias to the TS operator. This function
   *          replaces the operator mapping as necessary
   * @param qb
   * @throws SemanticException
   */
  private void genLateralViewPlans(Map<String, Operator> aliasToOpInfo, QB qb)
      throws SemanticException {
    Map<String, List<ASTNode>> aliasToLateralViews = qb.getParseInfo().getAliasToLateralViews();
    for (Entry<String, Operator> e : aliasToOpInfo.entrySet()) {
      String alias = e.getKey();
      // See if the alias has a lateral view. If so, chain the lateral view
      // operator on
      List<ASTNode> lateralViews = aliasToLateralViews.get(alias);
      if (lateralViews != null) {
        Operator op = e.getValue();

        for (ASTNode lateralViewTree : aliasToLateralViews.get(alias)) {
          // There are 2 paths from the TS operator (or a previous LVJ operator)
          // to the same LateralViewJoinOperator.
          // TS -> SelectOperator(*) -> LateralViewJoinOperator
          // TS -> SelectOperator (gets cols for UDTF) -> UDTFOperator0
          // -> LateralViewJoinOperator
          //

          op = genLateralViewPlan(qb, op, lateralViewTree);
        }
        e.setValue(op);
      }
    }
  }

  private Operator genLateralViewPlanForDest(String dest, QB qb, Operator op)
      throws SemanticException {
    ASTNode lateralViewTree = qb.getParseInfo().getDestToLateralView().get(dest);
    if (lateralViewTree != null) {
      return genLateralViewPlan(qb, op, lateralViewTree);
    }
    return op;
  }

  private Operator genLateralViewPlan(QB qb, Operator op, ASTNode lateralViewTree)
      throws SemanticException {
    RowResolver lvForwardRR = new RowResolver();
    RowResolver source = opParseCtx.get(op).getRowResolver();
    Map<String, ExprNodeDesc> lvfColExprMap = new HashMap<String, ExprNodeDesc>();
    Map<String, ExprNodeDesc> selColExprMap = new HashMap<String, ExprNodeDesc>();
    List<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    List<String> colNames = new ArrayList<String>();
    for (ColumnInfo col : source.getColumnInfos()) {
      String[] tabCol = source.reverseLookup(col.getInternalName());
      lvForwardRR.put(tabCol[0], tabCol[1], col);
      ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(col);
      colList.add(colExpr);
      colNames.add(colExpr.getColumn());
      lvfColExprMap.put(col.getInternalName(), colExpr);
      selColExprMap.put(col.getInternalName(), colExpr.clone());
    }

    Operator lvForward = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new LateralViewForwardDesc(), new RowSchema(lvForwardRR.getColumnInfos()),
        op), lvForwardRR);
    lvForward.setColumnExprMap(lvfColExprMap);

    // The order in which the two paths are added is important. The
    // lateral view join operator depends on having the select operator
    // give it the row first.

    // Get the all path by making a select(*).
    RowResolver allPathRR = opParseCtx.get(lvForward).getRowResolver();
    // Operator allPath = op;
    SelectDesc sDesc = new SelectDesc(colList, colNames, false);
    sDesc.setSelStarNoCompute(true);
    Operator allPath = putOpInsertMap(OperatorFactory.getAndMakeChild(
        sDesc, new RowSchema(allPathRR.getColumnInfos()),
        lvForward), allPathRR);
    allPath.setColumnExprMap(selColExprMap);
    int allColumns = allPathRR.getColumnInfos().size();
    // Get the UDTF Path
    QB blankQb = new QB(null, null, false);
    Operator udtfPath = genSelectPlan(null, (ASTNode) lateralViewTree
            .getChild(0), blankQb, lvForward, null,
        lateralViewTree.getType() == HiveParser.TOK_LATERAL_VIEW_OUTER);
    // add udtf aliases to QB
    for (String udtfAlias : blankQb.getAliases()) {
      qb.addAlias(udtfAlias);
    }
    RowResolver udtfPathRR = opParseCtx.get(udtfPath).getRowResolver();

    // Merge the two into the lateral view join
    // The cols of the merged result will be the combination of both the
    // cols of the UDTF path and the cols of the all path. The internal
    // names have to be changed to avoid conflicts

    RowResolver lateralViewRR = new RowResolver();
    List<String> outputInternalColNames = new ArrayList<String>();


    // For PPD, we need a column to expression map so that during the walk,
    // the processor knows how to transform the internal col names.
    // Following steps are dependant on the fact that we called
    // LVmerge.. in the above order
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    LVmergeRowResolvers(allPathRR, lateralViewRR, colExprMap, outputInternalColNames);
    LVmergeRowResolvers(udtfPathRR, lateralViewRR, colExprMap, outputInternalColNames);

    Operator lateralViewJoin = putOpInsertMap(OperatorFactory
        .getAndMakeChild(new LateralViewJoinDesc(allColumns, outputInternalColNames),
            new RowSchema(lateralViewRR.getColumnInfos()), allPath,
            udtfPath), lateralViewRR);
    lateralViewJoin.setColumnExprMap(colExprMap);
    return lateralViewJoin;
  }

  /**
   * A helper function that gets all the columns and respective aliases in the
   * source and puts them into dest. It renames the internal names of the
   * columns based on getColumnInternalName(position).
   *
   * Note that this helper method relies on RowResolver.getColumnInfos()
   * returning the columns in the same order as they will be passed in the
   * operator DAG.
   *
   * @param source
   * @param dest
   * @param colExprMap
   * @param outputInternalColNames
   *          - a list to which the new internal column names will be added, in
   *          the same order as in the dest row resolver
   */
  private void LVmergeRowResolvers(RowResolver source, RowResolver dest,
                                   Map<String, ExprNodeDesc> colExprMap, List<String> outputInternalColNames) {
    for (ColumnInfo c : source.getColumnInfos()) {
      String internalName = getColumnInternalName(outputInternalColNames.size());
      outputInternalColNames.add(internalName);
      ColumnInfo newCol = new ColumnInfo(internalName, c.getType(), c
          .getTabAlias(), c.getIsVirtualCol(), c.isHiddenVirtualCol());
      String[] tableCol = source.reverseLookup(c.getInternalName());
      String tableAlias = tableCol[0];
      String colAlias = tableCol[1];
      dest.put(tableAlias, colAlias, newCol);
      colExprMap.put(internalName, new ExprNodeColumnDesc(c));
    }
  }

  @SuppressWarnings("nls")
  Phase1Ctx initPhase1Ctx() {
    Phase1Ctx ctx_1 = new Phase1Ctx();
    ctx_1.nextNum = 0;
    ctx_1.dest = "reduce";

    return ctx_1;
  }

  @Override
  public void init(boolean clearPartsCache) {
    // clear most members
    reset(clearPartsCache);

    // init
    this.qb = new QB(null, null, false);
  }

  @Override
  @SuppressWarnings("nls")
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    analyzeInternal(ast, PlannerContext::new);
  }

  /**
   * Planner specific stuff goes in here.
   */
  static class PlannerContext {

    void setCTASToken(ASTNode child) {
    }

    void setViewToken(ASTNode child) {
    }

    void setInsertToken(ASTNode ast, boolean isTmpFileDest) {
    }

    void setMultiInsertToken(ASTNode child) {
    }

    void resetToken() {
    }
  }

  protected Table getTableObjectByName(String tableName, boolean throwException) throws HiveException {
    if (!tabNameToTabObject.containsKey(tableName)) {
      Table table = db.getTable(tableName, throwException);
      if (table != null) {
        tabNameToTabObject.put(tableName, table);
      }
      return table;
    } else {
      return tabNameToTabObject.get(tableName);
    }
  }

  public Table getTableObjectByName(String tableName) throws HiveException {
    return getTableObjectByName(tableName, true);
  }

  private void walkASTMarkTABREF(TableMask tableMask, ASTNode ast, Set<String> cteAlias, Context ctx)
      throws SemanticException {
    Queue<Node> queue = new LinkedList<>();
    queue.add(ast);
    Map<HivePrivilegeObject, MaskAndFilterInfo> basicInfos = new LinkedHashMap<>();
    while (!queue.isEmpty()) {
      ASTNode astNode = (ASTNode) queue.poll();
      if (astNode.getToken().getType() == HiveParser.TOK_TABREF) {
        int aliasIndex = 0;
        StringBuilder additionalTabInfo = new StringBuilder();
        for (int index = 1; index < astNode.getChildCount(); index++) {
          ASTNode ct = (ASTNode) astNode.getChild(index);
          if (ct.getToken().getType() == HiveParser.TOK_TABLEBUCKETSAMPLE
              || ct.getToken().getType() == HiveParser.TOK_TABLESPLITSAMPLE
              || ct.getToken().getType() == HiveParser.TOK_TABLEPROPERTIES) {
            additionalTabInfo.append(ctx.getTokenRewriteStream().toString(ct.getTokenStartIndex(),
                ct.getTokenStopIndex()));
          } else {
            aliasIndex = index;
          }
        }

        ASTNode tableTree = (ASTNode) (astNode.getChild(0));

        String tabIdName = getUnescapedName(tableTree);

        String alias;
        if (aliasIndex != 0) {
          alias = unescapeIdentifier(astNode.getChild(aliasIndex).getText());
        } else {
          alias = getUnescapedUnqualifiedTableName(tableTree);
        }

        // We need to know if it is CTE or not.
        // A CTE may have the same name as a table.
        // For example,
        // with select TAB1 [masking] as TAB2
        // select * from TAB2 [no masking]
        if (cteAlias.contains(tabIdName)) {
          continue;
        }

        Table table = null;
        try {
          table = getTableObjectByName(tabIdName, false);
        } catch (HiveException e) {
          // This should not happen.
          throw new SemanticException("Got exception though getTableObjectByName method should ignore it");
        }
        if (table == null) {
          // Table may not be found when materialization of CTE is on.
          STATIC_LOG.debug("Table " + tabIdName + " is not found in walkASTMarkTABREF.");
          continue;
        }

        if (table.isMaterializedView()) {
          // When we are querying a materialized view directly, we check whether the source tables
          // do not apply any policies.
          for (SourceTable sourceTable : table.getMVMetadata().getSourceTables()) {
            String qualifiedTableName = TableName.getDbTable(
                    sourceTable.getTable().getDbName(), sourceTable.getTable().getTableName());
            try {
              table = getTableObjectByName(qualifiedTableName, true);
            } catch (HiveException e) {
              // This should not happen.
              throw new SemanticException("Table " + qualifiedTableName +
                  " not found when trying to obtain it to check masking/filtering policies");
            }

            List<String> colNames = new ArrayList<>();
            extractColumnInfos(table, colNames, new ArrayList<>());

            basicInfos.put(new HivePrivilegeObject(table.getDbName(), table.getTableName(), colNames,
                table.getOwner(), table.getOwnerType()), null);
          }
        } else {
          List<String> colNames;
          List<String> colTypes;
          if (this.ctx.isCboSucceeded() && this.columnAccessInfo != null &&
              (colNames = this.columnAccessInfo.getTableToColumnAllAccessMap().get(table.getCompleteName())) != null) {
            Map<String, String> colNameToType = table.getAllCols().stream()
                .collect(Collectors.toMap(FieldSchema::getName, FieldSchema::getType));
            colTypes = colNames.stream().map(colNameToType::get).collect(Collectors.toList());
          } else {
            colNames = new ArrayList<>();
            colTypes = new ArrayList<>();
            extractColumnInfos(table, colNames, colTypes);
          }

          basicInfos.put(new HivePrivilegeObject(table.getDbName(), table.getTableName(), colNames,
              table.getOwner(), table.getOwnerType()),
              new MaskAndFilterInfo(colTypes, additionalTabInfo.toString(), alias, astNode, table.isView(), table.isNonNative()));
        }
      }
      if (astNode.getChildCount() > 0 && !IGNORED_TOKENS.contains(astNode.getToken().getType())) {
        for (Node child : astNode.getChildren()) {
          queue.offer(child);
        }
      }
    }
    List<HivePrivilegeObject> basicPrivObjs = new ArrayList<>(basicInfos.keySet());
    List<HivePrivilegeObject> needRewritePrivObjs = tableMask.applyRowFilterAndColumnMasking(basicPrivObjs);
    if (needRewritePrivObjs != null && !needRewritePrivObjs.isEmpty()) {
      for (HivePrivilegeObject privObj : needRewritePrivObjs) {
        MaskAndFilterInfo info = basicInfos.get(privObj);
        // First we check whether entity actually needs masking or filtering. Query based Compaction related queries are
        // excluded from all masking and filtering.
        if (tableMask.needsMaskingOrFiltering(privObj) && !SessionState.get().isCompaction()) {
          if (info == null) {
            // This is a table used by a materialized view
            // Currently we do not support querying directly a materialized view
            // when mask/filter should be applied on source tables
            throw new SemanticException(ErrorMsg.MASKING_FILTERING_ON_MATERIALIZED_VIEWS_SOURCES,
                privObj.getDbname(), privObj.getObjectName());
          } else {
            String replacementText = tableMask.create(privObj, info);
            // We don't support masking/filtering against ACID query at the moment
            if (ctx.getIsUpdateDeleteMerge()) {
              throw new SemanticException(ErrorMsg.MASKING_FILTERING_ON_ACID_NOT_SUPPORTED,
                  privObj.getDbname(), privObj.getObjectName());
            }
            tableMask.setNeedsRewrite(true);
            tableMask.addTranslation(info.astNode, replacementText);
          }
        }
      }
    }
  }

  private void extractColumnInfos(Table table, List<String> colNames, List<String> colTypes) {
    for (FieldSchema col : table.getAllCols()) {
      colNames.add(col.getName());
      colTypes.add(col.getType());
    }
  }

  // We walk through the AST.
  // We replace all the TOK_TABREF by adding additional masking and filter if
  // the table needs to be masked or filtered.
  // For the replacement, we leverage the methods that are used for
  // unparseTranslator.
  private ParseResult rewriteASTWithMaskAndFilter(TableMask tableMask, ASTNode ast, TokenRewriteStream tokenRewriteStream,
                                                Context ctx, Hive db)
      throws SemanticException {
    // 1. collect information about CTE if there is any.
    // The base table of CTE should be masked.
    // The CTE itself should not be masked in the references in the following main query.
    Set<String> cteAlias = new HashSet<>();
    if (ast.getChildCount() > 0
        && HiveParser.TOK_CTE == ((ASTNode) ast.getChild(0)).getToken().getType()) {
      // the structure inside CTE is like this
      // TOK_CTE
      // TOK_SUBQUERY
      // sq1 (may refer to sq2)
      // ...
      // TOK_SUBQUERY
      // sq2
      ASTNode cte = (ASTNode) ast.getChild(0);
      // we start from sq2, end up with sq1.
      for (int index = cte.getChildCount() - 1; index >= 0; index--) {
        ASTNode subq = (ASTNode) cte.getChild(index);
        String alias = unescapeIdentifier(subq.getChild(1).getText());
        if (cteAlias.contains(alias)) {
          throw new SemanticException("Duplicate definition of " + alias);
        } else {
          cteAlias.add(alias);
          walkASTMarkTABREF(tableMask, subq, cteAlias, ctx);
        }
      }
      // walk the other part of ast
      for (int index = 1; index < ast.getChildCount(); index++) {
        walkASTMarkTABREF(tableMask, (ASTNode) ast.getChild(index), cteAlias, ctx);
      }
    }
    // there is no CTE, walk the whole AST
    else {
      walkASTMarkTABREF(tableMask, ast, cteAlias, ctx);
    }
    // 2. rewrite the AST, replace TABREF with masking/filtering
    if (tableMask.needsRewrite()) {
      quoteIdentifierTokens(tokenRewriteStream);
      tableMask.applyTranslations(tokenRewriteStream);
      String rewrittenQuery = tokenRewriteStream.toString(
          ast.getTokenStartIndex(), ast.getTokenStopIndex());
      ASTNode rewrittenTree;
      try {
        // We pass a new empty context with our HiveConf so the lexer can
        // detect if allowQuotedId is enabled.
        Context rewriteCtx = new Context(conf);
        ctx.addSubContext(rewriteCtx);
        rewrittenTree = ParseUtils.parse(rewrittenQuery, rewriteCtx);
        return new ParseResult(rewrittenTree, rewriteCtx.getTokenRewriteStream(),
            rewriteCtx.getParsedTables());
      } catch (ParseException e) {
        throw new SemanticException(e);
      }
    } else {
      return new ParseResult(ast, ctx.getTokenRewriteStream(), ctx.getParsedTables());
    }
  }

  void gatherUserSuppliedFunctions(ASTNode ast) throws SemanticException {
    int tokenType = ast.getToken().getType();
    if (tokenType == HiveParser.TOK_FUNCTION ||
            tokenType == HiveParser.TOK_FUNCTIONDI ||
            tokenType == HiveParser.TOK_FUNCTIONSTAR) {
      if (ast.getChild(0).getType() == HiveParser.Identifier) {
        try {
          String functionName = unescapeIdentifier(ast.getChild(0).getText()).toLowerCase();
          String[] qualifiedFunctionName = FunctionUtils.getQualifiedFunctionNameParts(functionName);
          this.userSuppliedFunctions.add(qualifiedFunctionName[0]+"."+qualifiedFunctionName[1]);
        } catch (HiveException ex) {
          throw new SemanticException(ex.getMessage(), ex);
        }
      }
    }
    for (int i = 0; i < ast.getChildCount();i++) {
      gatherUserSuppliedFunctions((ASTNode) ast.getChild(i));
    }
  }

  boolean genResolvedParseTree(ASTNode ast, PlannerContext plannerCtx) throws SemanticException {
    ASTNode child = ast;
    this.ast = ast;
    viewsExpanded = new ArrayList<String>();

    // 1. analyze and process the position alias
    // step processPositionAlias out of genResolvedParseTree

    // 2. analyze create table command
    if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE) {
      // if it is not CTAS, we don't need to go further and just return
      if ((child = analyzeCreateTable(ast, qb, plannerCtx)) == null) {
        return false;
      }
    } else {
      // TODO: reiterate on this in HIVE-28750
      queryState.setCommandType(HiveOperation.QUERY);
    }

    // 3. analyze create view command
    if (ast.getToken().getType() == HiveParser.TOK_CREATE_MATERIALIZED_VIEW) {
      child = analyzeCreateView(ast, qb, plannerCtx);
      if (child == null) {
        return false;
      }
      viewSelect = child;
      // prevent view from referencing itself
      viewsExpanded.add(createVwDesc.getViewName());
    }

    if (forViewCreation) {
      viewsExpanded.add(fqViewName);
    }

    switch(ast.getToken().getType()) {
    case HiveParser.TOK_SET_AUTOCOMMIT:
      assert ast.getChildCount() == 1;
      if(ast.getChild(0).getType() == HiveParser.TOK_TRUE) {
        setAutoCommitValue(true);
      }
      else if(ast.getChild(0).getType() == HiveParser.TOK_FALSE) {
        setAutoCommitValue(false);
      }
      else {
        assert false : "Unexpected child of TOK_SET_AUTOCOMMIT: " + ast.getChild(0).getType();
      }
      //fall through
    case HiveParser.TOK_START_TRANSACTION:
    case HiveParser.TOK_COMMIT:
    case HiveParser.TOK_ROLLBACK:
      if(!(conf.getBoolVar(ConfVars.HIVE_IN_TEST) || conf.getBoolVar(ConfVars.HIVE_IN_TEZ_TEST))) {
        throw new IllegalStateException(HiveOperation.operationForToken(ast.getToken().getType()) +
            " is not supported yet.");
      }
      queryState.setCommandType(HiveOperation.operationForToken(ast.getToken().getType()));
      return false;
    }

    // masking and filtering should be created here
    // the basic idea is similar to unparseTranslator.
    tableMask = new TableMask(this, conf, ctx.isSkipTableMasking());

    // Gather UDFs referenced in query before VIEW expansion. This is used to
    // determine if authorization checks need to occur on the UDFs.
    gatherUserSuppliedFunctions(child);

    // 4. continue analyzing from the child ASTNode.
    Phase1Ctx ctx_1 = initPhase1Ctx();
    if (!doPhase1(child, qb, ctx_1, plannerCtx)) {
      // if phase1Result false return
      return false;
    }
    LOG.info("Completed phase 1 of Semantic Analysis");

    // 5. Resolve Parse Tree
    // Materialization is allowed if it is not a view definition
    getMetaData(qb, createVwDesc == null && !forViewCreation);
    LOG.info("Completed getting MetaData in Semantic Analysis");

    return true;
  }

  void getHintsFromQB(QB qb, List<ASTNode> hints) {
    if (qb.getParseInfo().getHints() != null) {
      hints.add(qb.getParseInfo().getHints());
    }

    Set<String> aliases = qb.getSubqAliases();

    for (String alias : aliases) {
      getHintsFromQB(qb.getSubqForAlias(alias), hints);
    }
  }

  private void getHintsFromQB(QBExpr qbExpr, List<ASTNode> hints) {
    QBExpr qbExpr1 = qbExpr.getQBExpr1();
    QBExpr qbExpr2 = qbExpr.getQBExpr2();
    QB qb = qbExpr.getQB();

    if (qbExpr1 != null) {
      getHintsFromQB(qbExpr1, hints);
    }
    if (qbExpr2 != null) {
      getHintsFromQB(qbExpr2, hints);
    }
    if (qb != null) {
      getHintsFromQB(qb, hints);
    }
  }

  Operator genOPTree(ASTNode ast, PlannerContext plannerCtx) throws SemanticException {
    // fetch all the hints in qb
    List<ASTNode> hintsList = new ArrayList<>();
    getHintsFromQB(qb, hintsList);
    getQB().getParseInfo().setHintList(hintsList);
    return genPlan(qb);
  }

  private void removeOBInSubQuery(QBExpr qbExpr) {
    if (qbExpr == null) {
      return;
    }

    if (qbExpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      QB subQB = qbExpr.getQB();
      QBParseInfo parseInfo = subQB.getParseInfo();
      String alias = qbExpr.getAlias();
      Map<String, ASTNode> destToOrderBy = parseInfo.getDestToOrderBy();
      Map<String, ASTNode> destToSortBy = parseInfo.getDestToSortBy();
      final String warning = "WARNING: Order/Sort by without limit in sub query or view [" +
          alias + "] is removed, as it's pointless and bad for performance.";
      if (destToOrderBy != null) {
        for (String dest : destToOrderBy.keySet()) {
          if (parseInfo.getDestLimit(dest) == null) {
            removeASTChild(destToOrderBy.get(dest));
            destToOrderBy.remove(dest);
            console.printInfo(warning);
          }
        }
      }
      if (destToSortBy != null) {
        for (String dest : destToSortBy.keySet()) {
          if (parseInfo.getDestLimit(dest) == null) {
            removeASTChild(destToSortBy.get(dest));
            destToSortBy.remove(dest);
            console.printInfo(warning);
          }
        }
      }
    } else {
      removeOBInSubQuery(qbExpr.getQBExpr1());
      removeOBInSubQuery(qbExpr.getQBExpr2());
    }
  }

  private void removeASTChild(ASTNode node) {
    Optional.ofNullable(node.getParent())
        .ifPresent(parent -> {
          parent.deleteChild(node.getChildIndex());
          node.setParent(null);
        });
  }

  protected void compilePlan(ParseContext pCtx) throws SemanticException{
    if (!ctx.getExplainLogical()) {
      TaskCompiler compiler = TaskCompilerFactory.getCompiler(conf, pCtx);
      compiler.init(queryState, console, db);
      compiler.compile(pCtx, rootTasks, inputs, outputs);
      fetchTask = pCtx.getFetchTask();
    }
  }

  @SuppressWarnings("checkstyle:methodlength")
  void analyzeInternal(ASTNode ast, Supplier<PlannerContext> pcf) throws SemanticException {
    LOG.info("Starting Semantic Analysis");
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.GENERATE_RESOLVED_PARSETREE);
    // 1. Generate Resolved Parse tree from syntax tree
    boolean needsTransform = needsTransform();
    //change the location of position alias process here
    processPositionAlias(ast);
    cacheTableHelper.populateCache(ctx.getParsedTables(), conf, getTxnMgr());
    PlannerContext plannerCtx = pcf.get();
    if (!genResolvedParseTree(ast, plannerCtx)) {
      return;
    }
    if (tablesFromReadEntities(inputs).stream().anyMatch(AcidUtils::isTransactionalTable)) {
      queryState.getValidTxnList();
    }

    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_REMOVE_ORDERBY_IN_SUBQUERY)) {
      for (String alias : qb.getSubqAliases()) {
        removeOBInSubQuery(qb.getSubqForAlias(alias));
      }
    }

    final String llapIOETLSkipFormat = HiveConf.getVar(conf, ConfVars.LLAP_IO_ETL_SKIP_FORMAT);
    if (qb.getParseInfo().hasInsertTables() || qb.isCTAS()) {
      if (llapIOETLSkipFormat.equalsIgnoreCase("encode")) {
        conf.setBoolean(ConfVars.LLAP_IO_ENCODE_ENABLED.varname, false);
        LOG.info("Disabling LLAP IO encode as ETL query is detected");
      } else if (llapIOETLSkipFormat.equalsIgnoreCase("all")) {
        conf.setBoolean(ConfVars.LLAP_IO_ENABLED.varname, false);
        LOG.info("Disabling LLAP IO as ETL query is detected");
      }
    }

    // Check query results cache.
    // If no masking/filtering required, then we can check the cache now, before
    // generating the operator tree and going through CBO.
    // Otherwise we have to wait until after the masking/filtering step.
    boolean isCacheEnabled = isResultsCacheEnabled();
    QueryResultsCache.LookupInfo lookupInfo = null;
    if (isCacheEnabled && !needsTransform && queryTypeCanUseCache(qb)) {
      lookupInfo = createLookupInfoForQuery(ast);
      if (checkResultsCache(lookupInfo, false)) {
        return;
      }
    }

    ASTNode astForMasking;
    if (isCBOExecuted() && needsTransform &&
        (qb.isCTAS() || forViewCreation || qb.isMaterializedView() || qb.isMultiDestQuery())) {
      // If we use CBO and we may apply masking/filtering policies, we create a copy of the ast.
      // The reason is that the generation of the operator tree may modify the initial ast,
      // but if we need to parse for a second time, we would like to parse the unmodified ast.
      astForMasking = (ASTNode) ParseDriver.adaptor.dupTree(ast);
    } else {
      astForMasking = ast;
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.GENERATE_RESOLVED_PARSETREE);

    // 2. Gen OP Tree from resolved Parse Tree
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.LOGICALPLAN_AND_HIVE_OPERATOR_TREE);
    sinkOp = genOPTree(ast, plannerCtx);

    boolean usesMasking = false;
    if (!forViewCreation && ast.getToken().getType() != HiveParser.TOK_CREATE_MATERIALIZED_VIEW &&
        (tableMask.isEnabled() && analyzeRewrite == null)) {
      // Here we rewrite the * and also the masking table
      ParseResult rewrittenResult = rewriteASTWithMaskAndFilter(tableMask, astForMasking, ctx.getTokenRewriteStream(),
          ctx, db);
      ASTNode rewrittenAST = rewrittenResult.getTree();
      if (astForMasking != rewrittenAST) {
        usesMasking = true;
        plannerCtx = pcf.get();
        ctx.setSkipTableMasking(true);
        ctx.setTokenRewriteStream(rewrittenResult.getTokenRewriteStream());
        init(true);
        //change the location of position alias process here
        processPositionAlias(rewrittenAST);
        genResolvedParseTree(rewrittenAST, plannerCtx);
        if (this instanceof CalcitePlanner) {
          ((CalcitePlanner) this).resetCalciteConfiguration();
        }
        sinkOp = genOPTree(rewrittenAST, plannerCtx);
      }
    }

    // validate if this sink operation is allowed for non-native tables
    if (sinkOp instanceof FileSinkOperator) {
      FileSinkOperator fileSinkOperator = (FileSinkOperator) sinkOp;
      Optional<HiveStorageHandler> handler = Optional.ofNullable(fileSinkOperator)
          .map(FileSinkOperator::getConf)
          .map(FileSinkDesc::getTable)
          .map(Table::getStorageHandler);
      if (handler.isPresent()) {
         handler.get().validateSinkDesc(fileSinkOperator.getConf());
      }
    }

    // Check query results cache
    // In the case that row or column masking/filtering was required, we do not support caching.
    // TODO: Enable caching for queries with masking/filtering
    if (isCacheEnabled && needsTransform && !usesMasking && queryTypeCanUseCache(qb)) {
      lookupInfo = createLookupInfoForQuery(ast);
      if (checkResultsCache(lookupInfo, false)) {
        return;
      }
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.LOGICALPLAN_AND_HIVE_OPERATOR_TREE);

    // 3. Deduce Resultset Schema
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.DEDUCE_RESULTSET_SCHEMA);
    if ((forViewCreation || createVwDesc != null) && !this.ctx.isCboSucceeded()) {
      resultSchema = convertRowSchemaToViewSchema(opParseCtx.get(sinkOp).getRowResolver());
    } else {
      // resultSchema will be null if
      // (1) cbo is disabled;
      // (2) or cbo is enabled with AST return path (whether succeeded or not,
      // resultSchema will be re-initialized)
      // It will only be not null if cbo is enabled with new return path and it
      // succeeds.
      if (resultSchema == null) {
        resultSchema = convertRowSchemaToResultSetSchema(opParseCtx.get(sinkOp).getRowResolver(),
            HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_RESULTSET_USE_UNIQUE_COLUMN_NAMES));
      }
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.DEDUCE_RESULTSET_SCHEMA);

    // 4. Generate Parse Context for Optimizer & Physical compiler
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.PARSE_CONTEXT_GENERATION);
    copyInfoToQueryProperties(queryProperties);
    ParseContext pCtx = new ParseContext(queryState, opToPartPruner, opToPartList, topOps,
        new HashSet<JoinOperator>(joinContext.keySet()),
        new HashSet<SMBMapJoinOperator>(smbMapJoinContext.keySet()),
        loadTableWork, loadFileWork, columnStatsAutoGatherContexts, ctx, idToTableNameMap, destTableId, uCtx,
        listMapJoinOpsNoReducer, prunedPartitions, tabNameToTabObject, opToSamplePruner,
        globalLimitCtx, nameToSplitSample, inputs, rootTasks, opToPartToSkewedPruner,
        viewAliasToInput, reduceSinkOperatorsAddedByEnforceBucketingSorting,
        analyzeRewrite, tableDesc, createVwDesc, materializedViewUpdateDesc,
        queryProperties, viewProjectToTableSchema);

    // Set the semijoin hints in parse context
    pCtx.setSemiJoinHints(parseSemiJoinHint(getQB().getParseInfo().getHintList()));
    // Set the mapjoin hint if it needs to be disabled.
    pCtx.setDisableMapJoin(disableMapJoinWithHint(getQB().getParseInfo().getHintList()));

    if (forViewCreation && shouldComputeLineage(conf)) {
      // Generate lineage info if LineageLogger hook is configured.
      // Add the transformation that computes the lineage information.
      List<Transform> transformations = new ArrayList<Transform>();
      transformations.add(new HiveOpConverterPostProc());
      transformations.add(Generator.fromConf(conf));
      for (Transform t : transformations) {
        pCtx = t.transform(pCtx);
      }
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.PARSE_CONTEXT_GENERATION);

    // 5. Take care of view creation
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.SAVE_AND_VALIDATE_VIEW);
    if (createVwDesc != null) {
      if (ctx.getExplainAnalyze() == AnalyzeState.RUNNING) {
        return;
      }

      if (!ctx.isCboSucceeded()) {
        saveViewDefinition();
      }

      // validate the create view statement at this point, the createVwDesc gets
      // all the information for semanticcheck
      validateCreateView();

      createVwDesc.setTablesUsed(pCtx.getTablesUsed());
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.SAVE_AND_VALIDATE_VIEW);

    // If we're creating views and ColumnAccessInfo is already created, we should not run these, since
    // it means that in step 2, the ColumnAccessInfo was already created
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.LOGICAL_OPTIMIZATION);
    if (!forViewCreation ||  getColumnAccessInfo() == null) {
      // 6. Generate table access stats if required
      if (HiveConf.getBoolVar(this.conf, HiveConf.ConfVars.HIVE_STATS_COLLECT_TABLEKEYS)) {
        TableAccessAnalyzer tableAccessAnalyzer = new TableAccessAnalyzer(pCtx);
        setTableAccessInfo(tableAccessAnalyzer.analyzeTableAccess());
      }
      AuxOpTreeSignature.linkAuxSignatures(pCtx);
      // 7. Perform Logical optimization
      if (LOG.isDebugEnabled()) {
        LOG.debug("Before logical optimization\n" + Operator.toString(pCtx.getTopOps().values()));
      }
      Optimizer optm = new Optimizer();
      optm.setPctx(pCtx);
      optm.initialize(conf);
      pCtx = optm.optimize();
      if (pCtx.getColumnAccessInfo() != null) {
        // set ColumnAccessInfo for view column authorization
        setColumnAccessInfo(pCtx.getColumnAccessInfo());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("After logical optimization\n" + Operator.toString(pCtx.getTopOps().values()));
      }

      // 8. Generate column access stats if required - wait until column pruning
      // takes place during optimization
      boolean isColumnInfoNeedForAuth = SessionState.get().isAuthorizationModeV2()
              && HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED);
      if (isColumnInfoNeedForAuth
              || HiveConf.getBoolVar(this.conf, HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS)) {
        ColumnAccessAnalyzer columnAccessAnalyzer = new ColumnAccessAnalyzer(pCtx);
        // view column access info is carried by this.getColumnAccessInfo().
        setColumnAccessInfo(columnAccessAnalyzer.analyzeColumnAccess(this.getColumnAccessInfo()));
      }
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.LOGICAL_OPTIMIZATION);

    if (forViewCreation) {
      return;
    }

    // 9. Optimize Physical op tree & Translate to target execution engine (MR,
    // TEZ..)
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.PHYSICAL_OPTIMIZATION);
    compilePlan(pCtx);
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.PHYSICAL_OPTIMIZATION);

    //find all Acid FileSinkOperatorS
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.POST_PROCESSING);
    new QueryPlanPostProcessor(rootTasks, acidFileSinks, ctx.getExecutionId());

    // 10. Attach CTAS/Insert-Commit-hooks for Storage Handlers
    final Optional<TezTask> optionalTezTask =
        rootTasks.stream().filter(task -> task instanceof TezTask).map(task -> (TezTask) task)
            .findFirst();
    if (optionalTezTask.isPresent()) {
      final TezTask tezTask = optionalTezTask.get();
      rootTasks.stream()
          .filter(task -> task.getWork() instanceof DDLWork)
          .map(task -> (DDLWork) task.getWork())
          .filter(ddlWork -> ddlWork.getDDLDesc() instanceof PreInsertTableDesc)
          .map(ddlWork -> (PreInsertTableDesc)ddlWork.getDDLDesc())
          .map(desc -> new InsertCommitHookDesc(desc.getTable(), desc.isOverwrite()))
          .forEach(insertCommitHookDesc -> tezTask.addDependentTask(
              TaskFactory.get(new DDLWork(getInputs(), getOutputs(), insertCommitHookDesc), conf)));
    }

    LOG.info("Completed plan generation");

    // 11. put accessed columns to readEntity
    if (HiveConf.getBoolVar(this.conf, HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS)) {
      putAccessedColumnsToReadEntity(inputs, columnAccessInfo);
    }

    if (isCacheEnabled && lookupInfo != null) {
      if (queryCanBeCached()) {
        // Last chance - check if the query is available in the cache.
        // Since we have already generated a query plan, using a cached query result at this point
        // requires SemanticAnalyzer state to be reset.
        if (checkResultsCache(lookupInfo, true)) {
          LOG.info("Cached result found on second lookup");
        } else {
          QueryResultsCache.QueryInfo queryInfo = createCacheQueryInfoForQuery(lookupInfo);

          // Specify that the results of this query can be cached.
          setCacheUsage(new CacheUsage(
              CacheUsage.CacheStatus.CAN_CACHE_QUERY_RESULTS, queryInfo));
        }
      }
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.POST_PROCESSING);
  }

  private void putAccessedColumnsToReadEntity(Set<ReadEntity> inputs, ColumnAccessInfo columnAccessInfo) {
    Map<String, List<String>> tableToColumnAccessMap = columnAccessInfo.getTableToColumnAccessMap();
    if (tableToColumnAccessMap != null && !tableToColumnAccessMap.isEmpty()) {
      for(ReadEntity entity: inputs) {
        List<String> cols;
        switch (entity.getType()) {
        case TABLE:
          cols = tableToColumnAccessMap.get(entity.getTable().getCompleteName());
          if (cols != null && !cols.isEmpty()) {
            entity.getAccessedColumns().addAll(cols);
          }
          break;
        case PARTITION:
          cols = tableToColumnAccessMap.get(entity.getPartition().getTable().getCompleteName());
          if (cols != null && !cols.isEmpty()) {
            entity.getAccessedColumns().addAll(cols);
          }
          break;
        default:
          // no-op
        }
      }
    }
  }

  @Override
  public List<FieldSchema> getResultSchema() {
    return resultSchema;
  }

  public List<FieldSchema> getOriginalResultSchema() {
    return originalResultSchema;
  }

  protected void saveViewDefinition() throws SemanticException {
    // Make a copy of the statement's result schema, since we may
    // modify it below as part of imposing view column names.
    List<FieldSchema> derivedSchema =
        new ArrayList<FieldSchema>(resultSchema);
    ParseUtils.validateColumnNameUniqueness(derivedSchema);

    List<FieldSchema> imposedSchema = createVwDesc.getCols();
    if (imposedSchema != null) {
      int explicitColCount = imposedSchema.size();
      int derivedColCount = derivedSchema.size();
      if (explicitColCount != derivedColCount) {
        throw new SemanticException(generateErrorMessage(
            viewSelect,
            ErrorMsg.VIEW_COL_MISMATCH.getMsg()));
      }
    }

    // Preserve the original view definition as specified by the user.
    if (createVwDesc.getViewOriginalText() == null) {
      String originalText = ctx.getTokenRewriteStream().toString(
          viewSelect.getTokenStartIndex(), viewSelect.getTokenStopIndex());
      createVwDesc.setViewOriginalText(originalText);
    }

    // Now expand the view definition with extras such as explicit column
    // references; this expanded form is what we'll re-parse when the view is
    // referenced later.
    unparseTranslator.applyTranslations(ctx.getTokenRewriteStream());
    String expandedText = ctx.getTokenRewriteStream().toString(
        viewSelect.getTokenStartIndex(), viewSelect.getTokenStopIndex());

    if (createVwDesc.getPartColNames() != null) {
      // If we are creating a materialized view and it has partition columns,
      // we may need to reorder column projection in expanded query. The reason
      // is that Hive assumes that in the partition columns are at the end of
      // the MV schema, and if we do not do this, we will have a mismatch between
      // the SQL query for the MV and the MV itself.
      boolean first = true;
      StringBuilder sb = new StringBuilder();
      sb.append("SELECT ");
      for (FieldSchema fieldSchema : derivedSchema) {
        if (!createVwDesc.getPartColNames().contains(fieldSchema.getName())) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          sb.append(HiveUtils.unparseIdentifier(fieldSchema.getName(), conf));
        }
      }
      for (String partColName : createVwDesc.getPartColNames()) {
        sb.append(", ");
        sb.append(HiveUtils.unparseIdentifier(partColName, conf));
      }
      sb.append(" FROM (");
      sb.append(expandedText);
      sb.append(") ");
      sb.append(HiveUtils.unparseIdentifier(Utilities.getDbTableName(createVwDesc.getViewName())[1], conf));
      expandedText = sb.toString();
    }

    // Set schema and expanded text for the view
    createVwDesc.setCols(derivedSchema);
    createVwDesc.setViewExpandedText(expandedText);
  }

  private List<FieldSchema> convertRowSchemaToViewSchema(RowResolver rr) throws SemanticException {
    List<FieldSchema> fieldSchema = convertRowSchemaToResultSetSchema(rr, false);
    ParseUtils.validateColumnNameUniqueness(fieldSchema);
    return fieldSchema;
  }

  List<FieldSchema> convertRowSchemaToResultSetSchema(RowResolver rr, boolean useTabAliasIfAvailable) {
    List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
    String[] qualifiedColName;
    String colName;

    for (ColumnInfo colInfo : rr.getColumnInfos()) {
      if (colInfo.isHiddenVirtualCol()) {
        continue;
      }

      qualifiedColName = rr.reverseLookup(colInfo.getInternalName());
      // __u<n> is a UNION ALL placeholder name
      if (useTabAliasIfAvailable && qualifiedColName[0] != null && (!qualifiedColName[0].isEmpty()) && (!qualifiedColName[0].startsWith("__u"))) {
        colName = qualifiedColName[0] + "." + qualifiedColName[1];
      } else {
        colName = qualifiedColName[1];
      }
      fieldSchemas.add(new FieldSchema(colName, colInfo.getType().getTypeName(), null));
    }
    return fieldSchemas;
  }

  /**
   * Generates an expression node descriptor for the expression with TypeCheckCtx.
   */
  public ExprNodeDesc genExprNodeDesc(ASTNode expr, RowResolver input)
      throws SemanticException {
    // Since the user didn't supply a customized type-checking context,
    // use default settings.
    return genExprNodeDesc(expr, input, true, false);
  }

  public ExprNodeDesc genExprNodeDesc(ASTNode expr, RowResolver input, boolean useCaching,
                                      boolean foldExpr) throws SemanticException {
    TypeCheckCtx tcCtx = new TypeCheckCtx(input, useCaching, foldExpr);
    return genExprNodeDesc(expr, input, tcCtx);
  }

  /**
   * Generates an expression node descriptors for the expression and children of it
   * with default TypeCheckCtx.
   */
  Map<ASTNode, ExprNodeDesc> genAllExprNodeDesc(ASTNode expr, RowResolver input)
      throws SemanticException {
    TypeCheckCtx tcCtx = new TypeCheckCtx(input);
    return genAllExprNodeDesc(expr, input, tcCtx);
  }

  /**
   * Returns expression node descriptor for the expression.
   * If it's evaluated already in previous operator, it can be retrieved from cache.
   */
  ExprNodeDesc genExprNodeDesc(ASTNode expr, RowResolver input,
                                      TypeCheckCtx tcCtx) throws SemanticException {
    // We recursively create the exprNodeDesc. Base cases: when we encounter
    // a column ref, we convert that into an exprNodeColumnDesc; when we
    // encounter
    // a constant, we convert that into an exprNodeConstantDesc. For others we
    // just
    // build the exprNodeFuncDesc with recursively built children.

    // If the current subExpression is pre-calculated, as in Group-By etc.
    ExprNodeDesc cached = null;
    if (tcCtx.isUseCaching()) {
      cached = getExprNodeDescCached(expr, input);
    }
    if (cached == null) {
      Map<ASTNode, ExprNodeDesc> allExprs = genAllExprNodeDesc(expr, input, tcCtx);
      return allExprs.get(expr);
    }
    return cached;
  }

  /**
   * Find ExprNodeDesc for the expression cached in the RowResolver. Returns null if not exists.
   */
  private ExprNodeDesc getExprNodeDescCached(ASTNode expr, RowResolver input)
      throws SemanticException {
    ColumnInfo colInfo = input.getExpression(expr);
    if (colInfo != null) {
      ASTNode source = input.getExpressionSource(expr);
      if (source != null) {
        unparseTranslator.addCopyTranslation(expr, source);
      }
      return new ExprNodeColumnDesc(colInfo.getType(), colInfo
          .getInternalName(), colInfo.getTabAlias(), colInfo
          .getIsVirtualCol(), colInfo.isSkewedCol());
    }
    return null;
  }

  /**
   * Generates all of the expression node descriptors for the expression and children of it
   * passed in the arguments. This function uses the row resolver and the metadata information
   * that are passed as arguments to resolve the column names to internal names.
   *
   * @param expr
   *          The expression
   * @param input
   *          The row resolver
   * @param tcCtx
   *          Customized type-checking context
   * @return expression to exprNodeDesc mapping
   * @throws SemanticException Failed to evaluate expression
   */
  @SuppressWarnings("nls")
  Map<ASTNode, ExprNodeDesc> genAllExprNodeDesc(ASTNode expr, RowResolver input,
                                                       TypeCheckCtx tcCtx) throws SemanticException {
    // Create the walker and  the rules dispatcher.
    tcCtx.setUnparseTranslator(unparseTranslator);

    Map<ASTNode, ExprNodeDesc> nodeOutputs =
        ExprNodeTypeCheck.genExprNode(expr, tcCtx);
    ExprNodeDesc desc = nodeOutputs.get(expr);
    if (desc == null) {
      String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(expr
          .getChild(0).getText());
      ColumnInfo colInfo = input.get(null, tableOrCol);
      String errMsg;
      if (colInfo == null && input.getIsExprResolver()){
        errMsg = ASTErrorUtils.getMsg(
            ErrorMsg.NON_KEY_EXPR_IN_GROUPBY.getMsg(), expr);
      } else {
        errMsg = tcCtx.getError();
      }
      throw new SemanticException(Optional.ofNullable(errMsg).orElse("Error in parsing "));
    }
    if (desc instanceof ExprNodeColumnListDesc) {
      throw new SemanticException("TOK_ALLCOLREF is not supported in current context");
    }

    if (!unparseTranslator.isEnabled()) {
      // Not creating a view, so no need to track view expansions.
      return nodeOutputs;
    }

    List<ASTNode> fieldDescList = new ArrayList<>();

    for (Map.Entry<ASTNode, ExprNodeDesc> entry : nodeOutputs.entrySet()) {
      if (!(entry.getValue() instanceof ExprNodeColumnDesc)) {
        // we need to translate the ExprNodeFieldDesc too, e.g., identifiers in
        // struct<>.
        if (entry.getValue() instanceof ExprNodeFieldDesc) {
          fieldDescList.add(entry.getKey());
        }
        continue;
      }
      ASTNode node = entry.getKey();
      ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) entry.getValue();
      if ((columnDesc.getTabAlias() == null)
          || (columnDesc.getTabAlias().length() == 0)) {
        // These aren't real column refs; instead, they are special
        // internal expressions used in the representation of aggregation.
        continue;
      }
      String[] tmp = input.reverseLookup(columnDesc.getColumn());
      // in subquery case, tmp may be from outside.
      // check if outer present && (tmp is null || tmp not null - contains tbl info)
      if (tcCtx.getOuterRR() != null && (tmp == null || (tmp[0] != null && columnDesc.getTabAlias() != null
          && !tmp[0].equals(columnDesc.getTabAlias())))) {
        tmp = tcCtx.getOuterRR().reverseLookup(columnDesc.getColumn());
      }
      StringBuilder replacementText = new StringBuilder();
      replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
      replacementText.append(".");
      replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
      unparseTranslator.addTranslation(node, replacementText.toString());
    }

    for (ASTNode node : fieldDescList) {
      Map<ASTNode, String> map = translateFieldDesc(node, conf);
      for (Entry<ASTNode, String> entry : map.entrySet()) {
        unparseTranslator.addTranslation(entry.getKey(), entry.getValue().toLowerCase());
      }
    }

    return nodeOutputs;
  }

  public static final Map<ASTNode, String> translateFieldDesc(ASTNode node, HiveConf conf) {
    Map<ASTNode, String> map = new HashMap<>();
    if (node.getType() == HiveParser.DOT) {
      for (Node child : node.getChildren()) {
        map.putAll(translateFieldDesc((ASTNode) child, conf));
      }
    } else if (node.getType() == HiveParser.Identifier) {
      map.put(node, HiveUtils.unparseIdentifier(node.getText(), conf));
    }
    return map;
  }

  @Override
  public void validate() throws SemanticException {
    boolean wasAcidChecked = false;
    // Validate inputs and outputs have right protectmode to execute the query
    for (ReadEntity readEntity : getInputs()) {
      ReadEntity.Type type = readEntity.getType();

      if (type != ReadEntity.Type.TABLE &&
          type != ReadEntity.Type.PARTITION) {
        // In current implementation it will never happen, but we leave it
        // here to make the logic complete.
        continue;
      }

      Table tbl = readEntity.getTable();
      Partition p = readEntity.getPartition();

      if (p != null) {
        tbl = p.getTable();
      }
      if (tbl != null && AcidUtils.isTransactionalTable(tbl)) {
        transactionalInQuery = true;
        if (!wasAcidChecked) {
          checkAcidTxnManager(tbl);
        }
        wasAcidChecked = true;
      }
    }

    for (WriteEntity writeEntity : getOutputs()) {
      WriteEntity.Type type = writeEntity.getType();

      if (type == WriteEntity.Type.PARTITION || type == WriteEntity.Type.DUMMYPARTITION) {
        String conflictingArchive = null;
        try {
          Partition usedp = writeEntity.getPartition();
          Table tbl = usedp.getTable();
          if (AcidUtils.isTransactionalTable(tbl)) {
            transactionalInQuery = true;
            if (!wasAcidChecked) {
              checkAcidTxnManager(tbl);
            }
            wasAcidChecked = true;
          }

          LOG.debug("validated " + usedp.getName());
          LOG.debug(usedp.getTable().getTableName());
          if (!AcidUtils.isTransactionalTable(tbl) && conf.getBoolVar(HIVE_ARCHIVE_ENABLED)) {
            // Do not check for ACID; it does not create new parts and this is expensive as hell.
            // TODO: add an API to get table name list for archived parts with a single call;
            //       nobody uses this so we could skip the whole thing.
            conflictingArchive = ArchiveUtils
                .conflictingArchiveNameOrNull(db, tbl, usedp.getSpec());
          }
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
        if (conflictingArchive != null) {
          String message = String.format("Insert conflict with existing archive: %s",
              conflictingArchive);
          throw new SemanticException(message);
        }
      } else if (type == WriteEntity.Type.TABLE) {
        Table tbl = writeEntity.getTable();
        if (AcidUtils.isTransactionalTable(tbl)) {
          transactionalInQuery = true;
          if (!wasAcidChecked) {
            checkAcidTxnManager(tbl);
          }
          wasAcidChecked = true;
        }
      }

      if (type != WriteEntity.Type.TABLE &&
          type != WriteEntity.Type.PARTITION) {
        LOG.debug("not validating writeEntity, because entity is neither table nor partition");
        continue;
      }
    }

    boolean reworkMapredWork = HiveConf.getBoolVar(this.conf,
        HiveConf.ConfVars.HIVE_REWORK_MAPREDWORK);

    // validate all tasks
    for (Task<?> rootTask : rootTasks) {
      validate(rootTask, reworkMapredWork);
    }
  }

  private void validate(Task<?> task, boolean reworkMapredWork)
      throws SemanticException {
    Utilities.reworkMapRedWork(task, reworkMapredWork, conf);
    if (task.getChildTasks() == null) {
      return;
    }

    for (Task<?> childTask : task.getChildTasks()) {
      validate(childTask, reworkMapredWork);
    }
  }

  /**
   * Update the default table properties with values fetch from the original table properties. The property names are
   * defined in {@link SemanticAnalyzer#UPDATED_TBL_PROPS}.
   * @param source properties of source table, must be not null.
   * @param target properties of target table.
   * @param skipped a list of properties which should be not overwritten. It can be null or empty.
   */
  private void updateDefaultTblProps(Map<String, String> source, Map<String, String> target, List<String> skipped) {
    if (source == null || target == null) {
      return;
    }
    for (String property : UPDATED_TBL_PROPS) {
      if ((skipped == null || !skipped.contains(property)) && source.containsKey(property)) {
        target.put(property, source.get(property));
      }
    }
  }

  /**
   * Add default properties for table property. If a default parameter exists
   * in the tblProp, the value in tblProp will be kept.
   *
   * @param tblProp
   *          property map
   * @return Modified table property map
   */
  private Map<String, String> validateAndAddDefaultProperties(
      Map<String, String> tblProp, boolean isExt, StorageFormat storageFormat,
      String qualifiedTableName, List<Order> sortCols, boolean isMaterialization,
      boolean isTemporaryTable, boolean isTransactional, boolean isManaged, String[] qualifiedTabName, boolean isTableTypeChanged) throws SemanticException {
    Map<String, String> retValue = Optional.ofNullable(tblProp).orElseGet(HashMap::new);

    String paraString = HiveConf.getVar(conf, ConfVars.NEW_TABLE_DEFAULT_PARA);
    if (paraString != null && !paraString.isEmpty()) {
      for (String keyValuePair : paraString.split(",")) {
        String[] keyValue = keyValuePair.split("=", 2);
        if (keyValue.length != 2) {
          continue;
        }
        if (!retValue.containsKey(keyValue[0])) {
          retValue.put(keyValue[0], keyValue[1]);
        }
      }
    }
    if (!retValue.containsKey(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL)
        && retValue.containsKey(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES)) {
      throw new SemanticException("Cannot specify "
        + hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES
        + " without " + hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    }
    isExt = isExternalTableChanged(retValue, isTransactional, isExt, isTableTypeChanged);

    if (isExt && HiveConf.getBoolVar(conf, ConfVars.HIVE_EXTERNALTABLE_PURGE_DEFAULT)) {
      if (retValue.get(MetaStoreUtils.EXTERNAL_TABLE_PURGE) == null) {
        retValue.put(MetaStoreUtils.EXTERNAL_TABLE_PURGE, "true");
      }
    }

    boolean makeInsertOnly = !isTemporaryTable && HiveConf.getBoolVar(
        conf, ConfVars.HIVE_CREATE_TABLES_AS_INSERT_ONLY);
    boolean makeAcid = !isTemporaryTable && makeAcid();
    // if not specify managed table and create.table.as.external is true
    // ignore makeInsertOnly and makeAcid.
    if (!isManaged && HiveConf.getBoolVar(conf, ConfVars.CREATE_TABLE_AS_EXTERNAL)) {
      makeInsertOnly = false;
      makeAcid = false;
    }
    if ((makeInsertOnly || makeAcid || isTransactional || isManaged)
        && !isExt  && !isMaterialization && StringUtils.isBlank(storageFormat.getStorageHandler())
        //don't overwrite user choice if transactional attribute is explicitly set
        && !retValue.containsKey(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL)) {
      if (makeInsertOnly || isTransactional) {
        retValue.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
        retValue.put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES,
            TransactionalValidationListener.INSERTONLY_TRANSACTIONAL_PROPERTY);
      }
      if (makeAcid || isTransactional || (isManaged && !makeInsertOnly)) {
        retValue = convertToAcidByDefault(storageFormat, qualifiedTableName, sortCols, retValue);
      }
    }
    if (!isExt) {
      addDbAndTabToOutputs(qualifiedTabName,
              TableType.MANAGED_TABLE, isTemporaryTable, retValue, storageFormat);
    } else {
      addDbAndTabToOutputs(qualifiedTabName,
              TableType.EXTERNAL_TABLE, isTemporaryTable, retValue, storageFormat);
    }

    if (isIcebergTable(retValue)) {
      SessionStateUtil.addResourceOrThrow(conf, SessionStateUtil.DEFAULT_TABLE_LOCATION,
          getDefaultLocation(qualifiedTabName[0], qualifiedTabName[1], true));
    }
    return retValue;
  }

  /**
   * This api is used to determine where to create acid tables are not.
   * if the default table type is set to external, then create transactional table should result in acid tables,
   * else create table should result in external table.
   * */
  private boolean isExternalTableChanged (Map<String, String> tblProp, boolean isTransactional, boolean isExt, boolean isTableTypeChanged) {
    if (isTableTypeChanged && tblProp != null && tblProp.getOrDefault(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "false").equalsIgnoreCase("true") || isTransactional) {
      isExt = false;
    }
    return isExt;
  }

  private Map<String, String> convertToAcidByDefault(
      StorageFormat storageFormat, String qualifiedTableName, List<Order> sortCols,
      Map<String, String> retValue) {
    /*for CTAS, TransactionalValidationListener.makeAcid() runs to late to make table Acid
     so the initial write ends up running as non-acid...*/
    try {
      Class inputFormatClass = storageFormat.getInputFormat() == null ? null :
          Class.forName(storageFormat.getInputFormat());
      Class outputFormatClass = storageFormat.getOutputFormat() == null ? null :
          Class.forName(storageFormat.getOutputFormat());
      if (inputFormatClass == null || outputFormatClass == null ||
          !AcidInputFormat.class.isAssignableFrom(inputFormatClass) ||
          !AcidOutputFormat.class.isAssignableFrom(outputFormatClass)) {
        return retValue;
      }
    } catch (ClassNotFoundException e) {
      LOG.warn("Could not verify InputFormat=" + storageFormat.getInputFormat() + " or OutputFormat=" +
          storageFormat.getOutputFormat() + "  for " + qualifiedTableName);
      return retValue;
    }
    if (sortCols != null && !sortCols.isEmpty()) {
      return retValue;
    }
    retValue.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    retValue.put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES,
        TransactionalValidationListener.DEFAULT_TRANSACTIONAL_PROPERTY);
    LOG.info("Automatically chose to make " + qualifiedTableName + " acid.");
    return retValue;
  }

  /**
   * Checks to see if given partition columns has DEFAULT or CHECK constraints (whether ENABLED or DISABLED)
   *  Or has NOT NULL constraints (only ENABLED)
   * @param partCols partition columns
   * @param defConstraints default constraints
   * @param notNullConstraints not null constraints
   * @param checkConstraints CHECK constraints
   * @return true or false
   */
  private boolean hasConstraints(final List<FieldSchema> partCols, final List<SQLDefaultConstraint> defConstraints,
                         final List<SQLNotNullConstraint> notNullConstraints,
                         final List<SQLCheckConstraint> checkConstraints) {
    for(FieldSchema partFS: partCols) {
      for(SQLDefaultConstraint dc:defConstraints) {
        if(dc.getColumn_name().equals(partFS.getName())) {
          return true;
        }
      }
      for(SQLCheckConstraint cc:checkConstraints) {
        if(cc.getColumn_name().equals(partFS.getName())) {
          return true;
        }
      }
      for(SQLNotNullConstraint nc:notNullConstraints) {
        if(nc.getColumn_name().equals(partFS.getName()) && nc.isEnable_cstr()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Analyze the create table command. If it is a regular create-table or
   * create-table-like statements, we create a DDLWork and return true. If it is
   * a create-table-as-select, we get the necessary info such as the SerDe and
   * Storage Format and put it in QB, and return false, indicating the rest of
   * the semantic analyzer need to deal with the select statement with respect
   * to the SerDe and Storage Format.
   */
  ASTNode analyzeCreateTable(
      ASTNode ast, QB qb, PlannerContext plannerCtx) throws SemanticException {
    TableName qualifiedTabName = getQualifiedTableName((ASTNode) ast.getChild(0));
    final String dbDotTab = qualifiedTabName.getNotEmptyDbTable();

    String likeTableName = null;
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> partColNames = new ArrayList<>();
    List<String> bucketCols = new ArrayList<String>();
    List<SQLPrimaryKey> primaryKeys = new ArrayList<SQLPrimaryKey>();
    List<SQLForeignKey> foreignKeys = new ArrayList<SQLForeignKey>();
    List<SQLUniqueConstraint> uniqueConstraints = new ArrayList<>();
    List<SQLNotNullConstraint> notNullConstraints = new ArrayList<>();
    List<SQLDefaultConstraint> defaultConstraints= new ArrayList<>();
    List<SQLCheckConstraint> checkConstraints= new ArrayList<>();
    List<Order> sortCols = new ArrayList<Order>();
    int numBuckets = -1;
    String comment = null;
    String location = null;
    Map<String, String> tblProps = null;
    boolean ifNotExists = false;
    boolean isExt = false;
    boolean isTemporary = false;
    boolean isManaged = false;
    boolean isMaterialization = false;
    boolean isTransactional = false;
    ASTNode selectStmt = null;
    final int CREATE_TABLE = 0; // regular CREATE TABLE
    final int CTLT = 1; // CREATE TABLE LIKE ... (CTLT)
    final int CTAS = 2; // CREATE TABLE AS SELECT ... (CTAS)
    final int CTT = 3; // CREATE TRANSACTIONAL TABLE
    final int CTLF = 4; // CREATE TABLE LIKE FILE
    int command_type = CREATE_TABLE;
    List<String> skewedColNames = new ArrayList<String>();
    List<List<String>> skewedValues = new ArrayList<List<String>>();
    Map<List<String>, String> listBucketColValuesMapping = new HashMap<List<String>, String>();
    boolean storedAsDirs = false;
    boolean isUserStorageFormat = false;
    boolean partitionTransformSpecExists = false;
    String likeFile = null;
    String likeFileFormat = null;
    String sortOrder = null;
    RowFormatParams rowFormatParams = new RowFormatParams();
    StorageFormat storageFormat = new StorageFormat(conf);

    LOG.info("Creating table " + dbDotTab + " position=" + ast.getCharPositionInLine());
    int numCh = ast.getChildCount();

    // set storage handler if default handler is provided in config
    String defaultStorageHandler = HiveConf.getVar(conf, HIVE_DEFAULT_STORAGE_HANDLER);
    if (defaultStorageHandler != null && !defaultStorageHandler.isEmpty()) {
      LOG.info("Default storage handler class detected in config. Using storage handler class if exists: '{}'",
          defaultStorageHandler);
      storageFormat.setStorageHandler(defaultStorageHandler);
      isUserStorageFormat = true;
    }

    // CREATE TABLE is a DDL and yet it's not handled by DDLSemanticAnalyzerFactory
    // FIXME: HIVE-28724
    queryProperties.setQueryType(QueryProperties.QueryType.DDL);

    /*
     * Check the 1st-level children and do simple semantic checks: 1) CTLT and
     * CTAS should not coexists. 2) CTLT or CTAS should not coexists with column
     * list (target table schema). 3) CTAS does not support partitioning (for
     * now).
     */
    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);
      if (storageFormat.fillStorageFormat(child)) {
        isUserStorageFormat = true;
        continue;
      }
      switch (child.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.KW_EXTERNAL:
        isExt = true;
        break;
      case HiveParser.KW_MANAGED:
        isManaged = true;
        isTransactional = true;
        break;
      case HiveParser.KW_TEMPORARY:
        isTemporary = true;
        isMaterialization = MATERIALIZATION_MARKER.equals(child.getText());
        break;
      case HiveParser.KW_TRANSACTIONAL:
        isTransactional = true;
        command_type = CTT;
        break;
      case HiveParser.TOK_LIKEFILE:
        if (cols.size() != 0) {
          throw new SemanticException(ErrorMsg.CTLT_COLLST_COEXISTENCE
              .getMsg());
        }
        likeFileFormat = getUnescapedName((ASTNode) child.getChild(0));
        likeFile = getUnescapedName((ASTNode) child.getChild(1));
        command_type = CTLF;
        break;
      case HiveParser.TOK_LIKETABLE:
        if (child.getChildCount() > 0) {
          likeTableName = getUnescapedName((ASTNode) child.getChild(0));
          if (likeTableName != null) {
            if (command_type == CTAS) {
              throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE
                  .getMsg());
            }
            if (cols.size() != 0) {
              throw new SemanticException(ErrorMsg.CTLT_COLLST_COEXISTENCE
                  .getMsg());
            }
          }
          command_type = CTLT;
        }
        break;

      case HiveParser.TOK_QUERY: // CTAS
        if (command_type == CTLT) {
          throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
        }
        if (cols.size() != 0) {
          throw new SemanticException(ErrorMsg.CTAS_COLLST_COEXISTENCE.getMsg());
        }
        if (partCols.size() != 0 || bucketCols.size() != 0) {
          boolean dynPart = HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMIC_PARTITIONING);
          if (dynPart == false) {
            throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
          } else {
            // TODO: support dynamic partition for CTAS
            throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
          }
        }
        if (!conf.getBoolVar(ConfVars.HIVE_CTAS_EXTERNAL_TABLES) && isExt) {
          throw new SemanticException(ErrorMsg.CTAS_EXTTBL_COEXISTENCE.getMsg());
        }
        command_type = CTAS;
        if (plannerCtx != null) {
          plannerCtx.setCTASToken(child);
        }
        selectStmt = child;
        break;
      case HiveParser.TOK_TABCOLLIST:
        cols = getColumns(child, true, ctx.getTokenRewriteStream(), primaryKeys, foreignKeys,
            uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints, conf);
        break;
      case HiveParser.TOK_TABLECOMMENT:
        comment = unescapeSQLString(child.getChild(0).getText());
        break;
      case HiveParser.TOK_TABLEPARTCOLS:
        partCols = getColumns(child, false, ctx.getTokenRewriteStream(), primaryKeys, foreignKeys,
            uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints, conf);
        if(hasConstraints(partCols, defaultConstraints, notNullConstraints, checkConstraints)) {
          //TODO: these constraints should be supported for partition columns
          throw new SemanticException(
              ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("NOT NULL,DEFAULT and CHECK Constraints are not allowed with " +
                                                      "partition columns. "));
        }
        break;
      case HiveParser.TOK_TABLEPARTCOLSBYSPEC:
        SessionStateUtil.addResourceOrThrow(conf, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC,
                PartitionTransform.getPartitionTransformSpec(child));
        partitionTransformSpecExists = true;
        break;
      case HiveParser.TOK_TABLEPARTCOLNAMES:
        partColNames = getColumnNames(child);
        break;
      case HiveParser.TOK_ALTERTABLE_BUCKETS:
        bucketCols = getColumnNames((ASTNode) child.getChild(0));
        if (child.getChildCount() == 2) {
          numBuckets = Integer.parseInt(child.getChild(1).getText());
        } else {
          sortCols = getColumnNamesOrder((ASTNode) child.getChild(1));
          numBuckets = Integer.parseInt(child.getChild(2).getText());
        }
        break;
      case HiveParser.TOK_WRITE_LOCALLY_ORDERED:
        sortOrder = getSortOrderJson((ASTNode) child.getChild(0));
        break;
      case HiveParser.TOK_TABLEROWFORMAT:
        rowFormatParams.analyzeRowFormat(child);
        break;
      case HiveParser.TOK_TABLELOCATION:
        location = unescapeSQLString(child.getChild(0).getText());
        location = EximUtil.relativeToAbsolutePath(conf, location);
        inputs.add(toReadEntity(location));
        break;
      case HiveParser.TOK_TABLEPROPERTIES:
        tblProps = getProps((ASTNode) child.getChild(0));
        addPropertyReadEntry(tblProps, inputs);
        break;
      case HiveParser.TOK_TABLESERIALIZER:
        child = (ASTNode) child.getChild(0);
        storageFormat.setSerde(unescapeSQLString(child.getChild(0).getText()));
        if (child.getChildCount() == 2) {
          readProps((ASTNode) (child.getChild(1).getChild(0)),
              storageFormat.getSerdeProps());
        }
        break;
      case HiveParser.TOK_TABLESKEWED:
        /**
         * Throw an error if the user tries to use the DDL with
         * hive.internal.ddl.list.bucketing.enable set to false.
         */
        HiveConf hiveConf = SessionState.get().getConf();

        // skewed column names
        skewedColNames = SkewedTableUtils.analyzeSkewedTableDDLColNames(child);
        // skewed value
        skewedValues = SkewedTableUtils.analyzeDDLSkewedValues(child);
        // stored as directories
        storedAsDirs = analyzeStoredAdDirs(child);

        break;
      default:
        throw new AssertionError("Unknown token: " + child.getToken());
      }
    }

    validateStorageFormat(storageFormat, tblProps, partitionTransformSpecExists);

    if (command_type == CREATE_TABLE || command_type == CTLT || command_type == CTT || command_type == CTLF) {
      queryState.setCommandType(HiveOperation.CREATETABLE);
    } else if (command_type == CTAS) {
      queryState.setCommandType(HiveOperation.CREATETABLE_AS_SELECT);
    } else {
      throw new SemanticException("Unrecognized command.");
    }

    if (isExt && ConstraintsUtils.hasEnabledOrValidatedConstraints(notNullConstraints, defaultConstraints,
        checkConstraints)) {
      throw new SemanticException(
          ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Constraints are disallowed with External tables. "
              + "Only RELY is allowed."));
    }
    if (checkConstraints != null && !checkConstraints.isEmpty()) {
      ConstraintsUtils.validateCheckConstraint(cols, checkConstraints, ctx.getConf());
    }

    storageFormat.fillDefaultStorageFormat(isExt, false);

    // check for existence of table
    if (ifNotExists) {
      try {
        Table table = getTable(qualifiedTabName, false);
        if (table != null) { // table exists
          return null;
        }
      } catch (HiveException e) {
        // should not occur since second parameter to getTableWithQN is false
        throw new IllegalStateException("Unexpected Exception thrown: " + e.getMessage(), e);
      }
    }

    if (isTemporary) {
      if (location == null) {
        // for temporary tables we set the location to something in the session's scratch dir
        // it has the same life cycle as the tmp table
        try {
          // Generate a unique ID for temp table path.
          // This path will be fixed for the life of the temp table.
          location = SessionState.generateTempTableLocation(conf);
        } catch (MetaException err) {
          throw new SemanticException("Error while generating temp table path:", err);
        }
      }
    }

    // Handle different types of CREATE TABLE command
    // Note: each branch must call addDbAndTabToOutputs after finalizing table properties.
    Database database  = getDatabase(qualifiedTabName.getDb());
    boolean isDefaultTableTypeChanged = false;
    if(database.getParameters() != null) {
      String defaultTableType = database.getParameters().getOrDefault(DEFAULT_TABLE_TYPE, null);
      if (defaultTableType != null && defaultTableType.equalsIgnoreCase("external")) {
        isExt = true;
        isDefaultTableTypeChanged = true;
      } else if (defaultTableType != null && defaultTableType.equalsIgnoreCase("acid")) {
        isDefaultTableTypeChanged = true;
        if (isExt) { // create external table on db with default type as acid
          isTransactional = false;
        } else {
          isTransactional = true;
        }
      }
    }
    switch (command_type) {
    case CTLF:
      try {
        if (!SchemaInferenceUtils.doesSupportSchemaInference(conf, likeFileFormat)) {
          throw new SemanticException(ErrorMsg.CTLF_UNSUPPORTED_FORMAT.getErrorCodedMsg(likeFileFormat));
        }
      } catch (HiveException e) {
        throw new SemanticException(e.getMessage(), e);
      }
    // fall through
    case CREATE_TABLE: // REGULAR CREATE TABLE DDL
      if (!CollectionUtils.isEmpty(partColNames)) {
        throw new SemanticException(
            "Partition columns can only declared using their name and types in regular CREATE TABLE statements");
      }
      tblProps = validateAndAddDefaultProperties(
          tblProps, isExt, storageFormat, dbDotTab, sortCols, isMaterialization, isTemporary,
          isTransactional, isManaged, new String[] {qualifiedTabName.getDb(), qualifiedTabName.getTable()}, isDefaultTableTypeChanged);
      isExt = isExternalTableChanged(tblProps, isTransactional, isExt, isDefaultTableTypeChanged);
      addDbAndTabToOutputs(new String[] {qualifiedTabName.getDb(), qualifiedTabName.getTable()},
          TableType.MANAGED_TABLE, isTemporary, tblProps, storageFormat);
      if (!Strings.isNullOrEmpty(sortOrder)) {
        tblProps.put("default-sort-order", sortOrder);
      }
      CreateTableDesc crtTblDesc = new CreateTableDesc(qualifiedTabName,
          isExt, isTemporary, cols, partCols,
          bucketCols, sortCols, numBuckets, rowFormatParams.fieldDelim,
          rowFormatParams.fieldEscape,
          rowFormatParams.collItemDelim, rowFormatParams.mapKeyDelim, rowFormatParams.lineDelim,
          comment,
          storageFormat.getInputFormat(), storageFormat.getOutputFormat(), location, storageFormat.getSerde(),
          storageFormat.getStorageHandler(), storageFormat.getSerdeProps(), tblProps, ifNotExists, skewedColNames,
          skewedValues, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints,
                                                       checkConstraints);
      crtTblDesc.setStoredAsSubDirectories(storedAsDirs);
      crtTblDesc.setNullFormat(rowFormatParams.nullFormat);
      crtTblDesc.setLikeFile(likeFile);
      crtTblDesc.setLikeFileFormat(likeFileFormat);

      crtTblDesc.validate(conf);
      // outputs is empty, which means this create table happens in the current
      // database.
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), crtTblDesc)));
      String tblLocation = null;
      if (location != null) {
        tblLocation = location;
      } else {
        tblLocation = getDefaultLocation(qualifiedTabName.getDb(), qualifiedTabName.getTable(), isExt);
      }
      try {
        HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(conf, storageFormat.getStorageHandler());
        if (storageHandler != null) {
          storageHandler.addResourcesForCreateTable(tblProps, conf);
        }
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
      SessionStateUtil.addResourceOrThrow(conf, META_TABLE_LOCATION, tblLocation);
      break;
    case CTT: // CREATE TRANSACTIONAL TABLE
      if (isExt && !isDefaultTableTypeChanged) {
        throw new SemanticException(
            qualifiedTabName.getTable() + " cannot be declared transactional because it's an external table");
      }
      tblProps = validateAndAddDefaultProperties(tblProps, isExt, storageFormat, dbDotTab, sortCols, isMaterialization,
          isTemporary, isTransactional, isManaged, new String[] {qualifiedTabName.getDb(), qualifiedTabName.getTable()}, isDefaultTableTypeChanged);
      isExt = isExternalTableChanged(tblProps, isTransactional, isExt, isDefaultTableTypeChanged);
      addDbAndTabToOutputs(new String[] {qualifiedTabName.getDb(), qualifiedTabName.getTable()},
          TableType.MANAGED_TABLE, false, tblProps, storageFormat);

      CreateTableDesc crtTranTblDesc =
          new CreateTableDesc(qualifiedTabName, isExt, isTemporary, cols, partCols, bucketCols, sortCols, numBuckets,
              rowFormatParams.fieldDelim, rowFormatParams.fieldEscape, rowFormatParams.collItemDelim,
              rowFormatParams.mapKeyDelim, rowFormatParams.lineDelim, comment, storageFormat.getInputFormat(),
              storageFormat.getOutputFormat(), location, storageFormat.getSerde(), storageFormat.getStorageHandler(),
              storageFormat.getSerdeProps(), tblProps, ifNotExists, skewedColNames, skewedValues, primaryKeys,
              foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
      crtTranTblDesc.setStoredAsSubDirectories(storedAsDirs);
      crtTranTblDesc.setNullFormat(rowFormatParams.nullFormat);

      crtTranTblDesc.validate(conf);
      // outputs is empty, which means this create table happens in the current
      // database.
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), crtTranTblDesc)));
      break;

    case CTLT: // create table like <tbl_name>

      tblProps = validateAndAddDefaultProperties(
          tblProps, isExt, storageFormat, dbDotTab, sortCols, isMaterialization, isTemporary,

          isTransactional, isManaged, new String[]{qualifiedTabName.getDb(), qualifiedTabName.getTable()}, isDefaultTableTypeChanged);
      tblProps.put(hive_metastoreConstants.TABLE_IS_CTLT, "true");
      isExt = isExternalTableChanged(tblProps, isTransactional, isExt, isDefaultTableTypeChanged);
      addDbAndTabToOutputs(new String[] {qualifiedTabName.getDb(), qualifiedTabName.getTable()},
          TableType.MANAGED_TABLE, isTemporary, tblProps, storageFormat);

      Table likeTable = getTable(likeTableName, false);
      if (likeTable != null) {
        if (isTemporary || isExt || isIcebergTable(tblProps)) {
          updateDefaultTblProps(likeTable.getParameters(), tblProps,
              new ArrayList<>(Arrays.asList(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL,
                  hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES)));
        } else {
          updateDefaultTblProps(likeTable.getParameters(), tblProps, null);
        }
      }
      if (likeTable.getTableType() == TableType.EXTERNAL_TABLE &&
          HiveConf.getBoolVar(conf, ConfVars.CREATE_TABLE_AS_EXTERNAL)) {
        isExt = true;
      }
      CreateTableLikeDesc crtTblLikeDesc = new CreateTableLikeDesc(dbDotTab, isExt, isTemporary,
          storageFormat.getInputFormat(), storageFormat.getOutputFormat(), location,
          storageFormat.getSerde(), storageFormat.getSerdeProps(), tblProps, ifNotExists,
          likeTableName, isUserStorageFormat);
      tblLocation = getDefaultLocation(qualifiedTabName.getDb(), qualifiedTabName.getTable(), isExt);
      SessionStateUtil.addResource(conf, META_TABLE_LOCATION, tblLocation);
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), crtTblLikeDesc)));
      break;

    case CTAS: // create table as select

      if (isTemporary) {
        if (!ctx.isExplainSkipExecution() && !isMaterialization) {
          SessionState ss = SessionState.get();
          if (ss == null) {
            throw new SemanticException("No current SessionState, cannot create temporary table "
                + qualifiedTabName.getNotEmptyDbTable());
          }
          Map<String, Table> tables = SessionHiveMetaStoreClient.
              getTempTablesForDatabase(qualifiedTabName.getDb(), qualifiedTabName.getTable());
          if (tables != null && tables.containsKey(qualifiedTabName.getTable())) {
            throw new SemanticException("Temporary table " + qualifiedTabName.getNotEmptyDbTable()
                + " already exists");
          }
        }
      } else {
        // Verify that the table does not already exist
        // dumpTable is only used to check the conflict for non-temporary tables
        try {
          Table dumpTable = db.newTable(dbDotTab);
          if (null != db.getTable(dumpTable.getDbName(), dumpTable.getTableName(), false) && !ctx.isExplainSkipExecution()) {
            throw new SemanticException(ErrorMsg.TABLE_ALREADY_EXISTS.getMsg(dbDotTab));
          }
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
      }

      if (location != null && location.length() != 0) {
        Path locPath = new Path(location);
        FileSystem curFs = null;
        FileStatus locStats = null;
        try {
          curFs = locPath.getFileSystem(conf);
          if(curFs != null) {
            locStats = curFs.getFileStatus(locPath);
          }
          if (locStats != null && locStats.isDir()) {
            FileStatus[] lStats = curFs.listStatus(locPath);
            if(lStats != null && lStats.length != 0) {
              // Don't throw an exception if the target location only contains the staging-dirs
              for (FileStatus lStat : lStats) {
                if (!lStat.getPath().getName().startsWith(HiveConf.getVar(conf, HiveConf.ConfVars.STAGING_DIR))) {
                  throw new SemanticException(ErrorMsg.CTAS_LOCATION_NONEMPTY.getMsg(location));
                }
              }
            }
          }
        } catch (FileNotFoundException nfe) {
          //we will create the folder if it does not exist.
        } catch (IOException ioE) {
          LOG.debug("Exception when validate folder", ioE);
        }
        tblLocation = location;
      } else {
        tblLocation = getDefaultLocation(qualifiedTabName.getDb(), qualifiedTabName.getTable(), isExt);
      }
      SessionStateUtil.addResource(conf, META_TABLE_LOCATION, tblLocation);
      if (!CollectionUtils.isEmpty(partCols)) {
        throw new SemanticException(
            "Partition columns can only declared using their names in CTAS statements");
      }

      tblProps = validateAndAddDefaultProperties(
          tblProps, isExt, storageFormat, dbDotTab, sortCols, isMaterialization, isTemporary,
          isTransactional, isManaged, new String[]{qualifiedTabName.getDb(), qualifiedTabName.getTable()}, isDefaultTableTypeChanged);
      isExt = isExternalTableChanged(tblProps, isTransactional, isExt, isDefaultTableTypeChanged);
      tblProps.put(TABLE_IS_CTAS, "true");
      addDbAndTabToOutputs(new String[] {qualifiedTabName.getDb(), qualifiedTabName.getTable()},
          TableType.MANAGED_TABLE, isTemporary, tblProps, storageFormat);
      tableDesc = new CreateTableDesc(qualifiedTabName, isExt, isTemporary, cols,
          partColNames, bucketCols, sortCols, numBuckets, rowFormatParams.fieldDelim,
          rowFormatParams.fieldEscape, rowFormatParams.collItemDelim, rowFormatParams.mapKeyDelim,
          rowFormatParams.lineDelim, comment, storageFormat.getInputFormat(),
          storageFormat.getOutputFormat(), location, storageFormat.getSerde(),
          storageFormat.getStorageHandler(), storageFormat.getSerdeProps(), tblProps, ifNotExists,
          skewedColNames, skewedValues, true, primaryKeys, foreignKeys,
          uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
      tableDesc.setMaterialization(isMaterialization);
      tableDesc.setStoredAsSubDirectories(storedAsDirs);
      tableDesc.setNullFormat(rowFormatParams.nullFormat);
      qb.setTableDesc(tableDesc);

      return selectStmt;

    default:
      throw new SemanticException("Unrecognized command.");
    }
    return null;
  }

  private String getDefaultLocation(String dbName, String tableName, boolean isExt) throws SemanticException {
    String tblLocation;
    try {
      Warehouse wh = new Warehouse(conf);
      tblLocation = wh.getDefaultTablePath(db.getDatabase(dbName), tableName, isExt).toUri().getPath();
    } catch (MetaException | HiveException e) {
      throw new SemanticException(e);
    }
    return tblLocation;
  }

  private static boolean isIcebergTable(Map<String, String> tblProps) {
    return AlterTableConvertOperation.ConversionFormats.ICEBERG.properties().get(META_TABLE_STORAGE)
        .equalsIgnoreCase(tblProps.get(META_TABLE_STORAGE));
  }

  private void validateStorageFormat(
          StorageFormat storageFormat, Map<String, String> tblProps, boolean partitionTransformSpecExists)
          throws SemanticException {
    HiveStorageHandler handler;
    try {
      handler = HiveUtils.getStorageHandler(conf, storageFormat.getStorageHandler());
    } catch (HiveException e) {
      throw new SemanticException("Failed to load storage handler:  " + e.getMessage());
    }

    if (handler != null) {
      if (partitionTransformSpecExists && !handler.supportsPartitionTransform()) {
        throw new SemanticException("Partition transform is not supported for " + handler.getClass().getName());
      }

      String fileFormatPropertyKey = handler.getFileFormatPropertyKey();
      if (fileFormatPropertyKey != null) {
        if (tblProps != null && tblProps.containsKey(fileFormatPropertyKey) && storageFormat.getSerdeProps() != null &&
                storageFormat.getSerdeProps().containsKey(fileFormatPropertyKey)) {
          String fileFormat = tblProps.get(fileFormatPropertyKey);
          throw new SemanticException(
                  "Provide only one of the following: STORED BY " + fileFormat + " or WITH SERDEPROPERTIES('" +
                          fileFormatPropertyKey + "'='" + fileFormat + "') or" + " TBLPROPERTIES('" + fileFormatPropertyKey
                          + "'='" + fileFormat + "')");
        }
      }
    } else if (partitionTransformSpecExists) {
      throw new SemanticException(ErrorMsg.UNEXPECTED_PARTITION_TRANSFORM_SPEC.getMsg());
    }
  }

  /** Adds entities for create table/create view. */
  private void addDbAndTabToOutputs(String[] qualifiedTabName, TableType type,
      boolean isTemporary, Map<String, String> tblProps, StorageFormat storageFormat) throws SemanticException {
    Database database  = getDatabase(qualifiedTabName[0]);
    outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_SHARED));

    Table t = new Table(qualifiedTabName[0], qualifiedTabName[1]);
    t.setParameters(tblProps);
    t.setTableType(type);
    t.setTemporary(isTemporary);
    HiveStorageHandler storageHandler = null;
    if (storageFormat.getStorageHandler() != null) {
      try {
        storageHandler = (HiveStorageHandler) ReflectionUtils.newInstance(
                conf.getClassByName(storageFormat.getStorageHandler()), SessionState.get().getConf());
        t.setProperty(META_TABLE_STORAGE, storageHandler.getClass().getName());
      } catch (ClassNotFoundException ex) {
        LOG.error("Class not found. Storage handler will be set to null: "+ex.getMessage() , ex);
      }
    }
    t.setStorageHandler(storageHandler);
    for (Map.Entry<String,String> serdeMap : storageFormat.getSerdeProps().entrySet()){
      t.setSerdeParam(serdeMap.getKey(), serdeMap.getValue());
    }
    WriteType lockType = tblProps != null && Boolean.parseBoolean(tblProps.get(TABLE_IS_CTAS))
        && AcidUtils.isExclusiveCTASEnabled(conf)
        // iceberg CTAS has it's own locking mechanism, therefore we should exclude them
        && (t.getStorageHandler() == null || !t.getStorageHandler().directInsert()) ?
      WriteType.CTAS : WriteType.DDL_NO_LOCK;
    
    outputs.add(new WriteEntity(t, lockType));
  }

  protected ASTNode analyzeCreateView(ASTNode ast, QB qb, PlannerContext plannerCtx) throws SemanticException {
    TableName qualTabName = getQualifiedTableName((ASTNode) ast.getChild(0));
    final String dbDotTable = qualTabName.getNotEmptyDbTable();
    List<FieldSchema> cols = null;
    boolean ifNotExists = false;
    boolean rewriteEnabled = true;
    String comment = null;
    ASTNode selectStmt = null;
    Map<String, String> tblProps = null;
    List<String> partColNames = null;
    List<String> sortColNames = null;
    List<String> distributeColNames = null;
    String location = null;
    RowFormatParams rowFormatParams = new RowFormatParams();
    StorageFormat storageFormat = new StorageFormat(conf);
    boolean partitionTransformSpecExists = false;

    LOG.info("Creating view " + dbDotTable + " position="
        + ast.getCharPositionInLine());
    int numCh = ast.getChildCount();

    // all the CREATE VIEW statements are DDLs (including MV)
    queryProperties.setQueryType(QueryProperties.QueryType.DDL);

    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);
      if (storageFormat.fillStorageFormat(child)) {
        continue;
      }
      switch (child.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.TOK_REWRITE_DISABLED:
        rewriteEnabled = false;
        break;
      case HiveParser.TOK_QUERY:
        // For CBO
        if (plannerCtx != null) {
          plannerCtx.setViewToken(child);
        }
        selectStmt = child;
        break;
      case HiveParser.TOK_TABCOLNAME:
        cols = getColumns(child);
        break;
      case HiveParser.TOK_TABLECOMMENT:
        comment = unescapeSQLString(child.getChild(0).getText());
        break;
      case HiveParser.TOK_TABLEPROPERTIES:
        tblProps = getProps((ASTNode) child.getChild(0));
        break;
      case HiveParser.TOK_VIEWPARTCOLS:
        partColNames = getColumnNames((ASTNode) child.getChild(0));
        break;
      case HiveParser.TOK_VIEWCLUSTERCOLS:
        assert distributeColNames == null && sortColNames == null;
        distributeColNames = getColumnNames((ASTNode) child.getChild(0));
        sortColNames = new ArrayList<>(distributeColNames);
        break;
      case HiveParser.TOK_VIEWDISTRIBUTECOLS:
        assert distributeColNames == null;
        distributeColNames = getColumnNames((ASTNode) child.getChild(0));
        break;
      case HiveParser.TOK_VIEWSORTCOLS:
        assert sortColNames == null;
        sortColNames = getColumnNames((ASTNode) child.getChild(0));
        break;
      case HiveParser.TOK_TABLEROWFORMAT:
        rowFormatParams.analyzeRowFormat(child);
        break;
      case HiveParser.TOK_TABLELOCATION:
        location = unescapeSQLString(child.getChild(0).getText());
        location = EximUtil.relativeToAbsolutePath(conf, location);
        inputs.add(toReadEntity(location));
        break;
      case HiveParser.TOK_TABLESERIALIZER:
        child = (ASTNode) child.getChild(0);
        storageFormat.setSerde(unescapeSQLString(child.getChild(0).getText()));
        if (child.getChildCount() == 2) {
          readProps((ASTNode) (child.getChild(1).getChild(0)),
              storageFormat.getSerdeProps());
        }
        break;
      case HiveParser.TOK_TABLEPARTCOLSBYSPEC:
        SessionStateUtil.addResourceOrThrow(conf, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC,
                PartitionTransform.getPartitionTransformSpec(child));
        partitionTransformSpecExists = true;
        break;
      default:
        assert false;
      }
    }

    validateStorageFormat(storageFormat, tblProps, partitionTransformSpecExists);

    storageFormat.fillDefaultStorageFormat(false, true);

    if (!ifNotExists) {
      // Verify that the table does not already exist
      // dumpTable is only used to check the conflict for non-temporary tables
      try {
        Table dumpTable = db.newTable(dbDotTable);
        if (null != db.getTable(dumpTable.getDbName(), dumpTable.getTableName(), false) &&
            !ctx.isExplainSkipExecution()) {
          throw new SemanticException(ErrorMsg.TABLE_ALREADY_EXISTS.getMsg(dbDotTable));
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
    }
    if (partColNames != null && (distributeColNames != null || sortColNames != null)) {
      // Verify that partition columns and data organization columns are not overlapping
      Set<String> partColNamesSet = new HashSet<>(partColNames);
      if (distributeColNames != null) {
        for (String colName : distributeColNames) {
          if (partColNamesSet.contains(colName)) {
            throw new SemanticException("Same column cannot be present in partition and cluster/distribute clause. "
                + "Column name: " + colName);
          }
        }
      }
      if (sortColNames != null) {
        for (String colName : sortColNames) {
          if (partColNamesSet.contains(colName)) {
            throw new SemanticException("Same column cannot be present in partition and cluster/sort clause. "
                + "Column name: " + colName);
          }
        }
      }
    }

    unparseTranslator.enable();

    if (makeAcid()) {
      if (tblProps == null) {
        tblProps = new HashMap<>();
      }
      tblProps = convertToAcidByDefault(storageFormat, dbDotTable, null, tblProps);
    }
    if (tblProps == null) {
      tblProps = new HashMap<>();
    }
    tblProps.put(hive_metastoreConstants.TABLE_IS_CTAS, "true");

    createVwDesc = new CreateMaterializedViewDesc(
        dbDotTable, cols, comment, tblProps, partColNames, sortColNames, distributeColNames,
        ifNotExists, rewriteEnabled,
        storageFormat.getInputFormat(), storageFormat.getOutputFormat(),
        location, storageFormat.getSerde(), storageFormat.getStorageHandler(),
        storageFormat.getSerdeProps());
    addDbAndTabToOutputs(new String[] {qualTabName.getDb(), qualTabName.getTable()}, TableType.MATERIALIZED_VIEW,
        false, tblProps, storageFormat);
    queryState.setCommandType(HiveOperation.CREATE_MATERIALIZED_VIEW);
    qb.setViewDesc(createVwDesc);

    return selectStmt;
  }

  private boolean makeAcid() {
    return MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID) &&
            HiveConf.getBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY) &&
            DbTxnManager.class.getCanonicalName().equals(HiveConf.getVar(conf, ConfVars.HIVE_TXN_MANAGER));
  }

  // validate the (materialized) view statement
  // check semantic conditions
  private void validateCreateView()
      throws SemanticException {
    try {
      // Do not allow view to be defined on temp table or other materialized view
      validateTablesUsed(this);
      if (createVwDesc.isRewriteEnabled()) {
        int nativeAcidCount = 0;
        int supportsSnapshotCount = 0;
        for (TableScanOperator ts : topOps.values()) {
          Table table = ts.getConf().getTableMetadata();
          if (SemanticAnalyzer.DUMMY_TABLE.equals(table.getTableName())) {
            continue;
          }
          if (AcidUtils.isTransactionalTable(table)) {
            ++nativeAcidCount;
          } else if (table.isNonNative() && table.getStorageHandler().areSnapshotsSupported()) {
            ++supportsSnapshotCount;
          } else {
            throw new SemanticException("Automatic rewriting for materialized view cannot "
                    + "be enabled if the materialized view uses non-transactional tables");
          }
          if (isNotBlank(ts.getConf().getAsOfTimestamp()) || isNotBlank(ts.getConf().getAsOfVersion())) {
            throw new SemanticException("Automatic rewriting for materialized view cannot "
                    + "be enabled if the materialized view uses time travel query.");
          }
        }
        if (nativeAcidCount > 0 && supportsSnapshotCount > 0) {
          throw new SemanticException("All materialized view source tables either must be native ACID tables or " +
                  "must support table snapshots.");
        }
      }

      if (!qb.hasTableDefined()) {
        throw new SemanticException("Materialized view must have a table defined.");
      }

      if (createVwDesc.isRewriteEnabled()) {
        if (!ctx.isCboSucceeded()) {
          String msg = "Cannot enable automatic rewriting for materialized view.";
          if (ctx.getCboInfo() != null) {
            msg += " " + ctx.getCboInfo();
          } else {
            msg += " Check CBO is turned on: set " + ConfVars.HIVE_CBO_ENABLED.varname;
          }
          throw new SemanticException(msg);
        }
        if (materializationValidationResult.getSupportedRewriteAlgorithms().isEmpty()) {
          createVwDesc.setRewriteEnabled(false);
        }
        String errorMessage = materializationValidationResult.getErrorMessage();
        if (isNotBlank(errorMessage)) {
          console.printError(errorMessage);
        }
      }
    } catch (HiveException e) {
      throw new SemanticException(e.getMessage(), e);
    }
  }

  // Process the position alias in GROUPBY and ORDERBY
  void processPositionAlias(ASTNode ast) throws SemanticException {
    boolean isBothByPos = HiveConf.getBoolVar(conf, ConfVars.HIVE_GROUPBY_ORDERBY_POSITION_ALIAS);
    boolean isGbyByPos = isBothByPos
        || HiveConf.getBoolVar(conf, ConfVars.HIVE_GROUPBY_POSITION_ALIAS);
    boolean isObyByPos = isBothByPos
        || HiveConf.getBoolVar(conf, ConfVars.HIVE_ORDERBY_POSITION_ALIAS);

    Deque<ASTNode> stack = new ArrayDeque<ASTNode>();
    stack.push(ast);

    while (!stack.isEmpty()) {
      ASTNode next = stack.pop();

      if (next.getChildCount()  == 0) {
        continue;
      }

      boolean isAllCol;
      ASTNode selectNode = null;
      ASTNode groupbyNode = null;
      ASTNode orderbyNode = null;

      // get node type
      int child_count = next.getChildCount();
      for (int child_pos = 0; child_pos < child_count; ++child_pos) {
        ASTNode node = (ASTNode) next.getChild(child_pos);
        int type = node.getToken().getType();
        if (type == HiveParser.TOK_SELECT || type == HiveParser.TOK_SELECTDI) {
          selectNode = node;
        } else if (type == HiveParser.TOK_GROUPBY) {
          groupbyNode = node;
        } else if (type == HiveParser.TOK_ORDERBY) {
          orderbyNode = node;
        }
      }

      if (selectNode != null) {
        int selectExpCnt = selectNode.getChildCount();

        // replace each of the position alias in GROUPBY with the actual column name
        if (groupbyNode != null) {
          for (int child_pos = 0; child_pos < groupbyNode.getChildCount(); ++child_pos) {
            ASTNode node = (ASTNode) groupbyNode.getChild(child_pos);
            if (node.getToken().getType() == HiveParser.Number) {
              if (isGbyByPos) {
                int pos = Integer.parseInt(node.getText());
                if (pos > 0 && pos <= selectExpCnt) {
                  groupbyNode.setChild(child_pos,
                      selectNode.getChild(pos - 1).getChild(0));
                } else {
                  throw new SemanticException(
                      ErrorMsg.INVALID_POSITION_ALIAS_IN_GROUPBY.getMsg(
                          "Position alias: " + pos + " does not exist\n" +
                              "The Select List is indexed from 1 to " + selectExpCnt));
                }
              } else {
                warn("Using constant number  " + node.getText() +
                    " in group by. If you try to use position alias when hive.groupby.position.alias is false, the position alias will be ignored.");
              }
            }
          }
        }

        // replace each of the position alias in ORDERBY with the actual column name,
        // if cbo is enabled, orderby position will be processed in genPlan
        if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED)
            && orderbyNode != null) {
          isAllCol = false;
          for (int child_pos = 0; child_pos < selectNode.getChildCount(); ++child_pos) {
            ASTNode node = (ASTNode) selectNode.getChild(child_pos).getChild(0);
            if (node != null && node.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
              isAllCol = true;
            }
          }
          for (int child_pos = 0; child_pos < orderbyNode.getChildCount(); ++child_pos) {
            ASTNode colNode = null;
            ASTNode node = null;
            if (orderbyNode.getChildCount() > 0) {
              colNode = (ASTNode) orderbyNode.getChild(child_pos).getChild(0);
              if (colNode.getChildCount() > 0) {
                node = (ASTNode) colNode.getChild(0);
              }
            }
            if (node != null && node.getToken().getType() == HiveParser.Number) {
              if (isObyByPos) {
                if (!isAllCol) {
                  int pos = Integer.parseInt(node.getText());
                  if (pos > 0 && pos <= selectExpCnt && selectNode.getChild(pos - 1).getChildCount() > 0) {
                    colNode.setChild(0, selectNode.getChild(pos - 1).getChild(0));
                  } else {
                    throw new SemanticException(
                        ErrorMsg.INVALID_POSITION_ALIAS_IN_ORDERBY.getMsg(
                            "Position alias: " + pos + " does not exist\n" +
                                "The Select List is indexed from 1 to " + selectExpCnt));
                  }
                } else {
                  throw new SemanticException(
                      ErrorMsg.NO_SUPPORTED_ORDERBY_ALLCOLREF_POS.getMsg());
                }
              } else { //if not using position alias and it is a number.
                warn("Using constant number " + node.getText() +
                    " in order by. If you try to use position alias when hive.orderby.position.alias is false, the position alias will be ignored.");
              }
            }
          }
        }
      }

      List<Node> childrenList = next.getChildren();
      for (int i = childrenList.size() - 1; i >= 0; i--) {
        stack.push((ASTNode)childrenList.get(i));
      }
    }
  }

  /**
   * process analyze ... noscan command
   * @param tree
   * @throws SemanticException
   */
  protected void processNoScanCommand (ASTNode tree) throws SemanticException {
    // check if it is noscan command
    checkNoScan(tree);

    //validate noscan
    if (this.noscan) {
      validateAnalyzeNoscan(tree);
    }
  }

  /**
   * Validate noscan command
   *
   * @param tree
   * @throws SemanticException
   */
  private void validateAnalyzeNoscan(ASTNode tree) throws SemanticException {
    // since it is noscan, it is true table name in command
    String tableName = getUnescapedName((ASTNode) tree.getChild(0).getChild(0));
    Table tbl;
    try {
      tbl = getTableObjectByName(tableName);
    } catch (InvalidTableException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName), e);
    }
    catch (HiveException e) {
      throw new SemanticException(e.getMessage(), e);
    }

    /* noscan uses hdfs apis to retrieve such information from Namenode.      */
    /* But that will be specific to hdfs. Through storagehandler mechanism,   */
    /* storage of table could be on any storage system: hbase, cassandra etc. */
    /* A nice error message should be given to user. */
    if (tbl.isNonNative()) {
      throw new SemanticException(ErrorMsg.ANALYZE_TABLE_NOSCAN_NON_NATIVE.getMsg(tbl
          .getTableName()));
    }
  }

  /**
   * It will check if this is analyze ... compute statistics noscan
   * @param tree
   */
  private void checkNoScan(ASTNode tree) {
    if (tree.getChildCount() > 1) {
      ASTNode child0 = (ASTNode) tree.getChild(0);
      ASTNode child1;
      if (child0.getToken().getType() == HiveParser.TOK_TAB) {
        child0 = (ASTNode) child0.getChild(0);
        if (child0.getToken().getType() == HiveParser.TOK_TABNAME) {
          child1 = (ASTNode) tree.getChild(1);
          if (child1.getToken().getType() == HiveParser.KW_NOSCAN) {
            this.noscan = true;
          }
        }
      }
    }
  }

  public QB getQB() {
    return qb;
  }

  void setQB(QB qb) {
    this.qb = qb;
  }

//--------------------------- PTF handling -----------------------------------

  /*
   * - a partitionTableFunctionSource can be a tableReference, a SubQuery or another
   *   PTF invocation.
   * - For a TABLEREF: set the source to the alias returned by processTable
   * - For a SubQuery: set the source to the alias returned by processSubQuery
   * - For a PTF invocation: recursively call processPTFChain.
   */
  private PTFInputSpec processPTFSource(QB qb, ASTNode inputNode) throws SemanticException{

    PTFInputSpec qInSpec = null;
    int type = inputNode.getType();
    String alias;
    switch(type)
    {
    case HiveParser.TOK_TABREF:
      alias = processTable(qb, inputNode);
      qInSpec = new PTFQueryInputSpec();
      ((PTFQueryInputSpec)qInSpec).setType(PTFQueryInputType.TABLE);
      ((PTFQueryInputSpec)qInSpec).setSource(alias);
      break;
    case HiveParser.TOK_SUBQUERY:
      alias = processSubQuery(qb, inputNode);
      qInSpec = new PTFQueryInputSpec();
      ((PTFQueryInputSpec)qInSpec).setType(PTFQueryInputType.SUBQUERY);
      ((PTFQueryInputSpec)qInSpec).setSource(alias);
      break;
    case HiveParser.TOK_PTBLFUNCTION:
      qInSpec = processPTFChain(qb, inputNode);
      break;
    default:
      throw new SemanticException(generateErrorMessage(inputNode,
          "Unknown input type to PTF"));
    }

    qInSpec.setAstNode(inputNode);
    return qInSpec;

  }

  /*
   * - tree form is
   *   ^(TOK_PTBLFUNCTION name alias? partitionTableFunctionSource partitioningSpec? arguments*)
   * - a partitionTableFunctionSource can be a tableReference, a SubQuery or another
   *   PTF invocation.
   */
  private PartitionedTableFunctionSpec processPTFChain(QB qb, ASTNode ptf)
      throws SemanticException{
    int child_count = ptf.getChildCount();
    if (child_count < 2) {
      throw new SemanticException(generateErrorMessage(ptf,
          "Not enough Children " + child_count));
    }

    PartitionedTableFunctionSpec ptfSpec = new PartitionedTableFunctionSpec();
    ptfSpec.setAstNode(ptf);

    /*
     * name
     */
    ASTNode nameNode = (ASTNode) ptf.getChild(0);
    ptfSpec.setName(nameNode.getText());

    int inputIdx = 1;

    /*
     * alias
     */
    ASTNode secondChild = (ASTNode) ptf.getChild(1);
    if ( secondChild.getType() == HiveParser.Identifier ) {
      ptfSpec.setAlias(secondChild.getText());
      inputIdx++;
    }

    /*
     * input
     */
    ASTNode inputNode = (ASTNode) ptf.getChild(inputIdx);
    ptfSpec.setInput(processPTFSource(qb, inputNode));

    int argStartIdx = inputIdx + 1;

    /*
     * partitioning Spec
     */
    int pSpecIdx = inputIdx + 1;
    ASTNode pSpecNode = ptf.getChildCount() > inputIdx ?
        (ASTNode) ptf.getChild(pSpecIdx) : null;
    if (pSpecNode != null && pSpecNode.getType() == HiveParser.TOK_PARTITIONINGSPEC)
    {
      PartitioningSpec partitioning = processPTFPartitionSpec(pSpecNode);
      ptfSpec.setPartitioning(partitioning);
      argStartIdx++;
    }

    /*
     * arguments
     */
    for(int i=argStartIdx; i < ptf.getChildCount(); i++)
    {
      ptfSpec.addArg((ASTNode) ptf.getChild(i));
    }
    return ptfSpec;
  }

  /*
   * - invoked during FROM AST tree processing, on encountering a PTF invocation.
   * - tree form is
   *   ^(TOK_PTBLFUNCTION name partitionTableFunctionSource partitioningSpec? arguments*)
   * - setup a PTFInvocationSpec for this top level PTF invocation.
   */
  private void processPTF(QB qb, ASTNode ptf) throws SemanticException{

    PartitionedTableFunctionSpec ptfSpec = processPTFChain(qb, ptf);

    Optional.ofNullable(ptfSpec.getAlias())
        .ifPresent(qb::addAlias);

    PTFInvocationSpec spec = new PTFInvocationSpec();
    spec.setFunction(ptfSpec);
    qb.addPTFNodeToSpec(ptf, spec);
  }

  private void handleQueryWindowClauses(QB qb, Phase1Ctx ctx_1, ASTNode node)
      throws SemanticException {
    WindowingSpec spec = qb.getWindowingSpec(ctx_1.dest);
    for(Node child : node.getChildren()) {
      processQueryWindowClause(spec, (ASTNode) child);
    }
  }

  private PartitionSpec processPartitionSpec(ASTNode node) {
    PartitionSpec pSpec = new PartitionSpec();
    for(Node child : node.getChildren()) {
      PartitionExpression exprSpec = new PartitionExpression();
      exprSpec.setExpression((ASTNode) child);
      pSpec.addExpression(exprSpec);
    }
    return pSpec;
  }

  private PartitioningSpec processPTFPartitionSpec(ASTNode pSpecNode)
  {
    PartitioningSpec partitioning = new PartitioningSpec();
    ASTNode firstChild = (ASTNode) pSpecNode.getChild(0);
    int type = firstChild.getType();

    if ( type == HiveParser.TOK_DISTRIBUTEBY || type == HiveParser.TOK_CLUSTERBY )
    {
      PartitionSpec pSpec = processPartitionSpec(firstChild);
      partitioning.setPartSpec(pSpec);
      ASTNode sortNode = pSpecNode.getChildCount() > 1 ? (ASTNode) pSpecNode.getChild(1) : null;
      if ( sortNode != null )
      {
        OrderSpec oSpec = processOrderSpec(sortNode);
        partitioning.setOrderSpec(oSpec);
      }
    }
    else if ( type == HiveParser.TOK_SORTBY || type == HiveParser.TOK_ORDERBY ) {
      OrderSpec oSpec = processOrderSpec(firstChild);
      partitioning.setOrderSpec(oSpec);
    }
    return partitioning;
  }

  private WindowFunctionSpec processWindowFunction(ASTNode node, ASTNode wsNode)
      throws SemanticException {
    WindowFunctionSpec wfSpec = new WindowFunctionSpec();

    switch(node.getType()) {
    case HiveParser.TOK_FUNCTIONSTAR:
      wfSpec.setStar(true);
      break;
    case HiveParser.TOK_FUNCTIONDI:
      wfSpec.setDistinct(true);
      break;
    }

    wfSpec.setExpression(node);

    ASTNode nameNode = (ASTNode) node.getChild(0);
    wfSpec.setName(nameNode.getText());

    for(int i=1; i < node.getChildCount()-1; i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      wfSpec.addArg(child);
    }

    if ( wsNode != null ) {
      WindowFunctionInfo functionInfo = FunctionRegistry.getWindowFunctionInfo(wfSpec.name);
      if (functionInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(wfSpec.name));
      }
      wfSpec.setRespectNulls(processRespectIgnoreNulls(functionInfo, wsNode));
      wfSpec.setWindowSpec(processWindowSpec(wsNode));
    }

    return wfSpec;
  }

  private boolean processRespectIgnoreNulls(WindowFunctionInfo functionInfo, ASTNode node)
      throws SemanticException {

    for(int i=0; i < node.getChildCount(); i++) {
      int type = node.getChild(i).getType();
      switch(type) {
      case HiveParser.TOK_RESPECT_NULLS:
        if (!functionInfo.isSupportsNullTreatment()) {
          throw new SemanticException(ErrorMsg.NULL_TREATMENT_NOT_SUPPORTED, functionInfo.getDisplayName());
        }
        return true;
      case HiveParser.TOK_IGNORE_NULLS:
        if (!functionInfo.isSupportsNullTreatment()) {
          throw new SemanticException(ErrorMsg.NULL_TREATMENT_NOT_SUPPORTED, functionInfo.getDisplayName());
        }
        return false;
      }
    }

    return true;
  }

  private boolean containsLeadLagUDF(ASTNode expressionTree) {
    int exprTokenType = expressionTree.getToken().getType();
    if (exprTokenType == HiveParser.TOK_FUNCTION) {
      assert (expressionTree.getChildCount() != 0);
      if (expressionTree.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = unescapeIdentifier(expressionTree.getChild(0)
            .getText());
        functionName = functionName.toLowerCase();
        if ( FunctionRegistry.LAG_FUNC_NAME.equals(functionName) ||
            FunctionRegistry.LEAD_FUNC_NAME.equals(functionName)
            ) {
          return true;
        }
      }
    }
    for (int i = 0; i < expressionTree.getChildCount(); i++) {
      if ( containsLeadLagUDF((ASTNode) expressionTree.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  private void processQueryWindowClause(WindowingSpec spec, ASTNode node)
      throws SemanticException {
    ASTNode nameNode = (ASTNode) node.getChild(0);
    ASTNode wsNode = (ASTNode) node.getChild(1);
    if(spec.getWindowSpecs() != null && spec.getWindowSpecs().containsKey(nameNode.getText())){
      throw new SemanticException(generateErrorMessage(nameNode,
          "Duplicate definition of window " + nameNode.getText() +
              " is not allowed"));
    }
    WindowSpec ws = processWindowSpec(wsNode);
    spec.addWindowSpec(nameNode.getText(), ws);
  }

  private WindowSpec processWindowSpec(ASTNode node) throws SemanticException {
    boolean hasSrcId = false, hasPartSpec = false, hasWF = false;
    int srcIdIdx = -1, partIdx = -1, wfIdx = -1;

    for(int i=0; i < node.getChildCount(); i++)
    {
      int type = node.getChild(i).getType();
      switch(type)
      {
      case HiveParser.Identifier:
        hasSrcId = true; srcIdIdx = i;
        break;
      case HiveParser.TOK_PARTITIONINGSPEC:
        hasPartSpec = true; partIdx = i;
        break;
      case HiveParser.TOK_WINDOWRANGE:
      case HiveParser.TOK_WINDOWVALUES:
        hasWF = true; wfIdx = i;
        break;
      }
    }

    WindowSpec ws = new WindowSpec();

    if (hasSrcId) {
      ASTNode nameNode = (ASTNode) node.getChild(srcIdIdx);
      ws.setSourceId(nameNode.getText());
    }

    if (hasPartSpec) {
      ASTNode partNode = (ASTNode) node.getChild(partIdx);
      PartitioningSpec partitioning = processPTFPartitionSpec(partNode);
      ws.setPartitioning(partitioning);
    }

    if (hasWF) {
      ASTNode wfNode = (ASTNode) node.getChild(wfIdx);
      WindowFrameSpec wfSpec = processWindowFrame(wfNode);
      ws.setWindowFrame(wfSpec);
    }

    return ws;
  }

  private WindowFrameSpec processWindowFrame(ASTNode node) throws SemanticException {
    int type = node.getType();
    BoundarySpec end = null;

    /*
     * A WindowFrame may contain just the Start Boundary or in the
     * between style of expressing a WindowFrame both boundaries
     * are specified.
     */
    BoundarySpec start = processBoundary((ASTNode) node.getChild(0));
    if ( node.getChildCount() > 1 ) {
      end = processBoundary((ASTNode) node.getChild(1));
    }
    // Note: TOK_WINDOWVALUES means RANGE type, TOK_WINDOWRANGE means ROWS type
    return new WindowFrameSpec(type == HiveParser.TOK_WINDOWVALUES ? WindowType.RANGE : WindowType.ROWS, start, end);
  }

  private BoundarySpec processBoundary(ASTNode node)  throws SemanticException {
    BoundarySpec bs = new BoundarySpec();
    int type = node.getType();
    boolean hasAmt = true;

    switch(type)
    {
    case HiveParser.KW_PRECEDING:
      bs.setDirection(Direction.PRECEDING);
      break;
    case HiveParser.KW_FOLLOWING:
      bs.setDirection(Direction.FOLLOWING);
      break;
    case HiveParser.KW_CURRENT:
      bs.setDirection(Direction.CURRENT);
      hasAmt = false;
      break;
    default:
      // no-op
    }

    if ( hasAmt )
    {
      ASTNode amtNode = (ASTNode) node.getChild(0);
      if ( amtNode.getType() == HiveParser.KW_UNBOUNDED)
      {
        bs.setAmt(BoundarySpec.UNBOUNDED_AMOUNT);
      }
      else
      {
        int amt = Integer.parseInt(amtNode.getText());
        if ( amt < 0 ) {
          throw new SemanticException(
              "Window Frame Boundary Amount must be a non-negative integer, provided amount is: " + amt);
        } else if (amt == 0) {
          // Convert 0 PRECEDING/FOLLOWING to CURRENT ROW
          LOG.info("Converting 0 {} to CURRENT ROW", bs.getDirection());
          bs.setDirection(Direction.CURRENT);
          hasAmt = false;
        } else {
          bs.setAmt(amt);
        }
      }
    }

    return bs;
  }

//--------------------------- PTF handling: PTFInvocationSpec to PTFDesc --------------------------

  private PTFDesc translatePTFInvocationSpec(PTFInvocationSpec ptfQSpec, RowResolver inputRR)
      throws SemanticException {
    PTFTranslator translator = new PTFTranslator();
    return translator.translate(ptfQSpec, this, conf, inputRR, unparseTranslator);
  }

  private Operator genPTFPlan(PTFInvocationSpec ptfQSpec, Operator input) throws SemanticException {
    List<PTFInvocationSpec> componentQueries = PTFTranslator.componentize(ptfQSpec);
    for (PTFInvocationSpec ptfSpec : componentQueries) {
      input = genPTFPlanForComponentQuery(ptfSpec, input);
    }
    LOG.debug("Created PTF Plan ");
    return input;
  }


  /**
   * Construct the data structures containing ExprNodeDesc for partition
   * columns and order columns. Use the input definition to construct the list
   * of output columns for the ReduceSinkOperator
   */
  private void buildPTFReduceSinkDetails(PartitionedTableFunctionDef tabDef,
                                 List<ExprNodeDesc> partCols,
                                 List<ExprNodeDesc> orderCols,
                                 StringBuilder orderString,
                                 StringBuilder nullOrderString) {

    List<PTFExpressionDef> partColList = tabDef.getPartition().getExpressions();

    for (PTFExpressionDef colDef : partColList) {
      ExprNodeDesc exprNode = colDef.getExprNode();
      if (ExprNodeDescUtils.indexOf(exprNode, partCols) < 0) {
        partCols.add(exprNode);
        orderCols.add(exprNode);
        orderString.append('+');
        nullOrderString.append('a');
      }
    }

    /*
     * Order columns are used as key columns for constructing
     * the ReduceSinkOperator
     * Since we do not explicitly add these to outputColumnNames,
     * we need to set includeKeyCols = false while creating the
     * ReduceSinkDesc
     */
    List<OrderExpressionDef> orderColList = tabDef.getOrder().getExpressions();
    for (OrderExpressionDef colDef : orderColList) {
      char orderChar = colDef.getOrder() == PTFInvocationSpec.Order.ASC ? '+' : '-';
      char nullOrderChar = colDef.getNullOrder() == PTFInvocationSpec.NullOrder.NULLS_FIRST ? 'a' : 'z';
      int index = ExprNodeDescUtils.indexOf(colDef.getExprNode(), orderCols);
      if (index >= 0) {
        orderString.setCharAt(index, orderChar);
        nullOrderString.setCharAt(index, nullOrderChar);
        continue;
      }
      orderCols.add(colDef.getExprNode());
      orderString.append(orderChar);
      nullOrderString.append(nullOrderChar);
    }
  }

  private Operator genPTFPlanForComponentQuery(PTFInvocationSpec ptfQSpec, Operator input)
      throws SemanticException {
    /*
     * 1. Create the PTFDesc from the Qspec attached to this QB.
     */
    RowResolver rr = opParseCtx.get(input).getRowResolver();
    PTFDesc ptfDesc = translatePTFInvocationSpec(ptfQSpec, rr);

    /*
     * 2. build Map-side Op Graph. Graph template is either:
     * Input -> PTF_map -> ReduceSink
     * or
     * Input -> ReduceSink
     *
     * Here the ExprNodeDescriptors in the QueryDef are based on the Input Operator's RR.
     */
    {
      PartitionedTableFunctionDef tabDef = ptfDesc.getStartOfChain();

      /*
       * a. add Map-side PTF Operator if needed
       */
      if (tabDef.isTransformsRawInput() )
      {
        RowResolver ptfMapRR = tabDef.getRawInputShape().getRr();

        ptfDesc.setMapSide(true);
        input = putOpInsertMap(OperatorFactory.getAndMakeChild(ptfDesc,
            new RowSchema(ptfMapRR.getColumnInfos()), input), ptfMapRR);
        rr = opParseCtx.get(input).getRowResolver();
      }

      /*
       * b. Build Reduce Sink Details (keyCols, valueCols, outColNames etc.) for this ptfDesc.
       */

      List<ExprNodeDesc> partCols = new ArrayList<ExprNodeDesc>();
      List<ExprNodeDesc> orderCols = new ArrayList<ExprNodeDesc>();
      StringBuilder orderString = new StringBuilder();
      StringBuilder nullOrderString = new StringBuilder();

      /*
       * Use the input RR of TableScanOperator in case there is no map-side
       * reshape of input.
       * If the parent of ReduceSinkOperator is PTFOperator, use it's
       * output RR.
       */
      buildPTFReduceSinkDetails(tabDef, partCols, orderCols, orderString, nullOrderString);
      input = genReduceSinkPlan(input, partCols, orderCols, orderString.toString(),
          nullOrderString.toString(), -1, Operation.NOT_ACID, false);
    }

    /*
     * 3. build Reduce-side Op Graph
     */
    {

      /*
       * c. Rebuilt the QueryDef.
       * Why?
       * - so that the ExprNodeDescriptors in the QueryDef are based on the
       *   Select Operator's RowResolver
       */
      rr = opParseCtx.get(input).getRowResolver();
      ptfDesc = translatePTFInvocationSpec(ptfQSpec, rr);

      /*
       * d. Construct PTF Operator.
       */
      RowResolver ptfOpRR = ptfDesc.getFuncDef().getOutputShape().getRr();
      input = putOpInsertMap(OperatorFactory.getAndMakeChild(ptfDesc,
          new RowSchema(ptfOpRR.getColumnInfos()),
          input), ptfOpRR);

    }
    return input;
  }

//--------------------------- Windowing handling: PTFInvocationSpec to PTFDesc --------------------

  private Operator genWindowingPlan(QB qb, WindowingSpec wSpec, Operator input) throws SemanticException {
    wSpec.validateAndMakeEffective();

    if (!isCBOExecuted() && !qb.getParseInfo().getDestToGroupBy().isEmpty()) {
      // If CBO did not optimize the query, we might need to replace grouping function
      final String selClauseName = qb.getParseInfo().getClauseNames().iterator().next();
      final boolean cubeRollupGrpSetPresent = (!qb.getParseInfo().getDestRollups().isEmpty()
          || !qb.getParseInfo().getDestGroupingSets().isEmpty()
          || !qb.getParseInfo().getDestCubes().isEmpty());
      for (WindowExpressionSpec wExprSpec : wSpec.getWindowExpressions()) {
        // Special handling of grouping function
        wExprSpec.setExpression(rewriteGroupingFunctionAST(
            getGroupByForClause(qb.getParseInfo(), selClauseName), wExprSpec.getExpression(),
            !cubeRollupGrpSetPresent));
      }
    }

    WindowingComponentizer groups = new WindowingComponentizer(wSpec);
    RowResolver rr = opParseCtx.get(input).getRowResolver();

    while(groups.hasNext() ) {
      wSpec = groups.next(conf, this, unparseTranslator, rr);
      input = genReduceSinkPlanForWindowing(wSpec, rr, input);
      rr = opParseCtx.get(input).getRowResolver();
      PTFTranslator translator = new PTFTranslator();
      PTFDesc ptfDesc = translator.translate(wSpec, this, conf, rr, unparseTranslator);
      RowResolver ptfOpRR = ptfDesc.getFuncDef().getOutputShape().getRr();
      input = putOpInsertMap(OperatorFactory.getAndMakeChild(ptfDesc,
          new RowSchema(ptfOpRR.getColumnInfos()), input), ptfOpRR);
      input = genSelectAllDesc(input);
      rr = ptfOpRR;
    }

    return input;
  }

  private Operator genReduceSinkPlanForWindowing(WindowingSpec spec,
                                                 RowResolver inputRR, Operator input) throws SemanticException{

    List<ExprNodeDesc> partCols = new ArrayList<ExprNodeDesc>();
    List<ExprNodeDesc> orderCols = new ArrayList<ExprNodeDesc>();
    StringBuilder order = new StringBuilder();
    StringBuilder nullOrder = new StringBuilder();

    for (PartitionExpression partCol : spec.getQueryPartitionSpec().getExpressions()) {
      ExprNodeDesc partExpr = genExprNodeDesc(partCol.getExpression(), inputRR);
      if (ExprNodeDescUtils.indexOf(partExpr, partCols) < 0) {
        partCols.add(partExpr);
        orderCols.add(partExpr);
        order.append('+');
        nullOrder.append('a');
      }
    }

    if (spec.getQueryOrderSpec() != null) {
      for (OrderExpression orderCol : spec.getQueryOrderSpec().getExpressions()) {
        ExprNodeDesc orderExpr = genExprNodeDesc(orderCol.getExpression(), inputRR);
        char orderChar = orderCol.getOrder() == PTFInvocationSpec.Order.ASC ? '+' : '-';
        char nullOrderChar = orderCol.getNullOrder() == PTFInvocationSpec.NullOrder.NULLS_FIRST ? 'a' : 'z';
        int index = ExprNodeDescUtils.indexOf(orderExpr, orderCols);
        if (index >= 0) {
          order.setCharAt(index, orderChar);
          nullOrder.setCharAt(index, nullOrderChar);
          continue;
        }
        orderCols.add(genExprNodeDesc(orderCol.getExpression(), inputRR));
        order.append(orderChar);
        nullOrder.append(nullOrderChar);
      }
    }

    return genReduceSinkPlan(input, partCols, orderCols, order.toString(), nullOrder.toString(),
        -1, Operation.NOT_ACID, false);
  }

  public static List<WindowExpressionSpec> parseSelect(String selectExprStr)
      throws SemanticException
  {
    ASTNode selNode = null;
    try {
      ParseDriver pd = new ParseDriver();
      selNode = pd.parseSelect(selectExprStr, null).getTree();
    } catch (ParseException pe) {
      throw new SemanticException(pe);
    }

    List<WindowExpressionSpec> selSpec = new ArrayList<WindowExpressionSpec>();
    int childCount = selNode.getChildCount();
    for (int i = 0; i < childCount; i++) {
      ASTNode selExpr = (ASTNode) selNode.getChild(i);
      if (selExpr.getType() != HiveParser.TOK_SELEXPR) {
        throw new SemanticException(String.format(
            "Only Select expressions supported in dynamic select list: %s", selectExprStr));
      }
      ASTNode expr = (ASTNode) selExpr.getChild(0);
      if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
        throw new SemanticException(
            String.format("'%s' column not allowed in dynamic select list", selectExprStr));
      }
      ASTNode aliasNode = selExpr.getChildCount() > 1
          && selExpr.getChild(1).getType() == HiveParser.Identifier ?
          (ASTNode) selExpr.getChild(1) : null;
      String alias = null;
      if ( aliasNode != null ) {
        alias = aliasNode.getText();
      }
      else {
        String[] tabColAlias = getColAlias(selExpr, null, null, true, -1);
        alias = tabColAlias[1];
      }
      WindowExpressionSpec exprSpec = new WindowExpressionSpec();
      exprSpec.setAlias(alias);
      exprSpec.setExpression(expr);
      selSpec.add(exprSpec);
    }

    return selSpec;
  }

  private void addAlternateGByKeyMappings(ASTNode gByExpr, ColumnInfo colInfo,
                                          Operator<? extends OperatorDesc> reduceSinkOp, RowResolver gByRR) {
    if ( gByExpr.getType() == HiveParser.DOT
        && gByExpr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL ) {
      String tab_alias = BaseSemanticAnalyzer.unescapeIdentifier(gByExpr
          .getChild(0).getChild(0).getText().toLowerCase());
      String col_alias = BaseSemanticAnalyzer.unescapeIdentifier(
          gByExpr.getChild(1).getText().toLowerCase());
      gByRR.put(tab_alias, col_alias, colInfo);
    } else if ( gByExpr.getType() == HiveParser.TOK_TABLE_OR_COL ) {
      String col_alias = BaseSemanticAnalyzer.unescapeIdentifier(gByExpr
          .getChild(0).getText().toLowerCase());
      String tab_alias = null;
      /*
       * If the input to the GBy has a tab alias for the column, then add an entry
       * based on that tab_alias.
       * For e.g. this query:
       * select b.x, count(*) from t1 b group by x
       * needs (tab_alias=b, col_alias=x) in the GBy RR.
       * tab_alias=b comes from looking at the RowResolver that is the ancestor
       * before any GBy/ReduceSinks added for the GBY operation.
       */
      Operator<? extends OperatorDesc> parent = reduceSinkOp;
      while ( parent instanceof ReduceSinkOperator ||
          parent instanceof GroupByOperator ) {
        parent = parent.getParentOperators().get(0);
      }
      RowResolver parentRR = opParseCtx.get(parent).getRowResolver();
      try {
        tab_alias = Optional.ofNullable(parentRR.get(null, col_alias))
            .map(ColumnInfo::getTabAlias)
            .orElse(null);
      } catch (SemanticException se) {
      }
      gByRR.put(tab_alias, col_alias, colInfo);
    }
  }

  private WriteEntity.WriteType determineWriteType(LoadTableDesc ltd, String dest) {

    if (ltd == null) {
      return WriteEntity.WriteType.INSERT_OVERWRITE;
    }
    return ((ltd.getLoadFileType() == LoadFileType.REPLACE_ALL || ltd
        .isInsertOverwrite()) ? WriteEntity.WriteType.INSERT_OVERWRITE : getWriteType(dest));

  }

  private WriteEntity.WriteType getWriteType(String dest) {
    return updating(dest) ? WriteEntity.WriteType.UPDATE :
        (deleting(dest) ? WriteEntity.WriteType.DELETE : WriteEntity.WriteType.INSERT);
  }
  private boolean isAcidOutputFormat(Class<? extends OutputFormat> of) {
    return Arrays.asList(of.getInterfaces()).contains(AcidOutputFormat.class);
  }

  // Note that this method assumes you have already decided this is an Acid table.  It cannot
  // figure out if a table is Acid or not.
  private AcidUtils.Operation getAcidType(String destination) {
    return deleting(destination) ? AcidUtils.Operation.DELETE :
        updating(destination) ? AcidUtils.Operation.UPDATE : AcidUtils.Operation.INSERT;
  }

  private Context.Operation getWriteOperation(String destination) {
    return deleting(destination) ? Context.Operation.DELETE :
        updating(destination) ? Context.Operation.UPDATE : 
        merging(destination) ? Context.Operation.MERGE : Context.Operation.OTHER;
  }

  private AcidUtils.Operation getAcidType(Class<? extends OutputFormat> of, String dest,
      boolean isMM) {

    // no need for any checks in the case of insert-only
    if (isMM) {
      return getAcidType(dest);
    }

    if (SessionState.get() == null || !getTxnMgr().supportsAcid()) {
      return AcidUtils.Operation.NOT_ACID;
    } else if (isAcidOutputFormat(of)) {
      return getAcidType(dest);
    } else {
      return AcidUtils.Operation.NOT_ACID;
    }
  }

  protected boolean updating(String destination) {
    return destination.startsWith(Context.DestClausePrefix.UPDATE.toString());
  }

  private boolean deleting(String destination) {
    return destination.startsWith(Context.DestClausePrefix.DELETE.toString());
  }
  
  private boolean merging(String destination) {
    return destination.startsWith(Context.DestClausePrefix.MERGE.toString());
  }

  // Make sure the proper transaction manager that supports ACID is being used
  private void checkAcidTxnManager(Table table) throws SemanticException {
    if (SessionState.get() != null && !getTxnMgr().supportsAcid()
        && !HiveConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST_REPL)) {
      throw new SemanticException(ErrorMsg.TXNMGR_NOT_ACID, table.getDbName(), table.getTableName());
    }
  }

  ASTNode genSelectDIAST(RowResolver rr) {
    Map<String, Map<String, ColumnInfo>> map = rr.getRslvMap();
    ASTNode selectDI = new ASTNode(SELECTDI_TOKEN);
    // Note: this will determine the order of columns in the result. For now, the columns for each
    //       table will be together; the order of the tables, as well as the columns within each
    //       table, is deterministic, but undefined - RR stores them in the order of addition.
    for (String tabAlias : map.keySet()) {
      for (Entry<String, ColumnInfo> entry : map.get(tabAlias).entrySet()) {
        selectDI.addChild(buildSelExprSubTree(tabAlias, entry.getKey()));
      }
    }
    return selectDI;
  }

  private ASTNode buildSelExprSubTree(String tableAlias, String col) {
    tableAlias = StringInternUtils.internIfNotNull(tableAlias);
    col = StringInternUtils.internIfNotNull(col);
    ASTNode selexpr = new ASTNode(SELEXPR_TOKEN);
    ASTNode tableOrCol = new ASTNode(TABLEORCOL_TOKEN);
    ASTNode dot = new ASTNode(DOT_TOKEN);
    tableOrCol.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, tableAlias)));
    dot.addChild(tableOrCol);
    dot.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, col)));
    selexpr.addChild(dot);
    return selexpr;
  }

  private void copyInfoToQueryProperties(QueryProperties queryProperties) {
    if (qb != null) {
      queryProperties.setQuery(qb.getIsQuery() && !forViewCreation);
      queryProperties.setAnalyzeCommand(qb.getParseInfo().isAnalyzeCommand());
      queryProperties.setNoScanAnalyzeCommand(qb.getParseInfo().isNoScanAnalyzeCommand());
      queryProperties.setAnalyzeRewrite(qb.isAnalyzeRewrite());
      queryProperties.setCTAS(qb.getTableDesc() != null);
      if (qb.getParseInfo().hasInsertTables()) {
        queryProperties.setQueryType(QueryProperties.QueryType.DML);
      }
      queryProperties.setHasOuterOrderBy(!qb.getParseInfo().getIsSubQ() &&
          !qb.getParseInfo().getDestToOrderBy().isEmpty());
      queryProperties.setOuterQueryLimit(qb.getParseInfo().getOuterQueryLimit());
      queryProperties.setView(forViewCreation);
      queryProperties.setMaterializedView(qb.isMaterializedView());
    }
  }

  private void warn(String msg) {
    SessionState.getConsole().printInfo(String.format("Warning: %s", msg));
  }

  public List<LoadFileDesc> getLoadFileWork() {
    return loadFileWork;
  }

  public List<LoadTableDesc> getLoadTableWork() {
    return loadTableWork;
  }

  public void setLoadFileWork(List<LoadFileDesc> loadFileWork) {
    this.loadFileWork = loadFileWork;
  }

  public void setLoadTableWork(List<LoadTableDesc> tblWork) {
    this.loadTableWork = tblWork;
  }

  private void quoteIdentifierTokens(TokenRewriteStream tokenRewriteStream) {
    if (conf.getVar(ConfVars.HIVE_QUOTEDID_SUPPORT).equals("none")) {
      return;
    }

    for (int idx = tokenRewriteStream.MIN_TOKEN_INDEX; idx <= tokenRewriteStream.size()-1; idx++) {
      Token curTok = tokenRewriteStream.get(idx);
      if (curTok.getType() == HiveLexer.Identifier) {
        // The Tokens have no distinction between Identifiers and QuotedIdentifiers.
        // Ugly solution is just to surround all identifiers with quotes.
        // Re-escape any backtick (`) characters in the identifier.
        String escapedTokenText = curTok.getText().replaceAll("`", "``");
        tokenRewriteStream.replace(curTok, "`" + escapedTokenText + "`");
      }
    }
  }

  /**
   * Generate the query string for this query (with fully resolved table references).
   * @return The query string with resolved references. NULL if an error occurred.
   */
  private String getQueryStringForCache(ASTNode ast) {
    unparseTranslator.applyTranslations(ctx.getTokenRewriteStream(), RESULTS_CACHE_KEY_TOKEN_REWRITE_PROGRAM);
    return ctx.getTokenRewriteStream()
            .toString(RESULTS_CACHE_KEY_TOKEN_REWRITE_PROGRAM, ast.getTokenStartIndex(), ast.getTokenStopIndex());
  }

  private ValidTxnWriteIdList getQueryValidTxnWriteIdList() throws SemanticException {
    // TODO: Once HIVE-18948 is in, should be able to retrieve writeIdList from the conf.
    //cachedWriteIdList = AcidUtils.getValidTxnWriteIdList(conf);
    //
    List<String> transactionalTables = tablesFromReadEntities(inputs)
      .stream()
      .filter(AcidUtils::isTransactionalTable)
      .map(Table::getFullyQualifiedName)
      .collect(Collectors.toList());
    
    if (transactionalTables.size() > 0) {
      String txnString = queryState.getValidTxnList();
      if (txnString == null) {
        return null;
      }
      try {
        return getTxnMgr().getValidWriteIds(transactionalTables, txnString);
      } catch (Exception err) {
        String msg = "Error while getting the txnWriteIdList for tables " + transactionalTables
                + " and validTxnList " + conf.get(ValidTxnList.VALID_TXNS_KEY);
        throw new SemanticException(msg, err);
      }
    }

    // No transactional tables.
    return null;
  }

  private QueryResultsCache.LookupInfo createLookupInfoForQuery(ASTNode astNode) throws SemanticException {
    QueryResultsCache.LookupInfo lookupInfo = null;
    String queryString = getQueryStringForCache(astNode);
    if (queryString != null) {
      ValidTxnWriteIdList writeIdList = getQueryValidTxnWriteIdList();
      lookupInfo = new QueryResultsCache.LookupInfo(queryString, () -> writeIdList);
    }
    return lookupInfo;
  }

  private boolean isResultsCacheEnabled() {
    return conf.getBoolVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_ENABLED) &&
        !(SessionState.get().isHiveServerQuery() && conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS));
  }

  /**
   * Set the query plan to use cache entry passed in to return the query results.
   * @param cacheEntry The results cache entry that will be used to resolve the query.
   */
  private void useCachedResult(QueryResultsCache.CacheEntry cacheEntry, boolean needsReset) {
    if (needsReset) {
      reset(true);
      inputs.clear();
    }

    // Change query FetchTask to use new location specified in results cache.
    FetchTask fetchTask = (FetchTask) TaskFactory.get(cacheEntry.getFetchWork());
    setFetchTask(fetchTask);

    queryState.setCommandType(cacheEntry.getQueryInfo().getHiveOperation());
    resultSchema = cacheEntry.getQueryInfo().getResultSchema();
    setTableAccessInfo(cacheEntry.getQueryInfo().getTableAccessInfo());
    setColumnAccessInfo(cacheEntry.getQueryInfo().getColumnAccessInfo());
    inputs.addAll(cacheEntry.getQueryInfo().getInputs());

    // Set recursive traversal in case the cached query was UNION generated by Tez.
    conf.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, true);

    // Indicate that the query will use a cached result.
    setCacheUsage(new CacheUsage(
        CacheUsage.CacheStatus.QUERY_USING_CACHE, cacheEntry));
  }

  private QueryResultsCache.QueryInfo createCacheQueryInfoForQuery(QueryResultsCache.LookupInfo lookupInfo) {
    long queryTime = SessionState.get().getQueryCurrentTimestamp().toEpochMilli();
    return new QueryResultsCache.QueryInfo(queryTime, lookupInfo, queryState.getHiveOperation(),
        resultSchema, getTableAccessInfo(), getColumnAccessInfo(), inputs);
  }

  /**
   * Some initial checks for a query to see if we can look this query up in the results cache.
   */
  private boolean queryTypeCanUseCache(QB qb) {
    if (qb == null || qb.getParseInfo() == null) {
      return false;
    }
    if (this instanceof ColumnStatsSemanticAnalyzer) {
      // Column stats generates "select compute_stats() .." queries.
      // Disable caching for these.
      LOG.debug("Query type cannot use cache (ColumnStatsSemanticAnalyzer)");
      return false;
    }
    if (queryState.getHiveOperation() != HiveOperation.QUERY) {
      LOG.debug("Query type cannot use cache (HiveOperation is not a QUERY)");
      return false;
    }
    if (Optional.of(qb.getParseInfo()).filter(pi ->
            pi.isAnalyzeCommand() || pi.hasInsertTables() || pi.isInsertOverwriteDirectory())
        .isPresent()) {
      LOG.debug("Query type cannot use cache (analyze, insert, or IOWD)");
      return false;
    }
    // HIVE-19096 - disable for explain and explain analyze
    if (ctx.getExplainAnalyze() != null) {
      LOG.debug("Query type cannot use cache (explain analyze command)");
      return false;
    }

    return true;
  }

  private boolean needsTransform() {
    return SessionState.get().getAuthorizerV2() != null &&
        SessionState.get().getAuthorizerV2().needTransform();
  }

  /**
   * Called after a query plan has been generated, to determine if the results of this query
   * can be added to the results cache.
   */
  private boolean queryCanBeCached() {
    if (!queryTypeCanUseCache(qb)) {
      LOG.info("Not eligible for results caching - wrong query type");
      return false;
    }

    // Query should have a fetch task.
    if (getFetchTask() == null) {
      LOG.info("Not eligible for results caching - no fetch task");
      return false;
    }

    // At least one mr/tez job
    if (Utilities.getNumClusterJobs(getRootTasks()) == 0) {
      LOG.info("Not eligible for results caching - no mr/tez jobs");
      return false;
    }

    // The query materialization validation check only occurs in CBO. Thus only cache results if CBO was used.
    if (!ctx.isCboSucceeded()) {
      LOG.info("Caching of query results is disabled if CBO was not run.");
      QueryResultsCache.incrementMetric(MetricsConstant.QC_INVALID_FOR_CACHING);
      return false;
    }

    if (!isValidQueryCaching()) {
      LOG.info("Not eligible for results caching - {}", getInvalidResultCacheReason());
      QueryResultsCache.incrementMetric(MetricsConstant.QC_INVALID_FOR_CACHING);
      return false;
    }

    if (!conf.getBoolVar(ConfVars.HIVE_QUERY_RESULTS_CACHE_NONTRANSACTIONAL_TABLES_ENABLED)) {
      List<Table> nonTransactionalTables = getNonTransactionalTables();
      if (nonTransactionalTables.size() > 0) {
        LOG.info("Not eligible for results caching - query contains non-transactional tables {}",
            nonTransactionalTables);
        return false;
      }
    }
    return true;
  }

  private Set<Table> tablesFromReadEntities(Set<ReadEntity> readEntities) {
    return readEntities.stream()
        .filter(entity -> entity.getType() == Entity.Type.TABLE)
        .map(Entity::getTable)
        .collect(Collectors.toSet());
  }

  private List<Table> getNonTransactionalTables() {
    // views have been expanded by CBO already and can be ignored
    return tablesFromReadEntities(inputs)
        .stream()
        .filter(table -> !table.isView())
        .filter(table -> !AcidUtils.isTransactionalTable(table))
        .collect(Collectors.toList());
  }

  /**
   * Check the query results cache to see if the query represented by the lookupInfo can be
   * answered using the results cache. If the cache contains a suitable entry, the semantic analyzer
   * will be configured to use the found cache entry to answer the query.
   */
  private boolean checkResultsCache(QueryResultsCache.LookupInfo lookupInfo, boolean needsReset) {
    if (lookupInfo == null) {
      return false;
    }
    try {
      // In case this has not been initialized elsewhere.
      QueryResultsCache.initialize(conf);
    } catch (Exception err) {
      throw new IllegalStateException(err);
    }
    // Don't increment the reader count for explain queries.
    boolean isExplainQuery = (ctx.getExplainConfig() != null);
    do {
      QueryResultsCache.CacheEntry cacheEntry = QueryResultsCache.getInstance().lookup(lookupInfo);
      if (cacheEntry != null) {
        // Potentially wait on the cache entry if entry is in PENDING status
        // Blocking here can potentially be dangerous - for example if the global compile lock
        // is used this will block all subsequent queries that try to acquire the compile lock,
        // so it should not be done unless parallel compilation is enabled.
        // We might not want to block for explain queries as well.
        if (cacheEntry.getStatus() == QueryResultsCache.CacheEntryStatus.PENDING) {
          if (!isExplainQuery &&
              conf.getBoolVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_WAIT_FOR_PENDING_RESULTS) &&
              conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_PARALLEL_COMPILATION)) {
            if (!cacheEntry.waitForValidStatus()) {
              LOG.info("Waiting on pending cacheEntry, but it failed to become valid");
              // The pending query we were waiting on failed, but there might still be another
              // pending or completed entry in the cache that can satisfy this query. Lookup again.
              continue;
            }
          } else {
            LOG.info("Not waiting for pending cacheEntry");
            return false;
          }
        }

        if (cacheEntry.getStatus() == QueryResultsCache.CacheEntryStatus.VALID) {
          if (!isExplainQuery) {
            if (!cacheEntry.addReader()) {
              return false;
            }
          }
          // Use the cache rather than full query execution.
          // At this point the caller should return from semantic analysis.
          useCachedResult(cacheEntry, needsReset);
          return true;
        }
      }
    } while (false);
    return false;
  }

  private static final class ColsAndTypes {
    public ColsAndTypes(String cols, String colTypes) {
      this.cols = cols;
      this.colTypes = colTypes;
    }
    public String cols;
    public String colTypes;
  }

  public MaterializationValidationResult getMaterializationValidationResult() {
    return materializationValidationResult;
  }

  public void setMaterializationValidationResult(
      MaterializationValidationResult materializationValidationResult) {
    this.materializationValidationResult =
        materializationValidationResult;
  }

  public String getInvalidResultCacheReason() {
    return invalidResultCacheReason;
  }

  public void setInvalidResultCacheReason(
      String invalidQueryMaterializationReason) {
    this.invalidResultCacheReason = invalidQueryMaterializationReason;
  }

  public boolean isValidQueryCaching() {
    return (invalidResultCacheReason == null);
  }

  public void forViewCreation(String fqViewName) {
    this.fqViewName = fqViewName;
    this.forViewCreation = true;
  }

  public Map<String, TableScanOperator> getTopOps() {
    return topOps;
  }

  public Map<String, ReadEntity> getViewAliasToInput() {
    return viewAliasToInput;
  }

  public Operator getSinkOp() {
    return sinkOp;
  }

  protected enum MaterializationRebuildMode {
    NONE,
    INSERT_OVERWRITE_REBUILD,
    AGGREGATE_INSERT_REBUILD,
    AGGREGATE_INSERT_DELETE_REBUILD,
    JOIN_INSERT_REBUILD
  }

  /**
   * Append list of partition columns to Insert statement, i.e. the 1st set of partCol1,partCol2
   * INSERT INTO T PARTITION(partCol1,partCol2...) SELECT col1, ... partCol1,partCol2...
   */
  protected void addPartitionColsToInsert(List<FieldSchema> partCols, StringBuilder rewrittenQueryStr) {
    addPartitionColsToInsert(partCols, null, rewrittenQueryStr);
  }

  /**
   * Append list of partition columns to Insert statement. If user specified partition spec, then
   * use it to get/set the value for partition column else use dynamic partition mode with no value.
   * Static partition mode:
   * INSERT INTO T PARTITION(partCol1=val1,partCol2...) SELECT col1, ... partCol1,partCol2...
   * Dynamic partition mode:
   * INSERT INTO T PARTITION(partCol1,partCol2...) SELECT col1, ... partCol1,partCol2...
   */
  protected void addPartitionColsToInsert(List<FieldSchema> partCols,
                                          Map<String, String> partSpec,
                                          StringBuilder rewrittenQueryStr) {
    // If the table is partitioned we have to put the partition() clause in
    if (partCols != null && partCols.size() > 0) {
      rewrittenQueryStr.append(" partition (");
      boolean first = true;
      for (FieldSchema fschema : partCols) {
        if (first) {
          first = false;
        } else {
          rewrittenQueryStr.append(", ");
        }
        // Would be nice if there was a way to determine if quotes are needed
        rewrittenQueryStr.append(HiveUtils.unparseIdentifier(fschema.getName(), this.conf));
        String partVal = (partSpec != null) ? partSpec.get(fschema.getName()) : null;
        if (partVal != null) {
          rewrittenQueryStr.append("=").append(partVal);
        }
      }
      rewrittenQueryStr.append(")");
    }
  }
  
  private String getSortOrderJson(ASTNode ast) {
    List<SortFieldDesc> sortFieldDescList = new ArrayList<>();
    SortFields sortFields = new SortFields(sortFieldDescList);
    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      SortFieldDesc.SortDirection sortDirection = child.getToken()
          .getType() == HiveParser.TOK_TABSORTCOLNAMEDESC ? SortFieldDesc.SortDirection.DESC : SortFieldDesc.SortDirection.ASC;
      child = (ASTNode) child.getChild(0);
      String name = unescapeIdentifier(child.getChild(0).getText()).toLowerCase();
      NullOrdering nullOrder = NullOrdering.fromToken(child.getToken().getType());
      sortFieldDescList.add(new SortFieldDesc(name, sortDirection, nullOrder));
    }
    try {
      return JSON_OBJECT_MAPPER.writer().writeValueAsString(sortFields);
    } catch (JsonProcessingException e) {
      LOG.warn("Can not create write order json. ", e);      
      return null;
    }
  }
  @Override
  public WriteEntity getAcidAnalyzeTable() {
    return acidAnalyzeTable;
  }

  @Override
  public void executeUnParseTranslations() {
    unparseTranslator.applyTranslations(ctx.getTokenRewriteStream());
  }

  @Override
  public void startAnalysis() {
    if (conf.getBoolVar(ConfVars.HIVE_OPTIMIZE_HMS_QUERY_CACHE_ENABLED)) {
      queryState.createHMSCache();
    }
  }
}
