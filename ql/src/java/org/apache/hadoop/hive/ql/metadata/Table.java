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

package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * A Hive Table: is a fundamental unit of data in Hive that shares a common schema/DDL.
 *
 * Please note that the ql code should always go through methods of this class to access the
 * metadata, instead of directly accessing org.apache.hadoop.hive.metastore.api.Table.  This
 * helps to isolate the metastore code and the ql code.
 */
public class Table implements Serializable {

  private static final long serialVersionUID = 1L;

  static final private Logger LOG = LoggerFactory.getLogger("hive.ql.metadata.Table");

  private org.apache.hadoop.hive.metastore.api.Table tTable;

  /**
   * These fields are all cached fields.  The information comes from tTable.
   */
  private transient Deserializer deserializer;
  private Class<? extends OutputFormat> outputFormatClass;
  private Class<? extends InputFormat> inputFormatClass;
  private Path path;

  private transient HiveStorageHandler storageHandler;
  private transient StorageHandlerInfo storageHandlerInfo;
  private transient MaterializedViewMetadata materializedViewMetadata;

  private TableSpec tableSpec;

  private boolean materializedTable;

  /** Note: This is set only for describe table purposes, it cannot be used to verify whether
   * a materialization is up-to-date or not. */
  private Boolean outdatedForRewritingMaterializedView;

  /** Constraint related objects */
  private TableConstraintsInfo tableConstraintsInfo;

  /** Constraint related flags
   *  This is to track if constraints are retrieved from metastore or not
   */
  private boolean isTableConstraintsFetched=false;

  private String metaTable;

  /**
   * The version of the table. For Iceberg tables this is the snapshotId.
   */
  private String asOfVersion = null;
  private String versionIntervalFrom = null;

  /**
   * The version of the table at the given timestamp. The format will be parsed with
   * TimestampTZUtil.parse.
   */
  private String asOfTimestamp = null;

  private String snapshotRef;

  /**
   * Used only for serialization.
   */
  public Table() {
  }

  public Table(org.apache.hadoop.hive.metastore.api.Table table) {
    initialize(table);
  }

  // Do initialization here, so as to keep the ctor minimal.
  protected void initialize(org.apache.hadoop.hive.metastore.api.Table table) {
    tTable = table;
    // Note that we do not set up fields like inputFormatClass, outputFormatClass
    // and deserializer because the Partition needs to be accessed from across
    // the metastore side as well, which will result in attempting to load
    // the class associated with them, which might not be available, and
    // the main reason to instantiate them would be to pre-cache them for
    // performance. Since those fields are null/cache-check by their accessors
    // anyway, that's not a concern.
  }

  public Table(String databaseName, String tableName) {
    this(getEmptyTable(databaseName, tableName));
  }

  /** This api is used by getMetaData which require deep copy of metastore.api.table
   * and constraints copy
   */
  public Table makeCopy() {

    // make deep copy of metastore.api.table
    Table newTab = new Table(this.getTTable().deepCopy());

    // copy constraints
    newTab.copyConstraints(this);

    newTab.setAsOfTimestamp(this.asOfTimestamp);
    newTab.setAsOfVersion(this.asOfVersion);
    newTab.setVersionIntervalFrom(this.versionIntervalFrom);

    newTab.setMetaTable(this.getMetaTable());
    newTab.setSnapshotRef(this.getSnapshotRef());
    return newTab;
  }

  public boolean isDummyTable() {
    return tTable.getTableName().equals(SemanticAnalyzer.DUMMY_TABLE);
  }

  /**
   * This function should only be used in serialization.
   * We should never call this function to modify the fields, because
   * the cached fields will become outdated.
   */
  public org.apache.hadoop.hive.metastore.api.Table getTTable() {
    return tTable;
  }

  /**
   * This function should only be called by Java serialization.
   */
  public void setTTable(org.apache.hadoop.hive.metastore.api.Table tTable) {
    this.tTable = tTable;
  }

  /**
   * Initialize an empty table.
   */
  public static org.apache.hadoop.hive.metastore.api.Table
    getEmptyTable(String databaseName, String tableName) {
    StorageDescriptor sd = new StorageDescriptor();
    {
      sd.setSerdeInfo(new SerDeInfo());
      sd.setNumBuckets(-1);
      sd.setBucketCols(new ArrayList<String>());
      sd.setCols(new ArrayList<FieldSchema>());
      sd.setParameters(new HashMap<String, String>());
      sd.setSortCols(new ArrayList<Order>());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      // We have to use MetadataTypedColumnsetSerDe because LazySimpleSerDe does
      // not support a table with no columns.
      sd.getSerdeInfo().setSerializationLib(MetadataTypedColumnsetSerDe.class.getName());
      //TODO setting serializaton format here is hacky. Only lazy simple serde needs it
      // so should be set by serde only. Setting it here sets it unconditionally.
      sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
      sd.setInputFormat(SequenceFileInputFormat.class.getName());
      sd.setOutputFormat(HiveSequenceFileOutputFormat.class.getName());
      SkewedInfo skewInfo = new SkewedInfo();
      skewInfo.setSkewedColNames(new ArrayList<String>());
      skewInfo.setSkewedColValues(new ArrayList<List<String>>());
      skewInfo.setSkewedColValueLocationMaps(new HashMap<List<String>, String>());
      sd.setSkewedInfo(skewInfo);
    }

    org.apache.hadoop.hive.metastore.api.Table t = new org.apache.hadoop.hive.metastore.api.Table();
    {
      t.setSd(sd);
      t.setPartitionKeys(new ArrayList<FieldSchema>());
      t.setParameters(new HashMap<String, String>());
      t.setTableType(TableType.MANAGED_TABLE.toString());
      t.setDbName(databaseName);
      t.setTableName(tableName);
      t.setOwner(SessionState.getUserFromAuthenticator());
      // set create time
      t.setCreateTime((int) (System.currentTimeMillis() / 1000));
    }
    // Explicitly set the bucketing version
    t.getParameters().put(hive_metastoreConstants.TABLE_BUCKETING_VERSION,
        "2");
    return t;
  }

  public void checkValidity(Configuration conf) throws HiveException {
    // check for validity
    validateName(conf);
    if (getCols().isEmpty()) {
      throw new HiveException(
          "at least one column must be specified for the table");
    }
    if (!isView()) {
      if (null == getDeserializer(false)) {
        throw new HiveException("must specify a non-null serDe");
      }
      if (null == getInputFormatClass()) {
        throw new HiveException("must specify an InputFormat class");
      }
      if (null == getOutputFormatClass()) {
        throw new HiveException("must specify an OutputFormat class");
      }
    }

    if (isView() || isMaterializedView()) {
      assert (getViewOriginalText() != null);
      assert (getViewExpandedText() != null);
    } else {
      assert(getViewOriginalText() == null);
      assert(getViewExpandedText() == null);
    }

    validateColumns(getCols(), getPartCols());
  }

  public void validateName(Configuration conf) throws HiveException {
    String name = tTable.getTableName();
    if (StringUtils.isBlank(name) || !MetaStoreUtils.validateName(name, conf)) {
      throw new HiveException("[" + name + "]: is not a valid table name");
    }
  }

  public StorageDescriptor getSd() {
    return tTable.getSd();
  }

  public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
    tTable.getSd().setInputFormat(inputFormatClass.getName());
  }

  public void setOutputFormatClass(Class<? extends OutputFormat> outputFormatClass) {
    this.outputFormatClass = outputFormatClass;
    tTable.getSd().setOutputFormat(outputFormatClass.getName());
  }

  final public Properties getMetadata() {
    return MetaStoreUtils.getTableMetadata(tTable);
  }

  final public Path getPath() {
    String location = tTable.getSd().getLocation();
    if (location == null) {
      return null;
    }
    return new Path(location);
  }

  final public String getTableName() {
    return tTable.getTableName();
  }

  public TableName getFullTableName() {
    return new TableName(getCatName(), getDbName(), getTableName(), getSnapshotRef());
  }

  final public Path getDataLocation() {
    if (path == null) {
      path = getPath();
    }
    return path;
  }

  final public Deserializer getDeserializer() {
    if (deserializer == null) {
      deserializer = getDeserializerFromMetaStore(false);
    }
    return deserializer;
  }

  final public Deserializer getDeserializer(boolean skipConfError) {
    if (deserializer == null) {
      deserializer = getDeserializerFromMetaStore(skipConfError);
    }
    return deserializer;
  }

  final public Deserializer getDeserializerFromMetaStore(boolean skipConfError) {
    try {
      return HiveMetaStoreUtils.getDeserializer(SessionState.getSessionConf(), tTable, metaTable, skipConfError);
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  public HiveStorageHandler getStorageHandler() {
    if (storageHandler != null || !isNonNative()) {
      return storageHandler;
    }
    try {
      storageHandler = HiveUtils.getStorageHandler(
          SessionState.getSessionConf(),
          getProperty(
              org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return storageHandler;
  }

  public HiveStorageHandler getStorageHandlerWithoutCaching() {
    if (storageHandler != null || !isNonNative()) {
      return storageHandler;
    }
    try {
      return HiveUtils.getStorageHandler(SessionState.getSessionConf(),
          getProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void setStorageHandler(HiveStorageHandler sh){
    storageHandler = sh;
  }

  public StorageHandlerInfo getStorageHandlerInfo() {
    return storageHandlerInfo;
  }

  public void setStorageHandlerInfo(StorageHandlerInfo storageHandlerInfo) {
    this.storageHandlerInfo = storageHandlerInfo;
  }

  final public Class<? extends InputFormat> getInputFormatClass() {
    if (inputFormatClass == null) {
      try {
        String className = tTable.getSd().getInputFormat();
        if (className == null) {
          if (getStorageHandler() == null) {
            return null;
          }
          inputFormatClass = getStorageHandler().getInputFormatClass();
        } else {
          inputFormatClass = (Class<? extends InputFormat>)
              Class.forName(className, true, Utilities.getSessionSpecifiedClassLoader());
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return inputFormatClass;
  }

  final public Class<? extends OutputFormat> getOutputFormatClass() {
    if (outputFormatClass == null) {
      try {
        String className = tTable.getSd().getOutputFormat();
        Class<?> c;
        if (className == null) {
          if (getStorageHandler() == null) {
            return null;
          }
          c = getStorageHandler().getOutputFormatClass();
        } else {
          c = Class.forName(className, true, Utilities.getSessionSpecifiedClassLoader());
        }
        // Replace FileOutputFormat for backward compatibility
        outputFormatClass = HiveFileFormatUtils.getOutputFormatSubstitute(c);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return outputFormatClass;
  }

  public boolean isMaterializedTable() {
    return materializedTable;
  }

  public void setMaterializedTable(boolean materializedTable) {
    this.materializedTable = materializedTable;
  }

  /**
   * Marker SemanticException, so that processing that allows for table validation failures
   * and appropriately handles them can recover from these types of SemanticExceptions
   */
  public class ValidationFailureSemanticException extends SemanticException{
    public ValidationFailureSemanticException(String s) {
      super(s);
    }
  }

  final public void validatePartColumnNames(
      Map<String, String> spec, boolean shouldBeFull) throws SemanticException {
    List<FieldSchema> partCols = tTable.getPartitionKeys();
    final String tableName = Warehouse.getQualifiedName(tTable);
    if (CollectionUtils.isEmpty(partCols)) {
      if (spec != null) {
        throw new ValidationFailureSemanticException(tableName +
            " table is not partitioned but partition spec exists: " + spec);
      }
      return;
    } else if (spec == null) {
      if (shouldBeFull) {
        throw new ValidationFailureSemanticException(tableName +
            " table is partitioned but partition spec is not specified");
      }
      return;
    }
    int columnsFound = 0;
    for (FieldSchema fs : partCols) {
      if (spec.containsKey(fs.getName())) {
        ++columnsFound;
      }
      if (columnsFound == spec.size()) {
        break;
      }
    }
    if (columnsFound < spec.size()) {
      throw new ValidationFailureSemanticException(tableName + ": Partition spec " + spec +
          " contains non-partition columns");
    }
    if (shouldBeFull && (spec.size() != partCols.size())) {
      throw new ValidationFailureSemanticException(tableName + ": partition spec " + spec
          + " doesn't contain all (" + partCols.size() + ") partition columns");
    }
  }

  public void setProperty(String name, String value) {
    tTable.getParameters().put(name, value);
  }

  // Please note : Be very careful in using this function. If not used carefully,
  // you may end up overwriting all the existing properties. If the use case is to
  // add or update certain properties use setProperty() instead.
  public void setParameters(Map<String, String> params) {
    tTable.setParameters(params);
  }

  public String getProperty(String name) {
    return tTable.getParameters() != null ? tTable.getParameters().get(name) : null;
  }

  public boolean isImmutable(){
    return (tTable.getParameters().containsKey(hive_metastoreConstants.IS_IMMUTABLE)
        && tTable.getParameters().get(hive_metastoreConstants.IS_IMMUTABLE).equalsIgnoreCase("true"));
  }

  public void setTableType(TableType tableType) {
    tTable.setTableType(tableType.toString());
  }

  public TableType getTableType() {
    return Enum.valueOf(TableType.class, tTable.getTableType());
  }

  public ArrayList<StructField> getFields() {

    ArrayList<StructField> fields = new ArrayList<StructField>();
    try {
      Deserializer decoder = getDeserializer();

      // Expand out all the columns of the table
      StructObjectInspector structObjectInspector = (StructObjectInspector) decoder
          .getObjectInspector();
      List<? extends StructField> fld_lst = structObjectInspector
          .getAllStructFieldRefs();
      fields.addAll(fld_lst);
    } catch (SerDeException e) {
      throw new RuntimeException(e);
    }
    return fields;
  }

  public StructField getField(String fld) {
    try {
      StructObjectInspector structObjectInspector = (StructObjectInspector) getDeserializer()
          .getObjectInspector();
      return structObjectInspector.getStructFieldRef(fld);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public int getBucketingVersion() {
    return Utilities.getBucketingVersion(
        getProperty(hive_metastoreConstants.TABLE_BUCKETING_VERSION));
  }

  @Override
  public String toString() {
    return tTable.getTableName();
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((tTable == null) ? 0 : tTable.hashCode());
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Table other = (Table) obj;
    if (tTable == null) {
      if (other.tTable != null) {
        return false;
      }
    } else if (!tTable.equals(other.tTable)) {
      return false;
    }
    if (!Objects.equals(asOfTimestamp, other.asOfTimestamp)) {
      return false;
    }
    if (!Objects.equals(asOfVersion, other.asOfVersion)) {
      return false;
    }
    if (!Objects.equals(versionIntervalFrom, other.versionIntervalFrom)) {
      return false;
    }
    return true;
  }

  public List<FieldSchema> getPartCols() {
    List<FieldSchema> partKeys = tTable.getPartitionKeys();
    if (partKeys == null) {
      partKeys = new ArrayList<>();
      tTable.setPartitionKeys(partKeys);
    }
    return partKeys;
  }

  public FieldSchema getPartColByName(String colName) {
    return getPartCols().stream()
      .filter(key -> key.getName().toLowerCase().equals(colName))
      .findFirst().orElse(null);
  }

  public List<String> getPartColNames() {
    List<FieldSchema> partCols = hasNonNativePartitionSupport() ?
        getStorageHandler().getPartitionKeys(this) : getPartCols();
    return partCols.stream().map(FieldSchema::getName)
      .collect(Collectors.toList());
  }

  public boolean hasNonNativePartitionSupport() {
    return getStorageHandler() != null && getStorageHandler().supportsPartitioning();
  }

  public boolean isPartitionKey(String colName) {
    return getPartColByName(colName) != null;
  }

  // TODO merge this with getBucketCols function
  public String getBucketingDimensionId() {
    List<String> bcols = tTable.getSd().getBucketCols();
    if (CollectionUtils.isEmpty(bcols)) {
      return null;
    }

    if (bcols.size() > 1) {
      LOG.warn(this
          + " table has more than one dimensions which aren't supported yet");
    }

    return bcols.get(0);
  }

  public void setDataLocation(Path path) {
    this.path = path;
    tTable.getSd().setLocation(path == null ? null : path.toString());
  }

  public void unsetDataLocation() {
    this.path = null;
    tTable.getSd().unsetLocation();
  }

  public void setBucketCols(List<String> bucketCols) throws HiveException {
    if (bucketCols == null) {
      return;
    }

    for (String col : bucketCols) {
      if (!isField(col)) {
        throw new HiveException("Bucket columns " + col
            + " is not part of the table columns (" + getCols());
      }
    }
    tTable.getSd().setBucketCols(bucketCols);
  }

  public void setSortCols(List<Order> sortOrder) throws HiveException {
    tTable.getSd().setSortCols(sortOrder);
  }

  public void setSkewedValueLocationMap(List<String> valList, String dirName) {
    Map<List<String>, String> mappings = tTable.getSd().getSkewedInfo()
        .getSkewedColValueLocationMaps();
    if (null == mappings) {
      mappings = new HashMap<List<String>, String>();
      tTable.getSd().getSkewedInfo().setSkewedColValueLocationMaps(mappings);
    }

    // Add or update new mapping
    mappings.put(valList, dirName);
  }

  public Map<List<String>, String> getSkewedColValueLocationMaps() {
    return (tTable.getSd().getSkewedInfo() != null) ? tTable.getSd().getSkewedInfo()
        .getSkewedColValueLocationMaps() : new HashMap<List<String>, String>();
  }

  public void setSkewedColValues(List<List<String>> skewedValues) {
    tTable.getSd().getSkewedInfo().setSkewedColValues(skewedValues);
  }

  public List<List<String>> getSkewedColValues(){
    return (tTable.getSd().getSkewedInfo() != null) ? tTable.getSd().getSkewedInfo()
        .getSkewedColValues() : new ArrayList<List<String>>();
  }

  public void setSkewedColNames(List<String> skewedColNames) {
    tTable.getSd().getSkewedInfo().setSkewedColNames(skewedColNames);
  }

  public List<String> getSkewedColNames() {
    return (tTable.getSd().getSkewedInfo() != null) ? tTable.getSd().getSkewedInfo()
        .getSkewedColNames() : new ArrayList<String>();
  }

  public SkewedInfo getSkewedInfo() {
    return tTable.getSd().getSkewedInfo();
  }

  public void setSkewedInfo(SkewedInfo skewedInfo) {
    tTable.getSd().setSkewedInfo(skewedInfo);
  }

  public boolean isStoredAsSubDirectories() {
    return tTable.getSd().isStoredAsSubDirectories();
  }

  public void setStoredAsSubDirectories(boolean storedAsSubDirectories) throws HiveException {
    tTable.getSd().setStoredAsSubDirectories(storedAsSubDirectories);
  }

  private boolean isField(String col) {
    for (FieldSchema field : getCols()) {
      if (field.getName().equals(col)) {
        return true;
      }
    }
    return false;
  }

  public List<FieldSchema> getCols() {
    return getColsInternal(false);
  }

  public List<FieldSchema> getColsForMetastore() {
    return getColsInternal(true);
  }

  private List<FieldSchema> getColsInternal(boolean forMs) {
    String serializationLib = getSerializationLib();
    try {
      // Do the lightweight check for general case.
      if (hasMetastoreBasedSchema(SessionState.getSessionConf(), serializationLib)) {
        return tTable.getSd().getCols();
      } else if (forMs && !shouldStoreFieldsInMetastore(
          SessionState.getSessionConf(), serializationLib, tTable.getParameters())) {
        return Hive.getFieldsFromDeserializerForMsStorage(this, getDeserializer(), SessionState.getSessionConf());
      } else {
        return HiveMetaStoreUtils.getFieldsFromDeserializer(getTableName(), getDeserializer(),
            SessionState.getSessionConf());
      }
    } catch (Exception e) {
      LOG.error("Unable to get field from serde: " + serializationLib, e);
    }
    return Collections.emptyList();
  }

  /**
   * Returns a list of all the columns of the table (data columns + partition
   * columns in that order.
   *
   * @return List&lt;FieldSchema&gt;
   */
  public List<FieldSchema> getAllCols() {
    ArrayList<FieldSchema> f_list = new ArrayList<FieldSchema>();
    f_list.addAll(getCols());
    f_list.addAll(getPartCols());
    return f_list;
  }

  public void setPartCols(List<FieldSchema> partCols) {
    tTable.setPartitionKeys(partCols);
  }

  public String getCatName() {
    return tTable.getCatName();
  }

  public String getDbName() {
    return tTable.getDbName();
  }

  public int getNumBuckets() {
    return tTable.getSd().getNumBuckets();
  }

  public void setInputFormatClass(String name) throws HiveException {
    if (name == null) {
      inputFormatClass = null;
      tTable.getSd().setInputFormat(null);
      return;
    }
    try {
      setInputFormatClass((Class<? extends InputFormat<WritableComparable, Writable>>) Class
          .forName(name, true, Utilities.getSessionSpecifiedClassLoader()));
    } catch (ClassNotFoundException e) {
      throw new HiveException("Class not found: " + name, e);
    }
  }

  public void setOutputFormatClass(String name) throws HiveException {
    if (name == null) {
      outputFormatClass = null;
      tTable.getSd().setOutputFormat(null);
      return;
    }
    try {
      Class<?> origin = Class.forName(name, true, Utilities.getSessionSpecifiedClassLoader());
      setOutputFormatClass(HiveFileFormatUtils.getOutputFormatSubstitute(origin));
    } catch (ClassNotFoundException e) {
      throw new HiveException("Class not found: " + name, e);
    }
  }
  
  public boolean isPartitioned() {
    return hasNonNativePartitionSupport() ? getStorageHandler().isPartitioned(this) : 
        CollectionUtils.isNotEmpty(getPartCols());
  }

  public void setFields(List<FieldSchema> fields) {
    tTable.getSd().setCols(fields);
  }

  public void setNumBuckets(int nb) {
    tTable.getSd().setNumBuckets(nb);
  }

  /**
   * @return The owner of the table.
   * @see org.apache.hadoop.hive.metastore.api.Table#getOwner()
   */
  public String getOwner() {
    return tTable.getOwner();
  }

  /**
   * @return The owner type of the table.
   * @see org.apache.hadoop.hive.metastore.api.Table#getOwnerType()
   */
  public PrincipalType getOwnerType() { return tTable.getOwnerType(); }

  /**
   * @return The table parameters.
   * @see org.apache.hadoop.hive.metastore.api.Table#getParameters()
   */
  public Map<String, String> getParameters() {
    return tTable.getParameters();
  }

  /**
   * @return The retention on the table.
   * @see org.apache.hadoop.hive.metastore.api.Table#getRetention()
   */
  public int getRetention() {
    return tTable.getRetention();
  }

  /**
   * @param owner
   * @see org.apache.hadoop.hive.metastore.api.Table#setOwner(java.lang.String)
   */
  public void setOwner(String owner) {
    tTable.setOwner(owner);
  }

  /**
   * @param ownerType
   * @see org.apache.hadoop.hive.metastore.api.Table#setOwnerType(org.apache.hadoop.hive.metastore.api.PrincipalType)
   */
  public void setOwnerType(PrincipalType ownerType) {
    tTable.setOwnerType(ownerType);
  }

  /**
   * @param retention
   * @see org.apache.hadoop.hive.metastore.api.Table#setRetention(int)
   */
  public void setRetention(int retention) {
    tTable.setRetention(retention);
  }

  private SerDeInfo getSerdeInfo() {
    return tTable.getSd().getSerdeInfo();
  }

  public void setSerializationLib(String lib) {
    getSerdeInfo().setSerializationLib(lib);
  }

  public String getSerializationLib() {
    return getSerdeInfo().getSerializationLib();
  }

  public String getSerdeParam(String param) {
    return getSerdeInfo().getParameters().get(param);
  }

  public String setSerdeParam(String param, String value) {
    return getSerdeInfo().getParameters().put(param, value);
  }

  public List<String> getBucketCols() {
    return tTable.getSd().getBucketCols();
  }

  public List<Order> getSortCols() {
    return tTable.getSd().getSortCols();
  }

  public void setTableName(String tableName) {
    tTable.setTableName(tableName);
  }

  public void setDbName(String databaseName) {
    tTable.setDbName(databaseName);
  }

  public List<FieldSchema> getPartitionKeys() {
    return tTable.getPartitionKeys();
  }

  /**
   * @return the original view text, or null if this table is not a view
   */
  public String getViewOriginalText() {
    return tTable.getViewOriginalText();
  }

  /**
   * @param viewOriginalText
   *          the original view text to set
   */
  public void setViewOriginalText(String viewOriginalText) {
    tTable.setViewOriginalText(viewOriginalText);
  }

  /**
   * @return the expanded view text, or null if this table is not a view
   */
  public String getViewExpandedText() {
    return tTable.getViewExpandedText();
  }

  /**
   * @param viewExpandedText
   *          the expanded view text to set
   */
  public void setViewExpandedText(String viewExpandedText) {
    tTable.setViewExpandedText(viewExpandedText);
  }

  /**
   * @return whether this view can be used for rewriting queries
   */
  public boolean isRewriteEnabled() {
    return tTable.isRewriteEnabled();
  }

  /**
   * @param rewriteEnabled
   *          whether this view can be used for rewriting queries
   */
  public void setRewriteEnabled(boolean rewriteEnabled) {
    tTable.setRewriteEnabled(rewriteEnabled);
  }

  /**
   * @return the creation metadata (only for materialized views)
   */
  public MaterializedViewMetadata getMVMetadata() {
    if (tTable.getCreationMetadata() == null) {
      return null;
    }
    if (materializedViewMetadata == null) {
      materializedViewMetadata = new MaterializedViewMetadata(tTable.getCreationMetadata());
    }

    return materializedViewMetadata;
  }

  /**
   * @param materializedViewMetadata
   *          the creation metadata (only for materialized views)
   */
  public void setMaterializedViewMetadata(MaterializedViewMetadata materializedViewMetadata) {
    this.materializedViewMetadata = materializedViewMetadata;
    tTable.setCreationMetadata(materializedViewMetadata.creationMetadata);
  }

  public void clearSerDeInfo() {
    tTable.getSd().getSerdeInfo().getParameters().clear();
  }

  /**
   * @return whether this table is actually a view
   */
  public boolean isView() {
    return TableType.VIRTUAL_VIEW.equals(getTableType());
  }

  public boolean isMaterializedView() {
    return TableType.MATERIALIZED_VIEW.equals(getTableType());
  }

  /**
   * Creates a partition name -&gt; value spec map object
   *
   * @param tp
   *          Use the information from this partition.
   * @return Partition name to value mapping.
   */
  public LinkedHashMap<String, String> createSpec(
      org.apache.hadoop.hive.metastore.api.Partition tp) {

    List<FieldSchema> fsl = getPartCols();
    List<String> tpl = tp.getValues();
    LinkedHashMap<String, String> spec = new LinkedHashMap<String, String>(fsl.size());
    for (int i = 0; i < fsl.size(); i++) {
      FieldSchema fs = fsl.get(i);
      String value = tpl.get(i);
      spec.put(fs.getName(), value);
    }
    return spec;
  }

  public Table copy() throws HiveException {
    return new Table(tTable.deepCopy());
  }

  public int getCreateTime() {
    return tTable.getCreateTime();
  }

  public void setCreateTime(int createTime) {
    tTable.setCreateTime(createTime);
  }

  public int getLastAccessTime() {
    return tTable.getLastAccessTime();
  }

  public void setLastAccessTime(int lastAccessTime) {
    tTable.setLastAccessTime(lastAccessTime);
  }

  public boolean isNonNative() {
    return getProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE)
        != null;
  }

  public String getFullyQualifiedName() {
    return Warehouse.getQualifiedName(tTable);
  }

  /**
   * @return include the db name
   */
  public String getCompleteName() {
    return getCompleteName(getDbName(), getTableName());
  }

  public static String getCompleteName(String dbName, String tabName) {
    return dbName + "@" + tabName;
  }

  @SuppressWarnings("nls")
  public FileStatus[] getSortedPaths() {
    try {
      // Previously, this got the filesystem of the Table, which could be
      // different from the filesystem of the partition.
      FileSystem fs = FileSystem.get(getPath().toUri(), SessionState.getSessionConf());
      String pathPattern = getPath().toString();
      if (getNumBuckets() > 0) {
        pathPattern = pathPattern + "/*";
      }
      LOG.info("Path pattern = " + pathPattern);
      FileStatus srcs[] = fs.globStatus(new Path(pathPattern), FileUtils.HIDDEN_FILES_PATH_FILTER);
      Arrays.sort(srcs);
      for (FileStatus src : srcs) {
        LOG.info("Got file: " + src.getPath());
      }
      if (srcs.length == 0) {
        return null;
      }
      return srcs;
    } catch (Exception e) {
      throw new RuntimeException("Cannot get path ", e);
    }
  }

  public boolean isEmpty() throws HiveException {
    Preconditions.checkNotNull(getPath());
    try {
      FileSystem fs = FileSystem.get(getPath().toUri(), SessionState.getSessionConf());
      return !fs.exists(getPath()) || fs.listStatus(getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER).length == 0;
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  public boolean isTemporary() {
    return tTable.isTemporary();
  }

  public void setTemporary(boolean isTemporary) {
    tTable.setTemporary(isTemporary);
  }

  public static boolean hasMetastoreBasedSchema(HiveConf conf, String serdeLib) {
    return StringUtils.isEmpty(serdeLib) ||
        MetastoreConf.getStringCollection(conf,
            MetastoreConf.ConfVars.SERDES_USING_METASTORE_FOR_SCHEMA).contains(serdeLib);
  }

  public static boolean shouldStoreFieldsInMetastore(
      HiveConf conf, String serdeLib, Map<String, String> tableParams) {
    if (hasMetastoreBasedSchema(conf, serdeLib)) {
      return true;
    }
    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_LEGACY_SCHEMA_FOR_ALL_SERDES)) {
      return true;
    }
    // Table may or may not be using metastore. Only the SerDe can tell us.
    AbstractSerDe deserializer = null;
    try {
      Class<?> clazz = conf.getClassByName(serdeLib);
      if (!AbstractSerDe.class.isAssignableFrom(clazz))
      {
        return true; // The default.
      }
      deserializer = ReflectionUtil.newInstance(
          conf.getClassByName(serdeLib).asSubclass(AbstractSerDe.class), conf);
    } catch (Exception ex) {
      LOG.warn("Cannot initialize SerDe: " + serdeLib + ", ignoring", ex);
      return true;
    }
    return deserializer.shouldStoreFieldsInMetastore(tableParams);
  }

  public static void validateColumns(List<FieldSchema> columns, List<FieldSchema> partCols)
      throws HiveException {
    Set<String> colNames = new HashSet<>();
    for (FieldSchema col: columns) {
      String colName = normalize(col.getName());
      if (colNames.contains(colName)) {
        throw new HiveException("Duplicate column name " + colName
            + " in the table definition.");
      }
      colNames.add(colName);
    }
    if (partCols != null) {
      // there is no overlap between columns and partitioning columns
      for (FieldSchema partCol: partCols) {
        String colName = normalize(partCol.getName());
        if (colNames.contains(colName)) {
          throw new HiveException("Partition column name " + colName
              + " conflicts with table columns.");
        }
      }
    }
  }

  private static String normalize(String colName) throws HiveException {
    if (!MetaStoreServerUtils.validateColumnName(colName)) {
      throw new HiveException("Invalid column name '" + colName
          + "' in the table definition");
    }
    return colName.toLowerCase();
  }

  public TableSpec getTableSpec() {
    return tableSpec;
  }

  public void setTableSpec(TableSpec tableSpec) {
    this.tableSpec = tableSpec;
  }

  public String getCatalogName() {
    return this.tTable.getCatName();
  }

  public void setOutdatedForRewriting(Boolean validForRewritingMaterializedView) {
    this.outdatedForRewritingMaterializedView = validForRewritingMaterializedView;
  }

  /** Note: This is set only for describe table purposes, it cannot be used to verify whether
   * a materialization is up-to-date or not. */
  public Boolean isOutdatedForRewriting() {
    return outdatedForRewritingMaterializedView;
  }

  public ColumnStatistics getColStats() {
    return tTable.isSetColStats() ? tTable.getColStats() : null;
  }

  /**
   * Setup the table level stats as if the table is new. Used when setting up Table for a new
   * table or during replication.
   */
  public void setStatsStateLikeNewTable() {
    if (isPartitioned()) {
      StatsSetupConst.setStatsStateForCreateTable(getParameters(), null,
          StatsSetupConst.FALSE);
    } else {
      StatsSetupConst.setStatsStateForCreateTable(getParameters(),
          MetaStoreUtils.getColumnNames(getCols()), StatsSetupConst.TRUE);
    }
  }

  /** Constraints related methods
   *  Note that set apis are used by DESCRIBE only, although get apis return RELY or ENABLE
   *  constraints DESCRIBE could set all type of constraints
   * */

  public TableConstraintsInfo getTableConstraintsInfo() {

    if (!isTableConstraintsFetched) {
      try {
        tableConstraintsInfo = Hive.get().getTableConstraints(this.getDbName(), this.getTableName(), true, true,
            this.getTTable() != null ? this.getTTable().getId() : -1);
        this.isTableConstraintsFetched = true;
      } catch (HiveException e) {
        LOG.warn("Cannot retrieve Table Constraints info for table : " + this.getTableName() + " ignoring exception: " + e);
        tableConstraintsInfo = new TableConstraintsInfo();
      }
    }
    return tableConstraintsInfo;
  }

  public void setTableConstraintsInfo(TableConstraintsInfo tableConstraintsInfo) {
    this.tableConstraintsInfo = tableConstraintsInfo;
    this.isTableConstraintsFetched = true;
  }

  /* This only return PK which are created with RELY */
  public PrimaryKeyInfo getPrimaryKeyInfo() {
    if (!isTableConstraintsFetched) {
      getTableConstraintsInfo();
    }
    return tableConstraintsInfo.getPrimaryKeyInfo();
  }

  /* This only return FK constraints which are created with RELY */
  public ForeignKeyInfo getForeignKeyInfo() {
    if (!isTableConstraintsFetched) {
      getTableConstraintsInfo();
    }
    return tableConstraintsInfo.getForeignKeyInfo();
  }

  /* This only return UNIQUE constraint defined with RELY */
  public UniqueConstraint getUniqueKeyInfo() {
    if (!isTableConstraintsFetched) {
      getTableConstraintsInfo();
    }
    return tableConstraintsInfo.getUniqueConstraint();
  }

  /* This only return NOT NULL constraint defined with RELY */
  public NotNullConstraint getNotNullConstraint() {
    if (!isTableConstraintsFetched) {
      getTableConstraintsInfo();
    }
    return tableConstraintsInfo.getNotNullConstraint();
  }

  /* This only return DEFAULT constraint defined with ENABLE */
  public DefaultConstraint getDefaultConstraint() {
    if (!isTableConstraintsFetched) {
      getTableConstraintsInfo();
    }
    return tableConstraintsInfo.getDefaultConstraint();
  }

  /* This only return CHECK constraint defined with ENABLE */
  public CheckConstraint getCheckConstraint() {
    if (!isTableConstraintsFetched) {
      getTableConstraintsInfo();
    }
    return tableConstraintsInfo.getCheckConstraint();
  }

  /** This shouldn't use get apis because those api call metastore
   * to fetch constraints.
   * getMetaData only need to make a reference copy of existing constraints, even if those are not fetched
   */
  public void copyConstraints(final Table tbl) {
    this.tableConstraintsInfo = tbl.tableConstraintsInfo;
    this.isTableConstraintsFetched = tbl.isTableConstraintsFetched;
  }

  /**
   * This method ignores the write Id, while comparing two tables.
   *
   * @param tbl table to compare with
   * @return
   */
  public boolean equalsWithIgnoreWriteId(Table tbl ) {
    long targetWriteId = getTTable().getWriteId();
    long entityWriteId = tbl.getTTable().getWriteId();
    getTTable().setWriteId(0L);
    tbl.getTTable().setWriteId(0L);
    boolean result = equals(tbl);
    getTTable().setWriteId(targetWriteId);
    tbl.getTTable().setWriteId(entityWriteId);
    return result;
  }

  public String getAsOfVersion() {
    return asOfVersion;
  }

  public void setAsOfVersion(String asOfVersion) {
    this.asOfVersion = asOfVersion;
  }

  public String getVersionIntervalFrom() {
    return versionIntervalFrom;
  }

  public void setVersionIntervalFrom(String versionIntervalFrom) {
    this.versionIntervalFrom = versionIntervalFrom;
  }

  public String getAsOfTimestamp() {
    return asOfTimestamp;
  }

  public void setAsOfTimestamp(String asOfTimestamp) {
    this.asOfTimestamp = asOfTimestamp;
  }

  public String getMetaTable() {
    return metaTable;
  }

  public void setMetaTable(String metaTable) {
    this.metaTable = metaTable;
  }

  public String getSnapshotRef() {
    return snapshotRef;
  }

  public void setSnapshotRef(String snapshotRef) {
    this.snapshotRef = snapshotRef;
  }

  public SourceTable createSourceTable() {
    SourceTable sourceTable = new SourceTable();
    sourceTable.setTable(this.tTable);
    sourceTable.setInsertedCount(0L);
    sourceTable.setUpdatedCount(0L);
    sourceTable.setDeletedCount(0L);
    return sourceTable;
  }

  public List<VirtualColumn> getVirtualColumns() {
    List<VirtualColumn> virtualColumns = new ArrayList<>();
    if (!isNonNative()) {
      virtualColumns.addAll(VirtualColumn.getRegistry());
    }
    if (isNonNative() && AcidUtils.isNonNativeAcidTable(this)) {
      virtualColumns.addAll(getStorageHandler().acidVirtualColumns());
    }
    if (isNonNative() && getStorageHandler().areSnapshotsSupported() &&
        isBlank(getMetaTable())) {
      virtualColumns.add(VirtualColumn.SNAPSHOT_ID);
    }
    
    return virtualColumns;
  }
}
