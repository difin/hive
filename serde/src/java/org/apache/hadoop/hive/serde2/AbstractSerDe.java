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

package org.apache.hadoop.hive.serde2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Abstract class for implementing SerDe. The abstract class has been created, so that
 * new methods can be added in the underlying interface, SerDe, and only implementations
 * that need those methods overwrite it.
 */
public abstract class AbstractSerDe implements Deserializer, Serializer {

  protected String configErrors;
  protected Logger log = LoggerFactory.getLogger(getClass());

  protected Optional<Configuration> configuration;
  protected Properties properties;
  protected Properties tableProperties;
  protected Optional<Properties> partitionProperties;

  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private List<String> columnComments;

  private List<String> partitionColumnNames;
  private List<TypeInfo> partitionColumnTypes;
  private List<String> partitionColumnComments;

  /**
   * Initialize the SerDe. By default, this will use one set of properties, either the
   * table properties or the partition properties. If a SerDe needs access to both sets,
   * it should override this method.
   *
   * Eventually, once all SerDes have implemented this method,
   * we should convert it to an abstract method.
   *
   * @param configuration        Hadoop configuration
   * @param tableProperties      Table properties
   * @param partitionProperties  Partition properties
   * @throws SerDeException
   */
  public void initialize(Configuration configuration, Properties tableProperties,
                         Properties partitionProperties) throws SerDeException {
    initialize(configuration,
               SerDeUtils.createOverlayedProperties(tableProperties, partitionProperties));
  }

  /**
   * Initialize the HiveSerializer.
   *
   * @param conf
   *          System properties. Can be null in compile time
   * @param tbl
   *          table properties
   * @throws SerDeException
   */
  @Deprecated
  public abstract void initialize(@Nullable Configuration conf, Properties tbl)
      throws SerDeException;

  // CDPD only change to avoid backporting the whole SerDe refactor (HIVE-24332)
  // With this change we can use this in IcebergSerDe for HIVE-25610
  // Fix HiveIcebergSerde when backporting HIVE-24332
  public void initializeColumns(Configuration configuration, Properties tableProperties, Properties partitionProperties)
      throws SerDeException {
    this.configuration = Optional.ofNullable(configuration);
    this.tableProperties = Objects.requireNonNull(tableProperties);
    this.partitionProperties = Optional.ofNullable(partitionProperties);
    this.properties = SerDeUtils.createOverlayedProperties(tableProperties, partitionProperties);
    this.columnNames = parseColumnNames();
    this.columnTypes = parseColumnTypes();
    this.columnComments = parseColumnComments(serdeConstants.LIST_COLUMN_COMMENTS);
    this.partitionColumnNames = parseColumnNames(serdeConstants.LIST_PARTITION_COLUMNS);
    this.partitionColumnTypes = parseColumnTypes(serdeConstants.LIST_PARTITION_COLUMN_TYPES);
    this.partitionColumnComments = parseColumnComments(serdeConstants.LIST_PARTITION_COLUMN_COMMENTS);

    Preconditions.checkArgument(this.columnNames.size() == this.columnTypes.size(),
        "Column names must match count of column types");

    log.debug("SerDe initialized: [{}][{}]", this.configuration, this.properties);
  }

  protected List<String> parseColumnNames() {
    return parseColumnNames(serdeConstants.LIST_COLUMNS);
  }

  protected List<String> parseColumnNames(String key) {
    final String columnNameProperty = this.properties.getProperty(key, "");
    final String columnNameDelimiter =
        this.properties.getProperty(serdeConstants.COLUMN_NAME_DELIMITER, String.valueOf(SerDeUtils.COMMA));

    return columnNameProperty.isEmpty() ? Collections.emptyList()
        : Collections.unmodifiableList(Arrays.asList(columnNameProperty.split(columnNameDelimiter)));
  }

  protected List<TypeInfo> parseColumnTypes() {
    return parseColumnTypes(serdeConstants.LIST_COLUMN_TYPES);
  }

  protected List<TypeInfo> parseColumnTypes(String key) {
    final String columnTypeProperty = this.properties.getProperty(key, "");

    return columnTypeProperty.isEmpty() ? Collections.emptyList()
        : Collections.unmodifiableList(TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty));
  }

  protected List<String> parseColumnComments(String key) {
    final String columnCommentProperty = this.properties.getProperty(key, "");

    return columnCommentProperty.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(
        Arrays.asList(columnCommentProperty.split(Character.toString(ColumnType.COLUMN_COMMENTS_DELIMITER))));
  }

  /**
   * Returns the Writable class that would be returned by the serialize method.
   * This is used to initialize SequenceFile header.
   */
  public abstract Class<? extends Writable> getSerializedClass();

  /**
   * Serialize an object by navigating inside the Object with the
   * ObjectInspector. In most cases, the return value of this function will be
   * constant since the function will reuse the Writable object. If the client
   * wants to keep a copy of the Writable, the client needs to clone the
   * returned value.
   */
  public abstract Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException;

  /**
   * Returns statistics collected when serializing
   */
  public abstract SerDeStats getSerDeStats();

  /**
   * Deserialize an object out of a Writable blob. In most cases, the return
   * value of this function will be constant since the function will reuse the
   * returned object. If the client wants to keep a copy of the object, the
   * client needs to clone the returned value by calling
   * ObjectInspectorUtils.getStandardObject().
   *
   * @param blob
   *          The Writable object containing a serialized object
   * @return A Java object representing the contents in the blob.
   */
  public abstract Object deserialize(Writable blob) throws SerDeException;

  /**
   * Get the object inspector that can be used to navigate through the internal
   * structure of the Object returned from deserialize(...).
   */
  public abstract ObjectInspector getObjectInspector() throws SerDeException;

  /**
   * Get the error messages during the Serde configuration
   *
   * @return The error messages in the configuration which are empty if no error occurred
   */
  public String getConfigurationErrors() {
    return configErrors == null ? "" : configErrors;
  }

  /**
   * @return Whether the SerDe that can store schema both inside and outside of metastore
   *        does, in fact, store it inside metastore, based on table parameters.
   */
  public boolean shouldStoreFieldsInMetastore(Map<String, String> tableParams) {
    return false; // The default, unless SerDe overrides it.
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<TypeInfo> getColumnTypes() {
    return columnTypes;
  }

  public List<String> getColumnComments() {
    return columnComments;
  }

  public List<String> getPartitionColumnNames() {
    return partitionColumnNames;
  }

  public List<TypeInfo> getPartitionColumnTypes() {
    return partitionColumnTypes;
  }

  public List<String> getPartitionColumnComments() {
    return partitionColumnComments;
  }

  public Optional<Configuration> getConfiguration() {
    return configuration;
  }

  @Override
  public String toString() {
    return "AbstractSerDe [log=" + log + ", configuration=" + configuration + ", properties=" + properties
        + ", tableProperties=" + tableProperties + ", partitionProperties=" + partitionProperties + ", columnNames="
        + columnNames + ", columnTypes=" + columnTypes + ", getClass()=" + getClass() + "]";
  }
}
