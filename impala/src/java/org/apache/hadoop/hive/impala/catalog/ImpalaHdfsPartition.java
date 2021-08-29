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
package org.apache.hadoop.hive.impala.catalog;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartitionLocationCompressor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.ListMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Extension of Impala's HdfsPartition.  In this extension, the partition name and hostIndex
 * get overridden.  The parent class has dependencies on the Table object in these methods.
 * We would like to avoid this because this object can be stored in the HMS client and the table
 * object using this partition will be instantiated for each query. Because of this, we pass in
 * null for the table object.
 * Also, the fileDescriptors are tracked here.  In the parent object, they are tracked in
 * a compressed format, but here they are expanded in order to save on compilation time.
 */
public class ImpalaHdfsPartition extends HdfsPartition {

  protected static final Logger LOG = LoggerFactory.getLogger(ImpalaHdfsPartition.class);
  public static final String DUMMY_PARTITION = "DUMMY";

  // Static Configuration object. On first iteration, getting a FileSystem object with a new
  // Configuration object is about 10ms, On subsequent calls to get a new FileSystem, it is much
  // quicker when using the same Configuration object.
  // Impala also declares this Configuration object as static when they fetch the FileSystem object.
  private static final Configuration CONF = new Configuration();

  private final ListMap<TNetworkAddress> hostIndex;

  private final FileSystemUtil.FsType fsType;

  private final FileSystem fs;

  private final List<HdfsPartition.FileDescriptor> fileDescriptors;

  private final List<HdfsPartition.FileDescriptor> insertFileDescriptors;

  private final List<HdfsPartition.FileDescriptor> deleteFileDescriptors;

  private final boolean isFullAcidTable;

  public ImpalaHdfsPartition(
        List<LiteralExpr> partitionKeyValues,
        HdfsStorageDescriptor fileFormatDescriptor,
        List<HdfsPartition.FileDescriptor> fileDescriptors, long id,
        HdfsPartitionLocationCompressor.Location location, TAccessLevel accessLevel,
        String partitionName, ListMap<TNetworkAddress> hostIndex, long numRows,
        boolean isFullAcidTable) throws HiveException {
    super(null /*table*/, id, -1, partitionName, partitionKeyValues,
        fileFormatDescriptor,
        null /*encodedFileDescriptors*/,
        null /*encodedInsertFileDescriptors*/, null /*encodedDeleteFileDescriptors*/,
        location, false, accessLevel, Maps.newHashMap() /*hmsParameters*/,
        null /*cachedMsPartitionDescriptor*/, null /*partitionStats*/, false, numRows, -1L,
        null /*inFlightEvents*/);
    try {
      this.hostIndex = hostIndex;
      Preconditions.checkNotNull(getLocationPath().toUri().getScheme(),
          "Cannot get scheme from path " + getLocationPath());
      fsType = FileSystemUtil.FsType.getFsType(getLocationPath().toUri().getScheme());
      fs = getLocationPath().getFileSystem(CONF);
      this.fileDescriptors = fileDescriptors;
    } catch (Exception e) {
      throw new HiveException("Could not create ImpalaHdfsPartition.", e);
    }
    this.isFullAcidTable = isFullAcidTable;
    this.insertFileDescriptors = getInsertFileDescriptors(isFullAcidTable, fileDescriptors);
    this.deleteFileDescriptors = getDeleteFileDescriptors(isFullAcidTable, fileDescriptors);
  }

  public ImpalaHdfsPartition(
        Table msTbl,
        List<LiteralExpr> partitionKeyValues,
        HdfsStorageDescriptor fileFormatDescriptor,
        List<HdfsPartition.FileDescriptor> fileDescriptors, long id,
        HdfsPartitionLocationCompressor.Location location, TAccessLevel accessLevel,
        String partitionName, ListMap<TNetworkAddress> hostIndex, long numRows
        ) throws HiveException {
    this(partitionKeyValues, fileFormatDescriptor, fileDescriptors, id, location, accessLevel,
        partitionName, hostIndex, numRows, isFullAcidTable(msTbl));
  }

  public ImpalaHdfsPartition(ImpalaHdfsPartition partition, List<HdfsPartition.FileDescriptor> fds)
        throws HiveException {
    this(partition.getPartitionValues(),
        partition.getInputFormatDescriptor(), fds, partition.getId(),
        partition.getLocationStruct(), partition.getAccessLevel(),
        partition.getPartitionName(), partition.getHostIndex(), partition.getNumRows(),
        partition.isFullAcidTable);
  }

  /**
   * Method called by constructor which separates out the delete file descriptors from
   * the list of all file descriptors.
   */
  private List<HdfsPartition.FileDescriptor> getDeleteFileDescriptors(boolean isFullAcidTable,
      List<HdfsPartition.FileDescriptor> fds) {
    ImmutableList.Builder<HdfsPartition.FileDescriptor> result = ImmutableList.builder();
    if (!isFullAcidTable) {
      return result.build();
    }
    for (HdfsPartition.FileDescriptor fd : fds) {
      if (AcidUtils.isDeleteDeltaFd(fd)) {
        result.add(fd);
      }
    }
    return result.build();
  }

  /**
   * Method called by constructor which separates out the insert file descriptors from
   * the list of all file descriptors.
   */
  private List<HdfsPartition.FileDescriptor> getInsertFileDescriptors(boolean isFullAcidTable,
      List<HdfsPartition.FileDescriptor> fds) {
    ImmutableList.Builder<HdfsPartition.FileDescriptor> result = ImmutableList.builder();
    if (!isFullAcidTable) {
      return result.build();
    }
    for (HdfsPartition.FileDescriptor fd : fds) {
      if (!AcidUtils.isDeleteDeltaFd(fd)) {
        result.add(fd);
      }
    }
    return result.build();
  }

  /**
   * Generate a similar partition to "this" but one that just contains
   * the insert file descriptors.
   */
  @Override
  public HdfsPartition genInsertDeltaPartition() {
    try {
      List<HdfsPartition.FileDescriptor> fds =
          insertFileDescriptors.isEmpty() ?
              fileDescriptors : insertFileDescriptors;
      return new ImpalaHdfsPartition(this, fds);
    } catch (HiveException e) {
      // parent method returns null when partition could not be generated.
      LOG.warn("Exception generating insert partition: " + e);
      return null;
    }
  }

  /**
   * Generate a similar partition to "this" but one that just contains
   * the delete file descriptors.
   */
  @Override
  public HdfsPartition genDeleteDeltaPartition() {
    try {
      if (deleteFileDescriptors.isEmpty()) {
        // parent method returns null when there are no delete fds.
        return null;
      }
      return new ImpalaHdfsPartition(this, deleteFileDescriptors);
    } catch (HiveException e) {
      LOG.warn("Exception generating delete partition: " + e);
      // parent method returns null when partition could not be generated.
      return null;
    }
  }

  @Override
  public FileSystemUtil.FsType getFsType() {
    return fsType;
  }

  @Override
  public ListMap<TNetworkAddress> getHostIndex() {
    return hostIndex;
  }

  @Override
  public FileSystem getFileSystem(Configuration conf) {
    return fs;
  }

  @Override
  public List<HdfsPartition.FileDescriptor> getFileDescriptors() {
    return fileDescriptors;
  }

  @Override
  public int getNumFileDescriptors() {
    return fileDescriptors.size();
  }

  @Override
  public boolean hasFileDescriptors() {
    return !fileDescriptors.isEmpty();
  }

  private static boolean isFullAcidTable(Table table) {
    return AcidUtils.isFullAcidTable(table.getParameters());
  }
}
