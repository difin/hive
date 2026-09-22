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
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Produces (and caches) the <b>S3 projection</b> of an Ozone {@code ofs://} Iceberg table:
 * a permanent, parallel, read-only copy of the full metadata chain with all {@code ofs://}
 * paths translated to {@code s3a://}, so external S3-compatible clients can read the table.
 *
 * <p>The projected metadata coexists indefinitely
 * alongside the canonical {@code ofs://} metadata and serves S3 clients on every request.
 * The canonical table is never modified.
 *
 * <p>The projection is cached by the table metadata UUID and regenerated only when the table
 * is updated (the UUID changes on every commit).
 *
 * <p>Projection layout :
 * <pre>
 * ofs://om/vol/bkt/table/
 *   metadata/
 *     v5.metadata.json                        &lt;- canonical (ofs://), used by Hive
 *     S3projection/
 *       metadata/
 *         v5.metadata.json                    &lt;- projected (s3a://)
 *         snap-123-1-abc.avro                  &lt;- projected manifest-list
 *         abc-m0.avro                          &lt;- projected data manifest
 * </pre>
 */
public class OzoneMetadataRewriter {
  private static final Logger LOG = LoggerFactory.getLogger(OzoneMetadataRewriter.class);

  /** Projection sub-directory name under {@code <tableLocation>/metadata/}. */
  static final String PROJECTION_SUFFIX = OzoneRewriteTablePathAction.PROJECTION_DIR_PREFIX;

  private final Configuration conf;

  public OzoneMetadataRewriter(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Returns the projected {@code s3a://} metadata for an {@code ofs://} table, creating or
   * refreshing the projection when the table has been updated.
   *
   * @param table the Iceberg table (must be a {@link BaseTable})
   * @return projected {@link TableMetadata} with {@code s3a://} paths, or {@code null}
   *         if the projection could not be created
   * @throws IOException if reading or writing metadata fails
   */
  public TableMetadata getOrCreateProjectedMetadata(Table table) throws IOException {
    if (!(table instanceof BaseTable)) {
      LOG.warn("Table is not a BaseTable, cannot project metadata: {}", table.getClass());
      return null;
    }

    BaseTable baseTable = (BaseTable) table;
    String location = baseTable.location();

    if (!OzonePathUtils.isOfsScheme(location)) {
      return baseTable.operations().current();
    }

    TableMetadata original = baseTable.operations().current();
    String projectedMetadataPath = getProjectedMetadataPath(location, original);

    if (isProjectionCurrent(baseTable.io(), projectedMetadataPath, original)) {
      LOG.debug("Projection is current (uuid={}), loading from: {}",
          original.uuid(), projectedMetadataPath);
      return loadProjectedMetadata(baseTable.io(), projectedMetadataPath);
    }

    LOG.info("Projecting full metadata chain for: {}", location);
    String s3aLocation = OzonePathUtils.translateOfsToS3a(location);

    RewriteTablePath action = new OzoneRewriteTablePathAction(baseTable)
        .rewriteLocationPrefix(location, s3aLocation);

    try {
      action.execute();
    } catch (Exception e) {
      throw new IOException("Failed to project metadata for: " + location, e);
    }

    return loadProjectedMetadata(baseTable.io(), projectedMetadataPath);
  }

  /**
   * The projected path for a given table version's {@code metadata.json}.
   *
   * <p>Resulting layout: {@code <tableLocation>/metadata/<PROJECTION_SUFFIX>/metadata/<filename>}
   */
  private String getProjectedMetadataPath(String tableLocation, TableMetadata metadata) {
    String metadataPath = metadata.metadataFileLocation();
    String metadataDir = metadataPath.substring(0, metadataPath.lastIndexOf('/') + 1);
    String projectionDir = metadataDir + PROJECTION_SUFFIX + "/";
    return RewriteTablePathUtil.stagingPath(metadataPath, tableLocation, projectionDir);
  }

  /**
   * Returns {@code true} when the projected metadata file exists and its UUID matches the
   * current table metadata UUID. The UUID changes on every commit, so this ensures stale
   * projections are never returned.
   */
  private boolean isProjectionCurrent(FileIO io, String projectedPath, TableMetadata original) {
    try {
      if (!io.newInputFile(projectedPath).exists()) {
        return false;
      }
      TableMetadata projected = TableMetadataParser.read(io, projectedPath);
      boolean current = original.uuid().equals(projected.uuid());
      if (!current) {
        LOG.debug("Projection is stale (projected uuid={}, current uuid={})",
            projected.uuid(), original.uuid());
      }
      return current;
    } catch (Exception e) {
      LOG.warn("Could not read projected metadata at {}, will recreate: {}",
          projectedPath, e.getMessage());
      return false;
    }
  }

  private TableMetadata loadProjectedMetadata(FileIO io, String projectedPath) throws IOException {
    try {
      return TableMetadataParser.read(io, projectedPath);
    } catch (Exception e) {
      throw new IOException("Failed to load projected metadata from: " + projectedPath, e);
    }
  }
}
