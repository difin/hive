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

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.RewriteTablePathUtil.PositionDeleteReaderWriter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.actions.ImmutableRewriteTablePath;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>The projection is not a temporary "staging" area on its way to a commit — it is a
 * long-lived, side-by-side view that coexists with the canonical {@code ofs://} metadata
 * and serves S3 clients. The canonical table is never modified or committed to.
 *
 * <p>The following files are written into the projection directory:
 * <ol>
 *   <li>{@code metadata.json} — snapshot manifest-list references and table location</li>
 *   <li>Manifest-list Avro files — manifest file references per snapshot</li>
 *   <li>Data manifest Avro files — data file path references</li>
 *   <li>Delete manifest Avro files — delete file path references</li>
 *   <li>Position delete files (Avro) — {@code file_path} column values</li>
 * </ol>
 *
 * <p>Data files are <em>never</em> copied. External clients read the single physical copy
 * through the Ozone S3 Gateway link bucket.
 *
 * <p>Default projection directory:
 * <pre>{@code <tableLocation>/metadata/<PROJECTION_DIR_PREFIX>/}</pre>
 */
public class OzoneRewriteTablePathAction implements RewriteTablePath {

  private static final Logger LOG = LoggerFactory.getLogger(OzoneRewriteTablePathAction.class);

  static final String PROJECTION_DIR_PREFIX = "S3projection";
  private static final String N_A = "N/A";

  private final Table table;
  private final PositionDeleteReaderWriter posDeleteReaderWriter;
  private String sourcePrefix;
  private String targetPrefix;
  private String projectionLocation;
  private String endVersionName;

  public OzoneRewriteTablePathAction(Table table) {
    this(table, new AvroPositionDeleteReaderWriter());
  }

  /** Package-private constructor for testing — allows injecting a mock reader/writer. */
  OzoneRewriteTablePathAction(Table table, PositionDeleteReaderWriter posDeleteReaderWriter) {
    this.table = table;
    this.posDeleteReaderWriter = posDeleteReaderWriter;
  }

  @Override
  public RewriteTablePath rewriteLocationPrefix(String source, String target) {
    this.sourcePrefix = source;
    this.targetPrefix = target;
    return this;
  }

  @Override
  public RewriteTablePath stagingLocation(String location) {
    this.projectionLocation = location;
    return this;
  }

  @Override
  public RewriteTablePath startVersion(String startVersion) {
    // Incremental rewrite not needed for Ozone data-sharing (always rewrites current snapshot).
    return this;
  }

  @Override
  public RewriteTablePath endVersion(String version) {
    this.endVersionName = version;
    return this;
  }
  @Override
  public Result execute() {
    validateInputs();

    FileIO io = table.io();
    TableMetadata currentMetadata = ((HasTableOperations) table).operations().current();

    // Resolve end version (defaults to current).
    String resolvedEndVersionPath;
    TableMetadata endMetadata;
    if (endVersionName != null) {
      resolvedEndVersionPath = resolveVersionPath(currentMetadata, endVersionName);
      endMetadata = new StaticTableOperations(resolvedEndVersionPath, io).current();
    } else {
      endMetadata = currentMetadata;
      resolvedEndVersionPath = currentMetadata.metadataFileLocation();
    }

    String projectionDir = resolveProjectionDir(endMetadata);
    LOG.info("Projecting rewritten metadata to: {}", projectionDir);

    String structTargetPrefix = toStructTargetPrefix(projectionDir);
    LOG.info("Rewrite prefixes: struct(metadata-chain)={}, data(leaf-files)={}",
        structTargetPrefix, targetPrefix);

    // Step 1: project metadata.json (manifest-list refs -> projected files).
    rewriteVersionFile(endMetadata, resolvedEndVersionPath, projectionDir, structTargetPrefix, io);

    // Collect all snapshot IDs for copy-plan filtering.
    Set<Long> allSnapshotIds = endMetadata.snapshots().stream()
        .map(Snapshot::snapshotId)
        .collect(Collectors.toSet());

    // Collect all manifests referenced by all snapshots (deduplicated).
    Set<String> allManifestPaths = new HashSet<>();
    List<ManifestFile> allManifestFiles = new ArrayList<>();
    for (Snapshot snapshot : endMetadata.snapshots()) {
      try {
        List<ManifestFile> snapshotManifests = snapshot.allManifests(io);
        for (ManifestFile mf : snapshotManifests) {
          if (allManifestPaths.add(mf.path())) {
            allManifestFiles.add(mf);
          }
        }
      } catch (Exception e) {
        LOG.warn("Could not read manifests for snapshot {}: {}", snapshot.snapshotId(), e.getMessage());
      }
    }

    // Step 2: project each snapshot's manifest-list.
    // Use structTargetPrefix so the manifest_path entries point at the PROJECTED manifests.
    for (Snapshot snapshot : endMetadata.snapshots()) {
      String outputPath = RewriteTablePathUtil.stagingPath(
          snapshot.manifestListLocation(), sourcePrefix, projectionDir);
      try {
        RewriteTablePathUtil.rewriteManifestList(
            snapshot, io, endMetadata,
            allManifestPaths,
            sourcePrefix, structTargetPrefix,
            projectionDir, outputPath);
      } catch (Exception e) {
        LOG.warn("Failed to project manifest-list for snapshot {}: {}",
            snapshot.snapshotId(), e.getMessage());
      }
    }

    // Step 3 & 4: project data and delete manifest Avro files.
    for (ManifestFile manifest : allManifestFiles) {
      String projectionPath = RewriteTablePathUtil.stagingPath(
          manifest.path(), sourcePrefix, projectionDir);
      OutputFile outputFile = io.newOutputFile(projectionPath);
      try {
        ManifestContent manifestContent = manifest.content();
        switch (manifestContent) {
          case DATA:
            // Leaf data_file paths -> table-root s3a (targetPrefix), so they resolve to
            // the real data files via the link bucket.
            RewriteTablePathUtil.rewriteDataManifest(
                manifest, allSnapshotIds, outputFile, io,
                endMetadata.formatVersion(),
                endMetadata.specsById(),
                sourcePrefix, targetPrefix);
            break;
          case DELETES:
            // Delete-file locations -> projected (structTargetPrefix), since we project the
            // rewritten (position) delete files. The data-file references *inside* those
            // delete files are translated to table-root s3a by rewritePositionDeleteFiles.
            RewriteTablePathUtil.rewriteDeleteManifest(
                manifest, allSnapshotIds, outputFile, io,
                endMetadata.formatVersion(),
                endMetadata.specsById(),
                sourcePrefix, structTargetPrefix,
                projectionDir);
            break;
          default:
            LOG.warn("Unknown manifest content type: {}", manifest.content());
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to project manifest: " + manifest.path(), e);
      }
    }

    // Step 5: project position delete files referenced from delete manifests.
    rewritePositionDeleteFiles(allManifestFiles, endMetadata, projectionDir, io);

    String latestVersion = RewriteTablePathUtil.fileName(resolvedEndVersionPath);
    LOG.info("Metadata projection complete. latestVersion={}, projectionDir={}",
        latestVersion, projectionDir);

    return ImmutableRewriteTablePath.Result.builder()
        .stagingLocation(projectionDir)
        .latestVersion(latestVersion)
        .fileListLocation(N_A)
        .build();
  }

  private void validateInputs() {
    if (sourcePrefix == null || sourcePrefix.isEmpty()) {
      throw new IllegalArgumentException("sourcePrefix must be set via rewriteLocationPrefix()");
    }
    if (targetPrefix == null || targetPrefix.isEmpty()) {
      throw new IllegalArgumentException("targetPrefix must be set via rewriteLocationPrefix()");
    }
    if (sourcePrefix.equals(targetPrefix)) {
      throw new IllegalArgumentException(
          "sourcePrefix and targetPrefix must be different: " + sourcePrefix);
    }
  }

  /**
   * Resolves the projection directory for this metadata version. The path is deterministic
   * given the table metadata UUID, so repeated calls for the same metadata version write to
   * the same location and can be detected as already current.
   */
  private String resolveProjectionDir(TableMetadata metadata) {
    if (projectionLocation != null && !projectionLocation.isEmpty()) {
      return RewriteTablePathUtil.maybeAppendFileSeparator(projectionLocation);
    }
    String metadataPath = metadata.metadataFileLocation();
    String metadataDir = metadataPath.substring(0, metadataPath.lastIndexOf('/') + 1);
    return metadataDir + PROJECTION_DIR_PREFIX + "/";
  }

  /**
   * Projects the version (metadata.json) file.
   *
   * <p>Uses {@code structTargetPrefix} (the s3a translation of the projection directory) as
   * the replacement target so that the snapshot {@code manifest-list} references point at
   * the PROJECTED manifest-list files rather than the original table-root locations (which,
   * through the link bucket, would resolve to the un-rewritten {@code ofs://} files).
   */
  private void rewriteVersionFile(TableMetadata metadata, String versionFilePath,
      String projectionDir, String structTargetPrefix, FileIO io) {
    String projectionPath = RewriteTablePathUtil.stagingPath(versionFilePath, sourcePrefix, projectionDir);
    TableMetadata rewritten =
        RewriteTablePathUtil.replacePaths(metadata, sourcePrefix, structTargetPrefix);
    rewritten = stripStatistics(rewritten);
    TableMetadataParser.overwrite(rewritten, io.newOutputFile(projectionPath));
    LOG.info("Projected metadata.json: {} -> {}", versionFilePath, projectionPath);
  }

  /**
   * Removes {@code statistics} and {@code partition-statistics} entries from the projected
   * metadata served to external clients.
   *
   * <p>Two reasons:
   * <ul>
   *   <li>Hive writes a Hive-specific Puffin blob type ({@code ColumnStatisticsObj}) that
   *       strict Iceberg clients reject with a validation error, making the table unreadable.</li>
   *   <li>The stats Puffin files are not projected, so their (rewritten) paths would dangle.</li>
   * </ul>
   *
   * <p>Statistics are a read-time optimization; omitting them from the external read-only
   * projection is safe and keeps it self-contained. The canonical table keeps its statistics.
   */
  private TableMetadata stripStatistics(TableMetadata metadata) {
    if (metadata.statisticsFiles().isEmpty() && metadata.partitionStatisticsFiles().isEmpty()) {
      return metadata;
    }
    TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);
    metadata.statisticsFiles().stream()
        .map(StatisticsFile::snapshotId)
        .distinct()
        .forEach(builder::removeStatistics);
    metadata.partitionStatisticsFiles().stream()
        .map(PartitionStatisticsFile::snapshotId)
        .distinct()
        .forEach(builder::removePartitionStatistics);
    TableMetadata result = builder.build();
    LOG.info("Stripped {} statistics and {} partition-statistics entries from projected metadata",
        metadata.statisticsFiles().size(), metadata.partitionStatisticsFiles().size());
    return result;
  }

  /**
   * Computes the structural target prefix: the {@code s3a://} translation of the projection
   * directory. Metadata-chain cross-references are rewritten to this prefix so they point
   * into the projection tree where the rewritten files live.
   */
  private String toStructTargetPrefix(String projectionDir) {
    String noSlash = projectionDir.endsWith("/")
        ? projectionDir.substring(0, projectionDir.length() - 1)
        : projectionDir;
    if (OzonePathUtils.isOfsScheme(noSlash)) {
      return OzonePathUtils.translateOfsToS3a(noSlash);
    }
    // Custom (already non-ofs) projection location: use as-is.
    return noSlash;
  }

  /**
   * Resolves a version file name (e.g. {@code 00003-abc.metadata.json}) to its full path by
   * searching the table's metadata log.
   */
  private String resolveVersionPath(TableMetadata metadata, String versionFileName) {
    if (versionFileName.equals(
        RewriteTablePathUtil.fileName(metadata.metadataFileLocation()))) {
      return metadata.metadataFileLocation();
    }
    for (MetadataLogEntry entry : metadata.previousFiles()) {
      if (versionFileName.equals(RewriteTablePathUtil.fileName(entry.file()))) {
        return entry.file();
      }
    }
    throw new IllegalArgumentException(
        "Cannot find version file '" + versionFileName + "' in metadata log.");
  }

  /**
   * Projects position delete files (Avro format) referenced from delete manifests.
   * DV/Puffin files are handled internally by
   * {@link RewriteTablePathUtil#rewritePositionDeleteFile}.
   */
  private void rewritePositionDeleteFiles(List<ManifestFile> manifests,
      TableMetadata metadata, String projectionDir, FileIO io) {

    Set<String> processedPaths = new HashSet<>();
    for (ManifestFile manifest : manifests) {
      if (manifest.content() != ManifestContent.DELETES) {
        continue;
      }
      PartitionSpec spec = metadata.specsById().get(manifest.partitionSpecId());
      try (org.apache.iceberg.ManifestReader<DeleteFile> reader =
               ManifestFiles.readDeleteManifest(manifest, io, metadata.specsById())) {
        for (DeleteFile deleteFile : reader) {
          if (deleteFile.content() != FileContent.POSITION_DELETES) {
            continue;
          }
          String path = deleteFile.location();
          if (!processedPaths.add(path)) {
            continue;
          }
          String projectionPath = RewriteTablePathUtil.stagingPath(path, sourcePrefix, projectionDir);
          OutputFile outputFile = io.newOutputFile(projectionPath);
          try {
            RewriteTablePathUtil.rewritePositionDeleteFile(
                deleteFile, outputFile, io, spec,
                sourcePrefix, targetPrefix,
                posDeleteReaderWriter);
            LOG.debug("Projected position delete file: {} -> {}", path, projectionPath);
          } catch (IOException e) {
            LOG.warn("Failed to project position delete file {}: {}", path, e.getMessage());
          }
        }
      } catch (IOException e) {
        LOG.warn("Could not open delete manifest {}: {}", manifest.path(), e.getMessage());
      }
    }
  }

  /**
   * Avro-based {@link PositionDeleteReaderWriter} for Iceberg v2 position delete files.
   */
  static class AvroPositionDeleteReaderWriter implements PositionDeleteReaderWriter {

    @Override
    public CloseableIterable<Record> reader(
        InputFile inputFile, FileFormat format, PartitionSpec spec) {
      validateAvro(format);
      return Avro.read(inputFile)
          .createReaderFunc(DataReader::create)
          .project(DeleteSchemaUtil.posDeleteReadSchema(spec.schema()))
          .reuseContainers()
          .build();
    }

    @SuppressWarnings("deprecation")
    @Override
    public PositionDeleteWriter<Record> writer(
        OutputFile outputFile, FileFormat format, PartitionSpec spec,
        StructLike partition, Schema rowSchema) throws IOException {
      validateAvro(format);
      Avro.DeleteWriteBuilder builder = Avro.writeDeletes(outputFile)
          .createWriterFunc(DataWriter::create)
          .withPartition(partition)
          .withSpec(spec);
      if (rowSchema != null) {
        builder = builder.rowSchema(rowSchema);
      }
      return builder.buildPositionWriter();
    }

    private static void validateAvro(FileFormat format) {
      if (format != FileFormat.AVRO) {
        throw new UnsupportedOperationException(
            "AvroPositionDeleteReaderWriter supports only AVRO format, got: " + format);
      }
    }
  }
}
