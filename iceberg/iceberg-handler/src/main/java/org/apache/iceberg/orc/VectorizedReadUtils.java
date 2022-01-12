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

package org.apache.iceberg.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.iceberg.org.apache.orc.TypeDescription;
import org.apache.hive.iceberg.org.apache.orc.impl.OrcTail;
import org.apache.hive.iceberg.org.apache.orc.impl.ReaderImpl;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;

/**
 * Utilities that rely on Iceberg code from org.apache.iceberg.orc package and are required for ORC vectorization.
 */
public class VectorizedReadUtils {

  private VectorizedReadUtils() {

  }

  /**
   * Opens the ORC inputFile and reads the metadata information to construct a byte buffer with OrcTail content.
   * Note that org.apache.orc (aka Hive bundled) ORC is used, as it is the older version compared to Iceberg's ORC.
   * @param inputFile - the original ORC file - this needs to be accessed to retrieve the original schema for mapping
   * @param job - JobConf instance to adjust
   * @throws IOException - errors relating to accessing the ORC file
   */
  public static ByteBuffer getSerializedOrcTail(InputFile inputFile, JobConf job)
      throws IOException {

    org.apache.orc.OrcFile.ReaderOptions readerOptions =
        org.apache.orc.OrcFile.readerOptions(job).useUTCTimestamp(true);
    if (inputFile instanceof HadoopInputFile) {
      readerOptions.filesystem(((HadoopInputFile) inputFile).getFileSystem());
    }

    try (org.apache.orc.impl.ReaderImpl orcFileReader =
             (org.apache.orc.impl.ReaderImpl) OrcFile.createReader(new Path(inputFile.location()), readerOptions)) {
      return orcFileReader.getSerializedFileFooter();
    }

  }

  /**
   * Returns an unshaded version of the OrcTail of the supplied input file. Used by Hive classes.
   * @param serializedTail - ByteBuffer containing the tail bytes
   * @throws IOException - errors relating to deserialization
   */
  public static org.apache.orc.impl.OrcTail deserializeToOrcTail(ByteBuffer serializedTail) throws IOException {
    return org.apache.orc.impl.ReaderImpl.extractFileTail(serializedTail);
  }

  /**
   * Returns an Iceberg-shaded version of the OrcTail of the supplied input file. Used by Iceberg classes.
   * @param serializedTail - ByteBuffer containing the tail bytes
   * @throws IOException - errors relating to deserialization
   */
  public static OrcTail deserializeToShadedOrcTail(ByteBuffer serializedTail) throws IOException {
    return ReaderImpl.extractFileTail(serializedTail);
  }

  /**
   * Adjusts the jobConf so that column reorders and renames that might have happened since this ORC file was written
   * are properly mapped to the schema of the original file.
   * @param task - Iceberg task - required for
   * @param job - JobConf instance to adjust
   * @param fileSchema - ORC file schema of the input file
   * @throws IOException - errors relating to accessing the ORC file
   */
  public static void handleIcebergProjection(FileScanTask task, JobConf job, TypeDescription fileSchema)
      throws IOException {

    // We need to map with the current (i.e. current Hive table columns) full schema (without projections),
    // as OrcInputFormat will take care of the projections by the use of an include boolean array
    Schema currentSchema = task.spec().schema();

    TypeDescription readOrcSchema;
    if (ORCSchemaUtil.hasIds(fileSchema)) {
      readOrcSchema = ORCSchemaUtil.buildOrcProjection(currentSchema, fileSchema);
    } else {
      TypeDescription typeWithIds =
          ORCSchemaUtil.applyNameMapping(fileSchema, MappingUtil.create(currentSchema));
      readOrcSchema = ORCSchemaUtil.buildOrcProjection(currentSchema, typeWithIds);
    }

    job.set(ColumnProjectionUtils.ICEBERG_ORC_SCHEMA_STRING, readOrcSchema.toString());

    // Predicate pushdowns needs to be adjusted too in case of column renames, we let Iceberg generate this into job
    if (task.residual() != null) {
      Expression boundFilter = Binder.bind(currentSchema.asStruct(), task.residual(), false);

      // Note the use of the unshaded version of this class here (required for SARG deseralization later)
      org.apache.hadoop.hive.ql.io.sarg.SearchArgument sarg =
          ExpressionToOrcSearchArgument.convert(boundFilter, readOrcSchema);
      if (sarg != null) {
        job.unset(TableScanDesc.FILTER_EXPR_CONF_STR);
        job.unset(ConvertAstToSearchArg.SARG_PUSHDOWN);

        job.set(ConvertAstToSearchArg.SARG_PUSHDOWN, ConvertAstToSearchArg.sargToKryo(sarg));
      }
    }
  }
}
