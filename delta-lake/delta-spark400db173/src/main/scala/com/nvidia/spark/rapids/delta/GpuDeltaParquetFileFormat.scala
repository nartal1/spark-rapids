/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.delta

import com.databricks.sql.transaction.tahoe.{DeltaColumnMappingMode, DeltaParquetFileFormat, IdMapping}
import com.databricks.sql.transaction.tahoe.actions.{Metadata, Protocol}
import com.nvidia.spark.rapids.{GpuMetric, SparkPlanMeta}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * Minimal GPU Delta Parquet file format for Databricks 17.3.
 *
 * DB-17.3 uses a fundamentally different DV mechanism than DB-14.3:
 * - No broadcastDvMap / broadcastHadoopConf
 * - Per-file DV via PartitionedFile.otherConstantMetadataColumnValues
 * - Constructor takes (protocol, metadata, ...) instead of (relation, columnMappingMode, ...)
 *
 * For Issue 1 (write path only), GPU DV reads are NOT supported. This will be enabled in
 * a follow-up issue. The tagSupportForGpuFileSourceScan method blocks GPU for DV tables.
 */
case class GpuDeltaParquetFileFormat(
    protocol: Protocol,
    metadata: Metadata,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false
  ) extends GpuDeltaParquetFileFormatBase {

  override val columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode
  override val referenceSchema: StructType = metadata.schema

  if (columnMappingMode == IdMapping) {
    val requiredReadConf = SQLConf.PARQUET_FIELD_ID_READ_ENABLED
    require(TrampolineConnectShims.getActiveSession
      .sessionState.conf.getConf(requiredReadConf),
      s"${requiredReadConf.key} must be enabled to support Delta id column mapping mode")
    val requiredWriteConf = SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED
    require(TrampolineConnectShims.getActiveSession
      .sessionState.conf.getConf(requiredWriteConf),
      s"${requiredWriteConf.key} must be enabled to support Delta id column mapping mode")
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = false

  override def buildReaderWithPartitionValuesAndMetrics(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      metrics: Map[String, GpuMetric])
  : PartitionedFile => Iterator[InternalRow] = {
    // For Issue 1, just delegate to the base Parquet reader.
    // DV tables will be tagged as not-GPU-supported via tagSupportForGpuFileSourceScan.
    super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      filters,
      options,
      hadoopConf,
      metrics)
  }
}

object GpuDeltaParquetFileFormat {
  def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val requiredSchema = meta.wrapped.requiredSchema
    if (requiredSchema.exists(_.name.startsWith("_databricks_internal"))) {
      meta.willNotWorkOnGpu(
        s"reading metadata columns starting with prefix _databricks_internal is not supported")
    }
    // For Issue 1, block GPU for all DV-related scans.
    // DV reads will be enabled in a follow-up issue.
    val format = meta.wrapped.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
    if (format.tablePath.isDefined) {
      meta.willNotWorkOnGpu("deletion vector reads are not yet supported for DB-17.3")
    }
  }

  /**
   * Convert a CPU Delta Parquet file format to the GPU version.
   * Called from DatabricksDeltaProviderBase.getReadFileFormat.
   */
  def convertToGpu(relation: HadoopFsRelation): GpuDeltaParquetFileFormat = {
    val fmt = relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
    GpuDeltaParquetFileFormat(
      fmt.protocol,
      fmt.metadata,
      fmt.tablePath,
      fmt.isCDCRead)
  }
}
