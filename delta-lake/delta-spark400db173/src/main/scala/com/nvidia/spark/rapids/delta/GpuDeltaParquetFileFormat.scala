/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat
import com.databricks.sql.transaction.tahoe.actions.{Metadata, Protocol}
import com.nvidia.spark.rapids.SparkPlanMeta

import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

/**
 * GPU Delta Parquet file format for Databricks 17.3.
 *
 * DB-17.3 uses a fundamentally different DV mechanism than DB-14.3:
 * - No broadcastDvMap / broadcastHadoopConf
 * - Per-file DV via PartitionedFile.otherConstantMetadataColumnValues or TahoeFileIndex metadata
 * - Constructor takes (protocol, metadata, ...) instead of (relation, columnMappingMode, ...)
 */
case class GpuDeltaParquetFileFormat(
    @transient relation: HadoopFsRelation,
    protocol: Protocol,
    metadata: Metadata,
    generateRowIndexFilterId: Boolean = false,
    generateRowIndexFilterColumn: Boolean = false,
    generateDeltaFileInScanId: Boolean = false,
    nullableRowTrackingConstantFields: Boolean = false,
    nullableRowTrackingGeneratedFields: Boolean = false,
    optimizationsEnabled: Boolean = true,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false
  ) extends GpuDeltaParquetFileFormatDV(
    relation,
    protocol,
    metadata,
    nullableRowTrackingConstantFields,
    nullableRowTrackingGeneratedFields,
    optimizationsEnabled,
    tablePath,
    isCDCRead,
    generateRowIndexFilterId || generateRowIndexFilterColumn || tablePath.isDefined)

object GpuDeltaParquetFileFormat {
  private[delta] val EDGE_COMPUTED_COLUMN_SKIP_ROW =
    "_databricks_internal_edge_computed_column_skip_row"

  def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val requiredSchema = meta.wrapped.requiredSchema
    if (requiredSchema.exists { field =>
      field.name.startsWith("_databricks_internal") &&
        field.name != EDGE_COMPUTED_COLUMN_SKIP_ROW
    }) {
      meta.willNotWorkOnGpu(
        s"reading metadata columns starting with prefix _databricks_internal is not supported")
    }
    val format = meta.wrapped.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
    if (format.isCDCRead && format.tablePath.isDefined) {
      meta.willNotWorkOnGpu(
        "CDC reads with deletion vectors are not yet supported on GPU for DB-17.3")
    }
    if (format.generateDeltaFileInScanId) {
      meta.willNotWorkOnGpu(
        "Delta file-in-scan metadata columns are not supported on GPU for DB-17.3")
    }
    if (format.nullableRowTrackingConstantFields) {
      meta.willNotWorkOnGpu(
        "nullable Delta row-tracking constant fields are not supported on GPU for DB-17.3")
    }
    if (format.nullableRowTrackingGeneratedFields) {
      meta.willNotWorkOnGpu(
        "nullable Delta row-tracking generated fields are not supported on GPU for DB-17.3")
    }
  }

  /**
   * Convert a CPU Delta Parquet file format to the GPU version.
   * Called from DatabricksDeltaProviderBase.getReadFileFormat.
   */
  def convertToGpu(relation: HadoopFsRelation): GpuDeltaParquetFileFormat = {
    val fmt = relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
    val dvEnabled =
      fmt.generateRowIndexFilterId || fmt.generateRowIndexFilterColumn || fmt.tablePath.isDefined
    GpuDeltaParquetFileFormat(
      relation = relation,
      protocol = fmt.protocol,
      metadata = fmt.metadata,
      generateRowIndexFilterId = fmt.generateRowIndexFilterId,
      generateRowIndexFilterColumn = fmt.generateRowIndexFilterColumn,
      generateDeltaFileInScanId = fmt.generateDeltaFileInScanId,
      nullableRowTrackingConstantFields = fmt.nullableRowTrackingConstantFields,
      nullableRowTrackingGeneratedFields = fmt.nullableRowTrackingGeneratedFields,
      optimizationsEnabled = if (dvEnabled) false else fmt.optimizationsEnabled,
      tablePath = fmt.tablePath,
      isCDCRead = fmt.isCDCRead)
  }
}
