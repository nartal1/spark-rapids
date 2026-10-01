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

package com.nvidia.spark.rapids.delta.delta42x

import com.nvidia.spark.rapids.{RapidsConf, SparkPlanMeta}
import com.nvidia.spark.rapids.delta.common.GpuDeltaParquetFileFormatBase2

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference,
  FileSourceConstantMetadataAttribute, FileSourceGeneratedMetadataAttribute,
  FileSourceGeneratedMetadataStructField, FileSourceMetadataAttribute}
import org.apache.spark.sql.delta.{DeltaParquetFileFormat, NoMapping}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

/** Experimental Delta 4.2 row-tracking scan, with CPU-equivalent constant metadata extraction. */
class GpuRowTrackingParquetFileFormat(cpu: DeltaParquetFileFormat)
  extends GpuDeltaParquetFileFormatBase2(
    cpu.protocol, cpu.metadata, false, cpu.optimizationsEnabled, cpu.tablePath, cpu.isCDCRead) {

  override protected def generateRowIndexesWithoutDVs: Boolean = true

  override def constantMetadataAttributes(output: Seq[Attribute]): Seq[Attribute] =
    output.collect { case a: AttributeReference
      if FileSourceConstantMetadataAttribute.unapply(a).isDefined => a }

  override def constantMetadataValues(file: PartitionedFile, attrs: Seq[Attribute]): Seq[Any] =
    attrs.map(a => FileFormat.getFileConstantMetadataColumnValue(
      a.name, file, cpu.fileConstantMetadataExtractors).value)

  override def prepareSchema(schema: StructType): StructType = {
    val marked = StructType(schema.fields.map { field =>
      if (field.name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME &&
          FileSourceGeneratedMetadataStructField.unapply(field).isDefined) {
        field.copy(metadata = GpuDeltaParquetFileFormatBase2.GPU_ROW_INDEX_STRUCT_FIELD.metadata)
      } else {
        field
      }
    })
    super.prepareSchema(marked)
  }
}

object GpuRowTrackingParquetFileFormat {
  // Deliberately narrow initial scope. Unsupported formats retain the existing CPU fallback.
  def eligible(format: DeltaParquetFileFormat, conf: RapidsConf): Boolean =
    format.optimizationsEnabled && !format.isCDCRead &&
      format.columnMappingMode == NoMapping &&
      format.metadata.configuration.get("delta.enableRowTracking").contains("true") &&
      Delta42xProvider.isPushDVPredicateDownEnabled(conf)

  def supports(meta: SparkPlanMeta[FileSourceScanExec]): Boolean =
    meta.wrapped.relation.fileFormat match {
      case format: DeltaParquetFileFormat if eligible(format, meta.conf) =>
        val fields = format.metadataSchemaFields.map(f => f.name -> f).toMap
        meta.wrapped.output.forall {
          case FileSourceConstantMetadataAttribute(a) =>
            fields.get(a.name).exists(_.dataType == a.dataType)
          case FileSourceGeneratedMetadataAttribute(a, _) =>
            // Row indexes and materialized row-tracking fields come from the Parquet reader.
            a.dataType == org.apache.spark.sql.types.LongType
          case FileSourceMetadataAttribute(_) => false
          case _ => true
        }
      case _ => false
    }
}
