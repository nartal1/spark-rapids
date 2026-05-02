/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, Scalar}
import com.databricks.sql.io.{RowIndexFilterProvider, RowIndexFilterType}
import com.databricks.sql.transaction.tahoe.{
  DeltaColumnMapping,
  DeltaColumnMappingMode,
  IdMapping,
  NameMapping,
  NoMapping
}
import com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat._
import com.databricks.sql.transaction.tahoe.actions.{DeletionVectorDescriptor, Metadata, Protocol}
import com.databricks.sql.transaction.tahoe.files.TahoeFileIndex
import com.databricks.sql.transaction.tahoe.schema.SchemaMergingUtils
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.databricks.sql.transaction.tahoe.util.DeltaFileOperations.absolutePath
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.parquet._
import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFsRelation, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuFileSourceScanExec, InputFileUtils}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{LongType, MetadataBuilder, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

abstract class GpuDeltaParquetFileFormatDV(
    @transient relation: HadoopFsRelation,
    protocol: Protocol,
    metadata: Metadata,
    nullableRowTrackingConstantFields: Boolean = false,
    nullableRowTrackingGeneratedFields: Boolean = false,
    optimizationsEnabled: Boolean = true,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false,
    hasDeletionVectorRead: Boolean = false)
    extends GpuDeltaParquetFileFormatBase with Logging {

  override val columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode
  override val referenceSchema: StructType = metadata.schema

  if (columnMappingMode == IdMapping) {
    val requiredReadConf = SQLConf.PARQUET_FIELD_ID_READ_ENABLED
    require(TrampolineConnectShims.getActiveSession.sessionState.conf.getConf(requiredReadConf),
      s"${requiredReadConf.key} must be enabled to support Delta id column mapping mode")
    val requiredWriteConf = SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED
    require(TrampolineConnectShims.getActiveSession.sessionState.conf.getConf(requiredWriteConf),
      s"${requiredWriteConf.key} must be enabled to support Delta id column mapping mode")
  }

  /**
   * Delta 3.3+ has an extra nested field-id metadata key that must not be passed into the
   * Parquet reader after name mapping rewrites.
   */
  override def prepareSchema(inputSchema: StructType): StructType = {
    val schema = DeltaColumnMapping.createPhysicalSchema(
      inputSchema, referenceSchema, columnMappingMode)
    if (columnMappingMode == NameMapping) {
      SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
        field.copy(metadata = new MetadataBuilder()
          .withMetadata(field.metadata)
          .remove(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY)
          .remove(DeltaColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY)
          .build())
      }
    } else {
      schema
    }
  }

  /**
   * Helper method copied from Apache Spark
   * sql/catalyst/src/main/scala/org/apache/spark/sql/connector/catalog/CatalogV2Implicits.scala
   */
  private def quoteIfNeeded(part: String): String = {
    if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
      part
    } else {
      s"`${part.replace("`", "``")}`"
    }
  }

  /**
   * Translates pushed filters to physical column names when Delta column mapping is enabled.
   */
  private def prepareFiltersForRead(filters: Seq[Filter]): Seq[Filter] = {
    if (!effectiveOptimizationsEnabled) {
      Seq.empty
    } else if (columnMappingMode != NoMapping) {
      val physicalNameMap = DeltaColumnMapping.getLogicalNameToPhysicalNameMap(referenceSchema)
        .map {
          case (logicalName, physicalName) =>
            (logicalName.map(quoteIfNeeded).mkString("."),
              physicalName.map(quoteIfNeeded).mkString("."))
        }
      filters.flatMap(RapidsDeletionVectors.translateFilterForColumnMapping(_, physicalNameMap))
    } else {
      filters
    }
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = effectiveOptimizationsEnabled

  def hasTablePath: Boolean = tablePath.isDefined

  @transient private lazy val tahoeFileIndexOpt: Option[TahoeFileIndex] = relation.location match {
    case t: TahoeFileIndex => Some(t)
    case _ => None
  }

  private lazy val hasDeletionVectorsInTahoeFileIndex: Boolean = {
    tahoeFileIndexOpt.exists { tahoeFileIndex =>
      tahoeFileIndex.rowIndexFilters.exists(_.nonEmpty) ||
        tahoeFileIndex
          .matchingFiles(partitionFilters = Seq(TrueLiteral), dataFilters = Seq(TrueLiteral))
          .exists(_.deletionVector != null)
    }
  }

  private def effectiveOptimizationsEnabled: Boolean =
    optimizationsEnabled && !hasDeletionVectorRead && !hasDeletionVectorsInTahoeFileIndex

  private def buildDeletionVectorReadInfo(
      deletionVectorReadRequired: Boolean): Option[RapidsDeletionVectorReadInfo] = {
    if (!deletionVectorReadRequired) {
      None
    } else {
      val tahoeFileIndex = tahoeFileIndexOpt.getOrElse(throw new IllegalStateException(
        s"Expected TahoeFileIndex for Delta deletion-vector read, " +
          s"found ${relation.location.getClass}"))
      val tahoeTablePath = tablePath.getOrElse(tahoeFileIndex.path.toString)
      val filterTypes = tahoeFileIndex.rowIndexFilters.getOrElse(Map.empty)
        .map(kv => kv._1 -> kv._2.getRowIndexFilterType)
      val matchingFiles = tahoeFileIndex
        .matchingFiles(partitionFilters = Seq(TrueLiteral), dataFilters = Seq(TrueLiteral))
      def fileKeys(relativePath: String): Seq[String] = {
        val absolute = absolutePath(tahoeFileIndex.path.toString, relativePath)
        Seq(relativePath, absolute.toString, absolute.toUri.toString).distinct
      }
      val filePathToFilterProvider = {
        val fromRowIndexFilters = tahoeFileIndex.rowIndexFilters.getOrElse(Map.empty)
          .flatMap { case (path, provider) =>
            fileKeys(path).map(_ -> provider)
          }
        val fromMatchingFiles = matchingFiles.flatMap { addFile =>
          try {
            tahoeFileIndex.getRowIndexFilterForFile(addFile.path).toSeq.flatMap { provider =>
              fileKeys(addFile.path).map(_ -> provider)
            }
          } catch {
            // DB-17.3 can assert here when the AddFile path and candidate path are equivalent
            // but rendered differently. The direct rowIndexFilters map and DV descriptor map still
            // cover the file lookup, so skip this optional provider path.
            case _: AssertionError => Seq.empty
          }
        }
        (fromRowIndexFilters ++ fromMatchingFiles).toMap
      }
      val filesWithDVs = matchingFiles.filter(_.deletionVector != null)
      val filePathToDVMap = filesWithDVs.map { addFile =>
        val filterType = filterTypes.getOrElse(addFile.path, RowIndexFilterType.IF_CONTAINED)
        fileKeys(addFile.path).map { key =>
          key -> RapidsDeletionVectorDescriptorWithFilterType(addFile.deletionVector, filterType)
        }
      }.flatten.toMap
      Some(RapidsDeletionVectorReadInfo(
        tahoeTablePath, filePathToDVMap, filePathToFilterProvider))
    }
  }

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

    val useMetadataRowIndexConf = DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX
    val useMetadataRowIndex = sparkSession.sessionState.conf.getConf(useMetadataRowIndexConf)

    val dataReader = super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      prepareFiltersForRead(filters),
      options,
      hadoopConf,
      metrics)

    val schemaWithIndices = requiredSchema.fields.zipWithIndex
    def findColumn(name: String): Option[ColumnMetadata] = {
      val results = schemaWithIndices.filter(_._1.name == name)
      if (results.length > 1) {
        throw new IllegalArgumentException(
          s"There are more than one column with name=`$name` requested in the reader output")
      }
      results.headOption.map(e => ColumnMetadata(e._2, e._1))
    }

    val isRowDeletedColumn = findColumn(GpuDeltaParquetFileFormat.EDGE_COMPUTED_COLUMN_SKIP_ROW)
    val rowIndexColumn = findColumn(ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME)

    if (isRowDeletedColumn.isEmpty && rowIndexColumn.isEmpty) return dataReader
    if (isRowDeletedColumn.isEmpty) return dataReader

    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val deletionVectorReadInfo = buildDeletionVectorReadInfo(isRowDeletedColumn.isDefined)
    val hasNonEmptyRowIndexFilters = deletionVectorReadInfo.exists { info =>
      info.filePathToDVMap.values.exists(_.descriptor.cardinality != 0) ||
        info.filePathToFilterProvider.values.exists(_.getCardinality != 0)
    }

    require(useMetadataRowIndex || !effectiveOptimizationsEnabled || !hasNonEmptyRowIndexFilters,
      "Cannot generate row index related metadata with file splitting or predicate pushdown")

    if (hasTablePath && isRowDeletedColumn.isEmpty) {
      throw new IllegalArgumentException(
        s"Expected a column ${GpuDeltaParquetFileFormat.EDGE_COMPUTED_COLUMN_SKIP_ROW} " +
          "in the schema")
    }
    (file: PartitionedFile) => {
      val iter = dataReader(file)
      RapidsDeletionVectorUtils.iteratorWithAdditionalMetadataColumns(
        file,
        iter,
        isRowDeletedColumn,
        rowIndexColumn,
        tablePath,
        deletionVectorReadInfo,
        serializableHadoopConf,
        metrics).asInstanceOf[Iterator[InternalRow]]
    }
  }

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    val hasSkipRowColumn = fileScan.requiredSchema
      .exists(_.name == GpuDeltaParquetFileFormat.EDGE_COMPUTED_COLUMN_SKIP_ROW)

    if (fileScan.rapidsConf.isParquetCoalesceFileReadEnabled) {
      logWarning("Coalescing is not supported when `delta.enableDeletionVectors=true`, " +
        "using the multi-threaded reader. For more details on the Parquet reader types " +
        "please look at 'spark.rapids.sql.format.parquet.reader.type' config at " +
        "https://nvidia.github.io/spark-rapids/docs/additional-functionality/advanced_configs.html")
    }

    new DeltaMultiFileReaderFactory(
      fileScan.conf,
      broadcastedConf,
      prepareSchema(fileScan.relation.dataSchema),
      prepareSchema(fileScan.requiredSchema),
      prepareSchema(fileScan.readPartitionSchema),
      prepareFiltersForRead(pushedFilters).toArray,
      fileScan.rapidsConf,
      fileScan.allMetrics,
      useMetadataRowIndex = false,
      tablePath,
      buildDeletionVectorReadInfo(hasSkipRowColumn),
      queryUsesInputFile = hasSkipRowColumn || hasDeletionVectorRead ||
        fileScan.queryUsesInputFile)
  }
}

// Note: this class extends GpuParquetMultiFilePartitionReaderFactory, but this is an anti-pattern
// as GpuParquetMultiFilePartitionReaderFactory is a case class.
class DeltaMultiFileReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric],
    useMetadataRowIndex: Boolean,
    tablePath: Option[String],
    deletionVectorReadInfo: Option[RapidsDeletionVectorReadInfo],
    queryUsesInputFile: Boolean)
    extends GpuParquetMultiFilePartitionReaderFactory(
      sqlConf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      partitionSchema,
      filters,
      rapidsConf,
      poolConfBuilder = ThreadPoolConfBuilder(rapidsConf),
      metrics = metrics,
      queryUsesInputFile = queryUsesInputFile) {

  private val schemaWithIndices = readDataSchema.fields.zipWithIndex
  def findColumn(name: String): Option[ColumnMetadata] = {
    val results = schemaWithIndices.filter(_._1.name == name)
    require(results.length <= 1,
      s"There are more than one column with name=`$name` requested in the reader output")
    results.headOption.map(e => ColumnMetadata(e._2, e._1))
  }

  private val isRowDeletedColumn =
    findColumn(GpuDeltaParquetFileFormat.EDGE_COMPUTED_COLUMN_SKIP_ROW)
  private val rowIndexColumn = findColumn(ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME)

  override def createColumnarReader(p: InputPartition): PartitionReader[ColumnarBatch] = {
    val files = SparkShimImpl.getPartitionFiles(p.asInstanceOf[FilePartition]).toArray
    val reader = super.createColumnarReader(p)
    new DeltaMultiFileParquetPartitionReader(files, reader,
      isRowDeletedColumn, rowIndexColumn, broadcastedConf.value, tablePath,
      deletionVectorReadInfo, metrics)
  }
}

class DeltaMultiFileParquetPartitionReader(
    files: Array[PartitionedFile],
    reader: PartitionReader[ColumnarBatch],
    isRowDeletedColumnOpt: Option[ColumnMetadata],
    rowIndexColumnOpt: Option[ColumnMetadata],
    serializableConf: SerializableConfiguration,
    tablePath: Option[String],
    deletionVectorReadInfo: Option[RapidsDeletionVectorReadInfo],
    metrics: Map[String, GpuMetric]) extends PartitionReader[ColumnarBatch] {

  private val filesMap = files.flatMap { f =>
    Seq(f.filePath.toString, f.pathUri.toString, f.urlEncodedPath).distinct.map(_ -> f)
  }.toMap
  private var file: PartitionedFile = _
  private var rowIndex: Long = 0L
  private var rowIndexFilterOpt: Option[RapidsRowIndexFilter] = None

  override def next(): Boolean = reader.next()

  override def close(): Unit = {
    reader.close()
  }

  private def compareFile(file: PartitionedFile): Boolean = {
    val currentPath = InputFileUtils.getCurInputFilePath()
    (currentPath.isEmpty && files.length == 1) ||
      (currentPath == file.urlEncodedPath &&
        InputFileUtils.getCurInputFileStartOffset == file.start &&
        InputFileUtils.getCurInputFileLength == file.length)
  }

  private def currentFile(): PartitionedFile = {
    val currentPath = InputFileUtils.getCurInputFilePath()
    filesMap.get(currentPath)
      .orElse(if (currentPath.isEmpty && files.length == 1) Some(files.head) else None)
      .getOrElse(throw new IllegalStateException(
        s"Could not find Delta partitioned file for current input path: $currentPath"))
  }

  override def get(): ColumnarBatch = {
    val batch = reader.get()
    if (isRowDeletedColumnOpt.isEmpty) {
      return batch
    } else if (file == null || !compareFile(file)) {
      file = currentFile()
      rowIndex = 0
      rowIndexFilterOpt = RapidsDeletionVectorUtils
        .getRowIndexFilter(
          file, isRowDeletedColumnOpt, serializableConf, tablePath, deletionVectorReadInfo)
    }

    val newBatch = RapidsDeletionVectorUtils.processBatchWithDeletionVector(
      batch,
      rowIndex,
      isRowDeletedColumnOpt,
      rowIndexFilterOpt,
      rowIndexColumnOpt,
      metrics)
    rowIndex += batch.numRows()
    newBatch
  }
}

case class RapidsDeletionVectorDescriptorWithFilterType(
    descriptor: DeletionVectorDescriptor,
    filterType: RowIndexFilterType)

case class RapidsDeletionVectorReadInfo(
    tablePath: String,
    filePathToDVMap: Map[String, RapidsDeletionVectorDescriptorWithFilterType],
    filePathToFilterProvider: Map[String, RowIndexFilterProvider])

object RapidsDeletionVectorUtils {
  def processBatchWithDeletionVector(
      batch: ColumnarBatch,
      rowIndex: Long,
      isRowDeletedColumnOpt: Option[ColumnMetadata],
      rowIndexFilterOpt: Option[RapidsRowIndexFilter],
      rowIndexColumnOpt: Option[ColumnMetadata],
      metrics: Map[String, GpuMetric]): ColumnarBatch = {
    replaceBatch(
      rowIndex,
      batch,
      batch.numRows(),
      rowIndexColumnOpt,
      isRowDeletedColumnOpt,
      rowIndexFilterOpt,
      metrics)
  }

  def iteratorWithAdditionalMetadataColumns(
      partitionedFile: PartitionedFile,
      iterator: Iterator[Any],
      isRowDeletedColumnOpt: Option[ColumnMetadata],
      rowIndexColumnOpt: Option[ColumnMetadata],
      tablePath: Option[String],
      deletionVectorReadInfo: Option[RapidsDeletionVectorReadInfo],
      serializableConf: SerializableConfiguration,
      metrics: Map[String, GpuMetric]): Iterator[Any] = {

    val rowIndexFilterOpt =
      getRowIndexFilter(
        partitionedFile, isRowDeletedColumnOpt, serializableConf, tablePath,
        deletionVectorReadInfo)

    var rowIndex = 0L

    iterator.map {
      case cb: ColumnarBatch =>
        val size = cb.numRows()
        val newBatch = replaceBatch(rowIndex, cb, size, rowIndexColumnOpt, isRowDeletedColumnOpt,
          rowIndexFilterOpt, metrics)
        rowIndex += size
        newBatch

      case other =>
        throw new RuntimeException("Parquet reader returned an unknown row type: " +
          s"${other.getClass.getName}")
    }
  }

  private def getRowIndexPosSimple(start: Long, end: Long): GpuColumnVector = {
    val size = (end - start).toInt
    withResource(Scalar.fromLong(start)) { startScalar =>
      GpuColumnVector.from(ColumnVector.sequence(startScalar, size), LongType)
    }
  }

  private def replaceVectors(
      batch: ColumnarBatch,
      indexVectorTuples: (Int, org.apache.spark.sql.vectorized.ColumnVector)*): ColumnarBatch = {
    val vectors = ArrayBuffer[org.apache.spark.sql.vectorized.ColumnVector]()
    for (i <- 0 until batch.numCols()) {
      var replaced = false
      for (indexVectorTuple <- indexVectorTuples) {
        val (index, vector) = indexVectorTuple
        if (index == i) {
          vectors += vector
          batch.column(i).close()
          replaced = true
        }
      }
      if (!replaced) {
        vectors += batch.column(i)
      }
    }
    new ColumnarBatch(vectors.toArray, batch.numRows())
  }

  def getRowIndexFilter(
      partitionedFile: PartitionedFile,
      isRowDeletedColumnOpt: Option[ColumnMetadata],
      serializableHadoopConf: SerializableConfiguration,
      tablePath: Option[String],
      deletionVectorReadInfo: Option[RapidsDeletionVectorReadInfo])
  : Option[RapidsRowIndexFilter] = {
    isRowDeletedColumnOpt.map { _ =>
      val dvDescriptorOpt = partitionedFile.otherConstantMetadataColumnValues
        .get(FILE_ROW_INDEX_FILTER_ID_ENCODED)
      val filterTypeOpt = partitionedFile.otherConstantMetadataColumnValues
        .get(FILE_ROW_INDEX_FILTER_TYPE)
      if (dvDescriptorOpt.isDefined && filterTypeOpt.isDefined) {
        val dvDesc = DeletionVectorDescriptor.deserializeFromBase64(
          dvDescriptorOpt.get.asInstanceOf[String])
        val path = tablePath.orElse(deletionVectorReadInfo.map(_.tablePath))
        buildRowIndexFilter(
          dvDesc, filterTypeOpt.get.asInstanceOf[RowIndexFilterType],
          serializableHadoopConf, path)
      } else if (dvDescriptorOpt.isDefined || filterTypeOpt.isDefined) {
        throw new IllegalStateException(
          s"Both $FILE_ROW_INDEX_FILTER_ID_ENCODED and $FILE_ROW_INDEX_FILTER_TYPE " +
            "should either both have values or no values at all.")
      } else {
        partitionedFile.getRowIndexFilter.toSeq.headOption
          .map(provider => new RapidsCpuRowIndexFilter(
            provider.retrieve(serializableHadoopConf.value)): RapidsRowIndexFilter)
          .orElse {
            val lookupKeys = fileLookupKeys(partitionedFile)
            lookupKeys.flatMap(key =>
              deletionVectorReadInfo.flatMap(_.filePathToDVMap.get(key))).headOption
              .map { descriptorWithFilterType =>
                buildRowIndexFilter(
                  descriptorWithFilterType.descriptor,
                  descriptorWithFilterType.filterType,
                  serializableHadoopConf,
                  deletionVectorReadInfo.map(_.tablePath))
              }
          }.orElse {
            val lookupKeys = fileLookupKeys(partitionedFile)
            lookupKeys.flatMap(key =>
              deletionVectorReadInfo.flatMap(_.filePathToFilterProvider.get(key))).headOption
              .map(provider => new RapidsCpuRowIndexFilter(
                provider.retrieve(serializableHadoopConf.value)): RapidsRowIndexFilter)
          }.getOrElse(RapidsKeepAllRowsFilter)
      }
    }
  }

  private def fileLookupKeys(partitionedFile: PartitionedFile): Seq[String] = {
    Seq(
      partitionedFile.pathUri.toString,
      partitionedFile.filePath.toString,
      partitionedFile.urlEncodedPath).distinct
  }

  private def buildRowIndexFilter(
      dvDescriptor: DeletionVectorDescriptor,
      filterType: RowIndexFilterType,
      serializableHadoopConf: SerializableConfiguration,
      tablePath: Option[String]): RapidsRowIndexFilter = {
    val tablePathOpt = tablePath.map(new Path(_))
    filterType match {
      case RowIndexFilterType.IF_CONTAINED =>
        RapidsDropMarkedRowsFilter.createInstance(
          dvDescriptor, serializableHadoopConf.value, tablePathOpt)
      case RowIndexFilterType.IF_NOT_CONTAINED =>
        RapidsKeepMarkedRowsFilter.createInstance(
          dvDescriptor, serializableHadoopConf.value, tablePathOpt)
      case unexpectedFilterType => throw new IllegalStateException(
        s"Unexpected row index filter type: $unexpectedFilterType")
    }
  }

  private def replaceBatch(
      rowIndex: Long,
      batch: ColumnarBatch,
      size: Int,
      rowIndexColumnOpt: Option[ColumnMetadata],
      isRowDeletedColumnOpt: Option[ColumnMetadata],
      rowIndexFilterOpt: Option[RapidsRowIndexFilter],
      metrics: Map[String, GpuMetric]): ColumnarBatch = {

    var startTime = System.nanoTime()
    withResource(getRowIndexPosSimple(rowIndex, rowIndex + size)) { rowIndexGpuCol =>
      metrics("rowIndexColumnGenTime") += System.nanoTime() - startTime
      val indexVectorTuples = new ArrayBuffer[(Int, org.apache.spark.sql.vectorized.ColumnVector)]
      try {
        rowIndexColumnOpt.foreach { rowIndexCol =>
          indexVectorTuples += (rowIndexCol.index -> rowIndexGpuCol.incRefCount())
        }
        startTime = System.nanoTime()
        val isRowDeletedVector =
          rowIndexFilterOpt.get.materializeIntoVector(rowIndexGpuCol, rowIndex, size)
        metrics("isRowDeletedColumnGenTime") += System.nanoTime() - startTime
        indexVectorTuples += (isRowDeletedColumnOpt.get.index -> isRowDeletedVector)
        replaceVectors(batch, indexVectorTuples.toSeq: _*)
      } catch {
        case e: Throwable =>
          indexVectorTuples.map(_._2).safeClose(e)
          throw e
      }
    }
  }
}
