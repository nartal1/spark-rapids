/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from WriteIntoDelta.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package com.databricks.sql.transaction.tahoe.rapids

import scala.util.Try

import com.databricks.sql.DatabricksSQLConf
import com.databricks.sql.io.skipping.liquid.{ClusteredTableUtils, ClusteringColumnInfo}
import com.databricks.sql.transaction.tahoe.{DeltaOperations, OptimisticTransaction}
import com.databricks.sql.transaction.tahoe.DeltaCommitTag._
import com.databricks.sql.transaction.tahoe.actions.Action
import com.databricks.sql.transaction.tahoe.commands.{DMLUtils, WriteIntoDeltaEdge}
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.databricks.sql.transaction.tahoe.stats.{StatisticsOnLoadUpdater, StatisticsUtilities}

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ClusterBySpec}
import org.apache.spark.sql.internal.SQLConf

/** GPU version of Delta Lake's WriteIntoDelta. */
case class GpuWriteIntoDelta(
    gpuDeltaLog: GpuDeltaLog,
    cpuWrite: WriteIntoDeltaEdge)
    extends GpuWriteIntoDeltaBase(gpuDeltaLog, cpuWrite) {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    gpuDeltaLog.withNewTransaction { txn =>
      if (!hasBeenExecuted(txn)) {
        val clusterBySpecOpt = getClusterBySpecOpt
        val taggedCommitData = writeAndReturnCommitData(sparkSession, txn, clusterBySpecOpt)
        val statsOnLoad = registerStatsOnLoadIfNeeded(txn)
        val operation = DeltaOperations.Write(
          cpuWrite.mode,
          Option(cpuWrite.partitionColumns),
          cpuWrite.options.replaceWhere,
          cpuWrite.options.userMetadata,
          getClusterByForOperation(txn, clusterBySpecOpt),
          statsOnLoad,
          cpuWrite.getClusteredWriter.getStatus,
          cpuWrite.options.replaceOn,
          cpuWrite.options.replaceUsing,
          DeltaOperations.setIfPartitionColumnCheckEnabled {
            if (Try(cpuWrite.options.isDynamicPartitionOverwriteMode).getOrElse(false)) {
              Some(true)
            } else {
              None
            }
          },
          DeltaOperations.setIfPartitionColumnCheckEnabled {
            booleanOption(cpuWrite.options.canOverwriteSchema)
          },
          DeltaOperations.setIfPartitionColumnCheckEnabled {
            booleanOption(cpuWrite.options.canMergeSchema)
          })
        commitTaggedData(sparkSession, txn, taggedCommitData, operation)
      }
    }
    Seq.empty
  }

  override protected def commitActions(
      txn: OptimisticTransaction,
      actions: Seq[Action],
      operation: DeltaOperations.Operation): Unit = {
    // Plain writes never copy existing rows; replaceWhere may, so only stamp
    // NoRowsCopiedTag when no predicate is set.
    val tagged = DMLUtils.TaggedCommitData(actions)
      .withTag(NoRowsCopiedTag, cpuWrite.options.replaceWhere.isEmpty)
    txn.commit(tagged.actions, operation, tagged.stringTags)
  }

  private def getClusterBySpecOpt: Option[ClusterBySpec] = {
    cpuWrite.clusteringColumns.map { clusteringColumns =>
      val clusterBySpec = ClusterBySpec.fromColumnNamesAndAuto(clusteringColumns, None)
      ClusteredTableUtils.validateNumClusteringColumns(
        clusterBySpec.columnNames.map(_.fieldNames.toSeq),
        Some(cpuWrite.deltaLog))
      clusterBySpec
    }
  }

  private def getClusterByForOperation(
      txn: OptimisticTransaction,
      clusterBySpecOpt: Option[ClusterBySpec]): Option[Seq[String]] = {
    if (clusterBySpecOpt.nonEmpty || canOverwriteSchema) {
      clusterBySpecOpt.map(_.columnNames.map(_.toString))
    } else if (isClusteredSnapshot(txn)) {
      Some(ClusteringColumnInfo.extractLogicalNames(txn.snapshot))
    } else {
      None
    }
  }

  private def writeAndReturnCommitData(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      clusterBySpecOpt: Option[ClusterBySpec]): DMLUtils.TaggedCommitData[Action] = {
    def write = cpuWrite.writeAndReturnCommitData(
      txn,
      sparkSession,
      clusterBySpecOpt,
      isTableReplace = false)
    if (shouldUseCpuWritePath(txn, clusterBySpecOpt)) {
      txn match {
        case gpuTxn: GpuOptimisticTransaction =>
          GpuDeltaCpuFallback.withRapidsDisabled(sparkSession)(gpuTxn.withCpuWritePath(write))
        case _ =>
          GpuDeltaCpuFallback.withRapidsDisabled(sparkSession)(write)
      }
    } else {
      write
    }
  }

  private def shouldUseCpuWritePath(
      txn: OptimisticTransaction,
      clusterBySpecOpt: Option[ClusterBySpec]): Boolean =
    clusterBySpecOpt.nonEmpty || isClusteredSnapshot(txn)

  private def isClusteredSnapshot(txn: OptimisticTransaction): Boolean =
    ClusteredTableUtils.isSupported(txn.snapshot.protocol) &&
      ClusteringColumnInfo.extractLogicalNames(txn.snapshot).nonEmpty

  private def registerStatsOnLoadIfNeeded(txn: OptimisticTransaction): Boolean = {
    val statsOnLoad =
      (txn.writtenRowsOptStats.isDefined && isOverwriteOperation) ||
        StatisticsUtilities.canBeMergedWithCatalogStats(
          txn.catalogTable,
          txn.writtenRowsOptStats,
          txn.snapshot.sizeInBytes,
          txn.metadata)
    if (!isDLTTable(txn.catalogTable)) {
      txn.registerPostCommitHook(new StatisticsOnLoadUpdater(
        None,
        statsOnLoad,
        mergeWithExistingStats,
        skipEligibilityCheck = false))
    }
    statsOnLoad
  }

  private def mergeWithExistingStats: Boolean = {
    val treatPartialOverwriteAsNonReplace = SQLConf.get.getConf(
      DatabricksSQLConf.Optimizer
        .AUTO_STATS_TREAT_ALL_PARTIAL_OVERWRITE_OPERATIONS_AS_NON_REPLACE_OPERATION)
    if (treatPartialOverwriteAsNonReplace) {
      !isOverwriteOperation ||
        (isOverwriteOperation && cpuWrite.options.isInsertPartialOverwriteOp)
    } else {
      !isOverwriteOperation
    }
  }

  private def isOverwriteOperation: Boolean = cpuWrite.mode == SaveMode.Overwrite

  private def canOverwriteSchema: Boolean =
    cpuWrite.options.canOverwriteSchema && isOverwriteOperation &&
      cpuWrite.options.replaceWhere.isEmpty

  /**
   * Returns true if there is information in the spark session that indicates that this write, which
   * is part of a streaming query and a batch, has already been successfully written.
   */
  private def hasBeenExecuted(txn: OptimisticTransaction): Boolean = {
    val txnVersion = cpuWrite.options.txnVersion
    val txnAppId = cpuWrite.options.txnAppId
    val executed = for {
      version <- txnVersion
      appId <- txnAppId
      if txn.txnVersion(appId) >= version
    } yield {
      logInfo(s"Transaction write of version $version for application id $appId " +
          s"has already been committed in Delta table id ${txn.deltaLog.tableId}. " +
          s"Skipping this write.")
      true
    }
    executed.getOrElse(false)
  }

  private def isDLTTable(catalogTable: Option[CatalogTable]): Boolean = {
    catalogTable.exists(_.properties.keys.exists(_.startsWith("pipelines.")))
  }

  private def commitTaggedData(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      taggedCommitData: DMLUtils.TaggedCommitData[Action],
      operation: DeltaOperations.Operation): Unit = {
    if (sparkSession.conf.get(DeltaSQLConf.WRITE_INTO_DELTA_USES_COMMIT_IF_NEEDED)) {
      txn.commitIfNeeded(taggedCommitData.actions, operation, taggedCommitData.stringTags)
    } else {
      txn.commit(taggedCommitData.actions, operation, taggedCommitData.stringTags)
    }
  }

  private def booleanOption(enabled: Boolean): Option[Boolean] =
    if (enabled) Some(true) else None
}
