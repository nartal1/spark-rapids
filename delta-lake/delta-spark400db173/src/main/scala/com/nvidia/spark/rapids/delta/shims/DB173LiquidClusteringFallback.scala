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

package com.nvidia.spark.rapids.delta.shims

import com.databricks.sql.io.skipping.liquid.ClusteredTableUtils
import com.databricks.sql.transaction.tahoe.DeltaLog
import com.databricks.sql.transaction.tahoe.commands.{
  DeleteCommand,
  DeleteCommandEdge,
  MergeIntoCommand,
  MergeIntoCommandEdge,
  UpdateCommand,
  UpdateCommandEdge
}
import com.databricks.sql.transaction.tahoe.sources.DeltaDataSource
import com.nvidia.spark.rapids.{RapidsConf, RapidsMeta}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.apache.spark.sql.internal.SQLConf

object DB173LiquidClusteringFallback {
  private val reason =
    "Delta write to a liquid clustered table is not supported on GPU for DB-17.3"

  def isLiquidClustered(deltaLog: DeltaLog): Boolean = {
    ClusteredTableUtils.getClusterBySpecOptional(deltaLog.update()).isDefined
  }

  def tagForGpu(meta: RapidsMeta[_, _, _], deltaLog: DeltaLog): Unit = {
    if (isLiquidClustered(deltaLog)) {
      meta.willNotWorkOnGpu(reason)
    }
  }

  def writesLiquidClusteredTable(cmd: RunnableCommand): Boolean = cmd match {
    case saveCmd: SaveIntoDataSourceCommand =>
      saveCmd.dataSource.isInstanceOf[DeltaDataSource] && saveCmd.options.get("path").exists {
        path =>
          val deltaLog = DeltaLog.forTable(SparkSession.active, new Path(path), saveCmd.options)
          isLiquidClustered(deltaLog)
      }
    case deleteCmd: DeleteCommand =>
      isLiquidClustered(deleteCmd.deltaLog)
    case deleteCmd: DeleteCommandEdge =>
      isLiquidClustered(deleteCmd.deltaLog)
    case updateCmd: UpdateCommand =>
      isLiquidClustered(updateCmd.tahoeFileIndex.deltaLog)
    case updateCmd: UpdateCommandEdge =>
      isLiquidClustered(updateCmd.tahoeFileIndex.deltaLog)
    case mergeCmd: MergeIntoCommand =>
      isLiquidClustered(mergeCmd.targetFileIndex.deltaLog)
    case mergeCmd: MergeIntoCommandEdge =>
      isLiquidClustered(mergeCmd.targetFileIndex.deltaLog)
    case _ =>
      false
  }

  def withRapidsDisabled[T](spark: SparkSession)(body: => T): T = {
    val rapidsEnabled = RapidsConf.SQL_ENABLED.key
    val originalSessionConf = spark.conf.getOption(rapidsEnabled)
    val sqlConf = SQLConf.get
    val originalSqlConf = Option(sqlConf.getConfString(rapidsEnabled, null))
    spark.conf.set(rapidsEnabled, "false")
    sqlConf.setConfString(rapidsEnabled, "false")
    try {
      body
    } finally {
      originalSqlConf match {
        case Some(value) => sqlConf.setConfString(rapidsEnabled, value)
        case None => sqlConf.unsetConf(rapidsEnabled)
      }
      originalSessionConf match {
        case Some(value) => spark.conf.set(rapidsEnabled, value)
        case None => spark.conf.unset(rapidsEnabled)
      }
    }
  }
}
