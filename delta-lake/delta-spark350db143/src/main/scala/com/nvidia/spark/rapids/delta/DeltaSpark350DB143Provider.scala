/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 *
 * Some portion of this class has been taken from DeltaTableV2 class 
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

package com.nvidia.spark.rapids.delta

import com.databricks.sql.transaction.tahoe.DeltaOptions
import com.databricks.sql.transaction.tahoe.commands.{DeleteCommand, DeleteCommandEdge, MergeIntoCommand, MergeIntoCommandEdge, UpdateCommand, UpdateCommandEdge, WriteIntoDeltaEdge}
import com.databricks.sql.transaction.tahoe.rapids.{GpuDeltaCatalog, GpuDeltaLog, GpuDeltaV1Write, GpuWriteIntoDelta}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.shims.DeltaLogShim

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec}
import org.apache.spark.sql.execution.datasources.v2.rapids.{GpuAtomicCreateTableAsSelectExec, GpuAtomicReplaceTableAsSelectExec}
import org.apache.spark.sql.sources.InsertableRelation

object DeltaSpark350DB143Provider extends DatabricksDeltaProviderBase {

  // Override to enable Delta 3.3.0 functionality for Databricks 14.3
  // This shim is based on Delta 3.3.0, so we enable Delete, Update, and Merge operations
  override def getRunnableCommandRules: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
    Seq(
      GpuOverrides.runnableCmd[DeleteCommand](
        "Delete rows from a Delta Lake table",
        (a, conf, p, r) => new DeleteCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[DeleteCommandEdge](
        "Delete rows from a Delta Lake table",
        (a, conf, p, r) => new DeleteCommandEdgeMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[MergeIntoCommand](
        "Merge of a source query/table into a Delta table",
        (a, conf, p, r) => new MergeIntoCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[MergeIntoCommandEdge](
        "Merge of a source query/table into a Delta table",
        (a, conf, p, r) => new MergeIntoCommandEdgeMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[UpdateCommand](
        "Update rows in a Delta Lake table",
        (a, conf, p, r) => new UpdateCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[UpdateCommandEdge](
        "Update rows in a Delta Lake table",
        (a, conf, p, r) => new UpdateCommandEdgeMeta(a, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[RunnableCommand]), r)).toMap
  }

  override protected def toGpuWrite(
     writeConfig: DeltaWriteV1Config,
     rapidsConf: RapidsConf): V1Write = new GpuDeltaV1Write {
    override def toInsertableRelation(): InsertableRelation = {
      new InsertableRelation {
        override def insert(data: DataFrame, overwrite: Boolean): Unit = {
          val session = data.sparkSession
          val deltaLog = writeConfig.deltaLog

          // TODO: Get the config from WriteIntoDelta's txn.
          val cpuWrite = WriteIntoDeltaEdge(
            deltaLog,
            if (writeConfig.forceOverwrite) SaveMode.Overwrite else SaveMode.Append,
            new DeltaOptions(writeConfig.options.toMap, session.sessionState.conf),
            Nil,
            DeltaLogShim.getMetadata(deltaLog).configuration,
            data)
          val gpuWrite = GpuWriteIntoDelta(new GpuDeltaLog(deltaLog, rapidsConf), cpuWrite)
          gpuWrite.run(session)

          // TODO: Push this to Apache Spark
          // Re-cache all cached plans(including this relation itself, if it's cached) that refer
          // to this data source relation. This is the behavior for InsertInto
          session.sharedState.cacheManager.recacheByPlan(
            session, LogicalRelation(deltaLog.createRelation()))
        }
      }
    }
  }

  override def convertToGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): GpuExec = {
    GpuAtomicCreateTableAsSelectExec(
      cpuExec.output,
      new GpuDeltaCatalog(cpuExec.catalog, meta.conf),
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.query,
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.ifNotExists)
  }

  override def convertToGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): GpuExec = {
    GpuAtomicReplaceTableAsSelectExec(
      cpuExec.output,
      new GpuDeltaCatalog(cpuExec.catalog, meta.conf),
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.query,
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.orCreate,
      cpuExec.invalidateCache)
  }
}
