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

package com.nvidia.spark.rapids.delta.shims

import com.databricks.sql.transaction.tahoe.commands.{MergeIntoCommand, MergeIntoCommandEdge}
import com.databricks.sql.transaction.tahoe.rapids.{GpuDeltaLog, GpuMergeIntoCommand}
import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta.{MergeIntoCommandEdgeMeta, MergeIntoCommandMeta}

import org.apache.spark.sql.execution.command.RunnableCommand

object MergeIntoCommandMetaShim {
  def tagForGpu(meta: MergeIntoCommandMeta, mergeCmd: MergeIntoCommand): Unit = {
    // GPU MERGE not yet enabled for DB-17.3 (will be enabled in a follow-up issue)
    meta.willNotWorkOnGpu("Delta Lake MERGE INTO is not yet supported on GPU for DB-17.3")
  }

  def tagForGpu(meta: MergeIntoCommandEdgeMeta, mergeCmd: MergeIntoCommandEdge): Unit = {
    // GPU MERGE not yet enabled for DB-17.3 (will be enabled in a follow-up issue)
    meta.willNotWorkOnGpu("Delta Lake MERGE INTO is not yet supported on GPU for DB-17.3")
  }

  def convertToGpu(mergeCmd: MergeIntoCommand, conf: RapidsConf): RunnableCommand = {
    GpuMergeIntoCommand(
      mergeCmd.source,
      mergeCmd.target,
      new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
      mergeCmd.condition,
      mergeCmd.matchedClauses,
      mergeCmd.notMatchedClauses,
      mergeCmd.notMatchedBySourceClauses,
      mergeCmd.migratedSchema)(conf)
  }

  def convertToGpu(mergeCmd: MergeIntoCommandEdge, conf: RapidsConf): RunnableCommand = {
    GpuMergeIntoCommand(
      mergeCmd.source,
      mergeCmd.target,
      new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
      mergeCmd.condition,
      mergeCmd.matchedClauses,
      mergeCmd.notMatchedClauses,
      mergeCmd.notMatchedBySourceClauses,
      mergeCmd.migratedSchema)(conf)
  }
}
