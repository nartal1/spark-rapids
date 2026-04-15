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

/*** spark-rapids-shim-json-lines
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.rapids.shims

import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._

object FilePartitionShims extends SplitFiles {
  def getPartitions(selectedPartitions: Array[PartitionDirectory]): Array[PartitionedFile] = {
    selectedPartitions.flatMap { p =>
      p.files.map { f =>
        PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values, 0, f.getLen)
      }
    }
  }

  def getFiles(p: FilePartition): Array[PartitionedFile] = p.filesWithAbsolutePaths

  def copyWithFiles(p: FilePartition, newFiles: Array[PartitionedFile]): FilePartition = {
    p.copy(innerFiles = newFiles)
  }

  /**
   * On Databricks 17.3, Unity Catalog managed tables store only bare filenames in
   * FilePartition.innerFiles, relying on pathPrefix to resolve them to absolute paths.
   * When FilePartition objects are recreated by FilePartition.getFilePartitions (e.g.,
   * in the non-bucketed read path), pathPrefix is lost. This method restores it from
   * the HadoopFsRelation's file index root paths so that filesWithAbsolutePaths can
   * resolve relative file paths correctly.
   */
  def withPathPrefixIfNeeded(
      partitions: Seq[FilePartition],
      relation: HadoopFsRelation): Seq[FilePartition] = {
    // Only set pathPrefix if it's missing and the file index has a single root path
    val rootPaths = relation.location.rootPaths
    if (rootPaths.size == 1) {
      val prefix = rootPaths.head.toString
      partitions.map { p =>
        if (p.pathPrefix.isEmpty) {
          p.copy(pathPrefix = Some(prefix))
        } else {
          p
        }
      }
    } else {
      partitions
    }
  }
}
