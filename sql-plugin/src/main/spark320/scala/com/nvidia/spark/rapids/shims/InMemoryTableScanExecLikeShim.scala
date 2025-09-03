/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.columnar.InMemoryRelation

/**
 * Shim trait that provides the InMemoryTableScanExecLike interface
 * for Spark versions before 4.0.0 where InMemoryTableScanLike doesn't exist.
 * 
 * For Spark 4.0+, this is overridden by the version-specific shim that extends
 * the actual InMemoryTableScanLike trait.
 */
trait InMemoryTableScanExecLikeBaseShim extends LeafExecNode {
  // These methods must be provided by the concrete implementation
  def attributes: Seq[Attribute]
  def predicates: Seq[Expression]
  def relation: InMemoryRelation

  /**
   * Returns true if the cached relation has been materialized (cached data is loaded).
   */
  def isMaterialized: Boolean = relation.cacheBuilder.isCachedColumnBuffersLoaded

  /**
   * Returns the base RDD of cached batches without any filtering or transformation.
   * This is used by AQE to access the raw cached data.
   */
  def baseCacheRDD(): RDD[CachedBatch] = {
    relation.cacheBuilder.cachedColumnBuffers
  }

  /**
   * Returns the runtime statistics after materialization.
   */
  def runtimeStatistics: Statistics = {
    // Return basic statistics after materialization
    Statistics(
      sizeInBytes = relation.stats.sizeInBytes,
      rowCount = relation.stats.rowCount,
      attributeStats = relation.stats.attributeStats
    )
  }
}
