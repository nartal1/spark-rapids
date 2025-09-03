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
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanLike}

/**
 * Shim trait that provides the same interface as InMemoryTableScanExecLike
 * from Apache Spark 4.0.0. This allows RAPIDS to extend InMemoryTableScan functionality
 * even if the trait is not yet available in the current Spark build.
 * 
 * For Spark 4.0+, this extends the actual InMemoryTableScanLike trait and provides
 * default implementations that delegate to the concrete class methods.
 */
trait InMemoryTableScanExecLikeShim extends InMemoryTableScanLike with LeafExecNode {
  // These methods must be provided by the concrete implementation
  def attributes: Seq[Attribute]
  def predicates: Seq[Expression]  
  def relation: InMemoryRelation

  // Default implementations for InMemoryTableScanLike methods
  override def isMaterialized: Boolean = relation.cacheBuilder.isCachedColumnBuffersLoaded

  override def baseCacheRDD(): RDD[CachedBatch] = {
    relation.cacheBuilder.cachedColumnBuffers
  }

  override def runtimeStatistics: Statistics = {
    // Return basic statistics after materialization
    Statistics(
      sizeInBytes = relation.stats.sizeInBytes,
      rowCount = relation.stats.rowCount,
      attributeStats = relation.stats.attributeStats
    )
  }
}
