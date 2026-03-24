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
package com.nvidia.spark.rapids.tests.datasourcev2

import java.util.concurrent.atomic.AtomicLong

import com.nvidia.spark.rapids.shims.GpuDataSourceRDD

import org.apache.spark.SparkContext
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Shared state used to verify that GpuDataSourceRDD propagates custom metrics between
 * grouped partition readers via initMetricsValues() (SPARK-55302).
 *
 * Python tests call reset() before invoking the RDD and check getReceivedNextRowId
 * afterward to confirm the value was propagated correctly.
 */
object KeyGroupedMetricsTestState {
  private val _receivedNextRowId = new AtomicLong(-1L)

  def reset(): Unit = _receivedNextRowId.set(-1L)
  def setReceivedNextRowId(v: Long): Unit = _receivedNextRowId.set(v)
  def getReceivedNextRowId: Long = _receivedNextRowId.get()
}

class MetricsTestPartition(val index: Int) extends InputPartition with Serializable

class MetricsTestReaderFactory extends PartitionReaderFactory with Serializable {
  override def createReader(
      partition: InputPartition): PartitionReader[org.apache.spark.sql.catalyst.InternalRow] =
    throw new UnsupportedOperationException("columnar reads only")

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    new MetricsTestReader(partition.asInstanceOf[MetricsTestPartition].index)

  override def supportColumnarReads(partition: InputPartition): Boolean = true
}

/**
 * A minimal PartitionReader[ColumnarBatch] that tests initMetricsValues propagation.
 *
 * Partition 0 reports currentMetricsValues() = [nextRowId=3].
 * When GpuDataSourceRDD (spark358+ shim) processes partition 1, it calls:
 *   batchReader.initMetricsValues(prev.currentMetricsValues())
 * which dispatches to our initMetricsValues() below via JVM virtual dispatch,
 * storing the received nextRowId in KeyGroupedMetricsTestState.
 *
 * Note: initMetricsValues() is intentionally defined WITHOUT 'override'.
 * The method does not exist in PartitionReader for Spark < 3.5.8, so using
 * 'override' would break compilation on older builds. JVM virtual dispatch
 * still calls this method on Spark 3.5.8+ because the method descriptor matches.
 */
class MetricsTestReader(index: Int) extends PartitionReader[ColumnarBatch] {
  private var done = false

  override def next(): Boolean =
    if (done) false else { done = true; true }

  override def get(): ColumnarBatch = new ColumnarBatch(new Array[ColumnVector](0), 0)

  override def close(): Unit = {}

  // currentMetricsValues() is a default method available since Spark 3.2 (SPARK-32659).
  // Partition 0 reports nextRowId=3; all others return empty.
  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    if (index == 0) {
      Array(new CustomTaskMetric {
        override def name(): String = "nextRowId"
        override def value(): Long = 3L
      })
    } else {
      Array.empty
    }
  }

  // initMetricsValues(CustomTaskMetric[]) is a default method added by SPARK-55302
  // (Spark 3.5.8+). We do NOT use 'override' to stay compilable on older Spark versions.
  // GpuDataSourceRDD (spark358+ shim) calls this via invokeinterface, and JVM dispatch
  // finds this method on the concrete MetricsTestReader class.
  def initMetricsValues(metrics: Array[CustomTaskMetric]): Unit = {
    metrics.foreach { m =>
      if (m.name() == "nextRowId") {
        KeyGroupedMetricsTestState.setReceivedNextRowId(m.value())
      }
    }
  }
}

/**
 * Helper invoked directly from Python integration tests via JVM bridge.
 *
 * Creates a GpuDataSourceRDD with two partitions grouped into one split
 * (simulating KeyGroupedPartitioning), runs count() to trigger execution,
 * and returns the nextRowId received via initMetricsValues().
 *
 * Expected return values:
 *   3  — GpuDataSourceRDD called initMetricsValues() correctly (fix present)
 *  -1  — initMetricsValues() was never called (fix absent or running on older Spark)
 */
object KeyGroupedMetricsTestHelper {
  def runAndGetReceivedNextRowId(sc: SparkContext): Long = {
    KeyGroupedMetricsTestState.reset()

    val factory = new MetricsTestReaderFactory()
    val part0 = new MetricsTestPartition(0)
    val part1 = new MetricsTestPartition(1)

    // Group both partitions into a single output split.
    // GpuDataSourceRDD.compute() iterates Seq(part0, part1) in one task,
    // calling initMetricsValues() on part1's reader with part0's metrics.
    val rdd = new GpuDataSourceRDD(sc, Seq(Seq(part0, part1)), factory)
    rdd.count()

    KeyGroupedMetricsTestState.getReceivedNextRowId
  }
}
