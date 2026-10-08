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
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, VariantType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuParseJsonRetrySuite extends RmmSparkRetrySuiteBase {
  private val documents = (0 until 64).map { i =>
    if (i % 7 == 0) null else s"""{"id":$i,"items":[true,null,"value"]}"""
  }

  override def afterEach(): Unit = {
    RmmSpark.getAndResetNumRetryThrow(1)
    RmmSpark.getAndResetNumSplitRetryThrow(1)
    super.afterEach()
  }

  private def project(inject: => Unit): Seq[Option[(Seq[Byte], Seq[Byte])]] = {
    val input = new ColumnarBatch(Array(GpuColumnVector.from(
      ColumnVector.fromStrings(documents: _*), StringType)), documents.length)
    val spillable = SpillableColumnarBatch(input, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    val child = GpuBoundReference(0, StringType, true)(NamedExpression.newExprId, "json")
    // A null CPU fallback makes accidental CPU replay fail this native-success retry test.
    val expressions = Seq(GpuAlias(GpuParseJson(child, VariantType, true, null), "v")())
    // Inject only after creating the input: the OOM must occur in the parser projection.
    inject
    val sqlConf = new SQLConf()
    sqlConf.setConfString(RapidsConf.PROJECT_SPLIT_RETRY_ENABLED.key, "true")
    SQLConf.withExistingConf(sqlConf) {
      withResource(GpuProjectExec.projectAndCloseWithRetrySingleBatch(spillable, expressions)) {
        result =>
          withResource(result.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
            (0 until result.numRows()).map { row =>
              if (host.isNullAt(row)) {
                None
              } else {
                val variant = host.getVariant(row)
                Some((variant.getValue.toSeq, variant.getMetadata.toSeq))
              }
            }
          }
      }
    }
  }

  test("native parser projection preserves bytes and nulls after retry OOM") {
    val expected = project(())
    val actual = project {
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
    }
    assert(actual == expected)
    assert(RmmSpark.getAndResetNumRetryThrow(1) > 0, "retry injection was not exercised")
  }

  test("native parser projection preserves row order and bytes after split-and-retry OOM") {
    val expected = project(())
    val actual = project {
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
    }
    assert(actual == expected)
    assert(RmmSpark.getAndResetNumSplitRetryThrow(1) > 0, "split injection was not exercised")
  }
}
