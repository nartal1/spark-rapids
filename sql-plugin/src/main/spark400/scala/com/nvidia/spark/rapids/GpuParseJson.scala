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

import java.util.Optional

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.VariantParser

import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, Literal,
  NamedExpression}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{BooleanType, DataType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** ParseJson is replaced by StaticInvoke during Spark analysis. */
case class GpuParseJsonStaticInvokeMeta(
    expr: StaticInvoke,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends StaticInvokeMeta(expr, conf, p, rule) {

  private def isParseJson: Boolean =
    expr.staticObject.getName.stripSuffix("$") ==
      "org.apache.spark.sql.catalyst.expressions.variant.VariantExpressionEvalUtils" &&
      expr.functionName == "parseJson"

  override val childExprs: Seq[BaseExprMeta[_]] = if (isParseJson ||
      (expr.staticObject.getName == "org.apache.spark.sql.catalyst.expressions.StringDecode" &&
        expr.functionName == "decode")) {
    expr.arguments.take(1).map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  } else {
    expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  }

  override def tagExprForGpu(): Unit = {
    if (!isParseJson) {
      super.tagExprForGpu()
    } else {
      if (!GpuColumnVector.isVariantType(expr.dataType) ||
          !Set(3, 4).contains(expr.arguments.size) ||
          expr.arguments.head.dataType != StringType ||
          !expr.arguments.tail.forall {
            case Literal(_: Boolean, BooleanType) => true
            case _ => false
          }) {
        willNotWorkOnGpu("unexpected Spark parseJson signature")
      } else {
        // Spark may exceed its size limit before last-wins duplicate compaction, while the
        // prototype checks the compacted size. Keep duplicate-allowed parsing on CPU.
        if (expr.arguments(1) == Literal(true, BooleanType)) {
          willNotWorkOnGpu("GPU parse_json does not yet support allowing duplicate keys")
        }
        // The JNI preflight excludes non-ASCII and Unicode escapes. Such accepted inputs
        // cannot produce invalid Unicode, so Spark 4.2's Unicode-validation option is safe
        // for the native path. CPU replay retains the original fourth argument for others.
      }
      if (!conf.isCpuBridgeEnabled) {
        willNotWorkOnGpu("parse_json requires the CPU bridge for unsupported runtime inputs")
      }
    }
  }

  override def convertToGpuImpl(): GpuExpression = {
    if (!isParseJson) {
      super.convertToGpuImpl()
    } else {
      val input = expr.arguments.head
      val fallback = expr.copy(arguments =
        Seq(BoundReference(0, input.dataType, input.nullable)) ++ expr.arguments.tail)
      GpuParseJson(childExprs.head.convertToGpu().asInstanceOf[Expression],
        expr.dataType, expr.nullable, fallback)
    }
  }
}

case class GpuParseJson(
    child: Expression,
    override val dataType: DataType,
    override val nullable: Boolean,
    cpuFallback: StaticInvoke)
  extends GpuUnaryExpression with Retryable with GpuMetricsInjectable {

  // Strict parsing can throw. IF/CASE/COALESCE must evaluate only selected rows.
  override def hasSideEffects: Boolean = true

  private var bridgeMetrics: Map[String, GpuMetric] = Map.empty

  override def injectMetrics(metrics: Map[String, GpuMetric]): Unit = {
    bridgeMetrics = metrics
  }

  @transient private lazy val fallbackBridge = {
    val input = GpuBoundReference(0, child.dataType, child.nullable)(
      NamedExpression.newExprId, "_json")
    val bridge = GpuCpuBridgeExpression(Seq(input), cpuFallback, dataType, nullable)
    bridge.injectMetrics(bridgeMetrics)
    bridge
  }

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    val strings = input.getBase
    if (VariantParser.requiresLegacySparkFallback(strings)) {
      evaluateOnCpu(input)
    } else {
      val nativeResult = withResource(VariantParser.parseJsonToVariant(strings, false)) { parsed =>
        if (!allRowsHandled(parsed.getStatus)) {
          // Replay BOTH strict and tolerant parsing: GPU rejection is not proof of Spark
          // rejection (e.g. trailing root content). Preserve errors and valid unsupported
          // inputs without inventing error translation or mapping UNSUPPORTED to SQL null.
          None
        } else {
          val variant = parsed.getVariant
          withResource(variant.getChildColumnView(0)) { metadata =>
            withResource(variant.getChildColumnView(1)) { value =>
              withResource(new ColumnView(DType.STRUCT, variant.getRowCount,
                Optional.of[java.lang.Long](variant.getNullCount), variant.getValid,
                null.asInstanceOf[ai.rapids.cudf.BaseDeviceMemoryBuffer],
                Array[ColumnView](value, metadata))) { sparkVariant =>
                Some(sparkVariant.copyToColumnVector())
              }
            }
          }
        }
      }
      // Release parser scratch/output before allocating CPU bridge results.
      nativeResult.getOrElse(evaluateOnCpu(input))
    }
  }

  private def allRowsHandled(status: ColumnView): Boolean = {
    withResource(Scalar.fromUnsignedByte(VariantParser.SUCCESS.toByte)) { success =>
      withResource(Scalar.fromUnsignedByte(VariantParser.ROW_NULL.toByte)) { rowNull =>
        withResource(status.equalTo(success)) { isSuccess =>
          withResource(status.equalTo(rowNull)) { isNull =>
            withResource(isSuccess.or(isNull))(BoolUtils.isAllValidTrue)
          }
        }
      }
    }
  }

  private def evaluateOnCpu(input: GpuColumnVector): ColumnVector = {
    val columns = Array[org.apache.spark.sql.vectorized.ColumnVector](input.incRefCount())
    withResource(new ColumnarBatch(columns, input.getBase.getRowCount.toInt)) { batch =>
      withResource(fallbackBridge.columnarEval(batch))(_.getBase.incRefCount())
    }
  }

  override def checkpoint(): Unit = ()
  override def restore(): Unit = ()
}
