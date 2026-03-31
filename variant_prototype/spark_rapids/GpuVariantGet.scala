/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

// SPARK-RAPIDS PROTOTYPE — GPU Variant expression mirroring Spark's
// `variant_get` / `try_variant_get`, backed by VariantUtils JNI.
//
// TARGET LOCATION when integrating:
//   sql-plugin/src/main/spark400/scala/com/nvidia/spark/rapids/GpuVariantGet.scala
//
// Shim scope: Spark 4.0+ only (VariantType / VariantExpression don't exist
// pre-4.0). Add the corresponding `spark-rapids-shim-json-lines` header and
// enumerate the applicable Spark versions before merging.
//
// This expression is narrow on purpose:
//   - Single-segment object paths only ($.field). Chained ($.a.b.c) is stubbed.
//   - Target types INT32 and STRING (the two cuDF supports today).
//   - Maps BOTH `variant_get` and `try_variant_get` to try-semantics for now,
//     because cuDF's null-on-mismatch behavior matches try_variant_get exactly
//     (see libcudf_variant_feedback.md §3.6). Strict `variant_get` requires
//     host-side validation that is NOT implemented here.
//
// Known limitation: even for the two supported target types, cuDF today is
// decode-if-exact-match, not cast. Rows whose stored type doesn't literally
// match the target will produce NULL on GPU where Spark would return a cast
// value. This is documented in the feedback doc and should be communicated
// via a plugin metric or log warning when this expression is planned.

// "spark-rapids-shim-json-lines" marker — update versions when merging:
// {"spark": "400"}
// {"spark": "401"}
// {"spark": "402"}
// {"spark": "411"}
// end "spark-rapids-shim-json-lines"
package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnVector, DType}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.VariantUtils
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.variant.{VariantGet, VariantPathParser}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, VariantType}
import org.apache.spark.unsafe.types.UTF8String

// ============================================================================
// Meta class — registered in GpuOverrides.scala
// ============================================================================

class GpuVariantGetMeta(
    expr: VariantGet,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[VariantGet](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    // 1. Target type must be one cuDF supports today.
    if (!GpuVariantGet.isSupportedTargetType(expr.dataType)) {
      willNotWorkOnGpu(s"target type ${expr.dataType.simpleString} not supported; " +
        "cuDF Variant branch supports INT32 and STRING only")
    }

    // 2. Path must be a literal string (so we can parse it at plan time).
    expr.path match {
      case Literal(str: UTF8String, _) =>
        GpuVariantGet.parseSingleSegmentPath(str.toString) match {
          case Some(_) =>  // OK
          case None =>
            willNotWorkOnGpu("only single-segment object paths ($.field) " +
              "are supported on GPU; array indexing and multi-level paths fall back to CPU")
        }
      case _ =>
        willNotWorkOnGpu("variant path must be a constant literal")
    }

    // 3. Strict failOnError requires host-side type validation that is not
    //    implemented yet — we only match try_variant_get's null-on-mismatch
    //    semantic. Strict variant_get falls back to CPU.
    if (expr.failOnError) {
      willNotWorkOnGpu("strict variant_get (failOnError=true) not yet supported on GPU; " +
        "only try_variant_get semantics are implemented")
    }

    // 4. Runtime check: the spark-rapids-jni build must include Variant support.
    if (!VariantUtils.isAvailable()) {
      willNotWorkOnGpu("spark-rapids-jni was built without VARIANT_EXTRACT enabled")
    }
  }

  override def convertToGpu(child: Expression): GpuExpression = {
    val pathLiteral = expr.path.asInstanceOf[Literal].value.asInstanceOf[UTF8String].toString
    val fieldName = GpuVariantGet.parseSingleSegmentPath(pathLiteral).get
    GpuVariantGet(child, fieldName, expr.dataType)
  }
}

// ============================================================================
// Runtime expression
// ============================================================================

case class GpuVariantGet(
    child: Expression,
    fieldName: String,
    override val dataType: DataType)
  extends GpuUnaryExpression
  with ShimExpression
  with Retryable {

  override def nullable: Boolean = true

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    // Input is a Variant column materialized as struct<list<uint8>, list<uint8>>.
    // Extract the two children and hand them to the JNI helper.
    val variantStruct = input.getBase
    require(variantStruct.getType.getTypeId == DType.DTypeEnum.STRUCT,
      s"expected Variant struct input, got ${variantStruct.getType}")
    require(variantStruct.getNumChildren == 2,
      s"expected 2 children (metadata, value), got ${variantStruct.getNumChildren}")

    withResource(variantStruct.getChildColumnView(0)) { metadata =>
      withResource(variantStruct.getChildColumnView(1)) { value =>
        val cudfType = GpuVariantGet.toCudfTargetType(dataType)
        VariantUtils.extractVariantField(metadata, value, fieldName, cudfType)
      }
    }
  }

  // TODO: for chained paths ($.a.b.c), extend to call getVariantField N-1
  // times, then castVariant at the end. Left as a follow-up.

  override def checkpoint(): Unit = ()
  override def restore(): Unit = ()
}

// ============================================================================
// Companion utilities
// ============================================================================

object GpuVariantGet {

  /** Types that cuDF's cast_variant decodes today. */
  def isSupportedTargetType(dt: DataType): Boolean = dt match {
    case IntegerType | StringType => true
    case _ => false
  }

  /** Convert a Spark DataType to the cuDF DType id the JNI layer expects. */
  def toCudfTargetType(dt: DataType): DType = dt match {
    case IntegerType => DType.INT32
    case StringType  => DType.STRING
    case other =>
      throw new IllegalArgumentException(s"unsupported variant target type: $other")
  }

  /**
   * Parse a JSONPath-style expression and, if it is a single top-level object
   * field reference (e.g. "$.user"), return the field name. Returns None for
   * anything else: multi-level paths, array indexing, wildcards, malformed.
   *
   * This keeps the first GPU slice trivially correct. Expanding to chained
   * paths is a follow-up in `convertToGpu`.
   *
   * TODO: when expanding to chained paths, reuse Spark's VariantPathParser
   * from org.apache.spark.sql.catalyst.expressions.variant to avoid
   * reimplementing JSONPath parsing. Then emit a sequence of field names that
   * the runtime walks with repeated getVariantField calls.
   */
  def parseSingleSegmentPath(path: String): Option[String] = {
    // Accept exactly "$.<ident>" where <ident> is a simple identifier
    // (letters, digits, underscore, no brackets/quotes). Keep it strict —
    // anything unusual defers to CPU.
    val SingleSegment = """^\$\.([A-Za-z_][A-Za-z0-9_]*)$""".r
    path match {
      case SingleSegment(name) => Some(name)
      case _ => None
    }
  }
}

// ============================================================================
// Registration hook (apply in GpuOverrides.scala)
// ============================================================================
//
// Add to the `expressions` list in GpuOverrides.scala, near the JSON family
// (search for `GpuGetJsonObjectMeta`):
//
//   GpuOverrides.expr[VariantGet](
//     "Extracts a sub-variant from a Variant by JSON path",
//     ExprChecks.projectOnly(
//       TypeSig.commonCudfTypes,  // adjust to include only supported output types
//       TypeSig.all,
//       Seq(
//         ParamCheck("v", TypeSig.VARIANT.withPsNote(
//           TypeEnum.VARIANT, "requires spark-rapids-jni with VARIANT_EXTRACT"),
//                    TypeSig.VARIANT),
//         ParamCheck("path", TypeSig.STRING, TypeSig.STRING))),
//     (expr, conf, p, r) => new GpuVariantGetMeta(expr, conf, p, r))
//
// The TypeSig.VARIANT entry also needs TypeEnum.VARIANT added to TypeChecks.scala
// first. See the Phase 0 steps in libcudf_variant_feedback.md and the plan file.
