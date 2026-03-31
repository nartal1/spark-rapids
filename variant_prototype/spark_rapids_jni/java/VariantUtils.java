/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

// SPARK-RAPIDS PROTOTYPE — spark-rapids-jni bridge for the cuDF Variant
// extraction branch. Mirror of the JSONUtils pattern.
//
// TARGET LOCATION when integrating:
//   spark-rapids-jni/src/main/java/com/nvidia/spark/rapids/jni/VariantUtils.java
//
// Depends on:
//   * cuDF branch: https://github.com/vuule/cudf/tree/variant-extraction-gpu
//     (for the three native entry points)
//   * Companion JNI C++: VariantUtilsJni.cpp in this prototype dir.

package com.nvidia.spark.rapids.jni;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.NativeDepsLoader;

/**
 * JNI bridge to cuDF's Parquet Variant field extraction API. Exposes three
 * operations that mirror the C++ header {@code cudf/io/variant.hpp}:
 *
 * <ul>
 *   <li>{@link #getVariantField} — extract raw Variant-encoded bytes of a
 *       top-level field by name, preserving the sub-variant as a struct of
 *       two list&lt;uint8&gt; columns (metadata + value).</li>
 *   <li>{@link #castVariant} — decode a Variant struct column's value blobs
 *       into a typed cuDF column. Target types supported by the underlying
 *       branch today: INT32 and STRING.</li>
 *   <li>{@link #extractVariantField} — convenience: get + cast in one call.</li>
 * </ul>
 *
 * <p><b>Known semantic gap (see libcudf_variant_feedback.md §3.6):</b> the
 * underlying {@code cast_variant} is decode-if-exact-match, not cast. Spark's
 * {@code variant_get}/{@code try_variant_get} performs a general cast. The
 * caller (GpuVariantGet) must either:
 * <ol>
 *   <li>Restrict usage to rows whose stored type exactly matches the target
 *       (most will produce NULL otherwise), or</li>
 *   <li>Call {@link #getVariantField} and perform host-side casting, or</li>
 *   <li>Wait for the cast-semantics fix in libcudf.</li>
 * </ol>
 */
public final class VariantUtils {

  static {
    NativeDepsLoader.loadNativeDeps();
  }

  private VariantUtils() {}

  /**
   * Extract the raw Variant-encoded bytes of a top-level object field by name.
   *
   * <p>Input: a Variant column materialized as {@code struct<list<uint8>
   * metadata, list<uint8> value>}. Here the caller supplies the two children
   * as independent column views; the JNI side builds the struct view.
   *
   * <p>Returns a new Variant column (same shape as input) where child 0 is a
   * deep copy of the input metadata and child 1 contains only the extracted
   * field's value bytes. Produces null when the struct row is null, the key
   * is absent, or the root value is not an object.
   *
   * @param metadata  list&lt;uint8&gt; column of per-row metadata blobs
   * @param value     list&lt;uint8&gt; column of per-row Variant value blobs
   * @param fieldName the object field name to extract (case-sensitive)
   * @return new Variant struct column owning its own device memory
   */
  public static ColumnVector getVariantField(
      ColumnView metadata, ColumnView value, String fieldName) {
    assert metadata != null : "metadata ColumnView must not be null";
    assert value != null : "value ColumnView must not be null";
    assert fieldName != null : "fieldName must not be null";
    long resultHandle =
        getVariantFieldNative(metadata.getNativeView(), value.getNativeView(), fieldName);
    return new ColumnVector(resultHandle);
  }

  /**
   * Decode a Variant struct column's value blobs into a typed cuDF column.
   *
   * @param variantStruct a struct&lt;list&lt;uint8&gt;, list&lt;uint8&gt;&gt;
   *                      column (metadata + value); typically the output of
   *                      {@link #getVariantField}.
   * @param targetType    target cuDF DType. Branch supports INT32 and STRING only.
   * @return typed column; rows whose stored type does not exactly match the
   *         target (or whose struct row is null) are NULL in the output.
   */
  public static ColumnVector castVariant(ColumnView variantStruct, DType targetType) {
    assert variantStruct != null : "variantStruct must not be null";
    assert targetType != null : "targetType must not be null";
    long resultHandle =
        castVariantNative(variantStruct.getNativeView(), targetType.getTypeId().getNativeId());
    return new ColumnVector(resultHandle);
  }

  /**
   * Convenience wrapper equivalent to
   * {@code castVariant(getVariantField(metadata, value, fieldName), targetType)}.
   *
   * <p>Same null-propagation semantics as the two-call form.
   */
  public static ColumnVector extractVariantField(
      ColumnView metadata, ColumnView value, String fieldName, DType targetType) {
    assert metadata != null : "metadata ColumnView must not be null";
    assert value != null : "value ColumnView must not be null";
    assert fieldName != null : "fieldName must not be null";
    assert targetType != null : "targetType must not be null";
    long resultHandle =
        extractVariantFieldNative(
            metadata.getNativeView(),
            value.getNativeView(),
            fieldName,
            targetType.getTypeId().getNativeId());
    return new ColumnVector(resultHandle);
  }

  /**
   * Reports whether the native library was built with Variant support enabled
   * (i.e. the {@code VARIANT_EXTRACT} CMake option). Callers should check this
   * at planning time and fall back to CPU when false, so a runtime built
   * against an older libcudf still works.
   */
  public static boolean isAvailable() {
    try {
      return isAvailableNative();
    } catch (UnsatisfiedLinkError e) {
      return false;
    }
  }

  // -------------------------------------------------------------------------
  // Native entry points (implemented in VariantUtilsJni.cpp)
  // -------------------------------------------------------------------------

  private static native long getVariantFieldNative(
      long metadataHandle, long valueHandle, String fieldName);

  private static native long castVariantNative(long variantStructHandle, int cudfTypeId);

  private static native long extractVariantFieldNative(
      long metadataHandle, long valueHandle, String fieldName, int cudfTypeId);

  private static native boolean isAvailableNative();
}
