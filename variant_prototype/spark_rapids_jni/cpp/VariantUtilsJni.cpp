/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

// SPARK-RAPIDS PROTOTYPE — C++ JNI wrapper for cuDF Variant extraction.
//
// TARGET LOCATION when integrating:
//   spark-rapids-jni/src/main/cpp/src/VariantUtilsJni.cpp
//
// Also add to spark-rapids-jni/src/main/cpp/CMakeLists.txt under a new
// `VARIANT_EXTRACT` option gated by a minimum libcudf version. Example:
//
//   option(VARIANT_EXTRACT "Enable GPU Variant extraction" OFF)
//   if(VARIANT_EXTRACT)
//     list(APPEND CUDF_SOURCES src/VariantUtilsJni.cpp)
//     target_compile_definitions(spark_rapids_jni PRIVATE VARIANT_EXTRACT_ENABLED)
//   endif()
//
// Depends on cuDF branch https://github.com/vuule/cudf/tree/variant-extraction-gpu
// which provides cudf/io/variant.hpp with the three entry points:
//   - cudf::io::parquet::get_variant_field
//   - cudf::io::parquet::cast_variant
//   - cudf::io::parquet::extract_variant_field
//
// This file follows the existing spark-rapids-jni idioms for JNI:
//   * CATCH_STD wraps each entry point to translate C++ exceptions to Java
//   * JNI_NULL_CHECK guards handle/string arguments
//   * native_jstring helper RAII-wraps the JNI string lifecycle
// If the current file names or macro names differ in spark-rapids-jni HEAD,
// check JSONUtilsJni.cpp and align.

#include "cudf_jni_apis.hpp"
#include "jni_utils.hpp"

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/default_stream.hpp>
#include <rmm/mr/device/per_device_resource.hpp>

#ifdef VARIANT_EXTRACT_ENABLED
#include <cudf/io/variant.hpp>
#endif

extern "C" {

// ---------------------------------------------------------------------------
// Helper: wrap two list<uint8> handles into a struct<list<uint8>, list<uint8>>
// view that cuDF's Variant API expects as input.
// ---------------------------------------------------------------------------

namespace {

#ifdef VARIANT_EXTRACT_ENABLED

cudf::column_view make_variant_struct_view(cudf::column_view const& metadata,
                                           cudf::column_view const& value)
{
  CUDF_EXPECTS(metadata.size() == value.size(),
               "metadata and value columns must have matching row counts");
  static std::vector<cudf::column_view> const empty_children{};
  std::vector<cudf::column_view> children{metadata, value};
  return cudf::column_view{cudf::data_type{cudf::type_id::STRUCT},
                           metadata.size(),
                           /*data=*/nullptr,
                           /*null_mask=*/nullptr,
                           /*null_count=*/0,
                           /*offset=*/0,
                           std::move(children)};
}

#endif  // VARIANT_EXTRACT_ENABLED

}  // namespace

// ---------------------------------------------------------------------------
// Java_com_nvidia_spark_rapids_jni_VariantUtils_getVariantFieldNative
// ---------------------------------------------------------------------------

JNIEXPORT jlong JNICALL
Java_com_nvidia_spark_rapids_jni_VariantUtils_getVariantFieldNative(
  JNIEnv* env, jclass, jlong metadata_handle, jlong value_handle, jstring j_field_name)
{
#ifndef VARIANT_EXTRACT_ENABLED
  cudf::jni::throw_java_exception(
    env, cudf::jni::RUNTIME_EXCEPTION_CLASS,
    "VariantUtils was built with VARIANT_EXTRACT disabled");
  return 0;
#else
  JNI_NULL_CHECK(env, metadata_handle, "metadata handle is null", 0);
  JNI_NULL_CHECK(env, value_handle, "value handle is null", 0);
  JNI_NULL_CHECK(env, j_field_name, "fieldName is null", 0);

  try {
    cudf::jni::auto_set_device(env);

    auto const meta = *reinterpret_cast<cudf::column_view const*>(metadata_handle);
    auto const val  = *reinterpret_cast<cudf::column_view const*>(value_handle);
    auto const variant_view = make_variant_struct_view(meta, val);

    cudf::jni::native_jstring field_name(env, j_field_name);

    auto result = cudf::io::parquet::get_variant_field(
      variant_view,
      std::string{field_name.get()},
      cudf::get_default_stream(),
      rmm::mr::get_current_device_resource_ref());

    return cudf::jni::release_as_jlong(result);
  }
  CATCH_STD(env, 0);
#endif
}

// ---------------------------------------------------------------------------
// Java_com_nvidia_spark_rapids_jni_VariantUtils_castVariantNative
// ---------------------------------------------------------------------------

JNIEXPORT jlong JNICALL
Java_com_nvidia_spark_rapids_jni_VariantUtils_castVariantNative(
  JNIEnv* env, jclass, jlong variant_struct_handle, jint cudf_type_id)
{
#ifndef VARIANT_EXTRACT_ENABLED
  cudf::jni::throw_java_exception(
    env, cudf::jni::RUNTIME_EXCEPTION_CLASS,
    "VariantUtils was built with VARIANT_EXTRACT disabled");
  return 0;
#else
  JNI_NULL_CHECK(env, variant_struct_handle, "variantStruct handle is null", 0);

  try {
    cudf::jni::auto_set_device(env);

    auto const struct_view = *reinterpret_cast<cudf::column_view const*>(variant_struct_handle);

    auto result = cudf::io::parquet::cast_variant(
      struct_view,
      cudf::data_type{static_cast<cudf::type_id>(cudf_type_id)},
      cudf::get_default_stream(),
      rmm::mr::get_current_device_resource_ref());

    return cudf::jni::release_as_jlong(result);
  }
  CATCH_STD(env, 0);
#endif
}

// ---------------------------------------------------------------------------
// Java_com_nvidia_spark_rapids_jni_VariantUtils_extractVariantFieldNative
// ---------------------------------------------------------------------------

JNIEXPORT jlong JNICALL
Java_com_nvidia_spark_rapids_jni_VariantUtils_extractVariantFieldNative(
  JNIEnv* env,
  jclass,
  jlong metadata_handle,
  jlong value_handle,
  jstring j_field_name,
  jint cudf_type_id)
{
#ifndef VARIANT_EXTRACT_ENABLED
  cudf::jni::throw_java_exception(
    env, cudf::jni::RUNTIME_EXCEPTION_CLASS,
    "VariantUtils was built with VARIANT_EXTRACT disabled");
  return 0;
#else
  JNI_NULL_CHECK(env, metadata_handle, "metadata handle is null", 0);
  JNI_NULL_CHECK(env, value_handle, "value handle is null", 0);
  JNI_NULL_CHECK(env, j_field_name, "fieldName is null", 0);

  try {
    cudf::jni::auto_set_device(env);

    auto const meta = *reinterpret_cast<cudf::column_view const*>(metadata_handle);
    auto const val  = *reinterpret_cast<cudf::column_view const*>(value_handle);
    auto const variant_view = make_variant_struct_view(meta, val);

    cudf::jni::native_jstring field_name(env, j_field_name);

    auto result = cudf::io::parquet::extract_variant_field(
      variant_view,
      std::string{field_name.get()},
      cudf::data_type{static_cast<cudf::type_id>(cudf_type_id)},
      cudf::get_default_stream(),
      rmm::mr::get_current_device_resource_ref());

    return cudf::jni::release_as_jlong(result);
  }
  CATCH_STD(env, 0);
#endif
}

// ---------------------------------------------------------------------------
// Java_com_nvidia_spark_rapids_jni_VariantUtils_isAvailableNative
// ---------------------------------------------------------------------------

JNIEXPORT jboolean JNICALL
Java_com_nvidia_spark_rapids_jni_VariantUtils_isAvailableNative(JNIEnv* /*env*/, jclass)
{
#ifdef VARIANT_EXTRACT_ENABLED
  return JNI_TRUE;
#else
  return JNI_FALSE;
#endif
}

}  // extern "C"
