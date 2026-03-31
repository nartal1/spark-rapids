/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

// SPARK-RAPIDS FEEDBACK TESTS for the Variant extraction branch.
//
// Purpose: demonstrate the cast-semantics gap between cuDF's `cast_variant` /
// `extract_variant_field` and Apache Spark's `variant_get` / `try_variant_get`.
//
// See libcudf_variant_feedback.md §3.6 for the verified analysis.
//
// Expected result when run against the current branch:
//   - The "ControlExactMatch*" tests PASS  (happy path already works).
//   - Every other test in this file FAILS, because cuDF returns NULL where
//     Spark would return a cast value. Each failing test is a concrete
//     reproducer for the cast-semantics design question.
//
// To integrate into the branch:
//   1. Copy this file to cpp/tests/io/variant_cast_semantics_test.cpp.
//   2. Add it to cpp/tests/CMakeLists.txt under the PARQUET_TEST target.
//   3. If build_metadata / build_object_value helpers already exist in
//      variant_extract_test.cpp, move them to a shared header and use them
//      here instead of the hand-rolled bytes below (noted per test).

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/io/variant.hpp>
#include <cudf/types.hpp>
#include <cudf_test/base_fixture.hpp>
#include <cudf_test/column_utilities.hpp>
#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/default_stream.hpp>
#include <cudf_test/iterator_utilities.hpp>

#include <cstdint>
#include <vector>

namespace {

// -------- Variant binary encoding helpers ----------------------------------
//
// Metadata layout (version 1, offset_size=1, sorted=0):
//   byte 0       : header = 0x01
//   byte 1       : dictionary_size (u8)
//   byte 2..N+2  : offset[i] for i in 0..dictionary_size   (u8 each)
//   byte N+3..   : concatenated key bytes
//
// Object value layout (small: is_large=0, id_size=1, offset_size=1):
//   byte 0       : header = 0x02
//   byte 1       : num_elements (u8)
//   byte 2..M+2  : field_ids     (u8 each, M = num_elements)
//   byte M+3..   : field_offsets (u8 each, M+1 of them)
//   rest         : concatenated field values
//
// Primitive headers (basic_type=0):
//   int8  : 0x0C   (value: 1 LE byte)
//   int16 : 0x10   (value: 2 LE bytes)
//   int32 : 0x14   (value: 4 LE bytes)
//   int64 : 0x18   (value: 8 LE bytes)
//   bool=true  : 0x04  (no value)
//   bool=false : 0x08  (no value)
//   double : 0x1C  (value: 8 IEEE-754 LE bytes)
//
// Short string (basic_type=1, length in upper 6 bits):
//   header = (length << 2) | 1  (length <= 63)
//   followed by length UTF-8 bytes.

// Build a valid 1-key metadata blob with a single dictionary entry.
std::vector<uint8_t> build_metadata_single_key(std::string const& key)
{
  std::vector<uint8_t> out;
  out.push_back(0x01);                                     // header
  out.push_back(static_cast<uint8_t>(1));                  // dictionary_size
  out.push_back(static_cast<uint8_t>(0));                  // offset[0]
  out.push_back(static_cast<uint8_t>(key.size()));         // offset[1] = terminal
  for (char c : key) out.push_back(static_cast<uint8_t>(c));
  return out;
}

// Wrap a raw primitive-value byte sequence inside a single-field object named
// "x" (dictionary index 0). The caller provides just the field value bytes.
std::vector<uint8_t> build_single_field_object(std::vector<uint8_t> const& field_value)
{
  std::vector<uint8_t> out;
  out.push_back(0x02);                                              // object header (small)
  out.push_back(static_cast<uint8_t>(1));                           // num_elements
  out.push_back(static_cast<uint8_t>(0));                           // field_id[0] = "x"
  out.push_back(static_cast<uint8_t>(0));                           // field_offset[0]
  out.push_back(static_cast<uint8_t>(field_value.size()));          // field_offset[1] (terminal)
  for (auto b : field_value) out.push_back(b);
  return out;
}

// Primitive value encoders.
std::vector<uint8_t> encode_int8(int8_t v)  { return {0x0C, static_cast<uint8_t>(v)}; }
std::vector<uint8_t> encode_int16(int16_t v)
{
  return {0x10,
          static_cast<uint8_t>(v & 0xFF),
          static_cast<uint8_t>((v >> 8) & 0xFF)};
}
std::vector<uint8_t> encode_int32(int32_t v)
{
  return {0x14,
          static_cast<uint8_t>(v & 0xFF),
          static_cast<uint8_t>((v >> 8) & 0xFF),
          static_cast<uint8_t>((v >> 16) & 0xFF),
          static_cast<uint8_t>((v >> 24) & 0xFF)};
}
std::vector<uint8_t> encode_int64(int64_t v)
{
  std::vector<uint8_t> out{0x18};
  for (int i = 0; i < 8; ++i) { out.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF)); }
  return out;
}
std::vector<uint8_t> encode_bool(bool v) { return {static_cast<uint8_t>(v ? 0x04 : 0x08)}; }
std::vector<uint8_t> encode_double(double v)
{
  std::vector<uint8_t> out{0x1C};
  uint64_t bits;
  std::memcpy(&bits, &v, sizeof(bits));                    // host is little-endian (CUDA host)
  for (int i = 0; i < 8; ++i) { out.push_back(static_cast<uint8_t>((bits >> (8 * i)) & 0xFF)); }
  return out;
}
std::vector<uint8_t> encode_short_string(std::string const& s)
{
  CUDF_EXPECTS(s.size() <= 63, "use encode_long_string for >= 64 byte strings");
  std::vector<uint8_t> out;
  out.push_back(static_cast<uint8_t>((s.size() << 2) | 1));
  for (char c : s) out.push_back(static_cast<uint8_t>(c));
  return out;
}

// Construct a 1-row variant struct column: struct<list<uint8> meta, list<uint8> value>.
// TODO: if the branch provides a helper like `make_variant_column`, prefer it.
std::unique_ptr<cudf::column> make_variant_struct_one_row(
  std::vector<uint8_t> const& metadata_bytes, std::vector<uint8_t> const& value_bytes)
{
  cudf::test::lists_column_wrapper<uint8_t> meta_col{
    std::initializer_list<uint8_t>(metadata_bytes.data(), metadata_bytes.data() + metadata_bytes.size())};
  cudf::test::lists_column_wrapper<uint8_t> val_col{
    std::initializer_list<uint8_t>(value_bytes.data(), value_bytes.data() + value_bytes.size())};

  std::vector<std::unique_ptr<cudf::column>> children;
  children.emplace_back(std::make_unique<cudf::column>(cudf::column_view{meta_col}));
  children.emplace_back(std::make_unique<cudf::column>(cudf::column_view{val_col}));
  return cudf::make_structs_column(
    1, std::move(children), 0, rmm::device_buffer{}, cudf::test::get_default_stream());
}

}  // namespace

// ----------------------------------------------------------------------------
// Test fixture
// ----------------------------------------------------------------------------

struct VariantCastSemanticsTest : public cudf::test::BaseFixture {
  // Convenience: build a one-row variant column where "x" maps to `field_value`.
  std::unique_ptr<cudf::column> one_row_with_x(std::vector<uint8_t> const& field_value)
  {
    return make_variant_struct_one_row(
      build_metadata_single_key("x"),
      build_single_field_object(field_value));
  }
};

// ============================================================================
// CONTROL TESTS — stored type exactly matches target; expected to pass today.
// ============================================================================

TEST_F(VariantCastSemanticsTest, ControlExactMatchInt32StoredInt32Target)
{
  auto v = one_row_with_x(encode_int32(42));

  auto got = cudf::io::parquet::extract_variant_field(
    v->view(), "x", cudf::data_type{cudf::type_id::INT32},
    cudf::test::get_default_stream(),
    rmm::mr::get_current_device_resource_ref());

  cudf::test::fixed_width_column_wrapper<int32_t> expected{42};
  CUDF_TEST_EXPECT_COLUMNS_EQUAL(got->view(), expected);
}

TEST_F(VariantCastSemanticsTest, ControlExactMatchStringStoredStringTarget)
{
  auto v = one_row_with_x(encode_short_string("hi"));

  auto got = cudf::io::parquet::extract_variant_field(
    v->view(), "x", cudf::data_type{cudf::type_id::STRING},
    cudf::test::get_default_stream(),
    rmm::mr::get_current_device_resource_ref());

  cudf::test::strings_column_wrapper expected{"hi"};
  CUDF_TEST_EXPECT_COLUMNS_EQUAL(got->view(), expected);
}

// ============================================================================
// MISMATCH TESTS — Spark casts, cuDF decodes-if-exact-match. EXPECTED FAILURES.
// Each of these is a reproducer for the cast-semantics design question.
// ============================================================================

// ---- Integer-width mismatch into INT32 target ------------------------------

TEST_F(VariantCastSemanticsTest, Int8StoredInt32Target_SparkWidens)
{
  // Stored: int8(42). Parse_json picks the narrowest type that fits, so this
  // is how Spark actually writes small integers.
  auto v = one_row_with_x(encode_int8(42));

  auto got = cudf::io::parquet::extract_variant_field(
    v->view(), "x", cudf::data_type{cudf::type_id::INT32},
    cudf::test::get_default_stream(),
    rmm::mr::get_current_device_resource_ref());

  // Spark: variant_get(..., 'int') widens int8 → int32 and returns 42.
  // cuDF today: returns NULL.
  cudf::test::fixed_width_column_wrapper<int32_t> expected{42};
  CUDF_TEST_EXPECT_COLUMNS_EQUAL(got->view(), expected);
}

TEST_F(VariantCastSemanticsTest, Int16StoredInt32Target_SparkWidens)
{
  auto v = one_row_with_x(encode_int16(42));

  auto got = cudf::io::parquet::extract_variant_field(
    v->view(), "x", cudf::data_type{cudf::type_id::INT32},
    cudf::test::get_default_stream(),
    rmm::mr::get_current_device_resource_ref());

  cudf::test::fixed_width_column_wrapper<int32_t> expected{42};
  CUDF_TEST_EXPECT_COLUMNS_EQUAL(got->view(), expected);
}

TEST_F(VariantCastSemanticsTest, Int64StoredInt32Target_SparkNarrowsInRange)
{
  // Stored: int64(42). In-range narrowing → Spark returns 42 (try_variant_get).
  // variant_get (strict) would throw on overflow, but 42 fits, so no overflow.
  auto v = one_row_with_x(encode_int64(42));

  auto got = cudf::io::parquet::extract_variant_field(
    v->view(), "x", cudf::data_type{cudf::type_id::INT32},
    cudf::test::get_default_stream(),
    rmm::mr::get_current_device_resource_ref());

  cudf::test::fixed_width_column_wrapper<int32_t> expected{42};
  CUDF_TEST_EXPECT_COLUMNS_EQUAL(got->view(), expected);
}

// ---- Cross-type casts into INT32 target ------------------------------------

TEST_F(VariantCastSemanticsTest, StringParseableStoredInt32Target_SparkParses)
{
  // Stored: short string "42". Spark's Cast expression parses to 42.
  auto v = one_row_with_x(encode_short_string("42"));

  auto got = cudf::io::parquet::extract_variant_field(
    v->view(), "x", cudf::data_type{cudf::type_id::INT32},
    cudf::test::get_default_stream(),
    rmm::mr::get_current_device_resource_ref());

  cudf::test::fixed_width_column_wrapper<int32_t> expected{42};
  CUDF_TEST_EXPECT_COLUMNS_EQUAL(got->view(), expected);
}

TEST_F(VariantCastSemanticsTest, BooleanStoredInt32Target_SparkCasts)
{
  // Stored: boolean true. Spark casts bool→int ⇒ 1.
  auto v = one_row_with_x(encode_bool(true));

  auto got = cudf::io::parquet::extract_variant_field(
    v->view(), "x", cudf::data_type{cudf::type_id::INT32},
    cudf::test::get_default_stream(),
    rmm::mr::get_current_device_resource_ref());

  cudf::test::fixed_width_column_wrapper<int32_t> expected{1};
  CUDF_TEST_EXPECT_COLUMNS_EQUAL(got->view(), expected);
}

TEST_F(VariantCastSemanticsTest, DoubleStoredInt32Target_SparkCasts)
{
  // Stored: double(42.0). Spark casts double→int (truncating) ⇒ 42.
  auto v = one_row_with_x(encode_double(42.0));

  auto got = cudf::io::parquet::extract_variant_field(
    v->view(), "x", cudf::data_type{cudf::type_id::INT32},
    cudf::test::get_default_stream(),
    rmm::mr::get_current_device_resource_ref());

  cudf::test::fixed_width_column_wrapper<int32_t> expected{42};
  CUDF_TEST_EXPECT_COLUMNS_EQUAL(got->view(), expected);
}

// ---- Numeric / boolean stringify into STRING target ------------------------

TEST_F(VariantCastSemanticsTest, Int32StoredStringTarget_SparkStringifies)
{
  // Stored: int32(42). Spark Cast int→string ⇒ "42".
  auto v = one_row_with_x(encode_int32(42));

  auto got = cudf::io::parquet::extract_variant_field(
    v->view(), "x", cudf::data_type{cudf::type_id::STRING},
    cudf::test::get_default_stream(),
    rmm::mr::get_current_device_resource_ref());

  cudf::test::strings_column_wrapper expected{"42"};
  CUDF_TEST_EXPECT_COLUMNS_EQUAL(got->view(), expected);
}

TEST_F(VariantCastSemanticsTest, BooleanStoredStringTarget_SparkStringifies)
{
  // Stored: boolean true. Spark Cast bool→string ⇒ "true".
  auto v = one_row_with_x(encode_bool(true));

  auto got = cudf::io::parquet::extract_variant_field(
    v->view(), "x", cudf::data_type{cudf::type_id::STRING},
    cudf::test::get_default_stream(),
    rmm::mr::get_current_device_resource_ref());

  cudf::test::strings_column_wrapper expected{"true"};
  CUDF_TEST_EXPECT_COLUMNS_EQUAL(got->view(), expected);
}

TEST_F(VariantCastSemanticsTest, DoubleStoredStringTarget_SparkStringifies)
{
  // Stored: double(3.14). Spark Cast double→string ⇒ "3.14".
  auto v = one_row_with_x(encode_double(3.14));

  auto got = cudf::io::parquet::extract_variant_field(
    v->view(), "x", cudf::data_type{cudf::type_id::STRING},
    cudf::test::get_default_stream(),
    rmm::mr::get_current_device_resource_ref());

  cudf::test::strings_column_wrapper expected{"3.14"};
  CUDF_TEST_EXPECT_COLUMNS_EQUAL(got->view(), expected);
}

// ============================================================================
// End of file. Nine mismatch tests, two control tests. Expected outcome
// against the current branch: 2 PASS, 9 FAIL. When the libcudf team adopts
// cast semantics for cast_variant, these tests define the acceptance criteria.
// ============================================================================
