# Variant Data Type: Deep-Dive Technical Reference

A comprehensive guide to the Variant data type in Apache Parquet, Apache Spark, and a GPU acceleration roadmap for RAPIDS/cuDF integration with spark-rapids.

---

## Table of Contents

1. [Introduction & Motivation](#1-introduction--motivation)
2. [Parquet Variant Binary Encoding](#2-parquet-variant-binary-encoding)
3. [Parquet Variant Shredding](#3-parquet-variant-shredding)
4. [Spark VariantType](#4-spark-varianttype)
5. [Variant vs JSON Strings](#5-variant-vs-json-strings)
6. [cuDF Implementation Roadmap for spark-rapids](#6-cudf-implementation-roadmap-for-spark-rapids)
7. [Current spark-rapids Status](#7-current-spark-rapids-status)
8. [Open Questions](#8-open-questions)
9. [References](#9-references)

---

## 1. Introduction & Motivation

### What is Variant?

The **Variant** data type is an open standard for representing semi-structured data (e.g., JSON, XML) in a compact binary format. It was introduced in the Apache Parquet specification and adopted by Apache Spark 4.0, Delta Lake 4.0, and Apache Iceberg.

### Why Variant?

Traditionally, semi-structured data in Spark is stored as **JSON strings**. This has significant drawbacks:

- **Repeated parsing**: Every query must re-parse the JSON string from scratch
- **No columnar benefits**: JSON strings are opaque blobs — no column pruning, no predicate pushdown, no compression benefits
- **Type loss**: All values are strings internally, requiring runtime type inference
- **Storage inefficiency**: JSON text encoding is verbose (e.g., `"42"` is 4 bytes vs 1 byte as int8)

Variant solves these problems by:

- **Parsing once at ingest time** into an efficient binary representation
- **Preserving type information** natively (integers, booleans, decimals, etc.)
- **Supporting columnar shredding** for individual fields, enabling predicate pushdown and column pruning
- **Achieving 8x faster reads** vs JSON strings, and **30x faster with shredding**

### Ecosystem Support

| System         | Version  | Status      |
|----------------|----------|-------------|
| Apache Parquet | 1.15+    | Supported   |
| Apache Spark   | 4.0+     | Supported   |
| Delta Lake     | 4.0+     | Supported   |
| Apache Iceberg | 1.8+     | Supported   |
| Databricks     | Runtime 15.3+ | Supported |

---

## 2. Parquet Variant Binary Encoding

A Variant value is encoded as **two binary blobs**: a **metadata** blob (containing a dictionary of field names) and a **value** blob (containing the actual data).

In Parquet, VARIANT annotates a group with two required `BINARY` fields:

```
required group variant_col (VARIANT) {
  required binary metadata;
  required binary value;
}
```

### 2.1 Metadata Encoding

The metadata blob contains a dictionary of all unique field names used in the variant value.

#### Header Byte

```
Bit 7  6  5  4  3  2  1  0
    |--|  |  |-----------|
     ^    ^       ^
     |    |       version (must be 1)
     |    sorted_strings (1 = sorted)
     offset_size_minus_one (0-3 → 1-4 bytes)
```

| Field                   | Bits | Description                                          |
|-------------------------|------|------------------------------------------------------|
| `version`               | 3:0  | Must be `1`                                          |
| `sorted_strings`        | 5    | `1` if dictionary strings are sorted lexicographically |
| `offset_size_minus_one` | 7:6  | Number of bytes per offset field minus 1 (0→1B, 1→2B, 2→3B, 3→4B) |

#### Layout After Header

```
[header: 1 byte]
[dictionary_size: offset_size bytes, unsigned little-endian]
[offsets: (dictionary_size + 1) × offset_size bytes]
[string_data: concatenated UTF-8 strings]
```

- `dictionary_size`: Number of unique string keys
- `offsets[i]`: Byte offset into `string_data` where string `i` starts
- `offsets[dictionary_size]`: Terminal offset (total string data length)
- String `i` = `string_data[offsets[i] .. offsets[i+1]]`

#### Example: `{"age": 42, "name": "Bob"}`

Metadata binary (12 bytes):
```
01          → header: version=1, offset_size=1, sorted=false
02          → dictionary_size = 2
00          → offset[0] = 0  (start of "age")
03          → offset[1] = 3  (start of "name")
07          → offset[2] = 7  (terminal, total string length)
61 67 65    → "age"
6E 61 6D 65 → "name"
```

Dictionary:
- Key 0 → "age" (bytes 0..3)
- Key 1 → "name" (bytes 3..7)

### 2.2 Value Encoding

Every value starts with a **1-byte header**:

```
Bit 7  6  5  4  3  2  1  0
    |-----------------|--|
           ^            ^
           |            basic_type (2 bits)
           value_header (6 bits)
```

#### Basic Types

| `basic_type` | Meaning      | `value_header` interpretation |
|--------------|--------------|-------------------------------|
| `0`          | Primitive    | Primitive type ID (0-20)      |
| `1`          | Short string | String length in bytes (0-63) |
| `2`          | Object       | Object header bits            |
| `3`          | Array        | Array header bits             |

### 2.3 Primitive Types (basic_type = 0)

The `value_header` (6 bits) encodes the primitive type ID:

| Type ID | Type               | Encoding                                  | Size     |
|---------|--------------------|-------------------------------------------|----------|
| 0       | `null`             | No data                                   | 0 bytes  |
| 1       | `boolean true`     | No data                                   | 0 bytes  |
| 2       | `boolean false`    | No data                                   | 0 bytes  |
| 3       | `int8`             | 1-byte signed                             | 1 byte   |
| 4       | `int16`            | 2-byte little-endian signed               | 2 bytes  |
| 5       | `int32`            | 4-byte little-endian signed               | 4 bytes  |
| 6       | `int64`            | 8-byte little-endian signed               | 8 bytes  |
| 7       | `double`           | IEEE 754 8-byte little-endian             | 8 bytes  |
| 8       | `decimal4`         | 1 byte scale + 4-byte little-endian value | 5 bytes  |
| 9       | `decimal8`         | 1 byte scale + 8-byte little-endian value | 9 bytes  |
| 10      | `decimal16`        | 1 byte scale + 16-byte little-endian value| 17 bytes |
| 11      | `date`             | 4-byte little-endian (days since epoch)   | 4 bytes  |
| 12      | `timestamp`        | 8-byte little-endian (micros since epoch, UTC) | 8 bytes |
| 13      | `timestamp_ntz`    | 8-byte little-endian (micros, no timezone)| 8 bytes  |
| 14      | `float`            | IEEE 754 4-byte little-endian             | 4 bytes  |
| 15      | `binary`           | 4-byte size (LE) + raw bytes              | 4+N bytes|
| 16      | `string`           | 4-byte size (LE) + UTF-8 bytes            | 4+N bytes|
| 17      | `time_ntz`         | 8-byte little-endian (micros since midnight)| 8 bytes|
| 18      | `timestamp_tz`     | 8-byte little-endian (micros since epoch, UTC) | 8 bytes |
| 19      | `timestamp_ntz_nanos` | 8-byte little-endian (nanos, no timezone)| 8 bytes |
| 20      | `uuid`             | 16-byte big-endian                        | 16 bytes |

### 2.4 Short String (basic_type = 1)

For strings with length < 64 bytes, the length is stored directly in the `value_header` (6 bits), followed by the UTF-8 bytes. No additional length prefix is needed.

```
[header: 1 byte]  →  basic_type=1, value_header=length
[string_data: length bytes]
```

Example: `"Bob"` → `0x0D 0x42 0x6F 0x62` (header=0x0D → basic_type=1, length=3)

### 2.5 Object Encoding (basic_type = 2)

The `value_header` (6 bits) is interpreted as:

```
Bit 5  4  3  2  1  0
    |  |--|  |--|
    ^   ^     ^
    |   |     field_offset_size_minus_one (2 bits)
    |   field_id_size_minus_one (2 bits)
    is_large (1 bit)
```

#### Object Layout

```
[header: 1 byte]
[num_elements: 1 byte if !is_large, 4 bytes if is_large]
[field_ids: num_elements × field_id_size bytes]
[field_offsets: (num_elements + 1) × field_offset_size bytes]
[field_values: concatenated variant-encoded values]
```

**Critical rule**: Field IDs and field offsets MUST be ordered by the **lexicographic order of the corresponding field names** in the metadata dictionary. Duplicate field names are an error.

#### Example: `{"age": 42, "name": "Bob"}`

Value binary (12 bytes):
```
08          → header: basic_type=2, is_large=0, id_size=1, offset_size=1
02          → num_elements = 2
00          → field_id[0] = 0 (→ "age")
01          → field_id[1] = 1 (→ "name")
00          → field_offset[0] = 0
02          → field_offset[1] = 2
05          → field_offset[2] = 5 (terminal)
0C 2A       → value[0]: header=0x0C (primitive int8, type_id=3), data=42
0D 42 6F 62 → value[1]: header=0x0D (short string, len=3), "Bob"
```

### 2.6 Array Encoding (basic_type = 3)

The `value_header` (6 bits) is interpreted as:

```
Bit 5  4  3  2  1  0
    |        |--|
    ^         ^
    |         field_offset_size_minus_one (2 bits)
    is_large (1 bit)
```

#### Array Layout

```
[header: 1 byte]
[num_elements: 1 byte if !is_large, 4 bytes if is_large]
[field_offsets: (num_elements + 1) × field_offset_size bytes]
[element_values: concatenated variant-encoded values]
```

### 2.7 Size Constraints

| Parameter       | `is_large=0` | `is_large=1` |
|-----------------|--------------|---------------|
| Max elements    | 255 (1 byte) | ~4.29B (4 bytes) |
| Offset sizes    | 1-4 bytes    | 1-4 bytes     |
| Field ID sizes  | 1-4 bytes    | 1-4 bytes     |

### 2.8 Key Constraints

- All strings MUST be UTF-8 encoded
- Object field names MUST be unique (duplicates are an error)
- Field IDs and offsets in objects MUST be sorted by lexicographic order of field names
- Metadata dictionary key order is independent of the value blob's field ordering

---

## 3. Parquet Variant Shredding

**Shredding** (also called subcolumnarization) decomposes Variant data into separate typed Parquet columns. This enables columnar encoding, compression, statistics-based data skipping, and partial projection — the key benefits of columnar storage that are lost with opaque binary blobs.

### 3.1 Shredded Schema Structure

A shredded Variant has three components:

```
optional group col (VARIANT) {
  required binary metadata;       -- always present
  optional binary value;          -- fallback for un-shredded data
  optional <type> typed_value;    -- shredded typed column
}
```

### 3.2 Value / Typed_Value Interaction

The `value` and `typed_value` fields work together to represent a single Variant value:

| `value`  | `typed_value` | Meaning                              |
|----------|---------------|--------------------------------------|
| null     | null          | Missing (object fields only)         |
| non-null | null          | Present, any type (stored in value)  |
| null     | non-null      | Present, matches shredded type       |
| non-null | non-null      | Partially shredded object            |

**Critical constraint**: Writers MUST NOT produce data where both `value` and `typed_value` are non-null, UNLESS the Variant value is an object (partial shredding).

### 3.3 Primitive Type Shredding

For non-object types, exactly one of `typed_value` or `value` must be non-null.

Variant types map to Parquet types:

| Variant Type        | Parquet Physical Type    | Logical Type                  |
|---------------------|--------------------------|-------------------------------|
| boolean             | BOOLEAN                  | —                             |
| int8, int16         | INT32                    | INT(8/16, signed)             |
| int32               | INT32                    | —                             |
| int64               | INT64                    | —                             |
| float               | FLOAT                    | —                             |
| double              | DOUBLE                   | —                             |
| decimal4/8/16       | INT32/INT64/BYTE_ARRAY   | DECIMAL(p, s)                 |
| date                | INT32                    | DATE                          |
| time_ntz            | INT64                    | TIME(MICROS)                  |
| timestamp_tz        | INT64                    | TIMESTAMP(true, MICROS)       |
| timestamp_ntz       | INT64                    | TIMESTAMP(false, MICROS)      |
| timestamp_ntz_nanos | INT64                    | TIMESTAMP(false, NANOS)       |
| binary              | BINARY                   | —                             |
| string              | BINARY                   | STRING                        |
| uuid                | FIXED_LEN_BYTE_ARRAY[16] | UUID                          |

### 3.4 Object Shredding

Object shredding extracts known fields into dedicated typed columns. The `typed_value` is a Parquet group containing one sub-group per shredded field.

```
optional group event (VARIANT) {
  required binary metadata;
  optional binary value;
  optional group typed_value {
    required group event_type {
      optional binary value;
      optional binary typed_value (STRING);
    }
    required group event_ts {
      optional binary value;
      optional int64 typed_value (TIMESTAMP(true, MICROS));
    }
  }
}
```

**Rules for object shredding:**
- If the Variant is an object, `typed_value` MUST be non-null
- If the Variant is NOT an object, `typed_value` MUST be null
- The `value` column of a partially shredded object MUST NOT contain fields that are already represented in `typed_value`
- Missing fields: both sub-field columns are null
- Null field value: `value` contains Variant null (`0x00`)

**Reconstruction**: Union the shredded fields from `typed_value` with the non-shredded fields from `value`.

### 3.5 Array Shredding

Arrays use the standard 3-level Parquet LIST encoding:

```
optional group tags (VARIANT) {
  required binary metadata;
  optional binary value;
  optional group typed_value (LIST) {
    repeated group list {
      required group element {
        optional binary value;
        optional binary typed_value (STRING);
      }
    }
  }
}
```

**Rules**: If the value is an array, `value` must be null and `typed_value` must be non-null. All elements must be present (not missing).

### 3.6 Data Skipping with Shredded Columns

Statistics for `typed_value` columns can be used for file, row group, or page skipping **only when `value` is always null** (i.e., all values match the shredded type). This enables:

- Min/max statistics for range filtering
- Null count statistics
- Bloom filters
- Dictionary-based filtering

### 3.7 Performance Impact

| Metric          | JSON String | Variant (no shredding) | Variant (with shredding) |
|-----------------|-------------|------------------------|--------------------------|
| Read speed      | 1x          | **8x**                 | **30x**                  |
| Write speed     | 1x          | ~1x                    | 0.5-0.8x (20-50% slower)|
| Column pruning  | No          | No                     | **Yes**                  |
| Predicate push  | No          | No                     | **Yes**                  |
| Compression     | Poor        | Good                   | **Excellent**            |

---

## 4. Spark VariantType

### 4.1 Type System

```
java.lang.Object
  └── org.apache.spark.sql.types.DataType
        └── org.apache.spark.sql.types.VariantType
```

- **Introduced**: Apache Spark 4.0.0
- **Purpose**: Represents semi-structured values with arbitrary hierarchical data structures
- **Limitation**: Cannot store maps with non-string key types
- **Internal storage**: `VariantVal` containing two byte arrays (metadata + value)

### 4.2 DDL Syntax

```sql
-- Create table with VARIANT column
CREATE TABLE events (
  event_id STRING,
  payload VARIANT
) USING DELTA;

-- Cast to VARIANT
SELECT CAST('{"a": 1}' AS VARIANT);

-- Insert with parse_json
INSERT INTO events VALUES ('e1', parse_json('{"user": "alice", "action": "click"}'));
```

### 4.3 SQL Functions (Complete Reference)

All functions available since Spark 4.0.0:

#### `parse_json(jsonStr) → VARIANT`

Parses a JSON string into a Variant value. Throws exception on invalid JSON.

```sql
SELECT parse_json('{"a":1,"b":0.8}');
-- {"a":1,"b":0.8}
```

#### `try_parse_json(jsonStr) → VARIANT`

Same as `parse_json` but returns NULL on invalid JSON instead of throwing.

```sql
SELECT try_parse_json('{"a":1,');  -- NULL
```

#### `variant_get(v, path[, type]) → type`

Extracts a sub-variant using JSONPath syntax and optionally casts to a type. Returns NULL if path doesn't exist. Throws exception if cast fails.

```sql
SELECT variant_get(parse_json('{"a": 1}'), '$.a', 'int');       -- 1
SELECT variant_get(parse_json('{"a": 1}'), '$.b', 'int');       -- NULL
SELECT variant_get(parse_json('[1, "2"]'), '$[1]', 'string');   -- 2
SELECT variant_get(parse_json('[1, "hello"]'), '$[1]');          -- "hello"
```

#### `try_variant_get(v, path[, type]) → type`

Same as `variant_get` but returns NULL on cast failure instead of throwing.

```sql
SELECT try_variant_get(parse_json('[1, "hello"]'), '$[1]', 'int');  -- NULL
```

#### `is_variant_null(expr) → BOOLEAN`

Checks if a Variant value is a **variant null** (distinct from SQL NULL).

```sql
SELECT is_variant_null(parse_json('null'));     -- true
SELECT is_variant_null(parse_json('"null"'));   -- false (it's a string)
SELECT is_variant_null(parse_json('13'));       -- false
SELECT is_variant_null(parse_json(null));       -- false (SQL NULL, not variant null)
```

#### `schema_of_variant(v) → STRING`

Returns the SQL schema representation of a single Variant value.

```sql
SELECT schema_of_variant(parse_json('null'));                      -- VOID
SELECT schema_of_variant(parse_json('[{"b":true,"a":0}]'));       -- ARRAY<OBJECT<a: BIGINT, b: BOOLEAN>>
```

#### `schema_of_variant_agg(v) → STRING` (Aggregate)

Merges schemas across multiple Variant values in a column.

```sql
SELECT schema_of_variant_agg(parse_json(j))
FROM VALUES ('{"a": 1}'), ('{"b": true}'), ('{"c": 1.23}') AS tab(j);
-- OBJECT<a: BIGINT, b: BOOLEAN, c: DECIMAL(3,2)>
```

#### `to_variant_object(expr) → VARIANT`

Converts nested Spark types (struct, array, map) to Variant. Maps must have string keys.

```sql
SELECT to_variant_object(named_struct('a', 1, 'b', 2));   -- {"a":1,"b":2}
SELECT to_variant_object(array(1, 2, 3));                  -- [1,2,3]
```

#### `variant_explode(expr) → TABLE`

Explodes a Variant object/array into rows. Returns `struct<pos INT, key STRING, value VARIANT>`.

```sql
SELECT * FROM variant_explode(parse_json('["hello", "world"]'));
-- pos | key  | value
-- 0   | NULL | "hello"
-- 1   | NULL | "world"

SELECT * FROM variant_explode(parse_json('{"a": true, "b": 3.14}'));
-- pos | key | value
-- 0   | a   | true
-- 1   | b   | 3.14
```

#### `variant_explode_outer(expr) → TABLE`

Same as `variant_explode` but as an outer join (produces NULL row for non-array/non-object input instead of no rows).

### 4.4 Dot-Notation Access

Spark supports natural dot-notation for querying Variant columns:

```sql
SELECT
  payload.user.id AS user_id,
  payload.metadata.source
FROM events
WHERE payload.type = 'click';
```

### 4.5 Internal Representation

In Spark's Tungsten memory format, Variant is stored as a `VariantVal`:

```
VariantVal {
  metadata: Array[Byte]   // field name dictionary
  value: Array[Byte]      // binary-encoded variant value
}
```

Both follow the Parquet Variant Binary Encoding specification (Section 2).

---

## 5. Variant vs JSON Strings

### 5.1 Storage and Parsing

| Aspect            | JSON String                  | Variant                          |
|-------------------|------------------------------|----------------------------------|
| Storage format    | UTF-8 text                   | Compact binary                   |
| Parsing           | Every query re-parses        | Parsed once at ingest            |
| Type preservation | All values are strings       | Native types (int, bool, etc.)   |
| Schema discovery  | Runtime inference per query   | Embedded in binary encoding      |
| Size (typical)    | Larger (text encoding)       | Smaller (binary encoding)        |

### 5.2 Query Behavior Differences

| Behavior            | JSON String             | Variant                    |
|---------------------|-------------------------|----------------------------|
| Field access        | Case-insensitive        | **Case-sensitive**         |
| Array wildcard `[*]`| Supported               | Not supported              |
| NULL encoding       | JSON `null` as string   | Distinct variant null type |
| Conversion          | `to_json()` / `from_json()` | `parse_json()` / `to_json()` |

**Important**: Converting between JSON string and Variant is NOT perfectly reversible due to whitespace normalization, arbitrary key ordering, and potential number truncation.

### 5.3 Performance Comparison

Based on Databricks benchmarks:

- **Read performance**: Variant is **8x faster** than JSON strings for both flat and nested schemas
- **With shredding**: Reads are **30x faster** than JSON strings
- **Write performance**: Variant writes are comparable; shredded writes are 20-50% slower
- **Query performance**: Up to **4x faster** end-to-end query execution on semi-structured datasets

---

## 6. cuDF Implementation Roadmap for spark-rapids

This section outlines the phased approach to implementing GPU-accelerated Variant support in spark-rapids using NVIDIA cuDF.

### 6.1 In-Memory Representation in cuDF

Apache Spark represents Variant as a struct with two binary fields:
- `metadata`: Binary blob containing field name dictionary
- `value`: Binary blob containing the encoded variant data

**cuDF modeling options:**

| Option | cuDF Type | Pros | Cons |
|--------|-----------|------|------|
| A (Initial) | `struct<list<int8>, list<int8>>` | Works with existing cuDF types | Hits 2GB row limit per list column |
| B (Better) | `struct<binary, binary>` | No row limit, natural offset-based access | Requires binary type support in cuDF |

**Recommended**: Start with `struct<list<int8>, list<int8>>` for prototyping, then introduce a proper `binary` cuDF type for production.

#### Binary Type in cuDF (Future)

A native `binary` type in cuDF would:
- Use a data buffer like strings with 32/64-bit offsetalator
- NOT need row operator support
- Primarily affect copy operations: `gather`, `scatter`, `concatenate`, `copy_if`, `transpose`
- Enable larger-than-2GB variant data per column

### 6.2 Step 1: `variant_get` for Fixed-Width and Variable-Width Types (No Nesting)

**Goal**: Support `variant_get(column, field_name) → typed_column` for top-level primitive extraction.

#### Algorithm: Metadata Key Lookup

```
Input: metadata blob, target field_name
Output: key_id (integer) or null

1. Read header byte → extract offset_size, sorted_strings flag
2. Read dictionary_size
3. Binary search over dictionary entries:
   - For each candidate index i:
     - Read offsets[i] and offsets[i+1]
     - Compare string_data[offsets[i]..offsets[i+1]] against field_name
   - Lexicographic comparison using unsigned byte ordering
4. Return matched key_id or null if not found
```

**GPU design**: A warp (32 threads) cooperatively performs the binary search for each row. Threads collaborate on string comparison.

#### Algorithm: Value Extraction

```
Input: value blob, key_id from metadata lookup
Output: extracted primitive value

1. Read value header byte → basic_type must be 2 (object)
2. Read num_elements
3. Scan field_ids array for key_id → get position p
4. Read field_offsets[p] and field_offsets[p+1] → value byte range
5. Read the sub-value at that offset
6. Decode based on basic_type and type_id
```

**GPU design**: Single thread per row can perform value extraction (sequential memory access within a row).

#### Supported Output Types (Step 1)

| variant_get Target Type | cuDF Output Column Type |
|------------------------|------------------------|
| int8/int16/int32/int64 | INT8/INT16/INT32/INT64 |
| float/double           | FLOAT32/FLOAT64        |
| decimal                | DECIMAL32/64/128       |
| string                 | STRING                 |
| boolean                | BOOL8                  |
| date                   | TIMESTAMP_DAYS         |
| timestamp              | TIMESTAMP_MICROSECONDS |
| binary                 | LIST<INT8> or BINARY   |

#### API Signature

```cpp
// Extract a single field from variant column, cast to target type
std::unique_ptr<column> variant_get(
    column_view const& variant_col,  // struct<list<int8>, list<int8>>
    string_scalar const& field_name,
    data_type target_type,
    rmm::device_async_resource_ref mr
);
```

#### Walkthrough Example: `{"age": 42, "name": "Bob"}`

**Metadata** (12 bytes): `01 02 00 03 07 61 67 65 6E 61 6D 65`

```
01       → header: version=1, offset_size=1
02       → dictionary_size = 2 keys
00       → offset[0] = 0
03       → offset[1] = 3
07       → offset[2] = 7 (terminal)
61 67 65 → "age"  (key 0)
6E 61 6D 65 → "name" (key 1)
```

**Value** (12 bytes): `08 02 00 01 00 02 05 0C 2A 0D 42 6F 62`

```
08       → header: basic_type=2 (object), is_large=0, id_size=1, offset_size=1
02       → num_elements = 2
00       → field_id[0] = 0 ("age")
01       → field_id[1] = 1 ("name")
00       → field_offset[0] = 0
02       → field_offset[1] = 2
05       → field_offset[2] = 5 (terminal)
0C 2A    → value[0]: 0x0C = primitive(type_id=3=int8), data = 42
0D 42 6F 62 → value[1]: 0x0D = short_string(len=3), "Bob"
```

**Extracting "age"**:
1. Binary search metadata for "age" → key_id = 0
2. Scan value field_ids for 0 → position 0
3. Read offsets[0]=0, offsets[1]=2 → 2 bytes at offset 0
4. Decode: header=0x0C → primitive int8, value = 42

### 6.3 Step 2: `variant_get` with Nested Fields and Arrays

**Goal**: Support JSONPath-like nested extraction: `variant_get(col, '$.user.address.city', 'string')`

#### Challenge: Nested Metadata References

Nested objects share the same metadata blob. When traversing nested fields, the GPU kernel must:
1. Parse the path expression (`$.field1.field2[0].field3`)
2. For each path segment, look up the key in metadata
3. Navigate into the sub-object/sub-array in the value blob
4. Repeat until the leaf value is reached

**GPU design**: Warp-per-row is recommended because:
- Each nesting level requires a metadata dictionary lookup (binary search)
- Path depth can vary per row
- Memory access patterns are irregular (pointer-chasing through offsets)

#### Path Expression Support

```
$.field          → object field access
$[index]         → array element access
$.field1.field2  → nested object traversal
$.field[0].name  → mixed object/array traversal
```

#### API Signature (Extended)

```cpp
// Extract a nested field using path expression
std::unique_ptr<column> variant_get(
    column_view const& variant_col,
    string_scalar const& path_expression,  // e.g. "$.user.address.city"
    data_type target_type,
    rmm::device_async_resource_ref mr
);
```

#### Handling `variant_get` Without Cast

When `variant_get` is called without a target type (returns VARIANT), the spark-rapids layer should handle this. The GPU extracts the sub-variant bytes and returns them as a new variant column (metadata + sub-value blob).

> **Note**: Bobby to confirm whether this is handled in the Spark plugin layer or requires cuDF support.

### 6.4 Step 3: Parquet TableScan for Variant Columns

**Goal**: Read Variant columns from Parquet files on GPU, including shredded columns.

#### Sub-tasks

1. **Read un-shredded Variant**: Read the `metadata` and `value` binary columns from Parquet into cuDF binary (or `list<int8>`) columns. This should work with existing Parquet reader if binary column reading is supported.

2. **Predicate pushdown with shredded columns**: When a Variant field is shredded, the `typed_value` column has Parquet statistics (min/max, null count). The spark-rapids query planner can push predicates down to these typed columns for row group skipping.

3. **Read shredded columns**: Read `typed_value` columns as native typed cuDF columns. Reconstruct the Variant from shredded + un-shredded components if needed, or use the typed column directly for query evaluation.

#### Shredded Column Read Flow

```
Parquet File
  ├── metadata (binary) ──────────────────→ cuDF binary column
  ├── value (binary) ─────────────────────→ cuDF binary column (fallback)
  └── typed_value
       ├── event_type (STRING) ──────────→ cuDF string column (direct use!)
       └── event_ts (TIMESTAMP) ─────────→ cuDF timestamp column (direct use!)
```

When a query only accesses shredded fields, the `value` column can be skipped entirely (column pruning).

### 6.5 Step 4: JSON String → Variant Conversion and Parquet Write

**Goal**: GPU-accelerated `parse_json()` and Variant Parquet writer with shredding support.

#### Option A: Via cuDF JSON Reader

```
JSON string column
  → cuDF JSON reader → cuDF table (typed columns)
  → Build metadata blob (column names → dictionary)
  → Build value blob (thread per row, write typed values)
  → Variant column (struct<binary, binary>)
```

**Pros**: Reuses existing JSON reader, handles complex parsing
**Cons**: Two-pass (parse then convert), intermediate materialization

#### Option B: Direct FST-Based Conversion

```
JSON string column
  → FST (Finite State Transducer) pass 1: extract field names → metadata blob
  → FST pass 2: identify field positions per row
  → Write variant value blob (thread per row)
  → Variant column (struct<binary, binary>)
```

**Pros**: Single logical pipeline, potentially faster
**Cons**: More complex implementation, FST needs to handle full JSON grammar

#### Parquet Write with Shredding

Writing shredded Variant to Parquet requires:
1. Schema inference or user-specified shredding hints (which fields to shred, what types)
2. For each row, split the Variant into shredded fields and remaining fields
3. Write `typed_value` columns as native Parquet columns
4. Write remaining fields into `value` as binary

**Open question**: How should the shredding schema be determined at write time? Spark's optimizer may provide hints based on query patterns.

### 6.6 Vectorized `variant_get` (Batch Field Extraction)

An optimization for queries that extract multiple fields:

```cpp
// Extract multiple fields in a single pass over the variant data
std::unique_ptr<table> variant_get_many(
    column_view const& variant_col,
    std::vector<string_scalar> const& field_names,
    std::vector<data_type> const& target_types,
    rmm::device_async_resource_ref mr
);
```

This avoids repeatedly scanning the same metadata/value blobs. A single kernel reads the variant once and extracts all requested fields, reducing memory bandwidth by N× for N fields.

---

## 7. Current spark-rapids Status

### 7.1 Support Level: NOT SUPPORTED

The spark-rapids plugin does **not** currently support GPU acceleration for Variant operations.

### 7.2 Existing Code

| File | Content |
|------|---------|
| `sql-plugin/src/main/spark350db143/.../CudfUnsafeRow.scala` | `getVariant()` throws `UnsupportedOperationException("Not Implemented yet")` |
| `sql-plugin/src/main/spark400/.../CudfUnsafeRow.scala` | `getVariant()` throws `UnsupportedOperationException("VariantVal is not supported")` |
| `sql-plugin/src/main/spark321/.../ParquetVariantShims.scala` | No-op shim for Spark 3.2.1-4.0.x (config doesn't exist) |
| `sql-plugin/src/main/spark411/.../ParquetVariantShims.scala` | Sets `PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE` for Spark 4.1.1+ |
| `sql-plugin/src/main/scala/.../GpuParquetFileFormat.scala:254` | Calls `ParquetVariantShims` during write |
| `sql-plugin/src/main/scala/.../ParquetCachedBatchSerializer.scala:1318` | Calls `ParquetVariantShims` for cache serialization |

### 7.3 Type System Gap

The `TypeEnum` in `TypeChecks.scala` does NOT include a `VARIANT` type. The supported types are:
`BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, STRING, DECIMAL, NULL, BINARY, CALENDAR, ARRAY, MAP, STRUCT, UDT, DAYTIME, YEARMONTH`

Adding Variant support will require extending `TypeEnum` and updating type check infrastructure.

---

## 8. Open Questions

1. **Are variant field names guaranteed to be sorted in the metadata blob?**
   - Per the spec, the `sorted_strings` flag in the header indicates this. When set, binary search is valid. When unset, linear scan may be needed (or the writer should always sort).

2. **Is shredding commonly used by writers?**
   - Databricks enables shredding by default in recent runtimes. Open-source Spark 4.0+ supports it but writer behavior may vary.

3. **Does `variant_get(column_view, vector<field_names>)` make sense?**
   - Yes — batch extraction of multiple fields in a single kernel pass would significantly reduce memory bandwidth. This is the `variant_get_many` API proposed in Section 6.6.

4. **Do we need to support `variant_get` on non-leaf fields?**
   - Yes, for queries like `SELECT payload.user FROM events` where `user` is a nested object. The result would be a new Variant column containing the sub-object.

5. **How should `variant_get` without casting be handled?**
   - Likely in the Spark plugin layer: extract sub-variant bytes from cuDF binary column and wrap as a new Variant column. Needs confirmation (Bobby to check).

6. **cuDF binary column 2GB limit**
   - `list<int8>` hits the 2GB row limit. A native `binary` cuDF type with 64-bit offsets would remove this constraint. This is a prerequisite for production deployment with large Variant data.

---

## 9. References

### Specifications
- [Parquet Variant Binary Encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/)
- [Parquet Variant Shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/)
- [Parquet Variant Encoding (GitHub)](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)
- [Parquet Variant Shredding (GitHub)](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)
- [Parquet Logical Types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)

### Apache Spark
- [Spark SQL Variant Functions (4.2.0)](https://spark.apache.org/docs/4.2.0-preview2/api/sql/variant-functions/)
- [VariantType JavaDoc (Spark 4.1.0)](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/VariantType.html)
- [PySpark VariantType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.VariantType.html)

### Databricks / Industry
- [Introducing the Open Variant Data Type in Delta Lake and Apache Spark](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)
- [Introducing Variant: A New Open Standard for Semi-Structured Data](https://www.databricks.com/blog/introducing-variant-new-open-standard-semi-structured-data-apache-parquettm-delta-lake)
- [Databricks: How is Variant Different from JSON Strings](https://docs.databricks.com/aws/en/semi-structured/variant-json-diff)
- [Databricks: Variant Shredding](https://docs.databricks.com/aws/en/delta/variant-shredding)
- [Snowflake Semi-Structured Data Types](https://docs.snowflake.com/en/sql-reference/data-types-semistructured)

### Related Work
- [Velox Variant Support (GitHub Issues)](https://github.com/facebookincubator/velox/issues?q=is%3Aissue%20variant)
- [cuDF - GPU DataFrame Library](https://github.com/rapidsai/cudf)
- [libcudf Nested Data Types Blog](https://developer.nvidia.com/blog/streamline-etl-workflows-with-nested-data-types-in-rapids-libcudf/)
