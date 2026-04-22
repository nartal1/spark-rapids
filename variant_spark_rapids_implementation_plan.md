# Variant Support Implementation Plan for spark-rapids

This note collects the conclusions from the Variant investigation and turns
them into an incremental implementation plan. The intended workflow is:

1. Build the needed cuDF Variant branch.
2. Add a small JNI bridge in the private `spark-rapids-jni` repo.
3. Install that JNI jar locally.
4. Point this `spark-rapids` checkout at that JNI jar.
5. Add one Spark-RAPIDS feature at a time, with CPU/GPU checks before moving
   to the next feature.

The goal is not to enable all of Spark Variant at once. The first useful slice
is Parquet Variant read plus object-field extraction for a small set of target
types.

## Current cuDF Status

The relevant cuDF work is split across several PRs/branches:

- `rapidsai/cudf#22310`: Parquet reader support for Variant logical type. This
  is the read-side foundation. It materializes a Parquet Variant column as a
  cuDF struct with `metadata` and `value` byte-list children.
- `rapidsai/cudf#22416`: core extraction PR. This adds experimental APIs for
  object-field extraction and primitive decode.
- `rapidsai/cudf#22434`: broader draft/testing branch. This includes a larger
  feature branch with array/quoted-key path work, examples, and benchmarks.

As of the latest inspection, `#22416` exposes APIs under:

```cpp
namespace cudf::io::parquet::experimental
```

The important entry points are:

```cpp
std::unique_ptr<cudf::column> get_variant_field(
    cudf::column_view const& variant_column,
    std::string_view path,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr);

std::unique_ptr<cudf::column> cast_variant(
    cudf::column_view const& values,
    cudf::data_type desired_type,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr);

std::unique_ptr<cudf::column> extract_variant_field(
    cudf::column_view const& variant_column,
    std::string_view path,
    cudf::data_type desired_type,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr);
```

### `get_variant_field`

Input:

```text
STRUCT<
  LIST<UINT8> metadata,
  LIST<UINT8> value,
  ...optional shredded children
>
```

Output:

```text
LIST<UINT8>
```

The output is the raw encoded Variant value bytes for the selected field. It is
not a full Variant struct by itself.

Supported paths in `#22416`:

- `x`
- `$.x`
- `$.a.b.c`

Unsupported paths in `#22416`:

- arrays: `$.a[0]`, `$[0]`
- quoted bracket keys: `$['a.b']`
- wildcard paths: `$.a[*]`

Null behavior:

- Missing key returns SQL null.
- Non-object intermediate returns SQL null.
- Invalid Variant metadata returns SQL null.
- Parent struct null stays null.
- Malformed path throws.

### `cast_variant`

Input:

```text
LIST<UINT8>
```

Supported output types:

- `INT8`
- `INT16`
- `INT32`
- `INT64`
- `STRING`

Important limitation: `cast_variant` is currently exact physical decode. It is
not Spark SQL cast semantics.

Examples:

| Stored Variant value | Requested target | Spark CPU `try_variant_get` | cuDF `cast_variant` today |
|---|---:|---:|---:|
| string `"42"` | int | `42` | `NULL` |
| int8 `42` | int | `42` | `NULL` unless target is `INT8` |
| int64 `42` | int | `42` if in range | `NULL` unless target is `INT64` |
| object `{...}` | string | JSON string | `NULL` |
| string `"abc"` | int | `NULL` | `NULL` |

This is the biggest semantic gap for Spark.

## Spark CPU Behavior to Keep in Mind

In Apache Spark OSS, there is no `cast_variant` operator in the plan. Spark's
cast is inside the `variant_get` or `try_variant_get` Catalyst expression.

Example:

```scala
spark.sql("""
  SELECT try_variant_get(parse_json('{"a":"42"}'), '$.a', 'int') AS a
""").explain(true)
```

The plan shows:

```text
try_variant_get(..., $.a, IntegerType, false, Some(...))
```

It will not show a separate `Cast(...)` node and will not show `cast_variant`.
The result on CPU is:

```text
+---+
|a  |
+---+
|42 |
+---+
```

For strict mode:

```scala
spark.sql("""
  SELECT variant_get(parse_json('{"a":"abc"}'), '$.a', 'int') AS a
""").show(false)
```

Spark throws on the invalid cast. The `try_variant_get` form returns null.

## Regular Spark-RAPIDS Cast Architecture

Regular Spark-RAPIDS casts are not implemented by simply delegating every case
to cuDF's generic cast. The plugin owns Spark semantics and uses a mix of:

- cuDF Java/libcudf primitives such as `ColumnView.castTo`, `asStrings`, and
  timestamp parsing helpers.
- `spark-rapids-jni` helpers such as `CastStrings`, `DecimalUtils`,
  `Arithmetic`, and `GpuTimeZoneDB`.
- Scala-side orchestration for null handling, ANSI errors, overflow checks,
  decimal rounding, complex types, and recursive nested casts.

The core implementation is:

```text
sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuCast.scala
```

Examples:

- Simple compatible casts can call `input.castTo(...)`.
- String to integer calls `CastStrings.toInteger(...)` from
  `spark-rapids-jni`, not plain cuDF.
- String to date/timestamp calls `CastStrings.toDate(...)` or
  `CastStrings.toTimestamp(...)`.
- Decimal casts use Spark-specific rounding and bounds checks before the final
  cuDF cast.
- Arrays, maps, and structs recursively call `GpuCast.doCast` on children.

Conclusion for Variant:

If cuDF keeps `cast_variant` as exact decode, Spark-RAPIDS needs either a
Spark-specific Variant cast helper in `spark-rapids-jni`, or very conservative
fallback rules. It would be consistent with regular Spark-RAPIDS cast handling
to implement Spark-specific Variant cast behavior outside core cuDF, but the
best long-term API would be a cuDF mode/helper designed for Spark-style casts.

## Implementation Strategy

Use feature gates. Do not try to enable everything in the first pass.

### Phase 0: Baseline and Branch Alignment

Goal: make sure the three repos agree on the same cuDF API shape.

Repos:

- cuDF branch or PR: `rapidsai/cudf#22416` or the requested testing branch.
- private JNI repo: referred to here as `spark-rapids-private`; this is the
  repo that builds the `spark-rapids-jni` artifact.
- this repo: `spark-rapids`.

Checklist:

1. Confirm which cuDF branch is the testing target.
2. Confirm whether `#22416` or `#22434` API shape is being used.
3. Pin the cuDF commit SHA in notes and build scripts.
4. Confirm the public include path:

```cpp
#include <cudf/io/experimental/variant.hpp>
```

5. Confirm the namespace:

```cpp
cudf::io::parquet::experimental
```

6. Confirm return types:

- `get_variant_field` returns `LIST<UINT8>`.
- `cast_variant` accepts `LIST<UINT8>`.
- `extract_variant_field` accepts Variant struct and returns typed column.

Exit criteria:

- A tiny C++ or JNI smoke test can link against the selected cuDF build and
  call `extract_variant_field`.

### Phase 1: Add JNI Bridge in `spark-rapids-private`

Goal: expose the cuDF Variant primitive to the Spark plugin.

Suggested files:

```text
src/main/java/com/nvidia/spark/rapids/jni/VariantUtils.java
src/main/cpp/src/VariantUtilsJni.cpp
src/main/cpp/CMakeLists.txt
```

Use the local prototype as a starting point:

```text
variant_prototype/spark_rapids_jni/java/VariantUtils.java
variant_prototype/spark_rapids_jni/cpp/VariantUtilsJni.cpp
```

Important update to the prototype:

The prototype was written against an older API shape. For `#22416`, prefer
passing a full Variant struct column handle to JNI so parent nullability is not
lost.

Recommended Java surface:

```java
public final class VariantUtils {
  public static ColumnVector getVariantFieldValue(ColumnView variantStruct, String path);

  public static ColumnVector castVariantValue(ColumnView valueBytes, DType targetType);

  public static ColumnVector extractVariantField(
      ColumnView variantStruct, String path, DType targetType);

  public static boolean isAvailable();
}
```

Recommended native surface:

```cpp
JNIEXPORT jlong JNICALL
Java_com_nvidia_spark_rapids_jni_VariantUtils_getVariantFieldValueNative(...);

JNIEXPORT jlong JNICALL
Java_com_nvidia_spark_rapids_jni_VariantUtils_castVariantValueNative(...);

JNIEXPORT jlong JNICALL
Java_com_nvidia_spark_rapids_jni_VariantUtils_extractVariantFieldNative(...);
```

JNI implementation notes:

- Use existing `spark-rapids-jni` idioms from `JSONUtilsJni.cpp`.
- Use `CATCH_STD` or the repo's current exception wrapper.
- Use `auto_set_device`.
- Validate input handles.
- Include `cudf/io/experimental/variant.hpp`.
- Call `cudf::io::parquet::experimental::*`.
- Add a compile-time flag such as `VARIANT_EXTRACT_ENABLED` if the cuDF API is
  still experimental or not always available.
- Do not reconstruct the Variant struct from separate metadata/value children
  unless you also preserve the parent null mask. Passing the struct view is
  safer.

Minimal C++ call:

```cpp
auto const variant_view =
    *reinterpret_cast<cudf::column_view const*>(variant_struct_handle);

auto result = cudf::io::parquet::experimental::extract_variant_field(
    variant_view,
    std::string{path.get()},
    cudf::data_type{static_cast<cudf::type_id>(cudf_type_id)},
    cudf::get_default_stream(),
    rmm::mr::get_current_device_resource_ref());
```

Initial JNI tests:

1. `VariantUtils.isAvailable()`.
2. Extract `$.a` from `{"a":"42"}` as `STRING`.
3. Extract missing field `$.b` as `STRING` and verify null.
4. Extract `$.a` from `{"a":42}` using the exact physical type Spark stores.
5. Confirm `castVariantValue` mismatch returns null.

Exit criteria:

- `spark-rapids-jni` builds a local jar containing `VariantUtils`.
- A unit test or smoke test can call `VariantUtils.extractVariantField`.

### Phase 2: Build and Install the JNI Jar

Goal: install a local `spark-rapids-jni` artifact that this repo can consume.

The exact command depends on the private repo layout, but the flow is:

```bash
cd /path/to/spark-rapids-private

# Build/install the JNI artifact for the CUDA version used by this repo.
# Use the private repo's current documented profile names.
mvn clean install -DskipTests
```

If the private repo supports classifier/profile selection, build the matching
CUDA classifier. This repo currently uses:

```text
spark-rapids-jni.version = 26.04.0-SNAPSHOT
jni.classifier = ${cuda.version}
```

from `pom.xml`.

After installing locally, verify:

```bash
ls ~/.m2/repository/com/nvidia/spark-rapids-jni/
```

If you publish a custom version, for example:

```text
26.08.0-SNAPSHOT-variant
```

use that exact version when building this repo.

Exit criteria:

- The local Maven repo contains the JNI jar with the expected classifier.
- `jar tf` shows `com/nvidia/spark/rapids/jni/VariantUtils.class`.

Example verification:

```bash
jar tf ~/.m2/repository/com/nvidia/spark-rapids-jni/<version>/spark-rapids-jni-<version>-cuda12.jar \
  | grep VariantUtils
```

### Phase 3: Point `spark-rapids` at the Local JNI Jar

Goal: build this repo against the local JNI artifact.

Option A: pass the version at build time:

```bash
cd /home/nartal/spark-rapids-26.06/spark-rapids

mvn package \
  -pl dist -am \
  -Dbuildver=400 \
  -Dspark-rapids-jni.version=<local-jni-version> \
  -Ddist.jar.compress=false
```

Option B: temporarily edit `pom.xml`:

```xml
<spark-rapids-jni.version>26.08.0-SNAPSHOT-variant</spark-rapids-jni.version>
```

For faster iteration after the first full build, use the existing local build
shortcut:

```bash
mvn package -pl dist -PnoSnapshots \
  -Ddist.jar.compress=false \
  -Drapids.jni.unpack.skip \
  -Dspark-rapids-jni.version=<local-jni-version>
```

or:

```bash
./build/buildall --rebuild-dist-only \
  --option="-Ddist.jar.compress=false -Drapids.jni.unpack.skip -Dspark-rapids-jni.version=<local-jni-version>"
```

Exit criteria:

- The built `rapids-4-spark` jar includes or unpacks the intended JNI jar.
- `spark-shell` with this plugin can load `VariantUtils.isAvailable()`.

### Phase 4: Add Minimal Spark Plugin Surface

Goal: get the narrowest safe GPU expression working.

Start from:

```text
variant_prototype/spark_rapids/GpuVariantGet.scala
```

Likely target path:

```text
sql-plugin/src/main/spark400/scala/com/nvidia/spark/rapids/GpuVariantGet.scala
```

Depending on supported Spark versions, add the shim marker for:

```text
400, 401, 402, 411
```

Add or update type support:

```text
sql-plugin/src/main/scala/com/nvidia/spark/rapids/TypeChecks.scala
```

Add:

- `TypeEnum.VARIANT`
- `TypeSig.VARIANT`
- project/read support where needed

Register `VariantGet` in the Spark 4.x expression rules near the JSON or cast
rules.

Initial planning restrictions:

- Only `try_variant_get`, not strict `variant_get`.
- Only literal paths.
- Only object paths supported by cuDF `#22416`.
- Only target types supported by exact decode.
- CPU fallback for anything else.

Initial target type choices:

- Safest first: `StringType` only.
- Next: exact integer widths if the data is known to be stored as that width.

Be careful with `IntegerType`. Spark CPU accepts many stored Variant primitive
types for target `int`; cuDF `cast_variant(INT32)` only accepts stored INT32.
Data produced by `parse_json('{"a":42}')` may be encoded as a narrower integer,
so GPU may return null where CPU returns `42`.

Exit criteria:

- Spark plan contains the GPU Variant expression for the supported case.
- Unsupported cases have clear fallback reasons.

### Phase 5: First Feature Gate - String Field Extraction

Goal: validate extraction when the stored Variant value is physically a string.

Spark shell setup:

```scala
import spark.implicits._

val path = "/tmp/variant_string_extract"

spark.conf.set("spark.sql.parquet.annotateVariantLogicalType", "true")
spark.conf.set("spark.rapids.sql.enabled", "false")

Seq("""{"a":"42"}""")
  .toDF("json")
  .selectExpr("parse_json(json) AS v")
  .write
  .mode("overwrite")
  .parquet(path)
```

CPU result:

```scala
val cpu = spark.read.parquet(path)
  .selectExpr("variant_get(v, '$.a', 'string') AS a")

cpu.explain(true)
cpu.show(false)
```

Expected:

```text
+---+
|a  |
+---+
|42 |
+---+
```

GPU result:

```scala
spark.conf.set("spark.rapids.sql.enabled", "true")
spark.conf.set("spark.rapids.sql.explain", "ALL")

val gpu = spark.read.parquet(path)
  .selectExpr("try_variant_get(v, '$.a', 'string') AS a")

gpu.explain(true)
gpu.show(false)
```

Expected:

```text
+---+
|a  |
+---+
|42 |
+---+
```

Gate:

- Pass CPU/GPU result comparison.
- Confirm the explain output shows the GPU expression.
- Confirm missing key returns null:

```scala
spark.read.parquet(path)
  .selectExpr("try_variant_get(v, '$.missing', 'string') AS missing")
  .show(false)
```

### Phase 6: Second Feature Gate - Exact Integer Decode

Goal: validate integer extraction only when the stored physical width matches
the requested cuDF target.

This is tricky with `parse_json`, because Spark can choose the narrowest integer
encoding. A value such as `42` may be stored as INT8, not INT32.

Use this phase to prove the limitation, not hide it.

CPU semantic check:

```scala
spark.sql("""
  SELECT
    try_variant_get(parse_json('{"a":42}'), '$.a', 'tinyint') AS as_tinyint,
    try_variant_get(parse_json('{"a":42}'), '$.a', 'int') AS as_int,
    try_variant_get(parse_json('{"a":42}'), '$.a', 'bigint') AS as_bigint,
    try_variant_get(parse_json('{"a":42}'), '$.a', 'string') AS as_string
""").show(false)
```

Spark CPU returns all compatible casts.

Expected cuDF `#22416` behavior:

- Only the exact stored integer width decodes.
- Other integer targets return null.
- String target for stored integer returns null.

Gate:

- Only enable GPU for integer cases if the semantics are proven safe.
- Otherwise keep integer typed `try_variant_get` on CPU until Spark-style
  Variant cast exists.

### Phase 7: Third Feature Gate - Nested Object Paths

Goal: enable paths such as `$.user.profile.name`.

`#22416` supports chained object path traversal directly, so the JNI can call:

```cpp
extract_variant_field(variant_column, "$.user.profile.name", STRING)
```

Spark shell data:

```scala
val nestedPath = "/tmp/variant_nested_extract"

spark.conf.set("spark.rapids.sql.enabled", "false")

Seq("""{"user":{"profile":{"name":"alice","age":42}}}""")
  .toDF("json")
  .selectExpr("parse_json(json) AS v")
  .write
  .mode("overwrite")
  .parquet(nestedPath)
```

Test:

```scala
spark.conf.set("spark.rapids.sql.enabled", "true")

spark.read.parquet(nestedPath)
  .selectExpr("try_variant_get(v, '$.user.profile.name', 'string') AS name")
  .show(false)
```

Expected:

```text
+-----+
|name |
+-----+
|alice|
+-----+
```

Gate:

- Nested string extraction passes.
- Missing intermediate returns null.
- Non-object intermediate returns null.

### Phase 8: Cast Semantics Follow-up

Goal: close the `cast_variant` gap.

There are two viable implementation paths.

Preferred cuDF/API request:

- Add a Spark-style cast mode or companion API, for example:

```cpp
cast_variant(values, desired_type, cast_policy::spark, fail_on_error, timezone, spark_version)
```

or:

```cpp
spark_cast_variant(values, desired_type, options)
```

Minimum Spark requirements:

- Integer widening and narrowing with overflow behavior.
- String to integer, float, decimal, boolean, date, timestamp.
- Numeric to string.
- Object/array to JSON string.
- `try_variant_get`: return null on cast failure.
- `variant_get`: throw on cast failure.
- Timezone-aware timestamp casts.

Spark-RAPIDS-only fallback path:

- Add `VariantUtils.castVariantSpark` in `spark-rapids-jni`.
- Decode Variant type tags and dispatch to Spark-compatible cast kernels.
- Reuse or mirror existing JNI helpers where possible:
  - `CastStrings.toInteger`
  - `CastStrings.toDecimal`
  - `CastStrings.toDate`
  - `CastStrings.toTimestamp`
  - decimal bounds helpers

Gate:

- The `"42"` string-to-int test returns `42` on GPU:

```scala
spark.read.parquet("/tmp/variant_string_extract")
  .selectExpr("try_variant_get(v, '$.a', 'int') AS a")
  .show(false)
```

Expected after Spark-style cast support:

```text
+---+
|a  |
+---+
|42 |
+---+
```

Before that support exists, this should remain CPU fallback. It should not run
on GPU and return null.

### Phase 9: Unsupported Features to Keep on CPU Initially

Keep these on CPU until cuDF/JNI support is explicit:

- Strict `variant_get` with throw-on-cast-failure.
- `variant_get` without a target type, returning `VariantType`.
- Array indexing:
  - `$.a[0]`
  - `$[0]`
  - `$.items[0].price`
- Quoted/bracket keys:
  - `$['a.b']`
- Wildcards:
  - `$.a[*]`
- `variant_explode`
- `parse_json` on GPU.
- `to_json` from Variant.
- Variant Parquet write.
- Shredded Variant typed-child reads and predicate pushdown.

## One-Feature-at-a-Time Test Matrix

Use this as the working checklist.

| Phase | Feature | CPU expected | GPU expected | Move on when |
|---:|---|---|---|---|
| 1 | JNI availability | n/a | `VariantUtils.isAvailable=true` | JNI jar loads |
| 2 | `$.a` string extract | `42` | `42` | plan shows GPU expression |
| 3 | missing string key | `NULL` | `NULL` | nulls match |
| 4 | nested string path | `alice` | `alice` | nulls at each depth match |
| 5 | exact integer decode | matching exact width | matching exact width | no accidental Spark-cast mismatch |
| 6 | string `"42"` to int | `42` | CPU fallback or `42` | never GPU-null silently |
| 7 | strict `variant_get` bad cast | throw | CPU fallback or throw | no silent null |
| 8 | array path | value/null | CPU fallback | no malformed-path crash in GPU path |

## Spark Shell Snippets

### Known Key as Sub-Variant

```scala
val df = spark.read.parquet(path)
df.selectExpr("variant_get(v, '$.a') AS a").show(false)
```

Expected:

```text
+----+
|a   |
+----+
|"42"|
+----+
```

### Known Key as String

```scala
df.selectExpr("variant_get(v, '$.a', 'string') AS a").show(false)
```

Expected:

```text
+---+
|a  |
+---+
|42 |
+---+
```

### Object Key/Value Printing

```scala
spark.sql("""
  SELECT e.pos, e.key, e.value
  FROM (
    SELECT parse_json('{"a":"42","b":1,"c":true}') AS v
  ) t
  LATERAL variant_explode(t.v) e
""").show(false)
```

Expected shape:

```text
+---+---+-----+
|pos|key|value|
+---+---+-----+
|0  |a  |"42" |
|1  |b  |1    |
|2  |c  |true |
+---+---+-----+
```

### CPU Proof of Spark-Style Variant Cast

```scala
spark.sql("""
  SELECT
    try_variant_get(parse_json('{"a":"42"}'), '$.a', 'int') AS try_ok,
    variant_get(parse_json('{"a":"42"}'), '$.a', 'int') AS strict_ok,
    try_variant_get(parse_json('{"a":"abc"}'), '$.a', 'int') AS try_bad
""").show(false)
```

Expected:

```text
+------+---------+-------+
|try_ok|strict_ok|try_bad|
+------+---------+-------+
|42    |42       |NULL   |
+------+---------+-------+
```

Strict failure:

```scala
spark.sql("""
  SELECT variant_get(parse_json('{"a":"abc"}'), '$.a', 'int') AS strict_bad
""").show(false)
```

Expected:

```text
INVALID_VARIANT_CAST
```

## Success Criteria for the First PR Slice

The first Spark-RAPIDS slice should be considered successful when:

1. The plugin can read a Parquet Variant column on GPU.
2. `try_variant_get(v, '$.a', 'string')` runs on GPU for object paths.
3. Missing fields and missing intermediates match Spark CPU null behavior.
4. Unsupported casts fall back to CPU with clear explain text.
5. The `"42"` to int case does not silently produce GPU null.

That last point is important. Until Spark-style Variant cast exists, a null GPU
result for `"42"` to int is a correctness bug if the expression was planned on
GPU. The safe behavior is CPU fallback.

## Open Requests for cuDF

These are the requests most important for Spark:

1. Clarify whether `cast_variant` is intended to be exact decode or Spark-style
   cast. If exact decode is intended, consider renaming or documenting it very
   clearly.
2. Add Spark-style Variant cast support or an explicit cast mode.
3. Add array indexing and quoted/bracket key path support.
4. Add support for returning a full sub-Variant, not only raw value bytes.
5. Add shredded Variant typed-child awareness.
6. Add a batch extraction API for multiple fields from the same Variant column.
7. Add a strict/error mode or error-row reporting suitable for `variant_get`.
8. Add GPU `parse_json` to Variant.
9. Add Variant-to-JSON support for `to_json`.
10. Add Variant Parquet write support when read/extract is stable.

## References in This Repo

- `variant_type_primer.md`
- `variant_data_type_deep_dive.md`
- `libcudf_variant_feedback.md`
- `variant_prototype/README.md`
- `variant_prototype/examples/spark_shell_variant_example.scala`
- `variant_prototype/spark_rapids_jni/java/VariantUtils.java`
- `variant_prototype/spark_rapids_jni/cpp/VariantUtilsJni.cpp`
- `variant_prototype/spark_rapids/GpuVariantGet.scala`
- `variant_prototype/cudf_tests/variant_cast_semantics_test.cpp`
