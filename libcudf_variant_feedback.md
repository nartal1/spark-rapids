# libcudf Variant Extraction Branch — spark-rapids Feedback

Review notes for the libcudf [`variant-extraction-gpu` branch](https://github.com/rapidsai/cudf/compare/main...vuule:cudf:variant-extraction-gpu)
from a spark-rapids consumer perspective. Captures what is usable today,
what we will still need, and open questions for the libcudf team.

Reviewed at commit from 2026-04-15 (head of the branch at review time). The
branch ships an implementation report at
[`docs/variant_extraction_report.md`](https://github.com/vuule/cudf/blob/variant-extraction-gpu/docs/variant_extraction_report.md)
which is the libcudf team's own description of scope, kernel design, and
benchmark results. Our findings below cross-reference that report where
relevant. Notably, the report contains **no "limitations", "scope",
"non-goals", or "future work" section** — so the libcudf team's intent for
target-type expansion and cast semantics is currently undocumented. This
motivates Q0 in §5.

A concrete, compileable set of test cases demonstrating the §3.6 mismatch —
plus skeleton JNI and spark-rapids Scala code — lives in this repo's
[`variant_prototype/`](variant_prototype/) directory. In particular,
`variant_prototype/cudf_tests/variant_cast_semantics_test.cpp` contains 11
GoogleTest-style tests that follow the branch's own fixture style: 2 control
tests (exact-match INT32/STRING) that pass today, plus 9 mismatch tests that
fail against the current branch and concretely reproduce the cast-semantics
gap. Ready to open as a PR once we align on the design-level question in
Q0/§3.6.

---

## 0. Summary — one verified semantic mismatch

After source-level verification against both the cuDF branch and Apache Spark
(citations in §3.6):

**Confirmed finding:** `cast_variant` is **decode-if-exact-match**, whereas Spark's
`variant_get` / `try_variant_get` perform a full cast (delegating to Spark's
general `Cast` expression). Even within the two target types cuDF currently
supports — INT32 and STRING — the implemented semantics return NULL for many
inputs that Spark would return a converted value for. Concrete failure matrix in
§3.6.

**Null-handling behavior is verified identical** between cuDF and Spark for all
three null-ish cases (missing field, SQL NULL input, stored variant-null primitive):
both systems collapse them to SQL NULL under `variant_get(..., target_type)`.

**Claims flagged as unverified** (Spark reference behavior not yet checked):
handling of malformed metadata and handling of duplicate field names. These
are questions for the libcudf team rather than confirmed gaps — marked in §3
accordingly.

---

## 1. What the branch delivers

### Public API — `namespace cudf::io::parquet` in `cpp/include/cudf/io/variant.hpp`

- `get_variant_field(variant_column, field_name, stream, mr)` — extracts the
  raw Variant-encoded bytes of a top-level field; returns a new VARIANT struct
  column (metadata + field's value bytes). Supports chained calls for nested
  object traversal.
- `cast_variant(variant_column, desired_type, stream, mr)` — decodes a VARIANT
  struct column's `value` blobs to a typed cuDF column. Target types supported:
  **INT32** and **STRING** only.
- `extract_variant_field(variant_column, field_name, desired_type, stream, mr)` —
  convenience wrapper equivalent to `cast_variant(get_variant_field(...))`.

### Parquet reader changes

- Recognizes the VARIANT logical type; adds VARIANT/GEOMETRY/GEOGRAPHY/UUID/FLOAT16
  to `parquet_schema.hpp` (additive enum entries).
- Materializes VARIANT columns as `struct<list<uint8> metadata, list<uint8> value>`
  via `PARQUET_COLUMN_BUFFER_FLAG_VARIANT_BINARY`.

### Input contract

Variant is modeled as `struct<list<uint8>, list<uint8>>`. Null struct row →
null output. Dictionary lookup is a linear scan with length-prefix early-exit;
the metadata `sorted_strings` flag is not yet used to switch to binary search.

### Implementation highlights

- Two-pass kernels (sizing + copy), one thread per row, block size 256.
- Uses `create_structs_hierarchy` instead of `make_structs_column` to avoid a
  deep-copy of the metadata child when outputs are partially null. **Contract:
  struct-level null mask is authoritative; children are not masked.** Downstream
  consumers must honor this.
- Device-side parse helpers:
  `device_read_uint_le`, `device_find_key_in_metadata`, `device_locate_object_field`,
  `device_decode_int32`, `device_decode_string_info`.

### Tests & benchmarks

- 19 unit tests in `variant_extract_test.cpp` + 2 Parquet round-trip tests.
- Apache parquet-testing fixtures: `primitive_int32`, `short_string`,
  `primitive_string`, `object_primitive`, `object_nested`.
- NVBench divergence benchmarks across 5 scenarios × {first-key, last-key} ×
  {32K, 128K, 512K, 2M} rows.

### Reference performance (A100, 2M rows)

- `cast_variant(int32)` standalone: **0.099 ms**.
- `get_variant_field` — 5-key dict, first key: **0.73 ms**.
- `get_variant_field` — 50-key dict, last key: **10.66 ms** (worst observed).
- Scaling is ~linear above 128K rows.

---

## 2. What is NOT in the branch (spark-rapids gap analysis)

### 2.1 Target type coverage (highest-priority gap)

`cast_variant` supports only INT32 and STRING. Spark's `variant_get(v, path, type)`
is commonly invoked with:

- **int8, int16, int64** (tinyint/smallint/bigint projections)
- **float, double** (numeric analytics)
- **decimal (decimal4/8/16)** — heavy use in financial workloads
- **boolean** — predicate pushdown target
- **date, timestamp, timestamp_ntz, time_ntz** — event-time extraction
- **binary** — raw byte extraction

Without these, any realistic query will fall back to CPU repeatedly.

**In addition**, even within the two target types that are supported, cuDF's
semantics are decode-if-exact-match rather than cast — meaning INT32 and STRING
work only when the stored Variant value is literally INT32 or STRING on the wire.
This is the most impactful finding of the review; full analysis in §3.6.

### 2.2 Array-element access

`get_variant_field` returns null for arrays. Spark supports `$[n]` in JSONPath,
so paths like `$.events[0].type` cannot be evaluated on GPU today.

### 2.3 JSONPath compilation

libcudf takes a single `std::string` field name. Spark ships a pre-parsed
JSONPath expression; the spark-rapids / JNI layer will have to split into
segments and emit N chained libcudf calls. That is workable but:

- Each chained call deep-copies the metadata child (see §3.3 below), so N calls
  = N × metadata copies of bandwidth overhead.
- Array indexing requires a separate call that does not exist.
- Wildcard (`[*]`) is not supported anywhere (and not in the Spark Variant spec).

### 2.4 `parse_json` / `try_parse_json` → Variant

There is no GPU path for parsing JSON strings into Variant. This is the most
common ingest-time operation in Spark Variant workflows — writing Parquet with
a Variant column typically goes through `parse_json`. Without it, ingest
pipelines stay on CPU.

### 2.5 Variant → JSON (`to_json`)

Not supported. Needed for debugging, display, and round-trip workflows.

### 2.6 Shredded read and write

- Read: no recognition of `typed_value` sub-groups inside a shredded VARIANT
  group. Predicate pushdown to typed columns — the headline performance win of
  shredding (30× vs JSON strings per Databricks benchmarks) — is not accessible.
- Write: no Variant writer at all. Writing a VARIANT Parquet column from a
  spark-rapids query stays on CPU today.

### 2.7 Native `binary` cuDF type

VARIANT data is materialized as `list<uint8>`. The cuDF list column carries a
32-bit offset, capping total binary data per column at ~2 GB. Production
workloads with long Variant values (large nested objects) will hit this.

### 2.8 Other Spark Variant SQL functions

No GPU support for: `variant_explode`, `variant_explode_outer`, `is_variant_null`,
`schema_of_variant`, `schema_of_variant_agg`, `to_variant_object`.

### 2.9 Batch extraction (`variant_get_many`)

Queries often project many fields of the same Variant (`SELECT v.a, v.b, v.c FROM t`).
Each is a separate kernel launch today. A batch API that amortizes metadata
parsing across N target fields was noted as an open question in our internal
design doc.

### 2.10 Strict-mismatch mode

libcudf returns null on type mismatch. This matches Spark's `try_variant_get`
exactly but not `variant_get` (which throws). We currently have to validate on
the host after extraction, which doubles the work.

---

## 3. Observations / design concerns

### 3.1 Namespace placement

API lives in `cudf::io::parquet`. Spark's `VariantType` is used in-memory
(`VariantVal`) independent of Parquet — e.g., for `parse_json` results passed
between operators without touching storage. A `cudf::variant` namespace (or
`cudf::strings`-style top-level module) may be a more natural long-term home.

### 3.2 Dictionary lookup is linear scan

`device_find_key_in_metadata` is a length-prefix-early-exit linear scan.
The metadata header has a `sorted_strings` flag that the implementation
ignores. For large dictionaries (50 keys × 2M rows → 10.66 ms worst case on
A100), a branch on `sorted_strings` to enter a binary-search path could close
most of the gap versus `uniform_small` (0.85 ms).

### 3.3 Metadata duplication across chained calls

`get_variant_field` deep-copies the metadata child into each result column so
a subsequent call can resolve keys. For a 4-level chained path (`$.a.b.c.d`),
that is 4 copies of the same dictionary. A `column_view`-based shared-metadata
contract (or a returned sub-column that aliases the parent's metadata) would
cut metadata bandwidth to 1×.

### 3.4 Null-children contract

The `create_structs_hierarchy` optimization relies on downstream code
treating the struct-level null mask as authoritative and not inspecting
child masks. This is not the libcudf norm for every consumer. Explicit
documentation on the function contract would help avoid subtle bugs
in code that copies the result.

The libcudf report §5.3 confirms this is intentional design, not merely a
performance tweak: *"The struct-level null mask is authoritative; consumers
check it before accessing child data. This is a correctness-preserving
optimization that benefits any call to `get_variant_field` that produces
nulls."* Because this invariant lives in the implementation report rather
than the public header or an API doc comment, any consumer (including our
spark-rapids-jni bridge) who hasn't read the report can get it wrong. This
is why the contract is called out in Q7 of §5 — it should be promoted into
the public-header docstring.

### 3.5 Silent-null on malformed metadata

Invalid/truncated metadata yields a null output (cuDF test
`InvalidMetadataYieldsNull` asserts this is the intended design). For ingest
validation and debugging, a "strict" mode that returns a diagnostic (or marks
the row via a separate error column) would help — currently a corrupted file
and a missing key are indistinguishable.

> **Unverified against Spark.** We have not confirmed whether Spark's reference
> implementation throws or silently returns NULL on malformed Variant metadata.
> This is a question for the libcudf team, not a confirmed mismatch.

### 3.6 `cast_variant` is decode-if-exact-match, not cast — VERIFIED MISMATCH

This is the single most impactful finding of the source-level review.

**cuDF behavior** (from
[`variant_extract.cu`](https://github.com/vuule/cudf/blob/variant-extraction-gpu/cpp/src/io/variant_extract.cu)):

- `device_decode_int32` requires the value header to be **exactly**
  `basic_type == 0 && header6 == 5`. Any other header → failure → NULL.
- `device_decode_string_info` requires **exactly** `basic_type == 1`
  (short string) or `basic_type == 0 && header6 == 16` (long string). Any other
  header → failure → NULL.
- The branch's own test
  [`WrongDesiredTypeYieldsNull`](https://github.com/vuule/cudf/blob/variant-extraction-gpu/cpp/tests/io/variant_extract_test.cpp)
  explicitly asserts this is the intended behavior ("Object holds INT32 at 'x';
  request STRING" → NULL). The implementation report
  [`docs/variant_extraction_report.md`](https://github.com/vuule/cudf/blob/variant-extraction-gpu/docs/variant_extraction_report.md)
  lists this test in §3.1 without flagging it as a limitation, confirming the
  semantics are intentional.

**Spark behavior** (from
[`variantExpressions.scala`](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/variant/variantExpressions.scala)
and
[`VariantUtil.java`](https://github.com/apache/spark/blob/master/common/variant/src/main/java/org/apache/spark/types/variant/VariantUtil.java)):

- `VariantGet.cast()` (lines 541-630) is the single entry point for all type
  conversion in `variant_get` / `try_variant_get`.
- For Object/Array → String, it serializes to JSON (lines 554-557).
- For all other target types, **it delegates to Spark's general `Cast` expression**
  at line 597:
  ```scala
  val result = Cast(input, dataType, castArgs.zoneStr, EvalMode.TRY).eval()
  ```
- `VariantGet` and `TryVariantGet` differ only in the `failOnError` flag
  (lines 429-430); both run the cast under `EvalMode.TRY`, then `VariantGet`
  throws if the result is NULL, `TryVariantGet` returns NULL.
- `VariantUtil.getLong()` (lines 400-420) reads `INT1`, `INT2`, `INT4`, `INT8`
  uniformly with sign-extension via `readLong(...)`, so any stored integer width
  is promoted to Java `long` before `Cast` ever sees it.

**Impact matrix** — the two target types cuDF claims to support:

| Stored Variant type | Target | Spark `variant_get` | Spark `try_variant_get` | cuDF `cast_variant` | Match? |
|---|---|---|---|---|---|
| `int32(42)` | INT32 | `42` | `42` | `42` | ✓ |
| `int8(42)` | INT32 | `42` (widening) | `42` | **NULL** | ✗ |
| `int16(42)` | INT32 | `42` (widening) | `42` | **NULL** | ✗ |
| `int64(42)` | INT32 | `42`; throws on overflow | `42`; NULL on overflow | **NULL** always | partial |
| `double(42.0)` | INT32 | `42` (numeric cast) | `42` | **NULL** | ✗ |
| `string("42")` | INT32 | `42` (parse) | `42` | **NULL** | ✗ |
| `boolean(true)` | INT32 | `1` | `1` | **NULL** | ✗ |
| `string("hi")` | STRING | `"hi"` | `"hi"` | `"hi"` | ✓ |
| `int32(42)` | STRING | `"42"` (stringify) | `"42"` | **NULL** | ✗ |
| `boolean(true)` | STRING | `"true"` | `"true"` | **NULL** | ✗ |
| `double(3.14)` | STRING | `"3.14"` | `"3.14"` | **NULL** | ✗ |

**Why this matters in practice:** `parse_json` chooses the narrowest integer
encoding that fits. So `parse_json('{"x":42}')` stores `x` as `int8`, and any
`variant_get(..., 'int')` on the GPU would return NULL today — even though the
CPU path returns 42. This means queries that look like they should work silently
produce all-null columns on GPU.

**What we need from the libcudf team (design-level question, not a patch):**

Is `cast_variant` intended to have cast semantics (matching Spark's
`variant_get`) or decode-if-exact-match semantics (the current behavior)?
The answer determines the integration strategy:

- **If cast semantics are in-scope for `cast_variant`** — the existing three
  functions are directly usable for Spark (modulo the missing target types).
- **If decode-if-match is the intended contract** — a separate function (e.g.,
  `variant_get_casted` or a mode flag) is needed, otherwise the casting must
  happen outside cuDF, significantly increasing host-side work and JNI traffic.

**Verified identical behavior (no mismatch)**:

| Situation | Spark | cuDF | Match? |
|---|---|---|---|
| Field is missing | NULL | NULL | ✓ |
| Input row is SQL NULL | NULL | NULL | ✓ |
| Field holds Variant null primitive (0x00 header) | NULL | NULL | ✓ |

Chaining `get_variant_field` for nested paths is also correct — the branch
deep-copies the parent's metadata into the result, preserving the dictionary
needed to resolve keys at deeper levels (confirmed by the branch's
`ApacheNestedGetVariantField` test).

### 3.7 API naming: `cast_variant` is misleading

Given the verified semantics in §3.6, the name `cast_variant` suggests cast
behavior that the function does not actually provide. A reader familiar with
SQL CAST or Spark's `variant_get(..., type)` will expect widening/narrowing
and cross-type conversion. What's implemented is closer to "decode the value
iff its stored header matches the target exactly."

Two ways to resolve the name ↔ behavior mismatch without breaking downstream
callers:

1. **Rename** to `decode_variant` (or similar), with a doxygen note that
   explicitly states "this is not a cast; results are NULL when the stored
   type does not match the target exactly." Keeps the implementation intact
   but sets correct expectations.
2. **Extend to actual cast semantics** (the preferred resolution and the P0
   ask in §4.0) so the name matches the behavior.

Either is fine from the spark-rapids side. The current state — name implies
cast, behavior decodes-if-match — is the worst option: it survives code
review because reviewers trust the name, and the mismatch only manifests as
silently-wrong results at runtime.

---

## 4. Prioritized asks for the libcudf team

Priority reflects what unblocks the broadest set of spark-rapids Variant queries.

### P0 — blocks practical adoption

0. **Cast semantics decision** (verified mismatch, §3.6). Either make
   `cast_variant` perform a true cast (matching Spark's `variant_get`) or add a
   companion function that does. Without this, even the supported INT32/STRING
   targets silently return NULL for the common case of narrow-integer-encoded
   fields produced by `parse_json`.
1. **Expand `cast_variant` target types** to at minimum: INT64, DOUBLE, BOOLEAN,
   DECIMAL32/64/128, TIMESTAMP_MICROSECONDS, TIMESTAMP_DAYS. INT8/INT16 and
   FLOAT32 are lower priority. BINARY is useful but not blocking. Ideally this
   is combined with the cast-semantics fix above so that each new target type
   also accepts cross-type inputs.
2. **Array-element access**: either `get_variant_element(variant_col, index, ...)`
   or an overload of `get_variant_field` that accepts a numeric index.
3. **Coordinated version bump plan**: since the `parquet_schema.hpp` enum and
   reader changes ship together, we need a target cuDF release version we can
   pin spark-rapids-jni to.

### P1 — performance / production-readiness

4. **`variant_get_many` batch API**: accept a vector of field names and target
   types, amortize metadata scan once per row.
5. **Binary-search path** when metadata `sorted_strings` bit is set.
6. **Shared-metadata contract** in chained calls (avoid 4× metadata copy for
   4-level paths).
7. **Native cuDF `binary` type** with 64-bit offsets to lift the 2 GB/column cap.

### P2 — needed before full feature parity

8. **`parse_json` → Variant on GPU** (biggest ingest-path unlock).
9. **Shredded read**: recognize `typed_value` sub-groups and expose them as
   typed columns so the plugin can push predicates.
10. **Strict-mismatch variant** (`variant_get_strict` or error column) alongside
    the existing null-on-mismatch path.

### P3 — nice to have

11. **`to_json` from Variant** for debug / round-trip.
12. **`variant_explode` / `variant_explode_outer`** — 1 input row → N output rows
    (object entries or array elements).
13. **`to_variant_object`**: struct/array/map → Variant for round-trip and
    `INSERT INTO variant_col SELECT struct(...)` workloads.
14. **Namespace move** from `cudf::io::parquet` to a storage-agnostic home.

---

## 5. Questions to raise with the libcudf team

Bring these to the next sync:

0. **Cast semantics** *(top priority — verified mismatch, §3.6)*. Is
   `cast_variant` intended to be cast semantics (matching Spark's
   `variant_get`) or decode-if-exact-match semantics (the current behavior)?
   If the latter, is there appetite for adding a companion `variant_get_casted`
   (or a mode flag) that integrates Spark-style widening/parsing? Branch test
   `WrongDesiredTypeYieldsNull` suggests the current behavior is intentional —
   we need confirmation before deciding where to place the cross-type casting
   logic.
1. **Target-type roadmap.** Which of {INT8, INT16, INT64, FLOAT, DOUBLE,
   BOOLEAN, DECIMAL32/64/128, DATE (INT32 days), TIMESTAMP_MICROS,
   TIMESTAMP_NANOS, TIME_NTZ, BINARY, UUID} are on the near-term plan, and
   which quarter?
2. **Array access.** Design preference: separate `get_variant_element(col, index)`
   vs. an index overload of `get_variant_field` vs. a unified `get_variant_path`
   accepting a mixed list of segments? Is there prior art in Velox or elsewhere
   that informs the choice?
3. **Strict vs. permissive.** Is a strict-mismatch mode (throws / emits error
   column) on the roadmap, or should we keep host-side validation?
4. **Batch API.** Is `variant_get_many` (vector of field names → table of typed
   columns) in scope for this branch or a follow-up? It is a significant
   performance item for our SELECT-many-fields workloads.
5. **Metadata sharing across chained calls.** Can `get_variant_field` return a
   view that aliases the parent's metadata buffer, or be parameterized to skip
   the metadata deep-copy if the caller promises not to outlive the parent?
6. **Sorted-strings binary search.** Is adding a binary-search path when the
   metadata flag is set in plan? Easy win for 50-key+ dictionaries.
7. **Null-children contract.** Will the "struct-level mask is authoritative,
   children unmasked" invariant be documented in the public header, and will
   consumers inside cuDF (gather, scatter, concatenate, hash, sort) respect it
   when operating on our outputs?
8. **Input column shape flexibility.** Must inputs always be
   `struct<list<uint8>, list<uint8>>`, or can the API accept `struct<binary, binary>`
   (once the native binary type lands) without source changes?
9. **`parse_json` on GPU.** Is there an owner / design for a Variant builder
   that can consume the cuDF JSON reader output and emit the binary
   metadata+value pair?
10. **Shredded read.** Is extending the Parquet reader to hand back
    `typed_value` sub-groups as native typed columns (skipping Variant binary
    entirely when a query touches only shredded fields) on the roadmap?
11. **Shredded write.** Writer-side support — is there an owner? spark-rapids
    cannot write Variant Parquet on GPU today.
12. **Namespace.** Is `cudf::io::parquet` intentional, or would the team accept
    a move to `cudf::variant` / `cudf::strings`-style top-level, given Variant
    is also used in-memory (Spark `VariantVal`)?
13. **Binary cuDF type.** Status and timeline. Needed to lift the 2 GB cap on
    Variant data per column for production deployment.
14. **Large dictionary performance.** 50-key × 2M rows at 10.66 ms on A100 is
    roughly 5 GB/s metadata throughput. Any plan for warp-cooperative dictionary
    scan to close the gap toward HBM bandwidth?
15. **Apache parquet-testing fixtures.** Are the reference fixtures used in
    tests available as a shared test dependency we can pull into spark-rapids-jni
    for end-to-end tests, or do we re-generate locally?
16. **Parquet version bump coordination.** What cuDF release version is the
    target for merging this branch? We need to align spark-rapids-jni.
17. **End-to-end pipeline timing.** Are there internal numbers for the full
    "Parquet read → variant extract → cast" pipeline, or does the current
    report's isolated kernel timing represent the primary performance claim?
    The latter under-represents Spark's effective throughput.
18. **CPU baseline.** Is a speedup comparison against Spark's CPU
    `variant_get` in the plan, or should we measure it ourselves from the
    spark-rapids side? Even a rough multiplier would help justify the feature
    to Spark audiences.
19. **Stream concurrency.** Is `get_variant_field` stream-clean — safe to run
    concurrently on multiple cuDF streams on one GPU? The report doesn't
    discuss per-stream behavior. Spark executors routinely multi-stream on a
    single device.
20. **Newer GPU targets.** Will the final report include H100 and/or L40S
    numbers? A100 is the benchmark standard but most new spark-rapids
    deployments target newer cards.

---

## 6. Benchmark and testing feedback

The libcudf team's implementation report explicitly invites feedback on the
benchmark design and results. This section captures what's strong, what's
missing, and concrete additions that would make the suite decision-useful
for Spark workloads.

### 6.1 What works well

- **Divergence scenario matrix.** The five axes (uniform_small, uniform_large,
  skewed_field_count, skewed_dict_size, half_missing) × {first-key, last-key}
  are well-chosen. They isolate dictionary-scan cost from field-id-scan cost
  from found-vs-null divergence. Keep this design.
- **Same-process measurement methodology.** The report notes that
  separate-process invocations contaminate timings with multi-millisecond
  CUDA/RMM init overhead. This is correct — worth promoting to a methodology
  note at the top of the report so consumers don't accidentally measure the
  wrong thing.
- **Perf fix discovered via benchmarking** (the `make_structs_column` →
  `create_structs_hierarchy` fix, §5.3). Exactly the kind of result
  benchmarking is supposed to surface.
- **Variable-length dictionary keys** (5 length groups: 3, 6, 10, 15, 21
  bytes). Realistically exercises the `slen != key_len` early-exit. Often
  missed by benchmarks that use uniform key lengths.
- **Apache parquet-testing reference fixtures.** Using community test vectors
  (`primitive_int32`, `short_string`, `primitive_string`, `object_primitive`,
  `object_nested`) is strong spec-compliance evidence.

### 6.2 Missing benchmark dimensions (asks)

- **End-to-end Parquet-read + extract pipeline.** Current benchmarks time
  `get_variant_field` with data already on GPU. For Spark query-planning
  decisions we need the "read VARIANT column from Parquet → extract field →
  cast" pipeline time. Without it we can't tell whether the reader or the
  extraction kernel dominates. Suggest a new benchmark variant that drives
  the same workloads through `parquet_variant_roundtrip_test.cpp`-style
  inputs via the real reader.
- **CPU baseline comparison.** Speedup vs Spark's CPU `variant_get` on the
  same workload. Even a ballpark ("~15× at 2M rows") is more compelling to
  Spark audiences than absolute ms numbers.
- **Dictionary-size sensitivity curve.** Benchmarks fix dict size at
  {5, 20, 50, 100}. A sweep across {1, 4, 16, 64, 256, 1024} keys at fixed
  2M rows would characterize the linear-scan cost model and let us project
  behavior on wider Variant columns (real-world schema drift yields large
  dictionaries).
- **Heterogeneous row-size distribution.** All benchmark rows have similar
  payload sizes. Real Spark data is skewed — some rows 5-field objects, some
  100+ deeply nested. A Zipfian-distributed payload benchmark would flush
  out warp-divergence pathologies and any hidden-quadratic costs.
- **Newer GPU targets.** A100 is standard, but H100 and L40S are where most
  new spark-rapids deployments land. One H100 row in the results table would
  meaningfully support projections.
- **Multi-stream concurrency.** Spark executors often run multiple concurrent
  kernels on one GPU. Benchmark under stream contention, or a statement that
  the kernel is stream-clean, would help production capacity planning.

### 6.3 Implications of the benchmark data (what the numbers are already saying)

- **Key-position 2.4× spread** (first-key 4.45 ms vs last-key 10.66 ms on
  uniform_large, 2M rows) is direct evidence that a `sorted_strings`-aware
  binary-search path (currently unused — §3.2) would close most of that gap.
  Probably the highest-ROI single optimization available without redesigning
  the algorithm.
- **"Pure cast is nearly free"** (`cast_variant_int32` at 2M rows = 0.099 ms)
  is a useful calibration today, but it will stop applying once `cast_variant`
  grows real cast semantics — especially string↔number parse/stringify, which
  is not free on GPU. Re-benchmark when cast semantics are added.
- **Scaling is linear above 128K rows.** Good — confirms kernel launch
  overhead is amortized at realistic batch sizes. No benchmark below 32K is
  particularly meaningful for spark-rapids since our batch sizes are larger.

### 6.4 Missing test coverage

Tests that would tighten spec-compliance confidence and are not currently in
`variant_extract_test.cpp`:

- **Integer-width casting** (the §3.6 core issue). Nine concrete test cases
  in `variant_prototype/cudf_tests/variant_cast_semantics_test.cpp` in this
  repo — two controls that pass today, seven mismatch cases that fail against
  the current branch. Ready to contribute.
- **Duplicate field names in an object.** The Variant spec §2.8 says
  duplicates are an error; today the branch silently picks the first match
  via linear scan. A test case would pin down the intended behavior (reject,
  warn, pick-first, pick-last).
- **Variant-null primitive as field value.** An extracted variant column
  whose value blob is the single byte `0x00` should round-trip correctly
  through Spark's `is_variant_null`. Useful edge case to lock down in tests.
- **Short-string/long-string boundary.** The spec switches encoding at
  length 64. Test pair at length 63 (short-string, basic_type=1) and 64
  (long-string, basic_type=0, header6=16) would verify the decoder handles
  the transition correctly.
- **Non-monotonic field offsets.** The tightest-end scan in
  `device_locate_object_field` exists precisely because the spec permits
  non-monotonic offsets. `ApacheNestedGetVariantField` exercises this
  indirectly; a hand-built test with deliberately reordered values would
  make the contract explicit.
