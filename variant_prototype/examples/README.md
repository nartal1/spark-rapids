# Variant type — spark-shell walkthrough

A Scala script you can paste into (or `:load` in) a Spark 4.0+ `spark-shell` to
see the Variant type end-to-end: build → write Parquet → read back → extract
with `variant_get` and dot-notation → observe Spark's cast semantics first-hand.

Same three-row example used elsewhere in this project
([`variant_type_primer.md`](../../variant_type_primer.md),
[`variant_data_type_deep_dive.md`](../../variant_data_type_deep_dive.md),
and the kernel-algorithm walkthrough):

```
Row 0: {"x": 7}
Row 1: {"x": 100, "y": "hi"}
Row 2: {"y": "hi"}
```

## Prerequisites

- **Spark 4.0+** — Variant is new in 4.0. Spark 3.x will not recognize
  `parse_json`/`variant_get`.
- **Java 17+** — required by Spark 4.
- Write access to `/tmp/` (or edit the `outPath` near the top of the script).

## How to run

**Option A — `:load` the whole script:**

```bash
$SPARK_HOME/bin/spark-shell --master local[*]
scala> :load variant_prototype/examples/spark_shell_variant_example.scala
```

**Option B — paste sections one at a time.** The script is structured into
eight numbered sections with clear headers; paste section by section to see
each step interactively.

## What the script does

Section by section:

1. **Verify Spark version** and import what's needed.
2. **Build a Variant column** via `parse_json` on a 3-row string DataFrame.
   Shows the logical schema (`variant` type) and how `.show()` renders
   Variant values back as JSON text.
3. **Write to Parquet and read back.** Confirms the type round-trips cleanly.
4. **Inspect the physical Parquet schema** — uses the parquet-mr reader to
   print the on-disk schema. You'll see the mandated `struct<metadata, value>`
   shape with the VARIANT logical type annotation. This is exactly the shape
   libcudf's Parquet reader materializes as `struct<list<uint8>, list<uint8>>`.
5. **Typed extraction** with `try_variant_get($.x, 'int')` and
   `try_variant_get($.y, 'string')`. Shows the expected NULLs for missing
   fields and the matching values for present fields.
6. **Dot-notation** (`payload.x`) as the ergonomic alternative, including a
   filter predicate `payload.x > 5`.
7. **Cast semantics demo** — this is the teaching moment. Extract the same
   stored value (`100`, encoded as int8 by `parse_json`) with six different
   target types (`tinyint`, `int`, `bigint`, `double`, `string`, `boolean`).
   All six succeed on CPU because Spark delegates to its general `Cast`
   expression. **None of these except `int` would succeed on GPU today**
   with cuDF's current `cast_variant` — see
   [`libcudf_variant_feedback.md §3.6`](../../libcudf_variant_feedback.md#36-cast_variant-is-decode-if-exact-match-not-cast--verified-mismatch).
   This lets you see the gap firsthand with real output.
8. **Nested object access** — a 4-level `{user: {profile: {name, age}, stats: {login_count, tier}}}`
   structure, extracted both via dotted JSONPath (`$.user.profile.name`) and
   via SQL dot-notation. Real Spark Variant data is usually 2-4 levels deep,
   so this is the common case. The path lowers in libcudf to N chained
   `get_variant_field` calls with a `cast_variant` at the leaf.
9. **Arrays and index access** — `$.tags[0]`, `$.scores[1]`, `$.items[0].price`.
   This is the section CPU does but **libcudf's current branch returns NULL
   for arrays** (§2.2 of the feedback doc). Running it lets you see what GPU
   support for array indexing needs to deliver.
10. **`variant_explode`** — table-valued function that expands an object or
    array Variant into rows. Used with `LATERAL` to join the exploded rows
    back to the outer DataFrame. Not in scope for the libcudf branch (P3 ask
    in §4).
11. **Shredding** — writes the three rows again with Spark's shredding
    configs turned on, so the Parquet file decomposes the Variant's `x` and
    `y` fields into native INT32 and STRING sub-columns. Prints the physical
    schema so you can compare to the unshredded one from section 4. Two
    important caveats inside the script:
    - Shredding config keys are still stabilizing across Spark 4.x; if your
      build rejects them, the try/catch prints a diagnostic and the rest of
      the script continues.
    - **libcudf's current branch does not read shredded Variant** (§2.6 of
      the feedback doc). A shredded Parquet file produced by Spark would
      fall back to CPU when read through spark-rapids.
12. **Cleanup** — notes the Parquet output paths so you can inspect or
    delete them.

## Expected output highlights

Schema after `parse_json`:

```
root
 |-- event_id: string (nullable = true)
 |-- payload:  variant (nullable = true)   ← new in Spark 4.0
```

Physical Parquet schema of the written file (approximate):

```
message spark_schema {
  optional binary event_id (STRING);
  optional group payload (VARIANT) {
    required binary metadata;
    required binary value;
  }
}
```

Typed extraction:

```
+--------+-----+-----+
|event_id|x_int|y_str|
+--------+-----+-----+
|e1      |7    |null |
|e2      |100  |hi   |
|e3      |null |hi   |
+--------+-----+-----+
```

Cast demo on row e2 (stored as int8, extracted as six types):

```
+--------+----------+------+---------+---------+---------+-------+
|event_id|as_tinyint|as_int|as_bigint|as_double|as_string|as_bool|
+--------+----------+------+---------+---------+---------+-------+
|e2      |100       |100   |100      |100.0    |100      |true   |
+--------+----------+------+---------+---------+---------+-------+
```

Every one of those conversions "just works" on CPU. That's the behavior
cuDF needs to grow.

Physical Parquet schema after section 11 (shredded write, abbreviated):

```
optional group payload (VARIANT) {
  required binary metadata;
  optional binary value;                     ← holds un-shredded leftovers only
  optional group typed_value {
    required group x {
      optional binary value;
      optional int32  typed_value;           ← native INT32 column
    }
    required group y {
      optional binary value;
      optional binary typed_value (STRING);  ← native STRING column
    }
  }
}
```

Compare this to the unshredded schema in section 4 (just `metadata` + `value`).
The `typed_value` sub-columns are what give shredded Variant its predicate-
pushdown and file-skipping wins — and what libcudf would need to recognize in
a future reader-expansion for the 30× speed target.

## After you've run it

Worth trying:

- **Change the stored type.** Modify row e1 to `{"x": 7.5}` or `{"x": "7"}`
  and re-run section 7 — observe how Spark's cast handles double→int
  (truncates to 7), string→int (parses), etc.
- **Try strict `variant_get` instead of `try_variant_get`.** On an impossible
  cast it throws `INVALID_VARIANT_CAST`; `try_variant_get` returns null.
- **Check `variant_get(v, '$.x')` with no type arg.** Returns a VARIANT
  column — the sub-variant. Chain a second `variant_get` for nested paths.
- **Push path depth further.** In section 8 try a 6-level path. The CPU
  handles it; each extra level is one more `get_variant_field` call in the
  GPU path, so you can reason about chaining cost.
- **Negative array index.** In section 9 try `$.tags[-1]` to see if your
  Spark build supports "last element" syntax (spec-optional).
- **Compare shredded vs unshredded file sizes.** After section 11, run
  `du -sh /tmp/variant_example.parquet /tmp/variant_shredded_example.parquet`
  to see the compression difference for a trivial 3-row dataset. The gap
  becomes much more dramatic as row counts grow.
- **Re-read shredded file with shredding disabled.** Flip
  `spark.sql.variant.allowReadingShredded` off and re-read the shredded
  Parquet from section 11 — observe whether Spark still reconstructs the
  Variant or errors out.

## Where this fits in the prototype directory

- [`cudf_tests/`](../cudf_tests/) — C++ test cases for the libcudf branch
- [`spark_rapids_jni/`](../spark_rapids_jni/) — JNI bridge skeleton
- [`spark_rapids/`](../spark_rapids/) — `GpuVariantGet` Scala skeleton
- [`cpu_benchmark/`](../cpu_benchmark/) — PySpark CPU baseline harness
- **[`examples/`](.) (this dir)** — learning-oriented walkthroughs on CPU Spark
