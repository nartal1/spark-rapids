// ============================================================================
// Spark 4.0 Variant Type — end-to-end walkthrough for spark-shell
// ============================================================================
//
// Same three-row example used in variant_type_primer.md and the kernel-algorithm
// deep dive:
//   Row 0: {"x": 7}
//   Row 1: {"x": 100, "y": "hi"}
//   Row 2: {"y": "hi"}
//
// Walks through:
//   (1) Build a Variant column via parse_json
//   (2) Inspect the logical schema
//   (3) Write to a Parquet file and read it back
//   (4) Inspect the physical Parquet schema (the struct<metadata, value> shape)
//   (5) Extract fields with typed and untyped variant_get
//   (6) Try dot-notation access
//   (7) Demonstrate Spark's cast semantics (§3.6 of libcudf_variant_feedback.md)
//   (8) Show is_variant_null vs missing vs SQL NULL
//
// USAGE:
//   $SPARK_HOME/bin/spark-shell --master local[*]          # Spark 4.0+
//   :load variant_prototype/examples/spark_shell_variant_example.scala
//
// Or paste the sections manually, one at a time.
// ============================================================================

// ----------------------------------------------------------------------------
// Prerequisite — verify we're on Spark 4.0+ (Variant is 4.0+ only)
// ----------------------------------------------------------------------------
println(s"Spark version: ${spark.version}")
assert(spark.version.split("\\.")(0).toInt >= 4,
       "This script requires Spark 4.0 or later. Variant type is not available in 3.x.")

import spark.implicits._
import org.apache.spark.sql.functions._

// ----------------------------------------------------------------------------
// (1) Build a Variant column via parse_json
// ----------------------------------------------------------------------------

val jsonRows = Seq(
  ("e1", """{"x": 7}"""),
  ("e2", """{"x": 100, "y": "hi"}"""),
  ("e3", """{"y": "hi"}""")
).toDF("event_id", "json_str")

// parse_json converts the JSON string to a Variant value. The result column
// has DataType = VariantType.
val events = jsonRows.selectExpr("event_id", "parse_json(json_str) AS payload")

println("\n=== Logical schema ===")
events.printSchema()
// Expected:
// root
//  |-- event_id: string (nullable = true)
//  |-- payload: variant (nullable = true)

println("\n=== Values (Variant renders as JSON text in .show) ===")
events.show(truncate = false)
// Expected:
// +--------+----------------------+
// |event_id|payload               |
// +--------+----------------------+
// |e1      |{"x":7}               |
// |e2      |{"x":100,"y":"hi"}    |
// |e3      |{"y":"hi"}            |
// +--------+----------------------+

// ----------------------------------------------------------------------------
// (2) Write to Parquet and read back
// ----------------------------------------------------------------------------

val outPath = "/home/nartal/variant_prototype_data/variant_example.parquet"

events.write.mode("overwrite").parquet(outPath)
println(s"\n=== Wrote Parquet to $outPath ===")

val read = spark.read.parquet(outPath)

println("\n=== Schema after read (should still show variant, not struct) ===")
read.printSchema()

println("\n=== Values after round-trip ===")
read.show(truncate = false)

// ----------------------------------------------------------------------------
// (3) Inspect the physical Parquet schema — see the struct<metadata, value>
//     representation the Variant spec mandates
// ----------------------------------------------------------------------------

println("\n=== Physical Parquet schema on disk ===")
{
  import org.apache.parquet.hadoop.ParquetFileReader
  import org.apache.parquet.hadoop.util.HadoopInputFile
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.{FileSystem, Path}

  val conf = new Configuration()
  val dir = new Path(outPath)
  val fs = dir.getFileSystem(conf)
  val partFiles = fs.listStatus(dir).filter(_.getPath.getName.endsWith(".parquet"))

  partFiles.foreach { f =>
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(f.getPath, conf))
    println(s"\n-- ${f.getPath.getName} --")
    println(reader.getFooter.getFileMetaData.getSchema.toString)
    reader.close()
  }
}
// Expected (abbreviated):
// message spark_schema {
//   optional binary event_id (STRING);
//   optional group payload (VARIANT) {
//     required binary metadata;
//     required binary value;
//   }
// }
//
// ^ This is the shape libcudf's Parquet reader now materializes as
//   struct<list<uint8> metadata, list<uint8> value>.

// ----------------------------------------------------------------------------
// (4) Extract fields with typed variant_get / try_variant_get
// ----------------------------------------------------------------------------

println("\n=== try_variant_get($.x AS int, $.y AS string) ===")
read.selectExpr(
  "event_id",
  "try_variant_get(payload, '$.x', 'int')    AS x_int",
  "try_variant_get(payload, '$.y', 'string') AS y_str"
).show(truncate = false)
// Expected:
// +--------+-----+-----+
// |event_id|x_int|y_str|
// +--------+-----+-----+
// |e1      |7    |null |
// |e2      |100  |hi   |
// |e3      |null |hi   |
// +--------+-----+-----+
//
// Notice: row e3 has x_int = null (field missing), row e1 has y_str = null
// (field missing). Row e2 has both. This matches Spark's documented semantics.

// ----------------------------------------------------------------------------
// (5) Dot-notation — the ergonomic alternative
// ----------------------------------------------------------------------------

println("\n=== Dot-notation access (payload.x, payload.y) ===")
read.selectExpr(
  "event_id",
  "payload.x AS x",          // returns VARIANT, not cast
  "payload.y AS y"           // returns VARIANT, not cast
).show(truncate = false)
// Dot-notation returns a Variant sub-value. You can then cast or compare.

// Filter example — mixes dot-notation with a typed cast in the predicate.
println("\n=== Filter: payload.x > 5 (field-level predicate) ===")
read.selectExpr(
  "event_id",
  "try_variant_get(payload, '$.x', 'int') AS x_int"
).filter("x_int > 5")
 .show(truncate = false)
// Expected: only e2 (x=100) survives. e1 (x=7) also survives since 7 > 5.
// ... wait: e1's x is 7, so 7 > 5 is true → e1 survives too. e3's x is null so
// the predicate is null → dropped. Final: e1 and e2.

// ----------------------------------------------------------------------------
// (6) Cast semantics demo — the §3.6 story, visible on CPU
// ----------------------------------------------------------------------------
//
// parse_json picks the narrowest integer encoding that fits. So "x": 7
// on row e1 is stored as int8 (1 byte). Let's ask Spark to extract the
// same field as many different types and see what happens:

println("\n=== Casting the same stored value to different target types ===")
read.filter($"event_id" === "e2").selectExpr(
  "event_id",
  "try_variant_get(payload, '$.x', 'tinyint') AS as_tinyint",
  "try_variant_get(payload, '$.x', 'int')     AS as_int",
  "try_variant_get(payload, '$.x', 'bigint')  AS as_bigint",
  "try_variant_get(payload, '$.x', 'double')  AS as_double",
  "try_variant_get(payload, '$.x', 'string')  AS as_string",
  "try_variant_get(payload, '$.x', 'boolean') AS as_bool"
).show(truncate = false)
// Expected: all six columns return a meaningful converted value:
// as_tinyint=100, as_int=100, as_bigint=100, as_double=100.0,
// as_string="100", as_bool=true (because 100 is non-zero).
//
// **This is the cast behavior on CPU.** Every one of those calls on GPU
// with cuDF's current cast_variant would return NULL (except possibly
// as_int, depending on how parse_json encodes "100"), because cuDF's
// decoder requires an exact header match. See libcudf_variant_feedback.md
// §3.6 for the full analysis.

// Bonus: show Spark gracefully returning NULL when the cast is impossible.
println("\n=== try_variant_get handles cast failures by returning NULL ===")
val weird = Seq("""{"n": "not-a-number"}""").toDF("j")
  .selectExpr("parse_json(j) AS v")
weird.selectExpr(
  "try_variant_get(v, '$.n', 'int') AS as_int_fail",
  "try_variant_get(v, '$.n', 'string') AS as_string_ok"
).show(truncate = false)
// Expected: as_int_fail=null, as_string_ok="not-a-number"
//
// variant_get (strict, not shown) would throw INVALID_VARIANT_CAST on the
// int cast. try_variant_get never throws.

// ----------------------------------------------------------------------------
// (7) is_variant_null — distinguishes JSON null from missing from SQL NULL
// ----------------------------------------------------------------------------

println("\n=== is_variant_null: JSON null vs missing vs SQL NULL ===")
val nullish = Seq(
  """{"a": null}""",        // field present with JSON null
  """{"b": 1}""",            // field "a" missing
  """null"""                  // top-level JSON null
).toDF("j").selectExpr("parse_json(j) AS v")

nullish.selectExpr(
  "v",
  "is_variant_null(v) AS variant_is_null",         // whole value is variant null?
  "try_variant_get(v, '$.a', 'int') AS a_as_int",  // extracted value
  "is_variant_null(try_variant_get(v, '$.a'))      AS a_is_variant_null"
).show(truncate = false)
// Row 1: variant_is_null=false (whole is an object), a_is_variant_null=true
// Row 2: variant_is_null=false, a_as_int=null (field missing)
// Row 3: variant_is_null=true (top-level is variant null)

// ----------------------------------------------------------------------------
// (8) Nested objects — multi-level paths
// ----------------------------------------------------------------------------
//
// Real Spark Variant data is typically 2-4 levels deep. Use either a dotted
// JSONPath in try_variant_get or dot-notation in SQL — both walk arbitrary
// depth on CPU.

val nested = Seq(
  ("u1", """{"user": {"profile": {"name": "alice", "age": 30},
                       "stats":   {"login_count": 42, "tier": "gold"}},
             "last_action": "click"}"""),
  ("u2", """{"user": {"profile": {"name": "bob", "age": 25}},
             "last_action": "view"}""")    // no stats sub-object
).toDF("uid", "json_str").selectExpr("uid", "parse_json(json_str) AS payload")

println("\n=== Multi-level try_variant_get (JSONPath with dots) ===")
nested.selectExpr(
  "uid",
  "try_variant_get(payload, '$.user.profile.name', 'string')   AS name",
  "try_variant_get(payload, '$.user.profile.age',  'int')      AS age",
  "try_variant_get(payload, '$.user.stats.login_count', 'int') AS logins",
  "try_variant_get(payload, '$.user.stats.tier',   'string')   AS tier"
).show(truncate = false)
// Expected:
// u1: alice, 30, 42, gold
// u2: bob,   25, null, null       (u2 has no stats sub-object)

println("\n=== Dot-notation through four levels ===")
nested.selectExpr(
  "uid",
  "payload.user.profile.name AS name",
  "payload.user.profile.age  AS age",
  "payload.user.stats.tier   AS tier"
).show(truncate = false)

// Note on what will map to libcudf's get_variant_field: the 3-level JSONPath
// "$.user.profile.name" would decompose in the plan into three chained
// get_variant_field calls in the libcudf branch, with a cast_variant at the
// leaf. Nothing fancy on the GPU side — just N kernel launches per path.

// ----------------------------------------------------------------------------
// (9) Arrays — $[n] index access
// ----------------------------------------------------------------------------
//
// Spark's JSONPath supports array-index syntax. libcudf's current branch
// returns NULL for arrays, so this section is the part that CPU Spark does
// but GPU (in its current state) cannot.

val arrays = Seq(
  ("a1", """{"tags": ["alpha", "beta", "gamma"], "scores": [10, 20, 30]}"""),
  ("a2", """{"tags": ["solo"], "scores": []}"""),
  ("a3", """{"tags": [], "scores": null}""")
).toDF("uid", "json_str").selectExpr("uid", "parse_json(json_str) AS payload")

println("\n=== Array index access ===")
arrays.selectExpr(
  "uid",
  "try_variant_get(payload, '$.tags[0]',    'string') AS first_tag",
  "try_variant_get(payload, '$.tags[2]',    'string') AS third_tag",
  "try_variant_get(payload, '$.scores[1]',  'int')    AS second_score",
  "try_variant_get(payload, '$.scores[99]', 'int')    AS out_of_bounds"
).show(truncate = false)
// Expected:
// a1: alpha, gamma, 20, null
// a2: solo,  null,  null, null
// a3: null,  null,  null, null

// Mixed object + array path — $.items[0].price
val cart = Seq(
  """{"items": [{"name": "A", "price": 9}, {"name": "B", "price": 15}]}"""
).toDF("j").selectExpr("parse_json(j) AS payload")

println("\n=== Mixed path: $.items[0].price ===")
cart.selectExpr(
  "try_variant_get(payload, '$.items[0].name',  'string') AS first_name",
  "try_variant_get(payload, '$.items[0].price', 'int')    AS first_price",
  "try_variant_get(payload, '$.items[1].price', 'int')    AS second_price"
).show(truncate = false)

// ----------------------------------------------------------------------------
// (10) variant_explode — expand an array or object into rows
// ----------------------------------------------------------------------------
//
// Returns a struct<pos INT, key STRING, value VARIANT> per input row.
//   - For arrays:  pos = element index, key = null,       value = element
//   - For objects: pos = field index,   key = field name, value = field value
//
// Variant_explode is a table-valued function — use LATERAL to join it
// with the outer rows.

val eventsJson = Seq(
  ("u1", """{"events": [{"type": "click",    "ts": 100},
                         {"type": "view",     "ts": 200},
                         {"type": "purchase", "ts": 300}]}""")
).toDF("uid", "json_str").selectExpr("uid", "parse_json(json_str) AS payload")

eventsJson.createOrReplaceTempView("events_rows")

println("\n=== variant_explode on an array (one row fans out to N rows) ===")
spark.sql("""
  SELECT outer.uid, e.pos,
         try_variant_get(e.value, '$.type', 'string') AS event_type,
         try_variant_get(e.value, '$.ts',   'int')    AS ts
  FROM events_rows outer,
  LATERAL variant_explode(try_variant_get(outer.payload, '$.events')) e
""").show(truncate = false)
// Expected: 3 rows — one per event in the array.

println("\n=== variant_explode on an object (keys become rows) ===")
val singleObj = Seq("""{"a": 1, "b": "two", "c": true}""")
  .toDF("j").selectExpr("parse_json(j) AS v")
singleObj.createOrReplaceTempView("obj_row")

spark.sql("""
  SELECT e.pos, e.key, e.value
  FROM obj_row outer,
  LATERAL variant_explode(outer.v) e
""").show(truncate = false)
// Expected:
// pos | key | value
//  0  |  a  |  1
//  1  |  b  | "two"
//  2  |  c  | true

// Note on GPU support: variant_explode is not in scope for the libcudf
// branch — listed as P3 in libcudf_variant_feedback.md §4.

// ----------------------------------------------------------------------------
// (11) Shredding — writing a Variant decomposed into typed Parquet sub-columns
// ----------------------------------------------------------------------------
//
// Shredding is Spark 4.0+'s optimization for Variant storage: known fields
// are pulled out of the binary value blob into their own typed Parquet
// sub-columns. Enables column pruning, predicate pushdown, and file-level
// skipping via statistics on the typed sub-columns.
//
// IMPORTANT CAVEATS:
//   * The Spark configs around Variant shredding are still stabilizing
//     across Spark 4.0.x / 4.1.x. Config key names may differ in your build.
//     If the keys below aren't recognized, check the Spark release notes
//     for `spark.sql.variant.*` — the names match the test suite fixtures
//     used in Spark's ParquetVariantShreddingSuite.
//   * libcudf's current branch does NOT support shredded reads — it only
//     recognizes the unshredded (metadata + value) layout (§2.6 of
//     libcudf_variant_feedback.md). Any shredded Parquet file produced by
//     Spark would force a CPU fallback when read through spark-rapids.
//   * This section is demonstrative — the concept + the physical schema is
//     what matters, not whether the exact write path works in your version.

val shreddedPath = "/tmp/variant_shredded_example.parquet"

// Enable shredding. If one of these keys is unrecognized you'll get a
// SparkException with "The SQL config ... is not public" or similar.
try {
  spark.conf.set("spark.sql.variant.writeShredding.enabled", "true")
  spark.conf.set("spark.sql.variant.allowReadingShredded",  "true")

  // Force Spark to shred `x` as INT and `y` as STRING. In production,
  // shredding schemas are usually inferred from query patterns; for the
  // teaching demo we spell it out.
  val shredSchema = """
    {
      "type": "struct",
      "fields": [
        {"name": "x", "type": {"type": "struct", "fields": [
            {"name": "value",       "type": "binary",  "nullable": true, "metadata": {}},
            {"name": "typed_value", "type": "integer", "nullable": true, "metadata": {}}
          ]}, "nullable": true, "metadata": {}},
        {"name": "y", "type": {"type": "struct", "fields": [
            {"name": "value",       "type": "binary", "nullable": true, "metadata": {}},
            {"name": "typed_value", "type": "string", "nullable": true, "metadata": {}}
          ]}, "nullable": true, "metadata": {}}
      ]
    }
  """.stripMargin
  spark.conf.set("spark.sql.variant.forceShreddingSchemaForTest", shredSchema)

  // Write the SAME three rows, now with shredding enabled.
  events.write.mode("overwrite").parquet(shreddedPath)
  println(s"\n=== Wrote shredded Parquet to $shreddedPath ===")

  // Inspect the physical schema — now with the typed_value sub-group.
  {
    import org.apache.parquet.hadoop.ParquetFileReader
    import org.apache.parquet.hadoop.util.HadoopInputFile
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.Path

    val conf = new Configuration()
    val dir = new Path(shreddedPath)
    val fs = dir.getFileSystem(conf)
    val partFiles = fs.listStatus(dir).filter(_.getPath.getName.endsWith(".parquet"))
    partFiles.foreach { f =>
      val reader = ParquetFileReader.open(HadoopInputFile.fromPath(f.getPath, conf))
      println(s"\n-- ${f.getPath.getName} (SHREDDED) --")
      println(reader.getFooter.getFileMetaData.getSchema.toString)
      reader.close()
    }
  }
  // Expected (abbreviated — compare to the unshredded schema from section 3):
  //   optional group payload (VARIANT) {
  //     required binary metadata;
  //     optional binary value;                -- holds only un-shredded leftovers
  //     optional group typed_value {
  //       required group x {
  //         optional binary value;
  //         optional int32  typed_value;       -- native INT32 column!
  //       }
  //       required group y {
  //         optional binary value;
  //         optional binary typed_value (STRING);   -- native STRING column!
  //       }
  //     }
  //   }

  // Read it back — queries are identical; the engine reconstructs Variant
  // automatically, and for shredded fields can skip the binary walker and
  // predicate-push into the typed column.
  println("\n=== Reading shredded file back — same queries work ===")
  spark.read.parquet(shreddedPath).selectExpr(
    "event_id",
    "try_variant_get(payload, '$.x', 'int')    AS x_int",
    "try_variant_get(payload, '$.y', 'string') AS y_str"
  ).show(truncate = false)
} catch {
  case t: Throwable =>
    println(s"\nShredding demo failed: ${t.getClass.getSimpleName}: ${t.getMessage}")
    println("The shredding config keys may differ in your Spark build.")
    println("Concept still holds — see the unshredded schema from section 3")
    println("and compare to the shredded schema diagram in ")
    println("variant_data_type_deep_dive.md §3.")
} finally {
  spark.conf.unset("spark.sql.variant.writeShredding.enabled")
  spark.conf.unset("spark.sql.variant.allowReadingShredded")
  spark.conf.unset("spark.sql.variant.forceShreddingSchemaForTest")
}

// ----------------------------------------------------------------------------
// (12) Cleanup
// ----------------------------------------------------------------------------

println(s"\nDone. Parquet files left at:")
println(s"  - $outPath")
println(s"  - $shreddedPath (if shredding demo succeeded)")
println(s"Inspect with a Parquet tool or delete with 'rm -rf'.")
// rm -rf /tmp/variant_example.parquet /tmp/variant_shredded_example.parquet
