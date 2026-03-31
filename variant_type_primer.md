# Variant Type — Primer

A learning-oriented introduction to Apache Spark 4.0's `VariantType`: why it exists, how you use it as a Spark user, how spark-rapids handles semi-structured data today, and how Parquet shredding turns Variant into a first-class columnar format.

This doc is the gentle on-ramp. For byte-level encoding details and a full GPU implementation roadmap, see the companion [variant_data_type_deep_dive.md](variant_data_type_deep_dive.md). For an assessment of the in-progress libcudf Variant branch and open questions for the cuDF team, see [libcudf_variant_feedback.md](libcudf_variant_feedback.md).

---

## Contents

1. [Why Variant exists](#1-why-variant-exists)
2. [Spark user's view — same query, two worlds](#2-spark-users-view--same-query-two-worlds)
3. [How spark-rapids handles semi-structured data today](#3-how-spark-rapids-handles-semi-structured-data-today)
4. [Shredding — the 30× read-speed unlock](#4-shredding--the-30-read-speed-unlock)
5. [Where to go next](#5-where-to-go-next)

---

## 1. Why Variant exists

Semi-structured data — JSON logs, event payloads, API responses — has traditionally lived in Spark as a `STRING` column containing JSON text:

```sql
CREATE TABLE events (
  event_id STRING,
  payload  STRING          -- JSON text, e.g. '{"user":"alice","clicks":3}'
);

SELECT get_json_object(payload, '$.user') FROM events;
```

Problems with this approach:

1. **Every query re-parses the text.** Running 100 queries against the same JSON means tokenizing it 100 times.
2. **Everything is a string internally.** `"3"` in the JSON has to be re-inferred as a number on every read.
3. **No columnar benefits.** The Parquet column is an opaque blob — Parquet can't compress it well, can't push predicates into it, can't skip files based on statistics.
4. **Text is verbose.** `"12345678"` is 10 bytes as text but 4 bytes as `int32`.

**Variant** (new in Spark 4.0, Parquet 1.15+, Delta Lake 4.0, Iceberg 1.8+) is the answer: a new column type that stores semi-structured data in a **compact binary format that has already been parsed once** at ingest time.

**Mental model:** a JSON-string column is a book in a foreign language that you re-translate every time you want to quote from it. Variant is the pre-translated book. Same content, same ability to query arbitrary fields, but the interpreter work was done once at the door.

**Cost:** writes are slightly slower (you pay the parse once). **Benefit:** reads are dramatically cheaper — ~8× faster than JSON strings without shredding, ~30× faster with shredding (per Databricks benchmarks). Reads dominate most workloads.

---

## 2. Spark user's view — same query, two worlds

### The scenario

You're logging website click events. Each event is a JSON object. Notice the shape is **not uniform** — the second row has a `"mobile"` field the others don't. This is the classic case where semi-structured storage beats a fixed schema.

```json
{"user": "alice", "clicks": 3}
{"user": "bob",   "clicks": 7, "mobile": true}
{"user": "carol", "clicks": 12}
```

We want to:

1. Store these events in a table.
2. Query "who had more than 5 clicks?"

---

### World A — before Variant (JSON stored as `STRING`)

**Table definition:**

```sql
CREATE TABLE events_old (
  event_id STRING,
  payload  STRING          -- JSON text
);
```

`payload` is a plain text column. Spark has no idea the text inside is JSON.

**Inserting:**

```sql
INSERT INTO events_old VALUES
  ('e1', '{"user":"alice","clicks":3}'),
  ('e2', '{"user":"bob","clicks":7,"mobile":true}'),
  ('e3', '{"user":"carol","clicks":12}');
```

On disk you have 3 rows, each with the raw JSON text. **No parsing happened at write time.** The byte `3` in `"clicks":3` is stored as the character `'3'` (ASCII 0x33), not as an integer.

**Querying — "users with more than 5 clicks":**

```sql
SELECT event_id,
       get_json_object(payload, '$.user') AS user
FROM events_old
WHERE CAST(get_json_object(payload, '$.clicks') AS INT) > 5;
```

What happens at query time, **per row**:

1. Spark reads the whole JSON string off disk.
2. `get_json_object` parses the JSON text from scratch, walks to `$.clicks`, returns the matching substring `'7'` (still a string).
3. `CAST(... AS INT)` re-parses that string into an integer.
4. Compares `7 > 5`.

Run 100 queries against this table and steps 1–3 happen **100 times**.

---

### World B — with Variant (Spark 4.0+)

**Table definition:**

```sql
CREATE TABLE events_new (
  event_id STRING,
  payload  VARIANT         -- new type; stores pre-parsed binary
);
```

**Inserting** — you still start with JSON text, but you call `parse_json` once at insert time to convert it to Variant's binary form:

```sql
INSERT INTO events_new VALUES
  ('e1', parse_json('{"user":"alice","clicks":3}')),
  ('e2', parse_json('{"user":"bob","clicks":7,"mobile":true}')),
  ('e3', parse_json('{"user":"carol","clicks":12}'));
```

On disk, each row's `payload` is now a compact binary blob. The integer `3` is stored as a tagged int, not the character `'3'`. The field name `"clicks"` appears in a small dictionary once per row (or shared across rows in shredded form). No more runtime parsing.

**Querying** — two equivalent styles. Dot-notation is the ergonomic one, because Spark knows the column is structured:

```sql
SELECT event_id, payload.user AS user
FROM events_new
WHERE payload.clicks > 5;
```

Or the explicit function form, where you also name the target type:

```sql
SELECT event_id,
       variant_get(payload, '$.user', 'string') AS user
FROM events_new
WHERE variant_get(payload, '$.clicks', 'int') > 5;
```

What happens at query time, **per row**:

1. Spark reads the binary blob off disk.
2. A binary walker jumps to `clicks` (dictionary lookup + offset read — no parsing).
3. Reads the integer value directly (already `7` as a real int).
4. Compares `7 > 5`.

No string parsing anywhere. No casting.

---

### Side-by-side summary

| | World A (JSON as STRING) | World B (Variant) |
|---|---|---|
| Column type | `STRING` | `VARIANT` |
| Insert form | Raw JSON literal | `parse_json('{...}')` |
| Parse work at write time | None | Once per row |
| Parse work at read time | Every query, every row | None — just binary walking |
| Query filter | `CAST(get_json_object(..., '$.clicks') AS INT) > 5` | `payload.clicks > 5` |
| Storage of integer `3` | ASCII char `'3'` (1 byte) | Tagged int (2 bytes: header + value) |
| Storage of field name `"clicks"` | Inline in every row's text | In a per-value (or shared) dictionary |
| Field access | Case-insensitive | **Case-sensitive** |
| Compression | Poor — text is high-entropy | Good — binary is dense |

### Variant SQL function reference

All available in Spark 4.0+.

| Function | What it does |
|---|---|
| `parse_json(str)` | JSON text → Variant. Throws on invalid JSON. |
| `try_parse_json(str)` | JSON text → Variant. Returns NULL on invalid JSON. |
| `variant_get(v, path, type)` | Extract a field by path, cast to type. Throws on type mismatch. |
| `try_variant_get(v, path, type)` | Same, but returns NULL on mismatch instead of throwing. |
| `is_variant_null(v)` | `true` only if the value is a JSON `null` (distinct from SQL `NULL`). |
| `schema_of_variant(v)` | Inferred SQL schema of one Variant value, as a string. |
| `schema_of_variant_agg(v)` | Same, merged across a column (aggregate). |
| `variant_explode(v)` | Turn an object/array Variant into rows (`pos`, `key`, `value`). |
| `variant_explode_outer(v)` | Outer variant of above. |
| `to_json(v)` | Render Variant back as JSON text. |
| `to_variant_object(x)` | Build a Variant from typed Spark data (struct/array/map). |

Paths follow JSONPath-lite syntax: `$.field`, `$[0]`, `$.a.b[3].c`. Wildcards (`[*]`) are **not** supported for Variant (unlike `get_json_object` on JSON strings).

---

## 3. How spark-rapids handles semi-structured data today

Today, spark-rapids accelerates the **JSON-string world** — not Variant (which isn't wired up yet). The GPU path covers most of the common JSON functions, plus the JSON file reader.

### What's on the GPU today

| Spark function | Plugin file | Status |
|---|---|---|
| `get_json_object(str, path)` | [GpuGetJsonObject.scala](sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuGetJsonObject.scala) | Accelerated via `JSONUtils.getJsonObject`. Falls back on `[*]` wildcards or paths > 16 levels. |
| `json_tuple(str, f1, f2, …)` | [GpuJsonTuple.scala](sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuJsonTuple.scala) | Accelerated via `JSONUtils.getJsonObjectMultiplePaths` — a batch API extracting many fields in one kernel pass. Falls back if > 50 fields. |
| `from_json(str, schema)` | `GpuJsonToStructs.scala` | Produces `STRUCT` or `MAP<STRING,STRING>`. Falls back on duplicate field names and (by default) Date/Timestamp types. |
| `to_json(struct/map)` | `GpuStructsToJson.scala` | Disabled by default; opt-in. |
| Reading `.json` files | [GpuJsonScan.scala](sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuJsonScan.scala) | GPU replaces Spark's text-based JSON scan. Single-line only; falls back on `multiLine`, `allowComments`, non-UTF8 encoding, non-UTC timezones. |
| Reading `.csv` files | `GpuCSVScan.scala` | Separate path — the other "text → structured" ingest. |
| `json_object_keys`, `json_array_length`, … | — | **Not GPU-accelerated**; fall back to CPU. |
| Anything `VariantType` | — | **Not supported**; falls back to CPU. |

### The pattern (same sandwich we'd reuse for Variant)

Every JSON operator follows the same layered structure:

```
Spark expression                          (e.g. GetJsonObject)
   ↓ plugin override
GpuXxx expression in spark-rapids         (e.g. GpuGetJsonObject.scala)
   ↓ JNI call
JSONUtils Java class (spark-rapids-jni)   (external Maven jar)
   ↓ native call
libcudf C++ kernel                        (cudf::strings::get_json_object, etc.)
```

Note that `spark-rapids-jni` is an **external Maven artifact** (`com.nvidia:spark-rapids-jni:…:cuda12`), not a subdirectory in this workspace. The sandwich pattern is what the Variant integration plan mirrors: a new `VariantUtils` Java class + `VariantUtilsJni.cpp` in spark-rapids-jni, with new `GpuVariantGet` / `GpuTryVariantGet` expressions on the Spark side.

### The key limitation of the JSON-string GPU path

Even on the GPU, `get_json_object` still **parses the JSON text on every read**. The kernels are tight and give good throughput, but you don't get the "parse once, query N times" win that Variant promises. That is the gap Variant fills — and the reason the cuDF Variant branch ([libcudf_variant_feedback.md](libcudf_variant_feedback.md)) is interesting for this team.

### What falls back to CPU today

- `get_json_object` with wildcards or > 16-level paths.
- `from_json` with duplicate keys or datetime schemas (by default).
- `to_json` by default.
- All Variant expressions (`parse_json`, `variant_get`, `variant_explode`, etc.).
- Multi-line / non-UTF8 / non-UTC JSON file reads.
- `json_object_keys`, `json_array_length`, and similar niche functions.

---

## 4. Shredding — the 30× read-speed unlock

Same three rows, now watch them physically move to disk in three progressively smarter forms.

```
Row 1: {"user":"alice", "clicks":3}
Row 2: {"user":"bob",   "clicks":7, "mobile":true}
Row 3: {"user":"carol", "clicks":12}
```

### Form 1 — JSON-string column (baseline)

Parquet sees a column called `payload` of type `BINARY` (strings). Three opaque text blobs.

```
Parquet column: payload  (BINARY / STRING)

Row 1 │ "{\"user\":\"alice\",\"clicks\":3}"
Row 2 │ "{\"user\":\"bob\",\"clicks\":7,\"mobile\":true}"
Row 3 │ "{\"user\":\"carol\",\"clicks\":12}"
```

For `WHERE payload.clicks > 5`:

- Min/max statistics? Meaningless — they'd be min/max of the whole JSON string, lexicographically.
- File/row-group skipping? Impossible.
- Column pruning? Impossible — it's one column.
- Every row must be read and re-parsed.

### Form 2 — Variant, no shredding

Parquet now sees a group with **two binary sub-columns**:

```
required group payload (VARIANT) {
  required binary metadata;    -- dictionary of field names
  required binary value;       -- the encoded tree
}
```

On disk:

```
Parquet column: payload  (VARIANT group)
                ├── metadata (BINARY)     -- small dict per row
                └── value    (BINARY)     -- encoded variant bytes

Row 1 │ metadata = [dict: "clicks","user"]           value = {clicks:int8(3),  user:"alice"}
Row 2 │ metadata = [dict: "clicks","mobile","user"]  value = {clicks:int8(7),  mobile:true, user:"bob"}
Row 3 │ metadata = [dict: "clicks","user"]           value = {clicks:int8(12), user:"carol"}
```

Better than Form 1 — binary is denser, ints are real ints, no re-parsing. But Parquet still can't see inside the two binary blobs, so min/max stats, file skipping, and column pruning still don't work. Roughly **8× faster reads** than Form 1.

### Form 3 — Variant, with shredding

When the writer knows — from a schema hint or data observation — that most rows have a `user` (string) and a `clicks` (int), it **pulls those fields out of the value blob into their own typed Parquet columns**. What remains in `value` is only fields that weren't shredded.

```
optional group payload (VARIANT) {
  required binary metadata;                 -- still here, for un-shredded fields
  optional binary value;                    -- now holds only leftovers
  optional group typed_value {              -- the shredded fields
    required group user {
      optional binary value;
      optional binary typed_value (STRING);
    }
    required group clicks {
      optional binary value;
      optional int32  typed_value;
    }
  }
}
```

The three rows are now spread across real columns:

```
Row │ metadata                │ value (leftovers) │ typed_value.user        │ typed_value.clicks
    │                         │                   │  .value   .typed_value  │  .value   .typed_value
────┼─────────────────────────┼───────────────────┼─────────────────────────┼─────────────────────────
 1  │ [dict: "user",…]        │ NULL              │  NULL     "alice"       │  NULL      3
 2  │ [dict: "user","mobile",…│ {mobile: true}    │  NULL     "bob"         │  NULL      7
 3  │ [dict: "user",…]        │ NULL              │  NULL     "carol"       │  NULL      12
```

Three things to notice:

1. **`typed_value.clicks.typed_value` is a real `INT32` column.** Parquet computes min/max stats (`min=3, max=12`), writes bloom filters, dictionary-encodes, and supports predicate pushdown.
2. **`typed_value.user.typed_value` is a real `STRING` column.** Same treatment.
3. **`value` holds only `{mobile: true}`** on Row 2. The Variant is reconstructed at read time by unioning `typed_value` with leftover fields in `value`.

### What a query does now

```sql
SELECT event_id, payload.user
FROM events_new
WHERE payload.clicks > 5;
```

With shredding:

1. **File skipping** — Parquet sees `min=3, max=12` for `payload.clicks.typed_value` in this file. `3 < 5 < 12`, so keep it. A different file with `max=4` gets skipped entirely **without reading any rows**.
2. **Column pruning** — the query only touches `user` and `clicks`. Parquet skips `metadata`, `value`, and other shredded fields.
3. **Predicate pushdown** — `>5` runs as a native `INT32` comparison on `typed_value.clicks.typed_value`. No Variant walker involved.
4. **Reading `payload.user`** — directly reads the `typed_value.user.typed_value` string column. No Variant walker.

This is the path to **~30× faster reads** versus JSON strings. The binary Variant walker only runs for fields that weren't shredded.

### The `value` / `typed_value` interaction — the mental model

Every shredded field has two sub-columns, `value` and `typed_value`. Exactly one of these four states is allowed per row:

| `value` | `typed_value` | Meaning |
|---|---|---|
| null | null | The field is **missing** (objects only) |
| non-null | null | The field is **present but doesn't match the shredded type** — fallback as binary Variant |
| null | non-null | The field is **present and matches the shredded type** — the happy path |
| non-null | non-null | **Only legal for objects** (partial shredding): some sub-fields in `typed_value`, others in `value` |

The last row is the subtle one. For a Variant **object**, the writer shreds the fields it knows about and keeps leftovers in `value`. For a Variant **scalar**, either it fits the shredded type (→ `typed_value`) or it doesn't (→ `value`); both being non-null for a scalar is illegal.

Examples from our three rows:

- Row 1's `clicks = 3` → fits INT32 → `typed_value=3, value=null`.
- Imagine a Row 4 with `"clicks": "lots"` (string where INT32 was expected) → `typed_value=null, value=short_string("lots")`. The value is preserved, just slower to read.
- Row 2's overall `payload` is an object with an un-shredded field `mobile` → outer `typed_value` group is non-null (for `user`, `clicks`), outer `value` is non-null (holding `{mobile:true}`). Partial shredding.

### Trade-offs — when shredding is worth it

**Write-time costs:**

- Writer pays 20–50% more to split Variant across multiple physical columns.
- Writer must know which fields to shred (schema hints, adaptive learning from query history, or explicit config).

**When shredding pays off:**

- Repeated queries on a predictable subset of fields.
- Predicate filters on those fields (huge — file skipping).
- Big tables (overhead is linear; skipping wins are sub-linear).

**When shredding doesn't help much:**

- Wild-west JSON where every row has completely different fields (nothing to shred).
- Queries that always want the whole object (column pruning wins evaporate).
- Types that vary per row (`"count": 5` on some rows, `"count": "lots"` on others) — values keep falling back to `value`.

**Data-skipping gotcha:** Parquet statistics on `typed_value` are trustworthy for file skipping **only when `value` is always null for that field** (every row matched the shredded type cleanly). If even one row fell back, the stats don't tell the whole story.

### Schema evolution

A writer that later learns `mobile` is queried a lot can add a new `typed_value.mobile` sub-group for future row groups or files. Older files keep their old shape. Readers union across files. Additive, no rewrite.

### The three forms at a glance

| | JSON string | Variant (no shredding) | Variant (shredded) |
|---|---|---|---|
| Physical columns | 1 (text) | 2 (metadata + value) | 2 + N typed fields |
| Parse per read | Yes, every time | No (binary walk) | No; direct for shredded fields |
| Column pruning | No | No | **Yes** |
| Predicate pushdown | No | No | **Yes** |
| File-level skipping | No | No | **Yes** (when `value` always null) |
| Compression | Poor (text) | Good (binary) | **Excellent** (typed, per-column) |
| Relative read speed | 1× | ~8× | ~30× |
| Write speed | 1× | ~1× | 0.5–0.8× |

### Implications for spark-rapids

- The cuDF Variant branch handles **Form 2** on GPU — reading `metadata` + `value` binary columns and walking them to extract fields. That's the 8× win, available once `VariantUtils` is wired up through spark-rapids-jni.
- The branch does **not** yet handle **Form 3** — the reader materializes VARIANT as `struct<list<uint8>, list<uint8>>` but doesn't recognize the `typed_value` sub-group. All the 30× gains — file skipping, predicate pushdown, shredded-field direct reads — stay on the table for now.
- This is why "shredded read" sits in the P2 asks in [libcudf_variant_feedback.md](libcudf_variant_feedback.md) — the biggest latent performance unlock once the Form 2 path is solid.

---

## 5. Where to go next

- **Byte-level binary encoding** (headers, primitives, objects, arrays, the `{"age":42,"name":"Bob"}` worked example): [variant_data_type_deep_dive.md §2](variant_data_type_deep_dive.md#2-parquet-variant-binary-encoding).
- **Full shredded schema spec** (object shredding, array shredding, rules for reconstruction): [variant_data_type_deep_dive.md §3](variant_data_type_deep_dive.md#3-parquet-variant-shredding).
- **Spark SQL function reference** with full examples and edge cases: [variant_data_type_deep_dive.md §4](variant_data_type_deep_dive.md#4-spark-varianttype).
- **GPU implementation roadmap** (4 phased steps, API signatures, walkthroughs): [variant_data_type_deep_dive.md §6](variant_data_type_deep_dive.md#6-cudf-implementation-roadmap-for-spark-rapids).
- **Current libcudf branch assessment + asks for the cuDF team**: [libcudf_variant_feedback.md](libcudf_variant_feedback.md).
- **External specs** — [Parquet Variant Binary Encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/), [Parquet Variant Shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/), [Databricks Variant blog](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark).
