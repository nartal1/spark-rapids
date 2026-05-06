# Issue 6 Plan: GPU Deletion Vector reads for Databricks 17.3 (`delta-spark400db173`)

## Context

DB-17.3 makes deletion vectors the default for new Delta tables. The current DB-17.3 shim explicitly blocks GPU for any DV-enabled scan in [delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala](delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala#L102-L107) — `tagSupportForGpuFileSourceScan` calls `willNotWorkOnGpu("deletion vector reads are not yet supported for DB-17.3")` whenever `format.tablePath.isDefined`. The result: virtually every Delta read on DB-17.3 falls back to a CPU `FileSourceScanExec`.

Because the target customers are read-heavy and not MERGE-heavy, we are reordering Issue 6 (GPU DV reads) ahead of Issues 3/4/5 (GPU DELETE/UPDATE/MERGE/OPTIMIZE) on the [delta-lake-db173-github-issues.md](delta-lake-db173-github-issues.md) plan. DV reads alone unlock GPU acceleration for SELECT/aggregate/join workloads on every DV-tagged Delta table — including DV tables created by writers outside this plugin (Databricks UI, dbt, CPU Spark jobs).

This plan covers only the **V1 read path**: GPU Parquet read followed by a post-read `skip_row` mask materialized from the Delta deletion vector. The V2 cuDF-native path (`GpuDeltaParquetFileFormatBase2` — bitmap fed directly into cuDF's Parquet reader) is deferred to a follow-up because compatibility between cuDF's expected DV serialization and Databricks' `c.d.s.t.tahoe.deletionvectors.StoredBitmap` wire format has not been verified.

## Outcome and non-goals

**Done means**
- DB-17.3 cluster runs `FileSourceScanExec` on the GPU for DV-enabled Delta tables.
- DV bitmap is loaded per file via `PartitionedFile.otherConstantMetadataColumnValues[FILE_ROW_INDEX_FILTER_ID_ENCODED]` and applied as a post-read `is_row_deleted` mask plus row-index column.
- All `supports_delta_lake_deletion_vectors`-gated DV-read tests in [integration_tests/src/main/python/delta_lake_test.py](integration_tests/src/main/python/delta_lake_test.py) pass on DB-17.3, with CPU/GPU result parity.
- Builds remain green for `spark400db173`, OSS `delta-33x` / `delta-40x`, and DB-14.3 (`delta-spark350db143`).

**Non-goals (explicitly deferred)**
- V2 cuDF-native DV reader path.
- GPU DELETE/UPDATE/MERGE/OPTIMIZE on DB-17.3 (Issues 3/4/5).
- Re-enabling Delta tests in CI for `spark400db173` — that flip is Issue 7 and waits for a clean Jenkins run.
- DV writes (Delta-log creation of DVs from GPU DML).

## Strategy: copy + namespace substitute (option b)

The OSS DV implementation lives under `delta-lake/common/src/main/delta-33x-40x/scala/`, which the OSS `delta-33x` and `delta-40x` modules pull in via their `pom.xml` `add-source` blocks. The `delta-spark400db173` `pom.xml` deliberately does NOT add this root because the OSS files import `org.apache.spark.sql.delta.*` types that do not exist on DB-17.3 (DB-17.3 namespace is `com.databricks.sql.transaction.tahoe.*`).

We will **copy** the V1 DV files into `delta-lake/delta-spark400db173/src/main/scala/`, substituting OSS namespaces with their Databricks equivalents. We will NOT introduce a new shared common source root in this PR. Rationale:

- Minimizes blast radius — no risk to the OSS modules or any other Databricks shim.
- Mirrors how the Databricks shims already handle Delta-namespaced code (each shim has its own `GpuDeltaParquetFileFormat.scala`).
- Refactoring into a shared `delta-lake/common/src/main/databricks-dv/scala/` root is a viable follow-up once the Databricks-side shape stabilizes.

Total port surface ≈ 1,150 LOC across 5 OSS files; we keep ≈ 80% verbatim with import rewrites.

## File-by-file deliverables

### New files under `delta-lake/delta-spark400db173/src/main/scala/`

| Path | Origin (port from) | Purpose |
|---|---|---|
| `com/databricks/sql/transaction/tahoe/deletionvectors/RapidsDeletionVectorStore.scala` | [delta-lake/common/.../delta-33x-40x/scala/org/apache/spark/sql/delta/deletionvectors/RapidsDeletionVectorStore.scala](delta-lake/common/src/main/delta-33x-40x/scala/org/apache/spark/sql/delta/deletionvectors/RapidsDeletionVectorStore.scala) | DV bitmap loader: `RapidsHadoopDVStore`, `DeltaSerializedBitmapLoader`, `Delta{Portable,Native}FormatLoader`. Reads serialized DV bytes from `HadoopFileSystemDVStore` into a `HostMemoryBuffer`. ~204 LOC. |
| `com/databricks/sql/transaction/tahoe/deletionvectors/RapidsStoredBitmap.scala` | [delta-lake/common/.../RapidsStoredBitmap.scala](delta-lake/common/src/main/delta-33x-40x/scala/org/apache/spark/sql/delta/deletionvectors/RapidsStoredBitmap.scala) | `RapidsDeletionVectorStoredBitmap(dvDescriptor, tableDataPath)` per-file holder; empty-bitmap fast path. ~71 LOC. |
| `com/nvidia/spark/rapids/delta/RapidsRowIndexFilters.scala` | [delta-lake/common/.../RapidsRowIndexFilters.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsRowIndexFilters.scala) | Trait `RapidsRowIndexFilter` plus `RapidsDropMarkedRowsFilter` / `RapidsKeepMarkedRowsFilter` / `RapidsDropAllRowsFilter` / `RapidsKeepAllRowsFilter`. Materializes the bitmap into an INT8 `skip_row` GPU `ColumnVector` via `cudf::ColumnVector.contains`/`not`. ~130 LOC. |
| `com/nvidia/spark/rapids/delta/RapidsDeletionVectors.scala` | [delta-lake/common/.../RapidsDeletionVectors.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsDeletionVectors.scala) (slim) | Keep `translateFilterForColumnMapping` (used by `prepareFiltersForRead` for `NameMapping`/`IdMapping` translation) and any V1-relevant helpers. Drop V2-only helpers (`loadDeletionVector(HostMemoryBuffer)`, `dropFirstColumn`, `getRowGroupMetadata`). ~110 LOC. |
| `com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatDV.scala` | [delta-lake/common/.../GpuDeltaParquetFileFormatBase.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala) | New abstract class `GpuDeltaParquetFileFormatDV` extending the Databricks-shared [delta-lake/common/.../databricks/scala/.../GpuDeltaParquetFileFormatBase.scala](delta-lake/common/src/main/databricks/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatBase.scala). Hosts: (a) `prepareSchema` override that also strips `PARQUET_FIELD_NESTED_IDS_METADATA_KEY` (extra in Delta 3.3+); (b) `prepareFiltersForRead`; (c) `isSplitable = optimizationsEnabled`; (d) DV-aware `buildReaderWithPartitionValuesAndMetrics` wrapping `super` with `RapidsDeletionVectorUtils.iteratorWithAdditionalMetadataColumns`; (e) `createMultiFileReaderFactory` returning a `DeltaMultiFileReaderFactory`. The companion utilities `RapidsDeletionVectorUtils`, `DeltaMultiFileReaderFactory`, `DeltaMultiFileParquetPartitionReader` live in the same file (one logical unit, mirroring the OSS layout). ~360 LOC. |

### Modified file

| Path | Change |
|---|---|
| [delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala](delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala) | (1) Extend `GpuDeltaParquetFileFormatDV` instead of the bare Databricks `GpuDeltaParquetFileFormatBase`. (2) Expand the case class to mirror the DB-17.3 CPU `DeltaParquetFileFormat`'s 10-field constructor (verified via `javap` on the live jar): `protocol`, `metadata`, `generateRowIndexFilterId: Boolean = false`, `generateRowIndexFilterColumn: Boolean = false`, `generateDeltaFileInScanId: Boolean = false`, `nullableRowTrackingConstantFields: Boolean = false`, `nullableRowTrackingGeneratedFields: Boolean = false`, `optimizationsEnabled: Boolean = true`, `tablePath: Option[String] = None`, `isCDCRead: Boolean = false`. **Note:** the OSS `Delta40xProvider` passes a single `nullableRowTrackingFields = false`; on DB-17.3 this is two separate fields (constant vs generated). Pass each through from `fmt`. (3) Drop the local `isSplitable` override (parent handles via `optimizationsEnabled`). (4) Drop the local `buildReaderWithPartitionValuesAndMetrics` override (parent now does the DV work). (5) Rewrite the `tagSupportForGpuFileSourceScan` body in the companion: keep the existing `_databricks_internal` column block verbatim — it guards an unrelated Databricks-internal feature, **not** DV columns (DV columns on DB-17.3 are named `__delta_internal_is_row_deleted` / `__delta_internal_row_index`, double-underscore-`delta`, verified via `javap -p -v` on `DeltaParquetFileFormat$.class`); **remove** the unconditional `format.tablePath.isDefined → willNotWorkOnGpu("deletion vector reads are not yet supported for DB-17.3")` block; add a CDC-with-DV guard `if (format.isCDCRead && format.tablePath.isDefined) meta.willNotWorkOnGpu("CDC reads with deletion vectors are not yet supported on GPU for DB-17.3")` until proven. (6) Update `convertToGpu` to pass each of the eight CPU flags through (`fmt.generateRowIndexFilterId`, `fmt.generateRowIndexFilterColumn`, `fmt.generateDeltaFileInScanId`, `fmt.nullableRowTrackingConstantFields`, `fmt.nullableRowTrackingGeneratedFields`, `fmt.optimizationsEnabled`, `fmt.tablePath`, `fmt.isCDCRead`) — or use `fmt.copyWithDVInfo(tablePath, isCDCRead)` then read fields, whichever is cleaner. Force `optimizationsEnabled = false` when DV is enabled (V1 path requires whole-file reads to keep `_metadata.row_index` consistent). |

### Provider — add DV-scan hooks

[DeltaSpark400DB173Provider.scala](delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala) inherits from `DatabricksDeltaProviderBase`, whose `tagSupportForGpuFileSourceScan` and `getReadFileFormat` already delegate to `GpuDeltaParquetFileFormat.{tagSupportForGpuFileSourceScan, convertToGpu}` — that part stays as-is.

However, `DeltaProvider` declares four DV-scan hooks at [DeltaProvider.scala:100-109](sql-plugin/src/main/scala/com/nvidia/spark/rapids/delta/DeltaProvider.scala#L100-L109) — `canPushDVPredicateDownToScan`, `pushDVPredicateDownToScan`, `pruneFileMetadata`, `isDVScan` — with no-op defaults. They are **called** in two places that matter for DV reads:

- [GpuTransitionOverrides.scala:807-809](sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuTransitionOverrides.scala#L807-L809) — `pruneFileMetadata` then conditional `pushDVPredicateDownToScan`.
- [ScanExecShims.scala:64](sql-plugin/src/main/spark330/scala/com/nvidia/spark/rapids/shims/ScanExecShims.scala#L64) — `if (!DeltaProvider().isDVScan(meta)) meta.willNotWorkOnGpu("hidden metadata columns are not supported on GPU")`.

OSS [DeltaProviderBase.scala:141-216](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/DeltaProviderBase.scala#L141-L216) overrides all four. The Databricks shared base does not (DB-14.3 had no GPU DV support).

**For our V1 stance** (`optimizationsEnabled=false` on DV tables → `useMetadataRowIndex=false`), Delta does NOT inject `_metadata` (`FileSourceMetadataAttribute`) into the scan; it uses `__delta_internal_row_index` instead. So strictly speaking these hooks are not needed for the happy V1 path. **But** if a user ever sets `DELETION_VECTORS_USE_METADATA_ROW_INDEX=true`, the default `isDVScan=false` will cause silent CPU fallback at [ScanExecShims.scala:64](sql-plugin/src/main/spark330/scala/com/nvidia/spark/rapids/shims/ScanExecShims.scala#L64) (the "hidden metadata columns are not supported" branch). Add defensive overrides in `DeltaSpark400DB173Provider`:

- `canPushDVPredicateDownToScan(conf): Boolean = false` — V1 path doesn't support pushdown; explicit override prevents accidental V2 path activation.
- `pushDVPredicateDownToScan(plan): SparkPlan = plan` — no-op, matches the gate above.
- `pruneFileMetadata(plan): SparkPlan = plan` — no-op for V1.
- `isDVScan(meta): Boolean` — port the OSS shape from [DeltaProviderBase.scala:202-216](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/DeltaProviderBase.scala#L202-L216) (matches `Project → Filter → Project → FileSourceScan` with `is_row_deleted` reference and `_metadata` input column). Use the DB-17.3 column name `__delta_internal_is_row_deleted` (i.e. `DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME`) instead of the OSS-imported constant. This protects against the `useMetadataRowIndex=true` config edge case.

These are additions to [DeltaSpark400DB173Provider.scala](delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala) (~20 LOC). No changes elsewhere.

### `pom.xml` — no change

[delta-lake/delta-spark400db173/pom.xml](delta-lake/delta-spark400db173/pom.xml) already pulls in `delta-lake/common/src/main/scala` and `delta-lake/common/src/main/databricks/scala`, and already declares the `RoaringBitmap` dependency. No new source roots, no new dependencies.

### Test helper edit

| Path | Change |
|---|---|
| [integration_tests/src/main/python/spark_session.py](integration_tests/src/main/python/spark_session.py) (`supports_delta_lake_deletion_vectors`) | Include `is_databricks173_or_later()` in the True branch. This auto-enables ~9 currently-skipped DV-read tests on DB-17.3. Do this **last**, after the Scala port passes spot-checks, so prior steps don't accidentally re-skip. |

### Out of this PR

- [jenkins/databricks/test.sh](jenkins/databricks/test.sh) lines 150–160 (the `spark400db173` Delta-tests skip) — flipped under Issue 7.
- Doc updates in [delta-lake-db173-github-issues.md](delta-lake-db173-github-issues.md) / [delta-lake-db173-checkpoint.md](delta-lake-db173-checkpoint.md) — small reordering of the dependency graph; will land alongside the implementation PR.

## Namespace substitution map

Used throughout the ported files. `sed`-style mapping:

| OSS import (delta-33x-40x) | Databricks import (DB-17.3) |
|---|---|
| `org.apache.spark.sql.delta.DeltaParquetFileFormat` | `com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat` |
| `org.apache.spark.sql.delta.DeltaParquetFileFormat._` (companion constants `IS_ROW_DELETED_COLUMN_NAME`, `ROW_INDEX_COLUMN_NAME`, `FILE_ROW_INDEX_FILTER_ID_ENCODED`, `FILE_ROW_INDEX_FILTER_TYPE`) | `com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat._` (same constant names; spot-verify on first build) |
| `org.apache.spark.sql.delta.actions.DeletionVectorDescriptor` | `com.databricks.sql.transaction.tahoe.actions.DeletionVectorDescriptor` |
| `org.apache.spark.sql.delta.deletionvectors.{StoredBitmap, RoaringBitmapArray}` | `com.databricks.sql.transaction.tahoe.deletionvectors.{StoredBitmap, RoaringBitmapArray}` |
| `org.apache.spark.sql.delta.deletionvectors.{NativeRoaringBitmapArraySerializationFormat, PortableRoaringBitmapArraySerializationFormat}` | `com.databricks.sql.transaction.tahoe.deletionvectors.{...}` (same names) |
| `org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore` | `com.databricks.sql.transaction.tahoe.storage.dv.HadoopFileSystemDVStore` |
| `org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaColumnMappingMode, IdMapping, NameMapping, NoMapping}` | `com.databricks.sql.transaction.tahoe.{DeltaColumnMapping, DeltaColumnMappingMode, IdMapping, NameMapping, NoMapping}` |
| `org.apache.spark.sql.delta.actions.{Metadata, Protocol}` | `com.databricks.sql.transaction.tahoe.actions.{Metadata, Protocol}` |
| `org.apache.spark.sql.delta.schema.SchemaMergingUtils` | `com.databricks.sql.transaction.tahoe.schema.SchemaMergingUtils` |
| `org.apache.spark.sql.delta.sources.DeltaSQLConf` | `com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf` |
| `org.apache.spark.sql.delta.RowIndexFilterType` | `com.databricks.sql.io.RowIndexFilterType` (DB moves this to `c.d.sql.io`) |
| `org.apache.spark.sql.delta.DeltaErrors` | `com.databricks.sql.transaction.tahoe.DeltaErrors` |
| `org.apache.spark.sql.delta.TypeWidening.assertTableReadable(...)` | **Drop** the call. DB-17.3's CPU `DeltaParquetFileFormat` enforces this on the CPU side. |
| `org.apache.spark.sql.delta.logging.DeltaLogKeys` | Drop. Replace `MDC` log usage in `translateFilterForColumnMapping` with plain `logError(s"Failed to translate filter $filter")`. |

V1 does **not** use `com.databricks.sql.io.RowIndexFilterProvider.retrieve(conf)`. We resolve the bitmap directly via `StoredBitmap.create(dvDescriptor, new Path(tablePath)).load(new HadoopFileSystemDVStore(conf))` — same shape as the OSS code. `RowIndexFilterProvider` stays in our pocket for the V2 follow-up.

## Constructor and provider wiring

The DB-17.3 CPU `DeltaParquetFileFormat` constructor (verified via `javap` on `/databricks/jars/...sql--core...`) has 10 fields, **not** the 6 the OSS Delta-4.0 shape has. Both are listed in the OSS `Delta40xProvider`'s 6-arg pass-through but DB-17.3 splits row-tracking-nullability into two fields and adds three row-index-filter-generation flags. Mirror the CPU shape exactly:

Modified case class:

```scala
case class GpuDeltaParquetFileFormat(
    protocol: Protocol,
    metadata: Metadata,
    generateRowIndexFilterId: Boolean = false,
    generateRowIndexFilterColumn: Boolean = false,
    generateDeltaFileInScanId: Boolean = false,
    nullableRowTrackingConstantFields: Boolean = false,
    nullableRowTrackingGeneratedFields: Boolean = false,
    optimizationsEnabled: Boolean = true,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false
) extends GpuDeltaParquetFileFormatDV(
    protocol, metadata,
    nullableRowTrackingConstantFields, nullableRowTrackingGeneratedFields,
    optimizationsEnabled, tablePath, isCDCRead)
```

The three `generateRowIndexFilter*` flags are CPU-only (they control which row-index columns appear in the read schema). The GPU FileFormat receives them so that `equals`/`hashCode` parity matches the CPU instance, but does not act on them — the GPU DV path always operates as if they're set when `tablePath.isDefined` (the V1 reader inserts `__delta_internal_row_index` and `__delta_internal_is_row_deleted` itself based on the schema it sees).

`convertToGpu` updates — pass each CPU field through, then force `optimizationsEnabled = false` when DV is on:

```scala
val fmt = relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
val dvEnabled = fmt.tablePath.isDefined
GpuDeltaParquetFileFormat(
  protocol = fmt.protocol,
  metadata = fmt.metadata,
  generateRowIndexFilterId = fmt.generateRowIndexFilterId,
  generateRowIndexFilterColumn = fmt.generateRowIndexFilterColumn,
  generateDeltaFileInScanId = fmt.generateDeltaFileInScanId,
  nullableRowTrackingConstantFields = fmt.nullableRowTrackingConstantFields,
  nullableRowTrackingGeneratedFields = fmt.nullableRowTrackingGeneratedFields,
  optimizationsEnabled = !dvEnabled, // V1 stance: no split / no pushdown when DV is on
  tablePath = fmt.tablePath,
  isCDCRead = fmt.isCDCRead)
```

Alternative implementation: call `fmt.copyWithDVInfo(tablePath, isCDCRead)` (DB-17.3 helper, see `javap` output) and then read fields off the result. Whichever is cleaner at compile time.

`tagSupportForGpuFileSourceScan` body:

```scala
val requiredSchema = meta.wrapped.requiredSchema
// Keep the existing _databricks_internal block. Verified via javap -p -v on
// DeltaParquetFileFormat$.class: DV columns are named __delta_internal_is_row_deleted
// and __delta_internal_row_index (double-underscore-delta), so they don't collide
// with this prefix. The block guards an unrelated Databricks-internal feature.
if (requiredSchema.exists(_.name.startsWith("_databricks_internal"))) {
  meta.willNotWorkOnGpu(
    "reading metadata columns starting with prefix _databricks_internal is not supported")
}
val format = meta.wrapped.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
if (format.isCDCRead && format.tablePath.isDefined) {
  meta.willNotWorkOnGpu(
    "CDC reads with deletion vectors are not yet supported on GPU for DB-17.3")
}
// no unconditional DV block — DV reads supported via V1 path
```

## Multi-file reader factory

`GpuDeltaParquetFileFormatDV.createMultiFileReaderFactory` overrides the Databricks-shared base's plain `GpuParquetMultiFilePartitionReaderFactory` with a `DeltaMultiFileReaderFactory` that wraps the standard parent reader in `DeltaMultiFileParquetPartitionReader`. The reader uses `InputFileUtils.getCurInputFilePath()` to identify the active `PartitionedFile`, reads the DV descriptor from `partitionedFile.otherConstantMetadataColumnValues[FILE_ROW_INDEX_FILTER_ID_ENCODED / TYPE]`, instantiates a per-file `RapidsRowIndexFilter`, and applies it to each batch via `RapidsDeletionVectorUtils.processBatchWithDeletionVector` (materializes the `is_row_deleted` INT8 column, plus optional row-index column, into the batch).

Notes:
- Coalescing reader path: log a warning ("Coalescing is not supported when `delta.enableDeletionVectors=true`") and fall through to multi-threaded — coalescing combines small files in ways incompatible with per-file DV bookkeeping.
- Pass `queryUsesInputFile = hasTablePath || fileScan.queryUsesInputFile` to defeat small-file combining when DV is on.
- Metric keys `rowIndexColumnGenTime` and `isRowDeletedColumnGenTime` must exist in `GpuFileSourceScanExec.allMetrics`. They come from `GpuReadParquetFileFormat`/parent today; if the first build run reports them missing, add them to a `metrics` override on `GpuDeltaParquetFileFormatDV` (cheap addition).

## Risks and unknowns

1. **DV serialized format compatibility (V2 only)** — not a V1 risk. V1 reads through Databricks's own `StoredBitmap.create(...).load(dvStore)`, which is byte-compatible by construction.
2. **Constants on the DB-17.3 companion** — `IS_ROW_DELETED_COLUMN_NAME`, `ROW_INDEX_COLUMN_NAME`, `FILE_ROW_INDEX_FILTER_ID_ENCODED`, `FILE_ROW_INDEX_FILTER_TYPE` are reportedly all on `com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat`. Spot-verify on first compile.
3. **`fmt.optimizationsEnabled` field presence** — OSS 33x/40x both have it. DB-17.3's `DeltaParquetFileFormat` may or may not expose it directly. If absent, fall back to `!fmt.tablePath.isDefined` (the same value we want for V1 anyway).
4. **`_metadata.row_index` predicate** — with `optimizationsEnabled=false` on DV tables, `prepareFiltersForRead` returns `Seq.empty`, so any `_metadata.row_index` predicate is dropped at the scan and re-applied above. This matches OSS 33x default behavior. Risk surfaced specifically by `test_delta_deletion_vector_read_drop_row_group`. If that test fails, fallback gate: add `if (requiredSchema.fieldNames.contains("_metadata")) meta.willNotWorkOnGpu(...)` in `tagSupportForGpuFileSourceScan`.
5. **CDC + DV** — guarded off in this PR. Re-enable later under a follow-up once CDC plumbing is reviewed.
6. **`Spark400PlusDBShims.getPartitionFiles` + `FilePartitionShims.withPathPrefixIfNeeded`** — already shipped (Issue 8 fix). DV reads depend on these because per-file DV metadata is keyed off the absolute path. Validate via `test_delta_scan_split_with_DV_enabled_with_DVs` in the test plan.
7. **`RowIndexFilterProvider` classpath at runtime** — not used by V1; only `RowIndexFilterType` (the enum) is imported from `com.databricks.sql.io`. Confirmed present.

## Step-by-step implementation order

Order minimizes time-to-first-failure. After each step, build with:

```bash
mvn -f scala2.13/pom.xml -Dbuildver=400db173 \
  -pl dist,delta-lake/delta-spark400db173 -am install -DskipTests
```

(Reminder from project CLAUDE.md: DB-17.3 is Scala 2.13 only.)

1. **Scaffold the bitmap-loading layer.** Add `RapidsDeletionVectorStore.scala` and `RapidsStoredBitmap.scala` under `com/databricks/sql/transaction/tahoe/deletionvectors/`. No dependents yet — build should be green immediately.
2. **Scaffold the GPU filter layer.** Add `RapidsRowIndexFilters.scala` and the slim `RapidsDeletionVectors.scala` under `com/nvidia/spark/rapids/delta/`. Build green.
3. **Add `GpuDeltaParquetFileFormatDV.scala`.** This is the largest single file (~360 LOC), bundling the abstract format class, `RapidsDeletionVectorUtils`, `DeltaMultiFileReaderFactory`, and `DeltaMultiFileParquetPartitionReader`. Build green.
4. **Wire `GpuDeltaParquetFileFormat.scala`.** Switch its parent class, expand the constructor, drop local overrides, rewrite `tagSupportForGpuFileSourceScan`, update `convertToGpu`. Build green. At this point GPU plans no longer block on DV.
5. **First cluster smoke test.**
   ```bash
   ./integration_tests/run_pyspark_from_build.sh --runtime_env=databricks --delta_lake \
     -k 'test_delta_deletion_vector_read and use_metadata_row_index-False and PERFILE'
   ```
   Triage failures. Common first-run issues: missing companion constant, `optimizationsEnabled` field absence, metric keys missing.
6. **Reader-type sweep.**
   ```bash
   ./integration_tests/run_pyspark_from_build.sh --runtime_env=databricks --delta_lake \
     -k 'test_delta_deletion_vector_read or test_delta_deletion_vector_multithreaded_read or
         test_delta_deletion_vector_multithreaded_combine_count_star or
         test_delta_deletion_vector_multithreaded_read_partitioned_table or
         test_delta_deletion_vector_coalescing_count_star or
         test_delta_deletion_vector_coalescing_partitioned_table'
   ```
7. **Edge-case sweep.** Empty DV, mixed DV / no-DV in one scan, split correctness, row-group skipping.
   ```bash
   ./integration_tests/run_pyspark_from_build.sh --runtime_env=databricks --delta_lake \
     -k 'test_delta_empty_deletion_vector_read or test_delta_deletion_vector_mixed_dv_no_dv or
         test_delta_scan_split_with_DV_enabled_with_DVs or
         test_delta_deletion_vector_read_drop_row_group'
   ```
8. **Column-mapping interaction.**
   ```bash
   ./integration_tests/run_pyspark_from_build.sh --runtime_env=databricks --delta_lake \
     -k 'test_delta_read_column_mapping or test_delta_name_column_mapping_no_field_ids'
   ```
9. **Negative / fallback coverage.** Confirm DV-related operations that should still fall back to CPU continue to do so (no regression of expected fallbacks).
   ```bash
   ./integration_tests/run_pyspark_from_build.sh --runtime_env=databricks --delta_lake \
     -k 'test_delta_deletion_vector_fallback or test_delta_update_fallback_with_deletion_vectors'
   ```
10. **Enable the test gate.** Edit `supports_delta_lake_deletion_vectors` in `integration_tests/src/main/python/spark_session.py` to include `is_databricks173_or_later()`. Re-run steps 6–9 to confirm none of them re-skipped.
11. **Final regression.** Run `delta_lake_test.py` end-to-end:
    ```bash
    ./integration_tests/run_pyspark_from_build.sh --runtime_env=databricks --delta_lake \
      delta_lake_test.py
    ```

CI re-enable (Jenkins skip flip in `jenkins/databricks/test.sh`) is **out of scope** here; lands under Issue 7.

## Verification

End-to-end success criteria, in order:

1. `mvn -f scala2.13/pom.xml -Dbuildver=400db173 -pl delta-lake/delta-spark400db173 -am install -DskipTests` is green.
2. `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh` is green.
3. `mvn -Dbuildver=330 install -DskipTests` is green (OSS 3.3 unaffected).
4. `mvn -f scala2.13/pom.xml -Dbuildver=400 install -DskipTests` is green (OSS 4.0 unaffected).
5. `mvn -f scala2.13/pom.xml -Dbuildver=350db143 install -DskipTests` is green (DB-14.3 unaffected).
6. The ~18 tests listed in steps 6–9 of the implementation order above all pass with CPU/GPU result parity on a live DB-17.3 cluster (or expected-fallback behavior in step 9).
7. Plan capture on at least one DV test (e.g. `test_delta_deletion_vector_read[PERFILE-...]`) confirms `GpuFileSourceScanExec` in the executed plan with no `ColumnarToRow` transitions on the read side.

## Effort estimate

- 5 new Scala files, 2 modified Scala files (`GpuDeltaParquetFileFormat.scala`, `DeltaSpark400DB173Provider.scala`), 1 modified Python helper.
- ~900–950 LOC net (≈80% verbatim port + import rewrites). Provider DV-scan hooks add ~20 LOC.
- Roughly 1 dev-week: 2 days for the mechanical port + initial build, 2 days for first-cluster bring-up + Spark 4.0 / DB-17.3 corner cases (10-field constructor, two-field row-tracking-nullability), 1 day for column-mapping / mixed-DV / row-group edge tests.

## Critical files (for the implementer)

Port sources (read-only references):
- [delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala)
- [delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsRowIndexFilters.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsRowIndexFilters.scala)
- [delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsDeletionVectors.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsDeletionVectors.scala)
- [delta-lake/common/src/main/delta-33x-40x/scala/org/apache/spark/sql/delta/deletionvectors/RapidsDeletionVectorStore.scala](delta-lake/common/src/main/delta-33x-40x/scala/org/apache/spark/sql/delta/deletionvectors/RapidsDeletionVectorStore.scala)
- [delta-lake/common/src/main/delta-33x-40x/scala/org/apache/spark/sql/delta/deletionvectors/RapidsStoredBitmap.scala](delta-lake/common/src/main/delta-33x-40x/scala/org/apache/spark/sql/delta/deletionvectors/RapidsStoredBitmap.scala)

Targets (to create / modify):
- [delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala](delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala) (modify)
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatDV.scala` (new)
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/RapidsRowIndexFilters.scala` (new)
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/RapidsDeletionVectors.scala` (new)
- `delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/deletionvectors/RapidsDeletionVectorStore.scala` (new)
- `delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/deletionvectors/RapidsStoredBitmap.scala` (new)

Reference / context (read-only):
- [delta-lake/common/src/main/databricks/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatBase.scala](delta-lake/common/src/main/databricks/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatBase.scala) — the Databricks-namespaced shared parent of our format class.
- [delta-lake/common/src/main/databricks/scala/com/nvidia/spark/rapids/delta/DatabricksDeltaProviderBase.scala:99-112](delta-lake/common/src/main/databricks/scala/com/nvidia/spark/rapids/delta/DatabricksDeltaProviderBase.scala#L99-L112) — confirms the provider already routes through `GpuDeltaParquetFileFormat.tagSupportForGpuFileSourceScan` / `.convertToGpu`.
- [delta-lake/delta-40x/src/main/scala/com/nvidia/spark/rapids/delta/delta40x/Delta40xProvider.scala:84](delta-lake/delta-40x/src/main/scala/com/nvidia/spark/rapids/delta/delta40x/Delta40xProvider.scala#L84) — Spark 4.0 baseline for `nullableRowTrackingFields = false`.
- [sql-plugin/src/main/spark400db173/scala/org/apache/spark/sql/execution/rapids/shims/FilePartitionShims.scala](sql-plugin/src/main/spark400db173/scala/org/apache/spark/sql/execution/rapids/shims/FilePartitionShims.scala) — `withPathPrefixIfNeeded` (DB-17.3 absolute-path resolution, prerequisite for DV reads — already shipped).
- [delta-lake-db173-design.md](delta-lake-db173-design.md), [delta-lake-db173-github-issues.md](delta-lake-db173-github-issues.md), [delta-lake-db173-checkpoint.md](delta-lake-db173-checkpoint.md) — design docs to update once this PR lands.
