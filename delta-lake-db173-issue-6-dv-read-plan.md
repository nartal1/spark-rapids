# Issue 6 — GPU Deletion Vector Reads on Databricks 17.3 (Source of Truth)

**Date:** 2026-04-29
**Tracking:** Issue 6 in [delta-lake-db173-github-issues.md](delta-lake-db173-github-issues.md), GitHub issue [#14600](https://github.com/NVIDIA/spark-rapids/issues/14600)
**Target module:** `delta-lake/delta-spark400db173`
**Status:** **Implemented and validated locally** (2026-05-02)

This document is the merged single source of truth for Issue 6 planning. It supersedes both
[delta-lake-db173-issue-6-dv-read-plan_claude.md](delta-lake-db173-issue-6-dv-read-plan_claude.md)
and [delta-lake-db173-deletion-vector-plan_codex.md](delta-lake-db173-deletion-vector-plan_codex.md);
those are kept for history. Where the two plans differed, this doc cites the verified
source (Databricks jar via `javap`, OSS source, or design doc).

## 2026-05-02 Implementation Status

Issue 6 is implemented for DB-17.3 using the V1/materialized DV read path
defined in this plan. The implementation landed DB-17.3-local versions of the
DV file-format wrapper and row-index helpers, and did not port the native
cuDF/Base2 path.

Implemented source changes:
- Added `GpuDeltaParquetFileFormatDV.scala`.
- Added `RapidsDeletionVectors.scala`.
- Added `RapidsRowIndexFilters.scala`.
- Updated `GpuDeltaParquetFileFormat.scala` to mirror the DB-17.3 CPU format
  flags, allow the DB edge skip-row column, and guard unsupported CDC / row
  tracking / file-in-scan paths.
- Updated `DeltaSpark400DB173Provider.scala` for GPU Delta format recognition,
  DV scan detection, metadata pruning, identity `pushDVPredicateDownToScan`, and
  `canPushDVPredicateDownToScan=false`.
- Updated DV integration tests to verify DB-17.3 GPU scan execution where this
  path is expected and to keep the DB-17.3 split expectation aligned with the V1
  no-pushdown path.

Validation completed:
- Build: `SKIP_DEP_INSTALL=1 WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`
  passed (`/tmp/db173-build-final-review-cleanups.log`).
- Focused post-cleanup DV run:
  `delta_lake_delete_test.py::test_delta_deletion_vector_read`
  + `delta_lake_test.py::test_delta_empty_deletion_vector_read`
  passed with `57 passed, 35 warnings in 182.72s`
  (`/tmp/db173-dv-focused-after-cleanups.log`).
- Full `delta_lake_test.py` was green before final cleanup:
  `180 passed, 40 skipped, 13 xpassed, 34 warnings in 726.67s`
  (`/tmp/db173-delta-lake-full-final2.log`).
- Full `delta_lake_delete_test.py` was green before final cleanup:
  `40 passed, 16 xfailed, 28 warnings in 276.22s`
  (`/tmp/db173-delta-lake-delete-full-final2.log`).

Known remaining gaps are unchanged from the blueprint: native cuDF DV reader,
DV predicate pushdown, split/native optimized DV scanning, true DV-aware
coalescing, CDC + DV reads, and Issue 7 CI enablement.

---

## Context

DB-17.3 makes deletion vectors (DVs) the default for new Delta tables. Before Issue 6,
the DB-17.3 shim explicitly blocked GPU for any DV-enabled scan in
[delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala](delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala#L102-L107):
`tagSupportForGpuFileSourceScan` calls `willNotWorkOnGpu("deletion vector reads are not yet
supported for DB-17.3")` whenever `format.tablePath.isDefined`. The result: virtually every
Delta read on DB-17.3 falls back to a CPU `FileSourceScanExec`.

We are reordering Issue 6 (GPU DV reads) ahead of Issues 3/4/5 (GPU DELETE/UPDATE/MERGE/
OPTIMIZE) because the target customers are **read-heavy**, not MERGE-heavy. DV reads alone
unlock GPU acceleration for SELECT/aggregate/join workloads on every DV-tagged Delta table,
including DV tables created by writers outside this plugin (Databricks UI, dbt, CPU Spark
jobs).

This plan covers only the **V1 read path**: GPU Parquet read followed by a post-read
`is_row_deleted` mask materialized from the Delta deletion vector. The V2 cuDF-native path
(`GpuDeltaParquetFileFormatBase2`, where the bitmap feeds cuDF's Parquet reader directly)
is deferred until cuDF compatibility with Databricks' `StoredBitmap` wire format is
verified.

---

## Outcome and non-goals

**Done means**

- DB-17.3 cluster uses `GpuFileSourceScanExec` for DV-enabled Delta table reads.
- DV bitmap is loaded per file via
  `PartitionedFile.otherConstantMetadataColumnValues[FILE_ROW_INDEX_FILTER_ID_ENCODED]`
  and applied as a post-read `is_row_deleted` mask plus row-index column.
- All `supports_delta_lake_deletion_vectors`-gated DV-read tests in
  [integration_tests/src/main/python/delta_lake_test.py](integration_tests/src/main/python/delta_lake_test.py)
  that are **not Databricks-skipped today** pass on DB-17.3 with CPU/GPU result parity.
  Several DV tests carry `@skipif(is_databricks_runtime())` markers (see "Databricks-skipped
  tests" below) — those stay skipped in this PR unless the marker is explicitly removed
  as part of the work.
- Builds remain green for `spark400db173`, OSS `delta-33x` / `delta-40x`, and DB-14.3
  (`delta-spark350db143`).

**Non-goals (explicitly deferred)**

- V2 cuDF-native DV reader path.
- GPU DELETE/UPDATE/MERGE/OPTIMIZE on DB-17.3 (Issues 3/4/5).
- DV writes (Delta-log creation of DVs from GPU DML).
- Re-enabling Delta tests in CI for `spark400db173` — that flip is Issue 7 and waits for
  a clean Jenkins run.

---

## DB-17.3 DV API surface (verified against the live jar)

The following was confirmed via `javap -p -v` on classes extracted from
`/databricks/jars/----ws_4_0--sql--core--core-hive-2.3__hadoop-3.2_2.13_deploy.jar` and
`/databricks/jars/----ws_4_0--sql--catalyst--catalyst-hive-2.3__hadoop-3.2_2.13_deploy.jar`:

### `com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat`

10-field constructor (this differs from the OSS Delta-4.0 6-field signature):

```scala
DeltaParquetFileFormat(
  protocol: Protocol,
  metadata: Metadata,
  generateRowIndexFilterId: Boolean,
  generateRowIndexFilterColumn: Boolean,
  generateDeltaFileInScanId: Boolean,
  nullableRowTrackingConstantFields: Boolean,   // split from OSS's nullableRowTrackingFields
  nullableRowTrackingGeneratedFields: Boolean,  // split from OSS's nullableRowTrackingFields
  optimizationsEnabled: Boolean,
  tablePath: Option[String],
  isCDCRead: Boolean
)
```

Helper: `copyWithDVInfo(tablePath: String, optimizationsEnabled: Boolean): DeltaParquetFileFormat` —
returns a copy with DV info set. The second boolean is **not** `isCDCRead`; DB-17.3's
`ScanWithDeletionVectors` passes `useMetadataRowIndex` here, and the helper preserves the
original `isCDCRead` value from the copied format. Prefer direct field pass-through in our GPU
conversion path unless implementation discovery finds a vanilla CPU `DeltaParquetFileFormat`
that still needs DV state attached before conversion.

Companion (`DeltaParquetFileFormat$`) constants (verified strings):

| Scala name                         | Actual string                       |
| ---------------------------------- | ----------------------------------- |
| `FILE_ROW_INDEX_FILTER_ID_ENCODED` | `row_index_filter_id_encoded`       |
| `FILE_ROW_INDEX_FILTER_TYPE`       | `row_index_filter_type`             |
| `FILE_ROW_INDEX_FILTER_ID`         | `row_index_filter_id`               |
| `INCLUDED_IN_ROW_INDEX_FILTER`     | `included_in_row_index_filter`      |

The `_databricks_internal` prefix used in the existing `tagSupportForGpuFileSourceScan`
guard is generally unsupported, but DB-17.3 plan capture showed one important
DV exception: `_databricks_internal_edge_computed_column_skip_row` is the
materialized skip-row mask used by the data-read filter. This specific column
must be allowed through scan tagging and generated by the GPU reader. For
row-index metadata, use Spark's temporary row-index column
(`ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME`, `_tmp_metadata_row_index`)
rather than Delta's private `ROW_INDEX_COLUMN_NAME`.

### `com.databricks.sql.io.RowIndexFilterProvider` (Java interface)

```java
RowIndexFilter retrieve(Configuration conf);
SerializedBitmap retrieveSerialized(Configuration conf);
long getSize();
long getCardinality();
String getUniqueId();
RowIndexFilterType getRowIndexFilterType();
```

`retrieve` returns a `RowIndexFilter` with CPU-targeting materialization
(`materializeIntoVector(WritableColumnVector)`). V1 does **not** use this; V1 resolves
the bitmap directly via `StoredBitmap.create(dvDescriptor, tablePath).load(dvStore)` and
applies it on GPU columns. `retrieveSerialized` is the V2 entry point (raw bytes).

### Other Databricks DV classes (all confirmed present)

- `com.databricks.sql.transaction.tahoe.deletionvectors.{StoredBitmap, RoaringBitmapArray, NativeRoaringBitmapArraySerializationFormat, PortableRoaringBitmapArraySerializationFormat, DropMarkedRowsFilter, KeepMarkedRowsFilter, DropAllRowsFilter, KeepAllRowsFilter}`
- `com.databricks.sql.transaction.tahoe.storage.dv.HadoopFileSystemDVStore`
- `com.databricks.sql.io.{RowIndexFilter, RowIndexFilterType}`

---

## Strategy: copy + namespace substitute (no shared OSS source root)

The OSS DV implementation lives under `delta-lake/common/src/main/delta-33x-40x/scala/`,
which OSS `delta-33x` and `delta-40x` modules pull in via `pom.xml` `add-source` blocks.
The `delta-spark400db173` `pom.xml` deliberately does NOT add this root because the OSS
files import `org.apache.spark.sql.delta.*` types that do not exist on DB-17.3.

We will **copy** the V1 DV files into `delta-lake/delta-spark400db173/src/main/scala/`
with OSS namespaces substituted with their Databricks equivalents. We will NOT introduce
a new shared common source root in this PR. Rationale:

- Minimizes blast radius — no risk to the OSS modules or any other Databricks shim.
- Mirrors how the Databricks shims already handle Delta-namespaced code (each shim has
  its own `GpuDeltaParquetFileFormat.scala`).
- Refactoring into a shared `delta-lake/common/src/main/databricks-dv/scala/` root is a
  viable follow-up once the Databricks-side shape stabilizes.

V1 port surface ≈ **600 LOC across 3 OSS files** (`GpuDeltaParquetFileFormatBase.scala`,
`RapidsRowIndexFilters.scala`, slim subset of `RapidsDeletionVectors.scala`); ≈ 80%
verbatim with import rewrites. The other two OSS files
(`RapidsDeletionVectorStore.scala`, `RapidsStoredBitmap.scala`) are V2-only and skipped
— see next section.

---

## File-by-file deliverables

### New files under `delta-lake/delta-spark400db173/src/main/scala/`

V1 uses Databricks's `StoredBitmap.create(...).load(new HadoopFileSystemDVStore(conf))`
directly (verified at [GpuDeltaParquetFileFormatBase.scala:485-486](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala#L485-L486)).
We do **not** port `RapidsDeletionVectorStore.scala` or `RapidsStoredBitmap.scala`:
they are consumed by `RapidsDeletionVectors.loadDeletionVector` (the V2-only
`HostMemoryBuffer` loader; verified at [RapidsDeletionVectors.scala:106-129](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsDeletionVectors.scala#L106-L129))
which `GpuDeltaParquetFileFormatBase2.scala` calls. The V1 helper
`RapidsDeletionVectors.loadScalaBitmap` (lines 145-169) does **not** use them — it calls
`StoredBitmap.create(...).load(new HadoopFileSystemDVStore(conf))` directly. Skipping the
two files saves ~275 LOC and avoids carrying V2 code we won't exercise.

| Path | Origin (port from) | Purpose | LOC |
|---|---|---|---|
| `com/nvidia/spark/rapids/delta/RapidsRowIndexFilters.scala` | [delta-lake/common/.../RapidsRowIndexFilters.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsRowIndexFilters.scala) | Trait `RapidsRowIndexFilter` plus `RapidsDropMarkedRowsFilter` / `RapidsKeepMarkedRowsFilter` / `RapidsDropAllRowsFilter` / `RapidsKeepAllRowsFilter`. Each takes a Databricks `RoaringBitmapArray` directly. Materializes the bitmap into an INT8 `skip_row` GPU `ColumnVector` via `cudf::ColumnVector.contains`/`not`. | ~130 |
| `com/nvidia/spark/rapids/delta/RapidsDeletionVectors.scala` | [delta-lake/common/.../RapidsDeletionVectors.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsDeletionVectors.scala) (slim) | Keep `translateFilterForColumnMapping` (used by `prepareFiltersForRead` for `NameMapping`/`IdMapping` translation) and any V1-relevant helpers. `loadScalaBitmap` may be kept only if useful for the materialized path because it uses regular `StoredBitmap`; drop V2-only helpers such as `loadDeletionVector(HostMemoryBuffer)`, `dropFirstColumn`, and `getRowGroupMetadata`. | ~110 |
| `com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatDV.scala` | [delta-lake/common/.../GpuDeltaParquetFileFormatBase.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala) | New abstract class `GpuDeltaParquetFileFormatDV` extending the Databricks-shared [GpuDeltaParquetFileFormatBase](delta-lake/common/src/main/databricks/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatBase.scala). Hosts: (a) `prepareSchema` override stripping `PARQUET_FIELD_NESTED_IDS_METADATA_KEY`; (b) `prepareFiltersForRead`; (c) `isSplitable = optimizationsEnabled`; (d) DV-aware `buildReaderWithPartitionValuesAndMetrics`; (e) `createMultiFileReaderFactory` returning `DeltaMultiFileReaderFactory`. The DV-loading inline (no wrapper) calls `StoredBitmap.create(dvDesc, tablePath).load(new HadoopFileSystemDVStore(conf))` directly. Companion utilities `RapidsDeletionVectorUtils`, `DeltaMultiFileReaderFactory`, `DeltaMultiFileParquetPartitionReader` live in the same file. | ~360 |

### Modified files

| Path | Change |
|---|---|
| [delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala](delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala) | (1) Extend `GpuDeltaParquetFileFormatDV` instead of bare Databricks `GpuDeltaParquetFileFormatBase`. (2) **Expand the case class to mirror the DB-17.3 CPU `DeltaParquetFileFormat`'s 10-field constructor** (see "DB-17.3 DV API surface" above) — `protocol`, `metadata`, `generateRowIndexFilterId`, `generateRowIndexFilterColumn`, `generateDeltaFileInScanId`, `nullableRowTrackingConstantFields`, `nullableRowTrackingGeneratedFields`, `optimizationsEnabled`, `tablePath`, `isCDCRead`. (3) Drop local `isSplitable` and `buildReaderWithPartitionValuesAndMetrics` overrides — `GpuDeltaParquetFileFormatDV` (the new abstract class added under "New files") declares both. Note: the Databricks-shared `GpuDeltaParquetFileFormatBase` does **not** declare `isSplitable`, so this method must live in `GpuDeltaParquetFileFormatDV` (verified by reading the shared base file). (4) Rewrite `tagSupportForGpuFileSourceScan` body in the companion: allow `_databricks_internal_edge_computed_column_skip_row` as the DV skip-row mask, keep other `_databricks_internal` columns unsupported, **remove** the unconditional `format.tablePath.isDefined → willNotWorkOnGpu` block, and add the CDC-with-DV guard `if (format.isCDCRead && format.tablePath.isDefined) meta.willNotWorkOnGpu(…)`. (5) Update `convertToGpu` to pass each of the 8 CPU flags through, then force `optimizationsEnabled = false` when DV is on. |
| [delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala](delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala) | Add DV-scan hook overrides on top of `DatabricksDeltaProviderBase`: (a) `isSupportedFormat` must return `super.isSupportedFormat(format) || format == classOf[GpuDeltaParquetFileFormat]`. This mirrors OSS `Delta33xProvider` / `Delta40xProvider` and is required after scan conversion because `GpuFileSourceScanExec.allMetrics` gates DV metric creation through `ExternalSource.isSupportedFormat(relation.fileFormat.getClass)`. Without this, `rowIndexColumnGenTime` / `isRowDeletedColumnGenTime` may be absent once the relation uses the GPU Delta format. (b) `canPushDVPredicateDownToScan = false` — V1 path doesn't support pushdown. (c) `pushDVPredicateDownToScan = identity` — paired with (b). (d) **`pruneFileMetadata`: port the OSS implementation from [DeltaProviderBase.scala:165-200](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/DeltaProviderBase.scala#L165-L200)** — this actively rewrites `Project → Filter → Project → GpuFileSourceScanExec` to drop `_metadata` and `_tmp_metadata_row_index` columns when only used for DV processing. Identity is wrong here: if `useMetadataRowIndex=true` (a session-level Delta config independent of `optimizationsEnabled`), Delta's analyzer injects `_metadata` columns and the GPU scan can't materialize them — without active pruning the read fails. (e) Real `isDVScan` (port from [DeltaProviderBase.scala:202-216](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/DeltaProviderBase.scala#L202-L216), substituting `c.d.s.t.tahoe.DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME`). Keep these overrides DB-17.3-specific in this provider; lift to `DatabricksDeltaProviderBase` only after the same shape is proven correct for other Databricks runtimes. ~85 LOC (the `pruneFileMetadata` port is the bulk). |

### `pom.xml` — no change

[delta-lake/delta-spark400db173/pom.xml](delta-lake/delta-spark400db173/pom.xml) already
pulls in `delta-lake/common/src/main/scala` and `delta-lake/common/src/main/databricks/scala`,
and already declares the `RoaringBitmap` dependency. No new source roots, no new dependencies.

### Python test helper — no change

[`supports_delta_lake_deletion_vectors`](integration_tests/src/main/python/spark_session.py#L325-L329)
already returns True for any Databricks runtime ≥ 12.2 — DB-17.3 satisfies this, so
DV-read tests gated by `@skipif(not supports_delta_lake_deletion_vectors())` will
auto-enable once the Scala port lands. No edit to `spark_session.py` is needed.

### Out of this PR

- [jenkins/databricks/test.sh](jenkins/databricks/test.sh) lines 150–160 (the
  `spark400db173` Delta-tests skip) — flipped under Issue 7.
- Doc updates in [delta-lake-db173-github-issues.md](delta-lake-db173-github-issues.md) /
  [delta-lake-db173-checkpoint.md](delta-lake-db173-checkpoint.md) — small reordering of
  the dependency graph; will land alongside the implementation PR.

---

## Provider DV-scan hooks (why we need them on DB-17.3)

[DeltaProvider.scala:100-109](sql-plugin/src/main/scala/com/nvidia/spark/rapids/delta/DeltaProvider.scala#L100-L109)
declares four DV-scan hooks with no-op defaults:

```scala
def canPushDVPredicateDownToScan(conf: RapidsConf): Boolean = false
def pushDVPredicateDownToScan(plan: SparkPlan): SparkPlan = plan
def pruneFileMetadata(plan: SparkPlan): SparkPlan = plan
def isDVScan(meta: SparkPlanMeta[FileSourceScanExec]): Boolean = false
```

They are called in two places that matter for DV reads:

- [GpuTransitionOverrides.scala:807-809](sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuTransitionOverrides.scala#L807-L809)
  calls `pruneFileMetadata` then conditionally `pushDVPredicateDownToScan`.
- [ScanExecShims.scala:64](sql-plugin/src/main/spark330/scala/com/nvidia/spark/rapids/shims/ScanExecShims.scala#L64)
  calls `isDVScan` to allow `FileSourceMetadataAttribute` (`_metadata`) columns through:
  `if (!DeltaProvider().isDVScan(meta)) meta.willNotWorkOnGpu("hidden metadata columns are not supported on GPU")`.

OSS [DeltaProviderBase.scala:141-216](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/DeltaProviderBase.scala#L141-L216)
overrides all four. The Databricks shared base does not (DB-14.3 had no GPU DV support).

**Important correction:** `optimizationsEnabled` and `useMetadataRowIndex` are
**independent**. Forcing `optimizationsEnabled=false` in our `convertToGpu` does NOT
imply `useMetadataRowIndex=false` — the latter is read directly from
`DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX` at the session-config level
(verified at [GpuDeltaParquetFileFormatBase.scala:162-163](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala#L162-L163)).
If a user sets that config to `true`:

1. Delta's analyzer (via `PreprocessTableWithDVs`) injects `_metadata` /
   `_tmp_metadata_row_index` columns into the plan tree, **above** the GPU's reach.
2. With default `isDVScan=false`, [ScanExecShims.scala:64](sql-plugin/src/main/spark330/scala/com/nvidia/spark/rapids/shims/ScanExecShims.scala#L64)
   blocks the scan with "hidden metadata columns are not supported on GPU" — silent CPU
   fallback.
3. Even with our `isDVScan=true` override (which gets the scan past tagging), the
   `_metadata` columns still flow into the `GpuFileSourceScanExec.requiredSchema` — the
   GPU Parquet reader cannot materialize them, and the read fails.

The OSS solution is `pruneFileMetadata` ([DeltaProviderBase.scala:165-200](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/DeltaProviderBase.scala#L165-L200)),
which pattern-matches `Project → Filter → Project → GpuFileSourceScanExec` and rewrites
the tree to strip the `_metadata` column from the inner project and the
`_tmp_metadata_row_index` from the scan's `requiredSchema`/`originalOutput`. We must
port this — `pruneFileMetadata = identity` is incorrect.

**Timing note:** do not add a tag-time `_metadata` guard while also porting
`pruneFileMetadata`. `tagSupportForGpuFileSourceScan` runs before CPU-to-GPU conversion,
while `pruneFileMetadata` runs later in `GpuTransitionOverrides` on the converted GPU plan.
A tag-time `_metadata` guard would therefore always block `useMetadataRowIndex=true` scans
before the pruning rule gets a chance to run, making the pruning port dead code.

The intended V1 stance is the OSS-style path: `isDVScan` allows the hidden metadata scan
through tagging, then `pruneFileMetadata` removes `_metadata` / `_tmp_metadata_row_index`
after conversion. Verify this on cluster with
`test_delta_deletion_vector_read[use_metadata_row_index-True-...]`. If DB-17.3's
`PreprocessTableWithDVsStrategy` produces a different shape than the OSS matcher expects,
adjust the matcher. If we later choose a conservative CPU fallback for
`useMetadataRowIndex=true`, make that an explicit alternative by dropping or bypassing the
pruning path rather than mixing both approaches.

---

## Namespace substitution map

Used throughout the ported files. `sed`-style mapping:

| OSS import (delta-33x-40x) | Databricks import (DB-17.3) |
|---|---|
| `org.apache.spark.sql.delta.DeltaParquetFileFormat` | `com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat` |
| `org.apache.spark.sql.delta.DeltaParquetFileFormat._` (companion constants) | `com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat._` (same names; verified via `javap`) |
| `org.apache.spark.sql.delta.actions.{DeletionVectorDescriptor, Metadata, Protocol}` | `com.databricks.sql.transaction.tahoe.actions.{DeletionVectorDescriptor, Metadata, Protocol}` |
| `org.apache.spark.sql.delta.deletionvectors.{StoredBitmap, RoaringBitmapArray}` | `com.databricks.sql.transaction.tahoe.deletionvectors.{StoredBitmap, RoaringBitmapArray}` |
| `org.apache.spark.sql.delta.deletionvectors.{NativeRoaringBitmapArraySerializationFormat, PortableRoaringBitmapArraySerializationFormat}` | `com.databricks.sql.transaction.tahoe.deletionvectors.{...}` (same names) |
| `org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore` | `com.databricks.sql.transaction.tahoe.storage.dv.HadoopFileSystemDVStore` |
| `org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaColumnMappingMode, IdMapping, NameMapping, NoMapping}` | `com.databricks.sql.transaction.tahoe.{DeltaColumnMapping, DeltaColumnMappingMode, IdMapping, NameMapping, NoMapping}` |
| `org.apache.spark.sql.delta.schema.SchemaMergingUtils` | `com.databricks.sql.transaction.tahoe.schema.SchemaMergingUtils` |
| `org.apache.spark.sql.delta.sources.DeltaSQLConf` | `com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf` |
| `org.apache.spark.sql.delta.RowIndexFilterType` | `com.databricks.sql.io.RowIndexFilterType` |
| `org.apache.spark.sql.delta.DeltaErrors` | `com.databricks.sql.transaction.tahoe.DeltaErrors` |
| `org.apache.spark.sql.delta.TypeWidening.assertTableReadable(...)` | **Drop** the call. DB-17.3's CPU `DeltaParquetFileFormat` enforces this on the CPU side. |
| `org.apache.spark.sql.delta.logging.DeltaLogKeys` | Drop. Replace `MDC` log usage with plain `logError(s"Failed to translate filter $filter")`. |

V1 does **not** use `com.databricks.sql.io.RowIndexFilterProvider.retrieve(conf)`. We resolve the bitmap
directly via `StoredBitmap.create(dvDescriptor, new Path(tablePath)).load(new HadoopFileSystemDVStore(conf))`
— same shape as the OSS code. `RowIndexFilterProvider.retrieveSerialized(conf)` stays in
our pocket for the V2 follow-up (it returns raw bitmap bytes that cuDF could consume).

---

## Constructor and provider wiring (concrete)

### `GpuDeltaParquetFileFormat` (modified case class)

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

The three `generateRowIndexFilter*` flags are CPU-only (they control row-index column
insertion in the CPU read schema). The GPU FileFormat carries them so `equals`/`hashCode`
parity matches the CPU instance, but does not act on them — the GPU DV path always
inserts `__delta_internal_row_index` and `__delta_internal_is_row_deleted` itself based
on the schema it sees.

### `convertToGpu` (modified)

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
  optimizationsEnabled = !dvEnabled,                         // V1 stance: no split / no pushdown when DV is on
  tablePath = fmt.tablePath,
  isCDCRead = fmt.isCDCRead)
```

Prefer direct field pass-through from the CPU `fmt` as shown above. Use
`fmt.copyWithDVInfo(tablePath, optimizationsEnabled)` only if implementation discovery shows
DB-17.3 constructs a vanilla `DeltaParquetFileFormat` and needs a CPU-side helper to attach DV
state before conversion. Do **not** pass `isCDCRead` to `copyWithDVInfo`; DB-17.3's helper
preserves `isCDCRead` from the copied format and treats the second boolean as
`optimizationsEnabled` / `useMetadataRowIndex`.

### `tagSupportForGpuFileSourceScan` body (modified)

```scala
val requiredSchema = meta.wrapped.requiredSchema
if (requiredSchema.exists { field =>
  field.name.startsWith("_databricks_internal") &&
    field.name != "_databricks_internal_edge_computed_column_skip_row"
}) {
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

### `DeltaSpark400DB173Provider` additions

```scala
override def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean =
  super.isSupportedFormat(format) || format == classOf[GpuDeltaParquetFileFormat]

override def canPushDVPredicateDownToScan(conf: RapidsConf): Boolean = false
override def pushDVPredicateDownToScan(plan: SparkPlan): SparkPlan = plan

// Pattern-matches Project -> Filter -> Project -> GpuFileSourceScanExec when the
// outer projection drops _metadata and the filter references is_row_deleted, then
// strips _metadata from the inner project and the Spark-side temporary row-index
// metadata column (_tmp_metadata_row_index) from the scan. Required when
// DELETION_VECTORS_USE_METADATA_ROW_INDEX=true (the DB-17.3 Delta default).
// Port from OSS DeltaProviderBase.scala:165-200, substituting the Databricks
// IS_ROW_DELETED_COLUMN_NAME constant. The literal "_tmp_metadata_row_index" is
// the Spark ParquetFileFormat$.ROW_INDEX_TEMPORARY_COLUMN_NAME, computed at runtime
// as TMP_METADATA_COL_PREFIX + ROW_INDEX = "_tmp_metadata_" + "row_index"; verified
// present on DB-17.3 via javap on ParquetFileFormat$.class. We do NOT strip
// __delta_internal_row_index — the V1 reader (`GpuDeltaParquetFileFormatDV`) reads
// that column out of `requiredSchema` to decide whether to populate it via
// `RapidsDeletionVectorUtils.getRowIndexPosSimple`. Stripping it would silently
// disable DV application.
override def pruneFileMetadata(plan: SparkPlan): SparkPlan = {
  plan match {
    case dvRoot @ GpuProjectExec(outputList,
      dvFilter @ GpuFilterExec(condition,
        dvFilterInput @ GpuProjectExec(inputList, fsse: GpuFileSourceScanExec, _)), _)
        if condition.references.exists(
             _.name == DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME) &&
           !outputList.flatMap(_.references).exists(_.name == "_metadata") &&
           inputList.exists(_.name == "_metadata") =>
      dvRoot.withNewChildren(Seq(
        dvFilter.withNewChildren(Seq(
          dvFilterInput.copy(projectList = inputList.filterNot(_.name == "_metadata"))
            .withNewChildren(Seq(
              fsse.copy(
                originalOutput = fsse.originalOutput
                  .filterNot(_.name == "_tmp_metadata_row_index"),
                requiredSchema = StructType(fsse.requiredSchema
                  .filterNot(_.name == "_tmp_metadata_row_index"))
              )(fsse.rapidsConf)))))))
    case _ =>
      plan.withNewChildren(plan.children.map(pruneFileMetadata))
  }
}

override def isDVScan(meta: SparkPlanMeta[FileSourceScanExec]): Boolean = {
  val maybeDVScan = meta.parent.flatMap(_.parent).flatMap(_.parent).map(_.wrapped)
  maybeDVScan.map {
    case ProjectExec(outputList, FilterExec(condition, ProjectExec(inputList, _))) =>
      condition.references.exists(
        _.name == DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME) &&
        inputList.exists(_.name == "_metadata") &&
        !outputList.flatMap(_.references).exists(_.name == "_metadata")
    case _ => false
  }.getOrElse(false)
}
```

**Note:** verified via `javap -p -v` on `ParquetFileFormat$.class` from
`/databricks/jars/----ws_4_0--sql--core--core-hive-2.3__hadoop-3.2_2.13_deploy.jar`:
DB-17.3 uses the **same** `ROW_INDEX_TEMPORARY_COLUMN_NAME` = `_tmp_metadata_row_index`
as OSS Spark — the static initializer concatenates `TMP_METADATA_COL_PREFIX` (`_tmp_metadata_`)
with `ROW_INDEX` (`row_index`) at runtime. Substituting `__delta_internal_row_index`
(the Delta-private column from `DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME`) here
would be incorrect: the V1 reader at
[GpuDeltaParquetFileFormatBase.scala:185-188](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala#L185-L188)
calls `findColumn(ROW_INDEX_COLUMN_NAME)` on `requiredSchema` and only generates the
row-index GPU column when present. Removing it from the schema would silently disable
DV mask application.

DB-17.3's plan-shape (`Project → Filter → Project → FileSourceScan`) is generated by
`PreprocessTableWithDVsStrategy` (verified present at
`com/databricks/sql/transaction/tahoe/PreprocessTableWithDVsStrategy.class`). It may
differ slightly from OSS `PreprocessTableWithDVs`. If captured plans during cluster
validation show a different shape, adjust the matcher rather than broadening the column
list.

---

## Multi-file reader factory

`GpuDeltaParquetFileFormatDV.createMultiFileReaderFactory` overrides the Databricks-shared
base's plain `GpuParquetMultiFilePartitionReaderFactory` with a `DeltaMultiFileReaderFactory`
that wraps the standard parent reader in `DeltaMultiFileParquetPartitionReader`. The
reader uses `InputFileUtils.getCurInputFilePath()` to identify the active `PartitionedFile`,
reads the DV descriptor from
`partitionedFile.otherConstantMetadataColumnValues[FILE_ROW_INDEX_FILTER_ID_ENCODED / TYPE]`,
instantiates a per-file `RapidsRowIndexFilter`, and applies it to each batch via
`RapidsDeletionVectorUtils.processBatchWithDeletionVector` (materializes the
`is_row_deleted` INT8 column, plus optional row-index column, into the batch).

Notes:

- Coalescing reader path: log a warning ("Coalescing is not supported when
  `delta.enableDeletionVectors=true`") and fall through to multi-threaded — coalescing
  combines small files in ways incompatible with per-file DV bookkeeping.
- Pass `queryUsesInputFile = hasTablePath || fileScan.queryUsesInputFile` to defeat
  small-file combining when DV is on.
- Metric keys `rowIndexColumnGenTime` and `isRowDeletedColumnGenTime` must exist in
  `GpuFileSourceScanExec.allMetrics`. They are created only when
  `ExternalSource.isSupportedFormat(relation.fileFormat.getClass)` recognizes the **converted
  GPU** Delta format, so `DeltaSpark400DB173Provider.isSupportedFormat` must include
  `classOf[GpuDeltaParquetFileFormat]` in addition to the CPU Databricks format. If first-build
  still reports them missing, add them to a `metrics` override on `GpuDeltaParquetFileFormatDV`.

---

## Risks and unknowns

1. **DV serialized format compatibility (V2 only)** — not a V1 risk. V1 reads through
   Databricks's own `StoredBitmap.create(...).load(dvStore)`, byte-compatible by construction.
2. **10-field constructor pass-through** — both prior plans had this wrong. The actual
   field names are `nullableRowTrackingConstantFields` and `nullableRowTrackingGeneratedFields`,
   plus three `generateRowIndexFilter*` flags. Verified via `javap` on the live jar.
3. **`_metadata.row_index` predicate** — with `optimizationsEnabled=false` on DV tables,
   `prepareFiltersForRead` returns `Seq.empty`, so any `_metadata.row_index` predicate is
   dropped at the scan and re-applied above. Matches OSS 33x default. Risk surfaced by
   `test_delta_deletion_vector_read_drop_row_group`. If this fails, inspect the captured
   plan and adjust pruning / filter-prep behavior; do not add a tag-time `_metadata` guard
   while keeping the pruning path, because that would disable `useMetadataRowIndex=true`
   GPU reads before pruning can run.
4. **`useMetadataRowIndex=true` config edge case** — handled only by the combination of
   `isDVScan` and active `pruneFileMetadata`. `isDVScan` alone merely gets the scan past
   tagging; without pruning, `_metadata` / temporary row-index columns can still reach
   `GpuFileSourceScanExec.requiredSchema`, where the GPU reader cannot materialize them.
   If DB-17.3's actual plan shape does not match the OSS pruning pattern, adjust the matcher.
   Only choose CPU fallback for `DELETION_VECTORS_USE_METADATA_ROW_INDEX=true` as an explicit
   conservative alternative, not as a tag-time guard layered on top of pruning.
5. **CDC + DV** — guarded off in this PR. Re-enable later under a follow-up once CDC
   plumbing is reviewed.
6. **`Spark400PlusDBShims.getPartitionFiles` + `FilePartitionShims.withPathPrefixIfNeeded`**
   — already shipped (Issue 8 fix). DV reads depend on these because per-file DV metadata
   is keyed off the absolute path. Validate via `test_delta_scan_split_with_DV_enabled_with_DVs`,
   but update that test's Databricks expectation for this V1 path: DB-17.3 will not use
   native predicate pushdown, so `pushdown_dv_predicate=True` should not expect split
   behavior from `GpuDeltaParquetFileFormatBase2`.
7. **GPU Delta format recognition after scan conversion** — needed for DV metrics and any
   `ExternalSource.isSupportedFormat(relation.fileFormat.getClass)` checks that run after
   `FileSourceScanExecMeta.convertToGpu` rebuilds the relation with `GpuDeltaParquetFileFormat`.
   OSS `Delta33xProvider` / `Delta40xProvider` explicitly add their GPU format classes; DB-17.3
   must do the same in `DeltaSpark400DB173Provider`.
8. **`RowIndexFilterProvider` classpath at runtime** — not used by V1; only
   `RowIndexFilterType` (the enum) is imported from `com.databricks.sql.io`. Confirmed present.
9. **Result parity does not prove GPU execution** — the core DV tests carry
   `@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", *delta_meta_allow)` at
   [delta_lake_test.py:125](integration_tests/src/main/python/delta_lake_test.py#L125)
   and similar lines, so they pass even if the entire scan runs on CPU. This means a
   green test run on its own is **not** evidence the V1 path is wired correctly.
   Mitigation: mandatory plan-capture on the six scenarios listed in the Verification
   section; treat any `FileSourceScanExec` (without `Gpu` prefix) on the read side as a
   regression.

---

## Step-by-step implementation order

DB-17.3 is Scala 2.13 only. After each step, build with:

```bash
mvn -f scala2.13/pom.xml -Dbuildver=400db173 \
  -pl dist,delta-lake/delta-spark400db173 -am install -DskipTests
```

0. **Pre-port DB plan-shape capture.** Before making Scala changes, capture CPU/initial
   DB-17.3 `EXPLAIN EXTENDED` plans for `test_delta_deletion_vector_read`-style queries
   with `use_metadata_row_index=False` and `use_metadata_row_index=True`, covering
   PERFILE and MULTITHREADED reader configs. Record whether DB-17.3 injects `_metadata`,
   `_tmp_metadata_row_index`, `__delta_internal_row_index`, or a different temporary
   column. This decides whether the OSS `isDVScan` / `pruneFileMetadata` pattern can be
   ported directly, whether the matcher must be adjusted for DB-17.3, or whether the V1
   implementation should explicitly choose the conservative CPU-fallback path for
   `use_metadata_row_index=True`.
1. **Scaffold the GPU filter layer.** Add `RapidsRowIndexFilters.scala` and the slim
   `RapidsDeletionVectors.scala` under `com/nvidia/spark/rapids/delta/`. No dependents yet
   — build green immediately.
2. **Add `GpuDeltaParquetFileFormatDV.scala`.** Largest single file (~360 LOC), bundling
   the abstract format class, `RapidsDeletionVectorUtils` (calling `StoredBitmap.create(…)
   .load(new HadoopFileSystemDVStore(…))` inline), `DeltaMultiFileReaderFactory`,
   `DeltaMultiFileParquetPartitionReader`. Build green.
3. **Wire `GpuDeltaParquetFileFormat.scala`.** Switch parent class, expand constructor to
   the 10-field shape, drop local overrides, rewrite `tagSupportForGpuFileSourceScan`,
   update `convertToGpu`. Build green. GPU plans no longer block on DV.
4. **Add DV-scan provider hooks** to `DeltaSpark400DB173Provider.scala` (~85 LOC), including
   `isSupportedFormat` recognition for `classOf[GpuDeltaParquetFileFormat]`. Build green.
5. **First cluster smoke test.**
   ```bash
   ./integration_tests/run_pyspark_from_build.sh --runtime_env=databricks --delta_lake \
     --collect-only -k 'test_delta_deletion_vector_read and PERFILE'

   ./integration_tests/run_pyspark_from_build.sh --runtime_env=databricks --delta_lake \
     '<exact collected node id for test_delta_deletion_vector_read with PERFILE and use_metadata_row_index=False>'
   ```
   Pytest parameter IDs here are value-based (`True` / `False`), not named
   `use_metadata_row_index-False`; collect the node IDs first and run the exact one to
   avoid accidentally selecting zero tests.
   Triage failures. Common first-run issues: missing companion constant, metric keys
   missing, constructor field mismatch, `isDVScan` plan-shape mismatch (DB-17.3 plans may
   differ slightly from OSS — check captured plan against the OSS pattern at
   [DeltaProviderBase.scala:202-216](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/DeltaProviderBase.scala#L202-L216)).
6. **Reader-type sweep.** The Databricks-skipped COUNT(*) variants stay out of this
   command; see "Databricks-skipped DV tests — disposition" for the exact skip decisions.
   ```bash
   ./integration_tests/run_pyspark_from_build.sh --runtime_env=databricks --delta_lake \
     -k 'test_delta_deletion_vector_read or test_delta_deletion_vector_multithreaded_read or
         test_delta_deletion_vector_multithreaded_read_partitioned_table or
         test_delta_deletion_vector_coalescing_partitioned_table'
   ```
7. **Edge-case sweep.** Empty DV, mixed DV / no-DV, row-group skipping, and the DB-17.3
   scan-split case after applying the disposition in "Databricks-skipped DV tests."
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
9. **Negative / fallback coverage.** Confirm DV-related operations that should still
   fall back to CPU continue to do so.
   ```bash
   ./integration_tests/run_pyspark_from_build.sh --runtime_env=databricks --delta_lake \
     -k 'test_delta_deletion_vector_fallback or test_delta_update_fallback_with_deletion_vectors'
   ```
10. **Final regression.**
    ```bash
    ./integration_tests/run_pyspark_from_build.sh --runtime_env=databricks --delta_lake \
      delta_lake_test.py
    ```

CI re-enable (Jenkins skip flip in `jenkins/databricks/test.sh`) is **out of scope** here;
lands under Issue 7.

---

## Databricks-skipped DV tests — disposition

Several DV tests carry `@skipif(is_databricks_runtime())` markers. Decision per test:

| Test (file:line) | Skip reason | Disposition for this PR |
|---|---|---|
| [`test_delta_scan_split_with_DV_enabled_with_DVs`](integration_tests/src/main/python/delta_lake_test.py#L349) | "Deletion vector scan is not supported on Databricks" | **Remove the skip on DB-17.3 only** — exactly what this PR enables. Replace the Databricks skip with `@skipif(is_databricks_runtime() and not is_databricks173_or_later(), ...)` and import `is_databricks173_or_later`; keep the existing `is_before_spark_353()` skip. Do not skip OSS Spark. Also adjust the expected partition count for DB-17.3 V1: `canPushDVPredicateDownToScan=false`, so `pushdown_dv_predicate=True` should not expect the native `GpuDeltaParquetFileFormatBase2` split behavior. Validates split correctness, the `withPathPrefixIfNeeded` interaction, and DV-aware partition counts. Must pass to ship. |
| [`test_delta_deletion_vector_multithreaded_combine_count_star`](integration_tests/src/main/python/delta_lake_test.py#L192) | "currently failing on Databricks due to issue #14319" | **Stay skipped.** Issue #14319 is unrelated zero-column-projection accounting bug; out of Issue 6 scope. Re-evaluate once #14319 is fixed. |
| [`test_delta_deletion_vector_coalescing_count_star`](integration_tests/src/main/python/delta_lake_test.py#L469) | "Databricks Spark generates a different query plan that is not convertible to a GPU plan" | **Stay skipped.** Plan-shape divergence — separate investigation. Out of scope for Issue 6 (this PR enables the read path; the plan-shape mismatch is upstream). |

Net: the original "all DV-read tests pass" wording was incorrect for tests with
`is_databricks_runtime()` skips. The corrected scope is: **non-Databricks-skipped tests
pass on DB-17.3 with proven GPU execution; one obsolete skip is explicitly removed.**

---

## Verification

End-to-end success criteria, in order:

1. `mvn -f scala2.13/pom.xml -Dbuildver=400db173 -pl delta-lake/delta-spark400db173 -am install -DskipTests` is green.
2. `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh` is green.
3. `mvn -Dbuildver=330 install -DskipTests` is green (OSS 3.3 unaffected).
4. `mvn -f scala2.13/pom.xml -Dbuildver=400 install -DskipTests` is green (OSS 4.0 unaffected).
5. `mvn -f scala2.13/pom.xml -Dbuildver=350db143 install -DskipTests` is green (DB-14.3 unaffected).
6. The non-Databricks-skipped tests listed in steps 6–9 above pass with CPU/GPU result
   parity on a live DB-17.3 cluster (or expected-fallback in step 9). The obsolete skip
   on `test_delta_scan_split_with_DV_enabled_with_DVs` is removed for DB-17.3 and that
   test passes.
7. **Plan capture proves GPU execution.** Result parity alone is insufficient because
   the core DV tests use `@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec",
   *delta_meta_allow)` ([delta_lake_test.py:125](integration_tests/src/main/python/delta_lake_test.py#L125))
   — they pass even when the entire scan runs on CPU. Capture the executed plan on
   **each** of these scenarios and confirm `GpuFileSourceScanExec` is present with no
   read-side `ColumnarToRow`:
   - `test_delta_deletion_vector_read` with `parquet_reader_type=PERFILE`,
     `use_metadata_row_index=False`
   - `test_delta_deletion_vector_read` with `parquet_reader_type=COALESCING`,
     `use_metadata_row_index=False`
   - `test_delta_deletion_vector_multithreaded_read` (MULTITHREADED reader)
   - `test_delta_deletion_vector_read` with `use_metadata_row_index=True`
     **if** the `pruneFileMetadata` port handles the DB-17.3 plan shape; otherwise
     adjust the matcher or explicitly document a conservative CPU fallback for that mode
   - `test_delta_deletion_vector_mixed_dv_no_dv` (covers both DV-bearing and non-DV
     files in one scan)
   - `test_delta_scan_split_with_DV_enabled_with_DVs` (covers split correctness +
     `withPathPrefixIfNeeded` interaction)

   Use `spark_jvm()` plan extraction (see [conftest.py](integration_tests/src/main/python/conftest.py))
   or `EXPLAIN FORMATTED` capture; record the captured plan in the PR description.

---

## Effort estimate

- **3 new Scala files** (down from 5 — `RapidsDeletionVectorStore.scala` and
  `RapidsStoredBitmap.scala` are V2-only, not needed for V1), 2 modified Scala files
  (`GpuDeltaParquetFileFormat.scala`, `DeltaSpark400DB173Provider.scala`), and one small
  Python test update if the obsolete DB skip on `test_delta_scan_split_with_DV_enabled_with_DVs`
  is removed or narrowed. No `spark_session.py` helper change is needed, but
  `delta_lake_test.py` will need to import `is_databricks173_or_later` if that skip is
  narrowed as planned.
- **~705 LOC net** — `RapidsRowIndexFilters` (~130) + slim `RapidsDeletionVectors` (~110) +
  `GpuDeltaParquetFileFormatDV` (~360) + provider DV-scan hooks (~85, of which ~60 is the
  ported `pruneFileMetadata` — no longer identity) + `GpuDeltaParquetFileFormat` edits
  (~30 net), plus a small test annotation / expected-partition adjustment if the
  scan-split test is enabled for DB-17.3.
- Roughly 5–6 dev-days: 1.5 days for the mechanical port + initial build, 2.5 days for
  first-cluster bring-up + Spark 4.0 / DB-17.3 corner cases (10-field constructor,
  two-field row-tracking-nullability, `pruneFileMetadata` plan-shape validation,
  `useMetadataRowIndex=true` handling), 1 day for column-mapping / mixed-DV / row-group
  edge tests, 1 day for plan-capture proof on the 6 scenarios in the verification section.

---

## Critical files (for the implementer)

### Port sources (read-only references)

- [delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala) — V1 reader template
- [delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsRowIndexFilters.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsRowIndexFilters.scala)
- [delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsDeletionVectors.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsDeletionVectors.scala) — port only V1-relevant helpers (`translateFilterForColumnMapping`)
- [delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/DeltaProviderBase.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/DeltaProviderBase.scala) — DV-scan hook reference (`isDVScan`, `pruneFileMetadata`, etc.)
- **Not needed for V1** (V2-only):
  [RapidsDeletionVectorStore.scala](delta-lake/common/src/main/delta-33x-40x/scala/org/apache/spark/sql/delta/deletionvectors/RapidsDeletionVectorStore.scala),
  [RapidsStoredBitmap.scala](delta-lake/common/src/main/delta-33x-40x/scala/org/apache/spark/sql/delta/deletionvectors/RapidsStoredBitmap.scala),
  [GpuDeltaParquetFileFormatBase2.scala](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase2.scala)

### Targets (to create / modify)

- [delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala](delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala) (modify)
- [delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala](delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala) (modify)
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatDV.scala` (new)
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/RapidsRowIndexFilters.scala` (new)
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/RapidsDeletionVectors.scala` (new)

### Reference / context (read-only)

- [delta-lake/common/src/main/databricks/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatBase.scala](delta-lake/common/src/main/databricks/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatBase.scala) — the Databricks-namespaced shared parent of our format class.
- [delta-lake/common/src/main/databricks/scala/com/nvidia/spark/rapids/delta/DatabricksDeltaProviderBase.scala:99-112](delta-lake/common/src/main/databricks/scala/com/nvidia/spark/rapids/delta/DatabricksDeltaProviderBase.scala#L99-L112) — confirms the provider already routes through `GpuDeltaParquetFileFormat.tagSupportForGpuFileSourceScan` / `.convertToGpu`.
- [sql-plugin/src/main/scala/com/nvidia/spark/rapids/delta/DeltaProvider.scala:100-109](sql-plugin/src/main/scala/com/nvidia/spark/rapids/delta/DeltaProvider.scala#L100-L109) — base trait declaring DV-scan hooks.
- [sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuTransitionOverrides.scala:807-809](sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuTransitionOverrides.scala#L807-L809) — call site for `pruneFileMetadata` / `pushDVPredicateDownToScan`.
- [sql-plugin/src/main/spark330/scala/com/nvidia/spark/rapids/shims/ScanExecShims.scala:64](sql-plugin/src/main/spark330/scala/com/nvidia/spark/rapids/shims/ScanExecShims.scala#L64) — call site for `isDVScan`.
- [sql-plugin/src/main/spark400db173/scala/org/apache/spark/sql/execution/rapids/shims/FilePartitionShims.scala](sql-plugin/src/main/spark400db173/scala/org/apache/spark/sql/execution/rapids/shims/FilePartitionShims.scala) — `withPathPrefixIfNeeded`, prerequisite for DV reads (already shipped).
- [delta-lake-db173-design.md](delta-lake-db173-design.md), [delta-lake-db173-github-issues.md](delta-lake-db173-github-issues.md), [delta-lake-db173-checkpoint.md](delta-lake-db173-checkpoint.md) — design docs to update once this PR lands.

---

## Provenance

This document merges:

- **claude plan** ([delta-lake-db173-issue-6-dv-read-plan_claude.md](delta-lake-db173-issue-6-dv-read-plan_claude.md)) — concrete file list, LOC budget, namespace substitution table, step-by-step build/test commands.
- **codex plan** ([delta-lake-db173-deletion-vector-plan_codex.md](delta-lake-db173-deletion-vector-plan_codex.md)) — caught the DV-scan provider hooks (`isDVScan` / `pruneFileMetadata` / `canPushDVPredicateDownToScan`), flagged the V2 `retrieveSerialized` entry point, included `test_delta_update_fallback_with_deletion_vectors` in the negative coverage. Updated 2026-04-29 with: confirmed 10-field DB-17.3 constructor, observation that `RapidsDeletionVectorStore` / `RapidsStoredBitmap` are V2-only and should not be ported for V1, and that `supports_delta_lake_deletion_vectors()` already returns True for DB ≥ 12.2 (no Python helper edit needed).
- **Live DB-17.3 jar verification** via `javap -p -v` on classes from
  `/databricks/jars/----ws_4_0--sql--core--core-hive-2.3__hadoop-3.2_2.13_deploy.jar` and
  `/databricks/jars/----ws_4_0--sql--catalyst--catalyst-hive-2.3__hadoop-3.2_2.13_deploy.jar`
  — corrected both plans on the constructor (10 fields, not 6;
  `nullableRowTrackingConstantFields` + `nullableRowTrackingGeneratedFields` separate;
  three new `generateRowIndexFilter*` flags) and the actual DV column name strings
  (`__delta_internal_…`, not `_databricks_internal_…`). Also verified contra codex's
  claim: Databricks-native `DropMarkedRowsFilter` / `KeepMarkedRowsFilter` **do** exist
  at `c.d.s.t.tahoe.deletionvectors.{DropMarkedRowsFilter, KeepMarkedRowsFilter}` —
  doesn't affect the plan because we port the RAPIDS-side filter classes
  (`RapidsDropMarkedRowsFilter` etc.) regardless.
- **Six-finding critical review (2026-04-29, late):** corrected the doc on six points
  surfaced after the prior merge. (1) `pruneFileMetadata` is now a real port of
  [DeltaProviderBase.scala:165-200](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/DeltaProviderBase.scala#L165-L200),
  not identity — identity would have failed when `useMetadataRowIndex=true` left
  `_metadata` columns in the GPU scan's required schema. (2) Removed the over-optimistic
  framing that `optimizationsEnabled=false` implies `useMetadataRowIndex=false`; the two
  configs are independent (verified at
  [GpuDeltaParquetFileFormatBase.scala:162-163](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala#L162-L163)),
  and `isDVScan` alone does not clean the metadata plan. (3) Three tests in the original
  validation list carry `@skipif(is_databricks_runtime())` markers; added a
  per-test disposition table and explicitly scoped the obsolete
  `test_delta_scan_split_with_DV_enabled_with_DVs` skip removal as in-scope work.
  (4) Strengthened the verification section: result parity is insufficient because the
  core DV tests use `@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", ...)` —
  added mandatory plan-capture for six specific scenarios. (5) Removed stale "1,150 LOC
  across 5 OSS files" wording and replaced with the correct V1 port surface (~600 LOC,
  3 OSS files, +~80 LOC for the `pruneFileMetadata` port). (6) Corrected the explanation
  that `loadScalaBitmap` consumes `RapidsDeletionVectorStore` / `RapidsStoredBitmap` —
  it does not. Those wrappers are used only by `loadDeletionVector` (the V2-only
  `HostMemoryBuffer` path); `loadScalaBitmap` calls `StoredBitmap` directly (verified at
  [RapidsDeletionVectors.scala:145-169](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsDeletionVectors.scala#L145-L169)).
  Conclusion (skip the two files for V1) unchanged; only the explanation was wrong.
- **Final critical review (2026-04-29, latest):** end-to-end review against the live
  jar identified one critical and two minor corrections. **Critical:** the
  `pruneFileMetadata` stub previously stripped both `_tmp_metadata_row_index` AND
  `__delta_internal_row_index` ("tolerant predicate"). Removing
  `__delta_internal_row_index` would silently disable DV mask application — the V1
  reader at [GpuDeltaParquetFileFormatBase.scala:185-188](delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala#L185-L188)
  reads that column from `requiredSchema` and uses it to populate the row-index
  output. Now strips only `_tmp_metadata_row_index`. Verified via `javap -p -v` on
  `ParquetFileFormat$.class` that DB-17.3 uses the **same** literal name as OSS
  Spark (computed at runtime as `TMP_METADATA_COL_PREFIX` + `ROW_INDEX` =
  `_tmp_metadata_row_index`). **Minor:** clarified that `isSplitable` lives in the
  new `GpuDeltaParquetFileFormatDV` (the shared Databricks
  `GpuDeltaParquetFileFormatBase` does not declare it). **Minor:** identified
  `_metadata` in `requiredSchema` as the runtime failure trigger if `pruneFileMetadata`
  does not match the DB-17.3 plan shape produced by `PreprocessTableWithDVsStrategy`.
  The final plan keeps the OSS-style pruning path rather than adding a tag-time
  `_metadata` guard, because the guard would run before pruning and short-circuit it. Also
  verified during implementation: most `_databricks_internal` columns remain unrelated
  and unsupported, but `_databricks_internal_edge_computed_column_skip_row` is the
  DB-17.3 DV skip-row mask and must be allowed/generated by the GPU reader.
- **Post-review corrections (2026-04-29):** folded in three final implementation details.
  (1) Corrected `copyWithDVInfo(tablePath, ...)`: the second boolean is
  `optimizationsEnabled` / `useMetadataRowIndex`, not `isCDCRead`; DB-17.3 preserves
  `isCDCRead` from the copied format. (2) Added `DeltaSpark400DB173Provider.isSupportedFormat`
  recognition for `classOf[GpuDeltaParquetFileFormat]`, mirroring OSS 33x/40x providers and
  ensuring DV metric keys are created after scan conversion. (3) Removed the previously proposed
  tag-time `_metadata` defensive guard after verifying ordering:
  `tagSupportForGpuFileSourceScan` runs before CPU-to-GPU conversion, while
  `pruneFileMetadata` runs later in `GpuTransitionOverrides`; keeping both would make pruning
  dead code for `useMetadataRowIndex=true`.
