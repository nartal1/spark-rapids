# Design: DB-17.3 GPU Deletion Vector Reads

**Target issue:** Issue 6, GitHub #14600
**Target module:** `delta-lake/delta-spark400db173`
**Status:** Implemented and validated locally on DB-17.3 (2026-05-02)
**Primary implementation plan:** `delta-lake-db173-issue-6-dv-read-plan.md`

This document captures the design for enabling GPU deletion-vector reads on
Databricks 17.3. It is a companion to the step-by-step implementation plan. The
goal is to make the intended scope, architecture, test enablement, and remaining
feature gaps explicit before implementation starts.

## Summary

DB-17.3 stores deletion-vector metadata per file in
`PartitionedFile.otherConstantMetadataColumnValues`. The current DB-17.3 Delta
shim blocks GPU scans whenever `DeltaParquetFileFormat.tablePath` is defined,
which effectively sends most DV-enabled Delta table reads to CPU.

Issue 6 enabled the RAPIDS materialized-DV read path for DB-17.3:

1. Read Parquet data on GPU.
2. Load the per-file Delta deletion vector through Databricks Delta APIs.
3. Materialize row-index and Databricks skip-row metadata columns.
4. Let the GPU filter remove deleted rows above the scan.

This is the V1/materialized path, equivalent in shape to the non-native path used
by the repo's OSS Delta 3.3/4.0 support. It is not the native cuDF DV reader path.

## 2026-05-02 Implementation Update

Issue 6 has been implemented in `delta-lake/delta-spark400db173`. The final
shape matches this design: DB-17.3-local DV reader files, post-read skip-row
mask materialization, no native cuDF DV path, and no DV predicate pushdown.

Delivered files:
- `GpuDeltaParquetFileFormatDV.scala`
- `RapidsDeletionVectors.scala`
- `RapidsRowIndexFilters.scala`
- updates to `GpuDeltaParquetFileFormat.scala`
- updates to `DeltaSpark400DB173Provider.scala`
- targeted test updates in `delta_lake_test.py` and `delta_lake_delete_test.py`

Validation completed:
- `SKIP_DEP_INSTALL=1 WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`
  passed.
- Focused post-cleanup DV run passed:
  `57 passed, 35 warnings in 182.72s`.
- Full `delta_lake_test.py` and `delta_lake_delete_test.py` passed before the
  final cleanup pass; the focused DV set was rerun afterward.

The remaining-work table at the end of this document is still current.

## Goals

- Enable `GpuFileSourceScanExec` for DB-17.3 Delta table reads with deletion
  vectors.
- Support PERFILE reader mode.
- Support MULTITHREADED reader mode, including the Delta-aware multi-file reader
  wrapper required for per-file DV bookkeeping.
- Preserve correctness for mixed DV/no-DV files, empty DVs, partition columns,
  column mapping, and `useMetadataRowIndex` plan shapes when supported by the
  DB-17.3 planner.
- Keep persistent DV writes on CPU.
- Keep existing plain Delta write support unchanged.
- Prove GPU execution with plan capture, not only CPU/GPU result parity.

## Non-Goals

- Native cuDF DV reader path (`GpuDeltaParquetFileFormatBase2` equivalent).
- DV predicate pushdown into the scan.
- Native/split-optimized DV scanning.
- True DV-aware coalescing reader support.
- GPU DELETE/UPDATE/MERGE/OPTIMIZE or DV writes.
- CDC + DV reads.
- CI-wide DB-17.3 Delta test enablement. That remains Issue 7.

## Pre-Issue-6 DB-17.3 State

Before this implementation, DB-17.3 read conversion lived in:

- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala`
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala`

The pre-Issue-6 `GpuDeltaParquetFileFormat` was intentionally minimal. It extended the
Databricks shared `GpuDeltaParquetFileFormatBase`, carries only
`protocol`, `metadata`, `tablePath`, and `isCDCRead`, and falls back for:

- any `tablePath.isDefined` DV scan
- row-index filter ID/column generation
- Delta file-in-scan metadata
- nullable row-tracking metadata
- disabled DB-17.3 scan optimizations

Issue 6 replaces the broad DV fallback with a real DB-17.3-local V1 DV reader.

## DB-17.3 API Shape

The DB-17.3 CPU file format has a 10-field constructor:

```scala
DeltaParquetFileFormat(
  protocol: Protocol,
  metadata: Metadata,
  generateRowIndexFilterId: Boolean,
  generateRowIndexFilterColumn: Boolean,
  generateDeltaFileInScanId: Boolean,
  nullableRowTrackingConstantFields: Boolean,
  nullableRowTrackingGeneratedFields: Boolean,
  optimizationsEnabled: Boolean,
  tablePath: Option[String],
  isCDCRead: Boolean)
```

The GPU DB-17.3 file format must mirror this shape so conversion preserves the
CPU format state. For DV tables, the V1 GPU path forces
`optimizationsEnabled = false` to avoid file splitting and scan-level predicate
pushdown until the native path is implemented.

DB-17.3 exposes the per-file DV descriptor constants on
`com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat`:

- `FILE_ROW_INDEX_FILTER_ID_ENCODED`
- `FILE_ROW_INDEX_FILTER_TYPE`
- `FILE_ROW_INDEX_FILTER_ID`
- `INCLUDED_IN_ROW_INDEX_FILTER`

Plan capture on DB-17.3 showed that the data-read filter uses the Databricks
edge skip-row column:

- `_databricks_internal_edge_computed_column_skip_row`

That column is the materialized DV mask for this runtime and must be allowed
through scan tagging. Other `_databricks_internal` metadata columns remain out
of scope. When a row-index column is present, the reader should use Spark's
temporary row-index metadata column (`ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME`,
`_tmp_metadata_row_index`) rather than Delta's private `ROW_INDEX_COLUMN_NAME`.

## Architecture

### New Local Files

Add these files under
`delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/`:

- `RapidsRowIndexFilters.scala`
- `RapidsDeletionVectors.scala`
- `GpuDeltaParquetFileFormatDV.scala`

These are local DB-17.3 ports of the V1-relevant OSS Delta 3.3/4.0 support with
Databricks namespaces substituted. Do not add the OSS `delta-33x-40x` source root
to the DB-17.3 pom.

Do not port these V2/native helper files for Issue 6:

- `RapidsDeletionVectorStore.scala`
- `RapidsStoredBitmap.scala`

They load serialized bitmap bytes for the native cuDF path, which is deferred.

### Modified Files

Modify:

- `GpuDeltaParquetFileFormat.scala`
- `DeltaSpark400DB173Provider.scala`

No pom changes are expected.

### Read Path

The intended read flow is:

```text
FileSourceScanExec with CPU DeltaParquetFileFormat
  -> DatabricksDeltaProviderBase tags and converts scan
  -> GpuDeltaParquetFileFormat
  -> GpuDeltaParquetFileFormatDV
  -> GPU Parquet reader
  -> per-file DV bitmap load
  -> generate row-index / is_row_deleted columns
  -> GPU filter removes deleted rows
```

The per-file DV descriptor is read from
`PartitionedFile.otherConstantMetadataColumnValues` using the Databricks
`FILE_ROW_INDEX_FILTER_ID_ENCODED` and `FILE_ROW_INDEX_FILTER_TYPE` constants.
The V1 path loads bitmaps through Databricks Delta:

```scala
StoredBitmap.create(dvDescriptor, tablePath)
  .load(new HadoopFileSystemDVStore(conf))
```

The implementation should use Databricks `RoaringBitmapArray` directly in the
RAPIDS row-index filter helpers.

### Reader Modes

PERFILE:

- Use `buildReaderWithPartitionValuesAndMetrics`.
- Wrap the regular data reader result with DV metadata-column generation.
- Apply the per-file row-index filter to each batch.

MULTITHREADED:

- Override `createMultiFileReaderFactory`.
- Use a Delta-aware multi-file reader that maps the active input file back to
  the correct `PartitionedFile`.
- Set `queryUsesInputFile = hasTablePath || fileScan.queryUsesInputFile` so
  small-file combining does not break per-file DV bookkeeping.

COALESCING:

- True coalescing support is not in scope.
- When the coalescing reader is requested for a DV table, log a warning and use
  the multithreaded path.

## Provider Hooks

DB-17.3 must add DV-specific provider hooks in `DeltaSpark400DB173Provider`, not
only the file-format port.

Required hooks:

- `isSupportedFormat`
- `canPushDVPredicateDownToScan`
- `pushDVPredicateDownToScan`
- `pruneFileMetadata`
- `isDVScan`

`isSupportedFormat` must recognize `classOf[GpuDeltaParquetFileFormat]` as well
as the CPU Databricks Delta format. This is needed after conversion so scan metric
creation and external-source checks recognize the GPU Delta format.

For Issue 6:

```scala
canPushDVPredicateDownToScan = false
pushDVPredicateDownToScan = identity
```

`pruneFileMetadata` must be a real port of the OSS plan rewrite. Identity is not
enough because `useMetadataRowIndex=true` can inject `_metadata` and
`_tmp_metadata_row_index` into the plan before GPU conversion. The pruning rewrite
should remove:

- `_metadata` from the inner project when it is only used for DV processing
- `_tmp_metadata_row_index` from the scan output/schema

When row-index metadata is present, the DB-17.3 reader uses Spark's temporary
row-index metadata column (`_tmp_metadata_row_index`). The reader does not rely
on Delta's private `ROW_INDEX_COLUMN_NAME` for the Databricks edge skip-row plan.

`isDVScan` should recognize the DB-17.3 DV plan shape equivalent to:

```text
Project
  Filter(condition references is_row_deleted)
    Project(input includes _metadata)
      FileSourceScanExec
```

If DB-17.3 produces a different shape, adjust the matcher based on captured plans
rather than adding an early tag-time `_metadata` fallback.

## Test Enablement

Issue 6 enables and validates the associated V1 DV read tests, but not every
DV test that is enabled for the repo's OSS Delta-40x support.

`supports_delta_lake_deletion_vectors()` already returns true for Databricks
12.2+, so no helper edit is needed for DB-17.3.

Expected in-scope tests include:

- `test_delta_deletion_vector_read`
- `test_delta_deletion_vector_multithreaded_read`
- `test_delta_deletion_vector_multithreaded_read_partitioned_table`
- `test_delta_deletion_vector_coalescing_partitioned_table` if it works through
  the multithreaded fallback
- `test_delta_empty_deletion_vector_read`
- `test_delta_deletion_vector_mixed_dv_no_dv`
- `test_delta_deletion_vector_read_drop_row_group`
- column-mapping read tests with DVs enabled
- expected-fallback tests for DV write paths

Narrow the Databricks skip for:

- `test_delta_scan_split_with_DV_enabled_with_DVs`

Only enable it for DB-17.3 if the V1 implementation can satisfy the corrected
DB-specific expectation. Because DB-17.3 Issue 6 sets
`canPushDVPredicateDownToScan=false`, `pushdown_dv_predicate=True` should not
expect the native split behavior used by the OSS Delta-40x Base2 path.

Keep skipped or out of scope unless they pass naturally without extra feature
work:

- `test_delta_deletion_vector_multithreaded_combine_count_star`
- `test_delta_deletion_vector_coalescing_count_star`
- ignore missing/corrupt file DV tests
- native-footer multi-row-group DV tests
- native-footer multi-row-group count-star tests
- CDC + DV reads

The Jenkins DB-17.3 Delta test skip remains in place until Issue 7.

## Validation

Build:

```bash
mvn -f scala2.13/pom.xml -Dbuildver=400db173 \
  -pl dist,delta-lake/delta-spark400db173 -am install -DskipTests
```

Regression builds:

```bash
mvn -Dbuildver=330 install -DskipTests
mvn -f scala2.13/pom.xml -Dbuildver=400 install -DskipTests
mvn -f scala2.13/pom.xml -Dbuildver=350db143 install -DskipTests
```

Cluster validation should run the targeted DV tests in the final implementation
plan. Because several tests allow CPU `FileSourceScanExec`, result parity is not
enough. Capture executed plans for representative cases and confirm:

- `GpuFileSourceScanExec` is present on the read side.
- No unexpected read-side CPU `FileSourceScanExec` remains.
- No read-side `ColumnarToRowExec` is inserted into the scan path.

Mandatory plan-capture scenarios:

- PERFILE DV read with `use_metadata_row_index=false`
- COALESCING requested for DV read, verifying multithreaded fallback behavior
- MULTITHREADED DV read
- DV read with `use_metadata_row_index=true`, if pruning supports the DB-17.3
  plan shape
- mixed DV/no-DV files
- DB-17.3-enabled scan-split DV test

## Remaining Work After Issue 6

These items should be tracked as follow-ups after the V1 DV read path lands:

| Feature | Status after Issue 6 | Notes |
|---|---|---|
| Native cuDF DV reader path | Remaining | Requires DB-17.3 equivalent of Base2 and serialized bitmap compatibility proof. |
| DV predicate pushdown | Remaining | Depends on native path and metadata row-index handling. |
| Split/native optimized DV scanning | Remaining | Tied to native path and predicate pushdown. |
| True coalescing DV reader | Remaining | V1 falls back to multithreaded for DV tables. |
| COUNT(*) / zero-column DV cases | Partial or remaining | Try only if cheap; otherwise keep skipped under existing issues. |
| Ignore missing/corrupt files with DVs | Optional stretch | Enable only if inherited Parquet behavior works cleanly. |
| Native-footer multi-row-group DV tests | Remaining | Better aligned with native/split follow-up. |
| CDC + DV reads | Remaining | Keep guarded off until CDC inline DV behavior is reviewed. |
| DB-17.3 CI Delta enablement | Remaining | Issue 7, after the broader Delta test matrix is clean. |

## Risks

- DB-17.3 DV plans may differ from OSS Delta 3.3/4.0 plan shapes, especially for
  `_metadata` pruning.
- Green tests may hide CPU fallback because several DV tests allow non-GPU
  `FileSourceScanExec`.
- Metric keys for row-index and is-row-deleted generation may be missing if the
  converted GPU Delta format is not recognized by `ExternalSource`.
- CDC + DV may use inline or change-data-specific semantics not covered by the V1
  materialized read path.
- Enabling native/split behavior without the Base2 path risks row-index
  misalignment.

## Implementation Order

Use the detailed order in `delta-lake-db173-issue-6-dv-read-plan.md`. At a high
level:

1. Capture pre-port DB-17.3 DV plan shapes.
2. Add row-index filter helpers.
3. Add slim DV helper utilities.
4. Add `GpuDeltaParquetFileFormatDV`.
5. Wire `GpuDeltaParquetFileFormat`.
6. Add provider DV hooks.
7. Run targeted cluster smoke tests.
8. Enable/narrow only the in-scope test skips.
9. Run full targeted DV test sweeps and plan-capture validation.
10. Update checkpoint and planning docs with exact passed/skipped test status.
