# Design: DB-17.3 GPU Deletion Vector Reads

**Target issue:** Issue 6, GitHub #14600
**Target module:** `delta-lake/delta-spark400db173`
**Status:** Implemented and validated locally on DB-17.3. Initial
V1/materialized DV reads landed as an intermediate checkpoint on 2026-05-02;
native cuDF DV parity landed locally on 2026-05-05; DBR-17.3 native-only
cleanup was squashed on 2026-05-06 and amended on 2026-05-11 with row-index
filter fallback hardening and on 2026-05-14 with a native-scan guard for
skip-row predicate removal plus a Python canary for DB missing-row-index-filter
assertion wording.
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

The final Issue 6 implementation enables DBR-17.3 GPU deletion-vector reads
through the native cuDF path only:

1. Select `GpuDeltaParquetFileFormatNativeDV` when DBR metadata-row-index mode
   and RAPIDS DV predicate pushdown are both enabled.
2. Load the per-file Delta deletion vector descriptor/provider through
   Databricks Delta APIs.
3. Convert DBR `SerializedBitmap` data to cuDF-compatible standard portable
   roaring bitmap bytes with `DeltaBitmapUtils.java`.
4. Let cuDF apply the deletion vector inside the native Parquet read path.

If either native gate is false for a DBR-17.3 DV scan, the scan falls back to
CPU. The materialized/non-cuDF DV path still exists for OSS Delta where those
modules support it, but the final DBR-17.3 shim does not keep a materialized
Scala/GPU DV fallback.

The native DBR-17.3 path also requires `RowIndexFilterType.IF_CONTAINED`
semantics. Other DBR row-index filter types, including the `IF_NOT_CONTAINED`
provider observed in Databricks merge validation, are tagged for CPU fallback
because cuDF's DV reader treats the bitmap as rows to drop.

The same native-only boundary applies to skip-row predicate removal. The DBR
skip-row predicate is redundant only when the child plan is a native GPU DV
scan; DBR DELETE/DML fallback plans that write persistent DVs keep the filter.

## 2026-05-06 Final Native-only Update

Final squashed commit:

```text
ada2580ea3ff0c558f62b1e503a0f302343e83f9 [databricks] Add native-only Delta DV reads for DBR 17.3
```

Final DBR-17.3 files:
- `DeltaBitmapUtils.java`
- `DeltaSpark400DB173Provider.scala`
- `GpuDeltaParquetFileFormat.scala`
- `GpuDeltaParquetFileFormatNativeDV.scala`
- `RapidsDeletionVectors.scala`
- `delta_lake_delete_test.py`
- `delta_lake_test.py`

The intermediate DBR materialized GPU files `GpuDeltaParquetFileFormatDV.scala`
and `RapidsRowIndexFilters.scala` were removed before the squash.

Final validation:
- `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh` passed.
- Targeted DBR Delta DV read slice 1 passed: 80 selected, 80 passed.
- Targeted DBR Delta DV read slice 2 passed: 120 selected, 108 passed,
  12 expected Databricks skips.
- 2026-05-11 focused DV selection passed: 194 tests, 0 failures.
- 2026-05-11 targeted merge regression passed: `test_delta_merge_query`, 1 test,
  0 failures.
- 2026-05-11 broader Delta read/delete selection passed: 289 tests, 0 failures,
  40 skips.
- 2026-05-11 `delta_lake_auto_compact_test.py` selection passed: 14 tests,
  0 failures.
- 2026-05-14 build passed again with
  `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`.
- 2026-05-14 targeted DML fallback regression passed:
  `delta_lake_delete_test.py::test_delta_delete_twice_with_dv` with seed
  `1778719081` and OOM injection enabled.
- 2026-05-14 assertion-message canary added:
  `delta_lake_test.py::test_db173_missing_row_index_filter_assertion_guard`.

## 2026-05-02 Implementation Update (historical checkpoint)

Issue 6 was initially implemented in `delta-lake/delta-spark400db173` with
DB-17.3-local DV reader files, post-read skip-row mask materialization, no
native cuDF DV path, and no DV predicate pushdown. That describes the
2026-05-02 checkpoint only; the final 2026-05-06 squashed commit removed this
DBR materialized GPU path.

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

## 2026-05-05 Native cuDF DV Follow-up Update

The native follow-up is now implemented locally. It supersedes the older notes
in this document that list native cuDF DV, predicate pushdown, split/native
scanning, true coalescing support, or native-footer DV tests as remaining Issue
6 work.

Delivered files and changes:
- `GpuDeltaParquetFileFormatNativeDV.scala`
- `DeltaBitmapUtils.java`
- updates to `RapidsDeletionVectors.scala`
- updates to `DeltaSpark400DB173Provider.scala`
- native DV test coverage in `delta_lake_test.py`

Current coverage:
- Narrow DBR-specific DV predicate pushdown for the current skip-row predicate
  shapes.
- Native cuDF DV scanning for PERFILE, MULTITHREADED, and COALESCING.
- Native-footer multi-row-group and count-star DV tests.
- Scan-split behavior under the native gate.

Validation completed:
- DBR-17.3 dist build passed with
  `mvn -B -f scala2.13/pom.xml -Ddatabricks -Dbuildver=400db173 package -pl dist -am -DskipTests -Dmaven.scaladoc.skip`.
- Focused native PERFILE footer test passed.
- Native footer matrix, delete/read, and scan-split targeted tests passed.

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

- Historical 2026-05-02 non-goals that are now delivered by the 2026-05-05
  native follow-up: native cuDF DV reader path, DV predicate pushdown into the
  scan, native/split-optimized DV scanning, and native COALESCING support.
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

Issue 6 replaces the broad DV fallback with a DBR-17.3 native cuDF DV reader.

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
CPU format state. The final native-only implementation uses the
metadata-row-index and RAPIDS DV pushdown gates before enabling native
scan-level DV handling; disabled-gate DBR DV scans fall back to CPU.

DB-17.3 exposes the per-file DV descriptor constants on
`com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat`:

- `FILE_ROW_INDEX_FILTER_ID_ENCODED`
- `FILE_ROW_INDEX_FILTER_TYPE`
- `FILE_ROW_INDEX_FILTER_ID`
- `INCLUDED_IN_ROW_INDEX_FILTER`

Plan capture on DB-17.3 showed that the data-read filter uses the Databricks
edge skip-row column:

- `_databricks_internal_edge_computed_column_skip_row`

That column identifies the DBR skip-row predicate shape used by DV scans and is
allowed through scan tagging/detection. Other `_databricks_internal` metadata
columns remain out of scope. When a row-index column is present, the native path
uses Spark's temporary row-index metadata column
(`ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME`, `_tmp_metadata_row_index`)
rather than Delta's private `ROW_INDEX_COLUMN_NAME`.

## Architecture

### Final Local Files

The final DBR-17.3 DV commit adds these source files:

- `RapidsDeletionVectors.scala`
- `GpuDeltaParquetFileFormatNativeDV.scala`
- `DeltaBitmapUtils.java`

These are local DBR-17.3 native cuDF DV pieces with Databricks namespaces and
DBR bitmap APIs. Do not add the OSS `delta-33x-40x` source root to the DB-17.3
pom.

Historical 2026-05-02 note: `RapidsRowIndexFilters.scala` and
`GpuDeltaParquetFileFormatDV.scala` were added for the intermediate
materialized GPU path, then removed by the 2026-05-06 native-only cleanup.

Historical 2026-05-02 note: do not port these V2/native helper files for the
initial materialized reader:

- `RapidsDeletionVectorStore.scala`
- `RapidsStoredBitmap.scala`

They load serialized bitmap bytes for the OSS native cuDF path. The 2026-05-05
DBR follow-up does not use that OSS storage path; it uses DBR
`SerializedBitmap` data plus `DeltaBitmapUtils.java` to produce cuDF-compatible
standard portable roaring bitmap bytes.

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
  -> DeltaSpark400DB173Provider checks native DV gates
  -> GpuDeltaParquetFileFormatNativeDV
  -> DBR DV descriptor/provider lookup
  -> serialized bitmap conversion to cuDF-compatible bytes
  -> native cuDF Parquet read applies the deletion vector
```

The per-file DV descriptor is read from
`PartitionedFile.otherConstantMetadataColumnValues` using the Databricks
`FILE_ROW_INDEX_FILTER_ID_ENCODED` and `FILE_ROW_INDEX_FILTER_TYPE` constants.
The native path may also recover descriptors/providers from `RowIndexFilterProvider`
or `TahoeFileIndex`. `RapidsDeletionVectors` uses DBR bitmap APIs for lookup and
bookkeeping, while `DeltaBitmapUtils.java` converts the serialized bitmap payload
that cuDF consumes.

### Reader Modes

PERFILE:

- Use the native cuDF DV Parquet reader path with one `PartitionedFile` at a time.
- Pass the serialized deletion-vector bytes and alive-row count metadata to the
  native read path.

MULTITHREADED:

- Override `createMultiFileReaderFactory`.
- Use a Delta-aware multi-file reader/factory that maps each active input file
  back to the correct `PartitionedFile` and its deletion-vector metadata.

COALESCING:

- Native coalescing is in scope and implemented.
- Preserve per-file DV metadata while coalescing input files into native reads.

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

For the historical 2026-05-02 V1 delivery:

```scala
canPushDVPredicateDownToScan = false
pushDVPredicateDownToScan = identity
```

For the final native-only implementation, `canPushDVPredicateDownToScan` is
enabled only when DBR metadata-row-index mode and
`spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled` are both
true. The pushdown recognizer is intentionally narrow and matches current DBR
skip-row predicates for `__delta_internal_is_row_deleted` and
`_databricks_internal_edge_computed_column_skip_row`.

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

Issue 6 enables and validates the associated DBR-17.3 native DV read tests, but
not every DV test that is enabled for the repo's OSS Delta-40x support.

`supports_delta_lake_deletion_vectors()` already returns true for Databricks
12.2+, so no helper edit is needed for DB-17.3.

Expected in-scope tests include:

- `test_delta_deletion_vector_read`
- `test_delta_deletion_vector_multithreaded_read`
- `test_delta_deletion_vector_multithreaded_read_partitioned_table`
- `test_delta_deletion_vector_coalescing_partitioned_table`
- `test_delta_empty_deletion_vector_read`
- `test_delta_deletion_vector_mixed_dv_no_dv`
- `test_delta_deletion_vector_read_drop_row_group`
- column-mapping read tests with DVs enabled
- expected-fallback tests for DV write paths

Narrow the Databricks skip for:

- `test_delta_scan_split_with_DV_enabled_with_DVs`

Initial 2026-05-02 V1 expectation: enable it for DB-17.3 only if the
materialized implementation can satisfy the corrected DB-specific expectation,
with `canPushDVPredicateDownToScan=false`. Final native-only expectation:
native pushdown is enabled behind the DBR metadata-row-index and RAPIDS DV
pushdown gates, disabled-gate DV scans fall back to CPU, and the scan-split test
passed in targeted validation.

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

These items should be tracked as follow-ups after the final native-only DV read
path lands:

| Feature | Status after Issue 6 | Notes |
|---|---|---|
| Native cuDF DV reader path | Done locally 2026-05-05 | Implemented as DBR-17.3 `GpuDeltaParquetFileFormatNativeDV`, not the OSS storage path. |
| DV predicate pushdown | Done locally 2026-05-05 | Narrow recognizer, gated by DBR metadata-row-index mode and RAPIDS DV pushdown config. |
| Skip-row predicate removal scope | Done locally 2026-05-14 | Removal is guarded by the presence of a native GPU DV scan; DELETE/DML fallback plans keep DB's filter. |
| Missing row-index-filter assertion canary | Done locally 2026-05-14 | Python integration test reflects DB's generated message and checks RAPIDS' substring still matches. |
| Split/native optimized DV scanning | Done locally 2026-05-05 | Covered by native scan-split targeted validation. |
| True coalescing DV reader | Done locally 2026-05-05 | Native COALESCING path added. |
| COUNT(*) / zero-column DV cases | Mostly validated locally | Targeted native count-star selections passed where not skipped by existing Databricks test skips. |
| Ignore missing/corrupt files with DVs | Optional stretch | Enable only if inherited Parquet behavior works cleanly. |
| Native-footer multi-row-group DV tests | Done locally 2026-05-05 | Native footer and count-star matrix passed. |
| CDC + DV reads | Remaining | Keep guarded off until CDC inline DV behavior is reviewed. |
| DB-17.3 CI Delta enablement | Remaining | Issue 7, after the broader Delta test matrix is clean. |

## Risks

- DB-17.3 DV plans may differ from OSS Delta 3.3/4.0 plan shapes, especially for
  `_metadata` pruning.
- Green tests may hide CPU fallback because several DV tests allow non-GPU
  `FileSourceScanExec`.
- Metric keys for row-index and is-row-deleted generation may be missing if the
  converted GPU Delta format is not recognized by `ExternalSource`.
- CDC + DV may use inline or change-data-specific semantics not covered by the
  native read path.
- Native/split behavior depends on the DBR metadata-row-index and RAPIDS DV
  pushdown gates; disabled-gate DV scans must remain CPU fallback.

## Implementation Order

Use the detailed order in `delta-lake-db173-issue-6-dv-read-plan.md`. At a high
level:

1. Capture pre-port DB-17.3 DV plan shapes.
2. Add native DV helper utilities.
3. Add `GpuDeltaParquetFileFormatNativeDV`.
4. Wire `GpuDeltaParquetFileFormat` and provider native-gate fallback tagging.
5. Run targeted cluster smoke tests.
6. Enable/narrow only the in-scope test skips.
7. Run full targeted DV test sweeps and plan-capture validation.
8. Update checkpoint and planning docs with exact passed/skipped test status.
