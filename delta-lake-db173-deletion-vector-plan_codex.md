# DB-17.3 Deletion Vector Read Support Plan

**Date:** 2026-04-29
**Tracking:** Issue 6 in `delta-lake-db173-github-issues.md`, GitHub issue
[#14600](https://github.com/NVIDIA/spark-rapids/issues/14600)
**Target module:** `delta-lake/delta-spark400db173`
**Status:** Planning handoff for implementation

## Summary

Enable GPU-accelerated Delta Lake Deletion Vector (DV) reads for Databricks 17.3.
The first implementation should port the OSS Delta 3.3/4.0 materialized-DV read path into
DB-17.3-local source files with Databricks package imports. Do not add the OSS
`delta-33x-40x` source directory to the DB-17.3 pom.

This plan is deliberately scoped to the V1/materialized DV read path: read Parquet on
GPU, materialize a per-file row mask from the Delta DV, and filter deleted rows after
the read. The native cuDF DV path is deferred until Databricks serialized bitmap
compatibility is validated.

DV writes remain out of scope for GPU acceleration. DELETE, UPDATE, MERGE, and OPTIMIZE
paths that would create or rewrite persistent DVs should continue to run on CPU.

## Current OSS DV Support

OSS Delta 3.3.x and 4.0.x include the shared source directory
`delta-lake/common/src/main/delta-33x-40x/scala`, which DB-17.3 cannot compile because it
uses the OSS `org.apache.spark.sql.delta.*` namespace.

The supported OSS DV read paths are:

- **Materialized DV path:** `GpuDeltaParquetFileFormatBase`
  - Reads per-file DV metadata from `PartitionedFile.otherConstantMetadataColumnValues`.
  - Uses `DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED` and
    `FILE_ROW_INDEX_FILTER_TYPE`.
  - Loads a Scala `RoaringBitmapArray` with
    `StoredBitmap.create(...).load(new HadoopFileSystemDVStore(conf))`.
  - Builds GPU row-index and `is_row_deleted` columns, then lets the regular GPU filter
    remove deleted rows.
  - Supports the per-file and multi-threaded Parquet readers.
  - Disables file splitting and predicate pushdown when a DV-bearing table is read through
    this path.

- **Native cuDF DV path:** `GpuDeltaParquetFileFormatBase2`
  - Passes serialized DV bitmaps directly to cuDF via
    `MakeParquetTableWithDVProducer`.
  - Uses `RapidsDeletionVectorStore` and `RapidsStoredBitmap` to load serialized bitmap
    bytes into host memory.
  - Supports DV predicate pushdown when metadata row index is enabled.
  - Requires serialized bitmap compatibility to be proven for DB-17.3 before use.

- **Provider wiring:** `Delta33xProvider` and `Delta40xProvider`
  - Choose `GpuDelta*ParquetFileFormat2` when DV predicate pushdown is enabled and the
    metadata row-index config allows it.
  - Otherwise choose `GpuDelta*ParquetFileFormat`, disabling read optimizations when
    `fmt.hasTablePath` indicates DV scan state.
  - Share plan rewrites from `DeltaProviderBase` for metadata-column pruning, DV scan
    detection, and optional DV predicate pushdown.

- **DV write handling**
  - GPU DELETE/UPDATE/MERGE/OPTIMIZE do not write persistent DVs.
  - Existing OSS meta rules fall back to CPU when persistent-DV write configs are enabled.
  - GPU command metrics include DV counters, but GPU paths keep DV-added/removed/updated
    counts at zero unless the operation is CPU-fallback.

## Confirmed DB-17.3 Facts

The current DB-17.3 shim blocks GPU scans for all DV tables in
`GpuDeltaParquetFileFormat.tagSupportForGpuFileSourceScan` whenever
`format.tablePath.isDefined`.

DB-17.3 uses the Databricks namespace and a per-file DV API similar to OSS Delta 3.3/4.0:

- `DeltaParquetFileFormat` has a 10-field constructor:
  `protocol`, `metadata`, `generateRowIndexFilterId`, `generateRowIndexFilterColumn`,
  `generateDeltaFileInScanId`, `nullableRowTrackingConstantFields`,
  `nullableRowTrackingGeneratedFields`, `optimizationsEnabled`, `tablePath`, and
  `isCDCRead`.
- `broadcastDvMap` and `broadcastHadoopConf` do not exist.
- DV metadata is embedded in `PartitionedFile.otherConstantMetadataColumnValues`.
- `FILE_ROW_INDEX_FILTER_ID_ENCODED`, `FILE_ROW_INDEX_FILTER_TYPE`,
  `IS_ROW_DELETED_COLUMN_NAME`, and `ROW_INDEX_COLUMN_NAME` are public companion
  constants. The implementation should import these constants instead of hardcoding
  column names.
- `com.databricks.sql.io.RowIndexFilterProvider` exists and can retrieve row-index
  filters and serialized bitmap bytes, but it is not the preferred V1 path.
- `com.databricks.sql.transaction.tahoe.deletionvectors.StoredBitmap` and
  `com.databricks.sql.transaction.tahoe.storage.dv.HadoopFileSystemDVStore` are present
  and match the materialized bitmap-loading shape needed for V1.
- Databricks-native `DropMarkedRowsFilter` / `KeepMarkedRowsFilter` classes are not
  available by those names; port/adapt the RAPIDS row-index filter helpers instead.
- `supports_delta_lake_deletion_vectors()` already returns true for Databricks 12.2+,
  so no DB-17.3 test-gate edit is needed just to enable DV tests.

## DB-17.3 Port Strategy

Implementation decisions:

- Keep `delta-lake/delta-spark400db173/pom.xml` source directories unchanged:
  `common/src/main/scala` and `common/src/main/databricks/scala` only.
- Port OSS DV read logic into DB-17.3-local files under `delta-lake/delta-spark400db173`.
- Use Databricks equivalents for all Delta imports, for example:
  - `com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat`
  - `com.databricks.sql.transaction.tahoe.actions.{DeletionVectorDescriptor, Metadata, Protocol}`
  - `com.databricks.sql.transaction.tahoe.deletionvectors.{RoaringBitmapArray, StoredBitmap}`
  - `com.databricks.sql.transaction.tahoe.storage.dv.HadoopFileSystemDVStore`
  - `com.databricks.sql.io.RowIndexFilterType`
- Enable the materialized-DV read path first.
- Do not port `RapidsDeletionVectorStore.scala` or `RapidsStoredBitmap.scala` in the
  first PR unless the implementation unexpectedly needs native serialized bitmaps. Those
  files belong to the deferred native cuDF path.
- Do not enable `canPushDVPredicateDownToScan` for V1. Keep native DV predicate pushdown
  disabled until `RowIndexFilterProvider.retrieveSerialized(conf)` and cuDF serialized
  bitmap compatibility are verified.
- Keep DV writes on CPU. Do not revive GPU persistent-DV DELETE/UPDATE/MERGE behavior as
  part of this work.

## Implementation Plan

1. Add `RapidsRowIndexFilters.scala` under
   `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta`.
   - Port the V1-relevant helpers from OSS
     `delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsRowIndexFilters.scala`.
   - Keep `RapidsRowIndexFilter`, `RapidsDropMarkedRowsFilter`,
     `RapidsKeepMarkedRowsFilter`, `RapidsDropAllRowsFilter`, and
     `RapidsKeepAllRowsFilter`.
   - Adapt imports to Databricks `RoaringBitmapArray`, `StoredBitmap`,
     `HadoopFileSystemDVStore`, and `RowIndexFilterType`.

2. Add a slim `RapidsDeletionVectors.scala` under the DB-17.3 Delta package.
   - Port only V1-relevant helpers, primarily column-mapping filter translation used by
     `prepareFiltersForRead`.
   - Drop native/V2-only helpers such as serialized `HostMemoryBuffer` DV loading,
     row-group metadata helpers, and cuDF-table DV plumbing.

3. Add `GpuDeltaParquetFileFormatDV.scala`.
   - Extend the current Databricks shared `GpuDeltaParquetFileFormatBase`.
   - Mirror OSS `GpuDeltaParquetFileFormatBase` V1 behavior with Databricks imports.
   - Override `prepareSchema` to also strip
     `DeltaColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY` when available.
   - Implement `prepareFiltersForRead`, returning no pushed filters when
     `optimizationsEnabled` is false for DV reads.
   - Implement `isSplitable = optimizationsEnabled`.
   - Implement DV-aware `buildReaderWithPartitionValuesAndMetrics`.
   - Implement `createMultiFileReaderFactory` using a Delta-aware multi-file reader that
     maps the current input file back to its `PartitionedFile`, reads DV descriptor/type
     from `otherConstantMetadataColumnValues`, and applies the per-file
     `RapidsRowIndexFilter`.
   - Ensure the reader does not combine files in ways that lose per-file DV bookkeeping.

4. Rewrite `GpuDeltaParquetFileFormat.scala`.
   - Extend `GpuDeltaParquetFileFormatDV` instead of the bare Databricks base.
   - Expand the GPU case class to mirror the DB-17.3 CPU
     `DeltaParquetFileFormat` 10-field shape:

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
         isCDCRead: Boolean = false)
     ```

   - Remove the current unconditional GPU fallback when `format.tablePath.isDefined`.
   - Add a conservative CDC+DV guard until CDC plumbing is reviewed:
     `format.isCDCRead && format.tablePath.isDefined`.
   - Keep the existing `_databricks_internal` fallback for unrelated Databricks-internal
     metadata columns.
   - Update `convertToGpu` to pass DB-17.3 CPU fields through. For DV tables, force
     `optimizationsEnabled = false` for the first V1 implementation so file splitting and
     predicate pushdown do not break row-index alignment.

5. Add/adapt DB-17.3 provider hooks for DV scan planning.
   - Do not assume the existing provider delegation is sufficient.
   - `ScanExecShims` rejects hidden metadata columns unless `DeltaProvider().isDVScan(meta)`
     returns true. DB-17.3 currently inherits the default `false`.
   - Add DB-17.3-specific `isDVScan` logic matching the DV plan shape, based on the OSS
     `DeltaProviderBase` implementation.
   - Add `pruneFileMetadata` logic to remove unused `_metadata` / temporary row-index
     metadata below the DV filter.
   - Keep `canPushDVPredicateDownToScan` false for V1, and leave
     `pushDVPredicateDownToScan` disabled until the native cuDF path is implemented.
   - Prefer adding these overrides in `DeltaSpark400DB173Provider` if the behavior is
     DB-17.3-specific. Move to `DatabricksDeltaProviderBase` only if the same shape is
     proven correct for other Databricks runtimes.

6. Preserve write-path behavior.
   - Keep current GPU plain-write support unchanged.
   - Keep DB-17.3 DELETE/UPDATE/MERGE stubs and CPU fallback behavior unchanged unless a
     separate DML issue is being implemented.
   - Confirm persistent-DV write configs still choose CPU.

7. Update DB-17.3 DV docs after code validation.
   - Update Issue 6 in `delta-lake-db173-github-issues.md` with final scope and status.
   - Update checkpoint/context docs only after implementation is validated on a DB-17.3
     cluster.

## Namespace Substitution Map

| OSS import | DB-17.3 import |
|---|---|
| `org.apache.spark.sql.delta.DeltaParquetFileFormat` | `com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat` |
| `org.apache.spark.sql.delta.actions.DeletionVectorDescriptor` | `com.databricks.sql.transaction.tahoe.actions.DeletionVectorDescriptor` |
| `org.apache.spark.sql.delta.actions.{Metadata, Protocol}` | `com.databricks.sql.transaction.tahoe.actions.{Metadata, Protocol}` |
| `org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, StoredBitmap}` | `com.databricks.sql.transaction.tahoe.deletionvectors.{RoaringBitmapArray, StoredBitmap}` |
| `org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore` | `com.databricks.sql.transaction.tahoe.storage.dv.HadoopFileSystemDVStore` |
| `org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaColumnMappingMode, IdMapping, NameMapping, NoMapping}` | `com.databricks.sql.transaction.tahoe.{DeltaColumnMapping, DeltaColumnMappingMode, IdMapping, NameMapping, NoMapping}` |
| `org.apache.spark.sql.delta.schema.SchemaMergingUtils` | `com.databricks.sql.transaction.tahoe.schema.SchemaMergingUtils` |
| `org.apache.spark.sql.delta.sources.DeltaSQLConf` | `com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf` |
| `org.apache.spark.sql.delta.RowIndexFilterType` | `com.databricks.sql.io.RowIndexFilterType` |

Avoid hardcoding DV column names. Import companion constants from
`com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat`.

## Validation Plan

Build from the Scala 2.13 mirror:

```bash
mvn -f scala2.13/pom.xml -Dbuildver=400db173 \
  -pl dist,delta-lake/delta-spark400db173 -am install -DskipTests
```

Run targeted DB-17.3 DV read tests first:

```bash
DRIVER_MEMORY=4g bash integration_tests/run_pyspark_from_build.sh \
  --runtime_env=databricks --delta_lake --test_type=$TEST_TYPE \
  -k "test_delta_deletion_vector_read or test_delta_empty_deletion_vector_read or test_delta_deletion_vector_multithreaded_read"
```

Then expand to these scenarios:

- `delta_lake_test.py`
  - `test_delta_deletion_vector_read`
  - `test_delta_empty_deletion_vector_read`
  - `test_delta_deletion_vector_multithreaded_read`
  - `test_delta_deletion_vector_multithreaded_read_partitioned_table`
  - `test_delta_deletion_vector_mixed_dv_no_dv`
  - `test_delta_deletion_vector_coalescing_partitioned_table`, if not Databricks-skipped
  - `test_delta_read_column_mapping` with DVs enabled
  - `test_delta_name_column_mapping_no_field_ids` with DVs enabled
- `delta_lake_delete_test.py`
  - `test_delta_deletion_vector`
  - `test_delta_deletion_vector_read`
  - `test_delta_deletion_vector_read_drop_row_group`
- Fallback coverage
  - `test_delta_deletion_vector_fallback`
  - `test_delta_update_fallback_with_deletion_vectors`

Acceptance criteria:

- DV-bearing Delta tables read correctly on GPU for per-file and multi-threaded reader modes.
- Plan capture on at least one DV test confirms `GpuFileSourceScanExec` in the executed
  plan and no read-side CPU fallback.
- Empty DVs and mixed DV/no-DV file batches match CPU results.
- Row-group dropping does not misalign DV row indexes.
- Partition values remain correct after DV filtering.
- Column mapping reads continue to match CPU.
- Existing Databricks-skipped tests remain intentionally skipped unless the
  implementation explicitly supports their plan shape.
- Persistent-DV write operations still fall back to CPU.

Regression builds to run before final handoff:

```bash
mvn -f scala2.13/pom.xml -Dbuildver=400db173 \
  -pl delta-lake/delta-spark400db173 -am install -DskipTests
mvn -Dbuildver=330 install -DskipTests
mvn -f scala2.13/pom.xml -Dbuildver=400 install -DskipTests
mvn -f scala2.13/pom.xml -Dbuildver=350db143 install -DskipTests
```

## Risks And Unknowns

- **Provider hooks:** a file-format-only port may still be rejected before scan conversion
  because hidden metadata columns require `DeltaProvider().isDVScan(meta)`.
- **DB plan shape:** DB-17.3 DV plans may differ slightly from OSS. Validate `isDVScan`
  and `pruneFileMetadata` against actual DB-17.3 executed plans.
- **Row-index predicates:** V1 should disable scan pushdown for DV tables. The
  `test_delta_deletion_vector_read_drop_row_group` scenario is the key guardrail.
- **CDC + DV:** guarded off in this PR until reviewed.
- **Native cuDF DV:** serialized bitmap compatibility is unknown and deferred.
- **Metrics:** if `rowIndexColumnGenTime` or `isRowDeletedColumnGenTime` are missing from
  `GpuFileSourceScanExec.allMetrics`, add the needed metric wiring in the DB-17.3 DV
  file format class.

## Assumptions

- DB-17.3 remains Scala 2.13 only; all builds use `-f scala2.13`.
- DB-17.3 has no `org.apache.spark.sql.delta.*` classes, so OSS Delta sources are a
  blueprint only.
- The first DV-read implementation should prioritize correctness over native cuDF DV
  performance.
- No Python test-helper change is required for `supports_delta_lake_deletion_vectors`
  unless later cluster validation proves a separate gating issue.
- CI enablement for all Delta tests remains a later step after Issues 3-6 are validated.
