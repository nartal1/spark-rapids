# Delta Lake DB-17.3 Implementation Checkpoint
**Date:** 2026-04-13 (build-only); **Scope correction:** 2026-04-23;
**Issue 2 delivered:** 2026-04-23; **PR slim-down:** 2026-04-24;
**Issue 8 resolved:** 2026-04-24; **Issue 10 resolved + #11169 xfails:** 2026-04-28;
**PR review hardening:** 2026-04-30; **Issue 6 DV reads:** 2026-05-02;
**Native DV follow-up:** 2026-05-05; **Native-only cleanup/squash:** 2026-05-06;
**shared DB-14.3/DB-17.3 transaction refactor:** 2026-05-06;
**review-comment sync + DV fallback hardening:** 2026-05-11;
**DML skip-row pushdown guard + DB assertion canary:** 2026-05-14
**Status:** Issue 1 (scaffolding) + Issue 2 (GPU writes) + Issue 6
(GPU DV reads, final DBR-17.3 native cuDF-only path) + Issue 8 + Issue 10
complete. `delta_lake_write_test.py`, targeted `delta_lake_test.py`, and
targeted `delta_lake_delete_test.py` are green on DB-17.3 for the validated
local runs below. Simple Delta writes run on GPU end-to-end with data + log
parity. DV-bearing DBR-17.3 Delta reads run on GPU only through the native cuDF
DV path; when either native gate is disabled, DV scans fall back to CPU. The OSS
Delta materialized/non-cuDF DV path still exists outside this DBR-17.3 shim.
DML (DELETE/UPDATE/MERGE), OPTIMIZE, CDC + DV reads, non-`IF_CONTAINED`
native DV filter semantics, and CI enablement remain follow-up CPU-fallback
areas. The 2026-05-14 hardening keeps DBR DML/DELETE fallback plans from losing
their DB skip-row predicate and adds a Python integration-test canary for DB's
missing row-index-filter assertion wording; GPU DV support remains read-only,
not GPU DELETE with persistent DVs. A follow-up refactor moved DB-14.3/DB-17.3
transaction, catalog, data-source, and shim code into
`delta-lake/common/src/main/db-350db143-400db173/scala`; the DB-17.3 module now
keeps only version-specific adapters, local CPU-fallback stubs, and native DV
read files.
**Build verified with:** `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`
**Branch:** `delta_db173_dv_native_ONLY_support`

---

## Final Native-only DV State (2026-05-06)

The three local implementation commits were squashed into:

```text
ada2580ea3ff0c558f62b1e503a0f302343e83f9 [databricks] Add native-only Delta DV reads for DBR 17.3
```

Final net commit shape:
- `DeltaBitmapUtils.java` added to bridge DBR bitmap serialization to cuDF-compatible
  standard portable roaring bitmap bytes.
- `GpuDeltaParquetFileFormatNativeDV.scala` added for PERFILE, MULTITHREADED,
  COALESCING, native-footer, and zero-column/count-star DV reads.
- `RapidsDeletionVectors.scala` added for DBR descriptor/provider lookup, column-mapping
  filter translation, native bitmap loading, and row-count metadata helpers.
- `DeltaSpark400DB173Provider.scala`, `GpuDeltaParquetFileFormat.scala`,
  `delta_lake_test.py`, and `delta_lake_delete_test.py` updated.
- The intermediate materialized DBR GPU files `GpuDeltaParquetFileFormatDV.scala` and
  `RapidsRowIndexFilters.scala` are not part of the final squashed commit.

DBR-17.3 native gates:
- `spark.databricks.delta.deletionVectors.useMetadataRowIndex=true`
- `spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled=true`

`useMetadataRowIndex` is effectively treated as true by default through the DBR
`DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX` default used by the provider.
If a DV scan explicitly disables either native gate, the DBR-17.3 shim tags the scan for
CPU fallback with a DBR-specific message. This native-only rule is DBR-17.3-specific and
does not remove the OSS Delta materialized DV fallback path.

2026-05-11 hardening amended this same DV commit after a broader Delta run found
`test_delta_merge_query` producing a DBR `RowIndexFilterType.IF_NOT_CONTAINED`
provider. The native cuDF DV path accepts only drop-marked-row semantics
(`IF_CONTAINED`), so DBR row-index filters with any other type are now tagged for
CPU fallback during scan planning instead of failing inside a reader task.

2026-05-14 hardening fixed a separate plugin-enabled DML fallback bug. The
native DV predicate pushdown rule now removes DBR skip-row predicates and prunes
the hidden skip-row column only when the child plan actually contains a
`GpuFileSourceScanExec` using `GpuDeltaParquetFileFormatNativeDV`. Databricks
DELETE/DML bitmap-writing plans can still fall back to CPU while the plugin is
enabled, and those plans must keep DB's skip-row filter. Dropping that filter
caused `test_delta_delete_twice_with_dv` to fail with
`DELTA_DELETION_VECTOR_CARDINALITY_MISMATCH`. This is a generic guard on the
plan invariant, not a test-specific exception-message workaround.

Final validation:
- Build: `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`
  passed after the native-only cleanup and again after the 2026-05-11 fallback hardening.
- Targeted DBR Delta DV slice 1 passed: 80 selected, 80 passed. Covered PERFILE,
  MULTITHREADED, COALESCING, mixed DV/no-DV, scan splitting, native footer, and disabled
  native-gate CPU fallback.
- Targeted DBR Delta DV slice 2 passed: 120 selected, 108 passed, 12 skipped. The skips
  are existing Databricks skips for count-star variants. Covered empty DVs, multithreaded
  reads, native-footer count-star, coalescing count-star, and multithreaded combined
  count-star selection.
- Combined directly affected DBR DV read surface: 200 selected; 188 passed,
  12 expected Databricks skips.
- 2026-05-11 focused DV selection passed: 194 tests, 0 failures, 0 errors.
- 2026-05-11 targeted merge regression passed:
  `test_delta_merge_query` 1 test, 0 failures.
- 2026-05-11 broader Delta read/delete selection passed: 289 tests, 0 failures,
  0 errors, 40 skips.
- 2026-05-11 `delta_lake_auto_compact_test.py` selection passed: 14 tests,
  0 failures.
- 2026-05-14 build passed again with
  `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`.
- 2026-05-14 targeted fallback regression passed:
  `delta_lake_delete_test.py::test_delta_delete_twice_with_dv` with
  `SPARK_RAPIDS_TEST_DATAGEN_SEED=1778719081` and OOM injection enabled,
  1 test, 0 failures.
- 2026-05-14 `test_db173_missing_row_index_filter_assertion_guard` protects the
  RAPIDS `MISSING_ROW_INDEX_FILTER_MESSAGE` substring against DB assertion wording
  drift. The canary was manually verified to fail after temporarily mutating the
  Scala constant, then pass after reverting.

## Shared DB-14.3/DB-17.3 Transaction Refactor (2026-05-06)

Commit `d8b1e4309` refactored the overlapping Databricks Delta write/catalog
surface out of the DB-14.3 and DB-17.3 modules into a shared source root:

```text
delta-lake/common/src/main/db-350db143-400db173/scala
```

Both `delta-spark350db143` and `delta-spark400db173` now add this source root
from their module `pom.xml` files, and the generated `scala2.13/` poms were
synced.

Shared files now hosted in that root:
- `GpuCreateDeltaTableCommandBase.scala`
- `GpuDeltaCatalog.scala`
- `GpuDeltaCatalogCommon.scala`
- `GpuOptimisticTransactionBaseCommon.scala`
- `GpuOptimisticTransactionWriteBase.scala`
- `GpuWriteIntoDeltaBase.scala`
- `GpuDeltaDataSource.scala`
- `DeltaLogShim.scala`
- `InvariantViolationExceptionShim.scala`
- `MetadataShims.scala`
- `ShimDeltaUDF.scala`

The DB-17.3 module-local transaction files remain, but they are now narrow
adapters:
- `GpuCreateDeltaTableCommand.scala` keeps the DB-17.3 CTAS/RTAS
  `WriteIntoDeltaEdge` guard and delegates shared create-table behavior to
  `GpuCreateDeltaTableCommandBase`.
- `GpuOptimisticTransactionBase.scala` handles DB-17.3 invariant-checker and
  Spark-4 logical-relation differences on top of
  `GpuOptimisticTransactionBaseCommon`.
- `GpuOptimisticTransaction.scala` keeps DB-17.3-specific write overrides,
  runtime-replaceable stats normalization, and the Delta writer hook while
  sharing the core GPU write pipeline through `GpuOptimisticTransactionWriteBase`.
- `GpuWriteIntoDelta.scala` keeps the DB-17.3 `NoRowsCopiedTag` commit behavior
  while sharing the transaction wrapper through `GpuWriteIntoDeltaBase`.

## PR Slim-down Note (2026-04-24)

To keep the Issue 1 PR reviewable and scoped, the DML/OPTIMIZE command implementations
that shipped in the first draft of the build-fixes commit were removed or stubbed:

- **`GpuMergeIntoCommand.scala`** (1,206 LOC) — **deleted**. `MergeIntoCommandMetaShim.convertToGpu`
  now throws `UnsupportedOperationException`; `tagForGpu` already unconditionally calls
  `willNotWorkOnGpu`, so the throw is unreachable.
- **`GpuOptimizeExecutor.scala`** (420 LOC) — **deleted**. Only referenced from
  `GpuDoAutoCompaction`.
- **`GpuDoAutoCompaction.scala`** (48 LOC) — **deleted**. `registerPostCommitHook(GpuDoAutoCompaction)`
  removed from `GpuOptimisticTransaction.gpuWriteFiles`; Databricks' CPU auto-compact
  path runs when the config / table property is set.
- **`GpuDeleteCommand.scala`** (382 LOC) — **stubbed** to ~30 LOC (empty case class with
  matching constructor, `run()` throws). The shared `DeleteCommandMeta` (in
  `delta-lake/common/src/main/databricks/.../DeleteCommandMeta.scala`) directly
  constructs `GpuDeleteCommand`, so the type must compile.
- **`GpuUpdateCommand.scala`** (287 LOC) — **stubbed** similarly.
- **`MergeIntoCommandMetaShim.scala`** — `convertToGpu` now throws; `GpuMergeIntoCommand`
  / `GpuDeltaLog` imports removed.

Full implementations will be re-added in their dedicated follow-up issues:
- [#14597](https://github.com/NVIDIA/spark-rapids/issues/14597) (DELETE/UPDATE)
- [#14598](https://github.com/NVIDIA/spark-rapids/issues/14598) (MERGE INTO)
- [#14599](https://github.com/NVIDIA/spark-rapids/issues/14599) (OPTIMIZE + auto-compaction)
- [#14600](https://github.com/NVIDIA/spark-rapids/issues/14600) (Deletion Vector reads)

The plain V1/path-style GPU write path (INSERT, append, overwrite, including
partitioned writes) is unaffected. CTAS/RTAS were later conservatively kept on
CPU by the 2026-04-30 review hardening until the DB-17.3 create-table path is
ported more faithfully. Net commit size: ~4,597 → ~2,289 lines.

## PR Review Hardening (2026-04-30)

Static review of the first PR against the live DB-17.3 JARs found DB-17.3 create-table
semantics that the old GPU `GpuCreateDeltaTableCommand` path must not silently drop.
`DeltaSpark400DB173Provider.tagForGpu` now conservatively falls back for CTAS/RTAS when
the `TableSpec` or session defaults request row filters, column masks, liquid clustering,
auto TTL, catalog-owned tables (explicit or default configuration), coordinated commits
(explicit table properties or default table-property conf), or deletion vectors
(explicit table property or default table-property conf). Explicit Delta property-key
checks are case-insensitive, and invalid DV boolean values also fall back so Delta's CPU
validation reports the error. Auto-enable DV triggers remain tracked with the broader
DB-17.3 create-command port under #14601.

The shared `GpuIdentityColumn` Spark-4 migration was also corrected. `col(name)` is not
equivalent to the old `UnresolvedAttribute.quoted(name)` behavior for column names that
contain dots. The code now uses
`DFUDFShims.exprToColumn(UnresolvedAttribute.quoted(name))`, preserving literal field
names while avoiding the removed Spark-4 `new Column(expr)` constructor.

### 2026-05-11 Review-comment Sync

Commit `362e9d346` is included under the amended DV commit on the
`delta_db173_dv_native_ONLY_support` branch. It carries the PR review follow-ups from
the base DBR-17.3 support branch:

- `FilePartitionShims.copyWithFiles` now clears `pathPrefix` after storing files that
  were already expanded through `filesWithAbsolutePaths`, avoiding a second root-prefix
  application on later partition processing.
- The DB-17.3 wide-schema test-size reduction now points at
  https://github.com/NVIDIA/spark-rapids/issues/14775.
- CTAS/RTAS conversion is guarded in shared `DatabricksDeltaProviderBase`; if those
  plans were tagged unsupported they now throw instead of accidentally converting.
- `GpuOptimisticTransaction` registers Databricks `AutoCompact` as a CPU fallback hook
  with RAPIDS temporarily disabled when auto-compaction is enabled.
- Shared `GpuCheckDeltaInvariant` now wraps only the real constraint child; DBR 17.3
  extractor children are CPU-side error-message helpers and are not converted.

### 2026-05-14 DML Skip-row Pushdown Guard

The native DV pushdown rule in `DeltaSpark400DB173Provider.scala` now checks for
a native GPU DV scan before removing the DBR skip-row predicate. The skip-row
predicate is redundant only for `GpuDeltaParquetFileFormatNativeDV` because
cuDF applies the deletion vector in the scan. DBR DELETE/DML plans that fall
back to CPU can still contain the same hidden skip-row column while writing
deletion-vector bitmaps, and they need the filter to preserve Delta's
cardinality accounting.

The regression was reproduced by
`delta_lake_delete_test.py::test_delta_delete_twice_with_dv` with seed
`1778719081`. It failed with
`DELTA_DELETION_VECTOR_CARDINALITY_MISMATCH` on the second delete even without
OOM injection, confirming that the issue was the fallback plan shape rather
than OOM retry. After the guard, the DBR build and the original OOM-injected
targeted test passed.

The same review pass added
`delta_lake_test.py::test_db173_missing_row_index_filter_assertion_guard`. Because
Databricks Scala tests are not run for this platform, the guard lives in Python:
it reflects DB's `SupportsRowIndexFilters` missing-filter assertion message and
asserts that RAPIDS' `MISSING_ROW_INDEX_FILTER_MESSAGE` remains a substring. If
Databricks changes the wording, the integration test fails before the matcher
silently stops recognizing the fallback condition.

---

## Issue 6 DV Read Delivery (historical 2026-05-02 checkpoint)

The 2026-05-02 checkpoint briefly landed a DBR-17.3-local V1/materialized
deletion-vector read path. That was an intermediate implementation only. The
final 2026-05-06 squashed commit removed the DBR-17.3 materialized GPU path and
keeps DBR-17.3 DV reads GPU-supported only through native cuDF scanning. The
historical notes below are retained to explain why the final cleanup deleted
`GpuDeltaParquetFileFormatDV.scala` and `RapidsRowIndexFilters.scala`.

Files added for the intermediate checkpoint:
- `GpuDeltaParquetFileFormatDV.scala`
- `RapidsDeletionVectors.scala`
- `RapidsRowIndexFilters.scala`

Files updated for Issue 6:
- `DeltaSpark400DB173Provider.scala`
- `GpuDeltaParquetFileFormat.scala`
- `integration_tests/src/main/python/delta_lake_test.py`
- `integration_tests/src/main/python/delta_lake_delete_test.py`

Validation completed:
- Build: `SKIP_DEP_INSTALL=1 WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`
  passed. Log: `/tmp/db173-build-final-review-cleanups.log`.
- Focused post-cleanup DV validation:
  `delta_lake_delete_test.py::test_delta_deletion_vector_read`
  + `delta_lake_test.py::test_delta_empty_deletion_vector_read`
  passed: `57 passed, 35 warnings in 182.72s`.
  Log: `/tmp/db173-dv-focused-after-cleanups.log`.
- Full DB-17.3 Delta read suite before final cleanup:
  `delta_lake_test.py` passed: `180 passed, 40 skipped, 13 xpassed,
  34 warnings in 726.67s`.
  Log: `/tmp/db173-delta-lake-full-final2.log`.
- Full DB-17.3 Delta delete suite before final cleanup:
  `delta_lake_delete_test.py` passed: `40 passed, 16 xfailed,
  28 warnings in 276.22s`.
  Log: `/tmp/db173-delta-lake-delete-full-final2.log`.

Historical remaining after the 2026-05-02 materialized-reader checkpoint:
native cuDF DV reader / Base2-equivalent path, DV predicate pushdown,
split/native optimized DV scanning, true DV-aware coalescing, CDC + DV reads,
and DB-17.3 CI Delta enablement (Issue 7). The native-reader items were closed
by the 2026-05-05 follow-up, then the materialized DBR path was stripped by the
2026-05-06 native-only cleanup.

## Native cuDF DV Follow-up and Native-only Cleanup (2026-05-05 / 2026-05-06)

The native DV follow-up is implemented and supersedes the "Remaining after
Issue 6" native-reader items above. After team review, the DBR-17.3 GPU support
scope was narrowed to the cuDF-native path only. The materialized/non-cuDF GPU
DV path remains valid for OSS Delta modules, but is not supported in the
DBR-17.3 shim.

Final files added or updated:
- `DeltaBitmapUtils.java`
- `DeltaSpark400DB173Provider.scala`
- `GpuDeltaParquetFileFormat.scala`
- `GpuDeltaParquetFileFormatNativeDV.scala`
- `RapidsDeletionVectors.scala`
- `integration_tests/src/main/python/delta_lake_delete_test.py`
- `integration_tests/src/main/python/delta_lake_test.py`

Feature coverage now matches the OSS Spark-4.0 sequence for the supported
DBR-17.3 read path:
- Narrow DBR-specific DV predicate pushdown is enabled when both DBR metadata
  row-index mode and
  `spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled` are true.
- Native cuDF DV scanning is enabled for PERFILE, MULTITHREADED, and COALESCING
  readers through `GpuDeltaParquetFileFormatNativeDV`.
- Native-footer multi-row-group and count-star DV paths are covered.
- DV lookup can recover descriptors/providers from `PartitionedFile`,
  `RowIndexFilterProvider`, or `TahoeFileIndex`.
- DBR `SerializedBitmap` bytes are converted to cuDF-compatible standard
  portable roaring bitmap bytes through `DeltaBitmapUtils.java`.

Validation completed after the native-only cleanup:
- Build passed:
  `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`.
- Targeted DBR Delta DV read slice 1 passed: 80 selected, 80 passed.
- Targeted DBR Delta DV read slice 2 passed: 120 selected, 108 passed,
  12 expected Databricks skips.
- 2026-05-11 validation after the `IF_NOT_CONTAINED` CPU-fallback hardening:
  focused DV selection 194/194 passed; targeted merge regression 1/1 passed;
  broader `delta_lake_test.py or delta_lake_delete_test.py` selection passed
  289 tests with 40 skips; `delta_lake_auto_compact_test.py` selection passed
  14/14.

Known DBR-17.3 caveats:
- Native DV pushdown intentionally recognizes only the current DBR skip-row
  predicate shapes:
  `__delta_internal_is_row_deleted` and
  `_databricks_internal_edge_computed_column_skip_row`.
- The native path requires DBR metadata-row-index mode plus the RAPIDS DV
  predicate-pushdown config. Otherwise DBR-17.3 DV scans fall back to CPU.
- The native path supports only `RowIndexFilterType.IF_CONTAINED`; other DBR
  row-index filter semantics, including `IF_NOT_CONTAINED`, are tagged for CPU
  fallback.
- Native skip-row predicate removal is valid only under a native GPU DV scan.
  Plugin-enabled DELETE/DML fallback plans keep the DBR skip-row filter and
  remain CPU fallback for persistent-DV writes.
- Existing unsupported DBR cases remain: CDC reads with DVs, Delta
  file-in-scan metadata columns, and nullable row-tracking metadata fields.
- DBR uses `DeletionVectorStoreEdge` / `SerializedBitmap` APIs instead of the
  OSS `RapidsDeletionVectorStore` / `RapidsFileIO` path.

---

## Scope-Correction Note (2026-04-23)

Cluster validation of `delta_lake_write_test.py` on DB-17.3 revealed that **the DB-14.3
write-interception pattern does not apply to DB-17.3**. Specifically:

- DB-14.3 `WriteIntoDeltaEdge.write(txn)` calls `txn.writeFiles(data, opts, ...)` —
  our `GpuOptimisticTransactionBase.writeFiles` override catches here and emits
  `RapidsDeltaWrite` + `GpuFileFormatWriter.write`.
- DB-17.3 `WriteIntoDeltaEdge.write(txn)` calls
  `TransactionalWriteEdge.writeFilesAndGetQueryExecution(...)` (new method), which does
  its own internal `executeCollect` on a
  `DataWritingCommandExec(WriteIntoDeltaCommand) + WriteFiles + query` plan.
  **Our `writeFiles` override is never invoked on DB-17.3.**

Intercepting the inner `WriteIntoDeltaCommand` via a `DataWritingCommandMeta` (attempted
2026-04-23) triggered a nested-transaction error
(`DELTA_ACTIVE_TRANSACTION_ALREADY_SET`) because an outer transaction is already open.
Stack trace confirmed the issue at
`com.databricks.sql.transaction.tahoe.files.TransactionalWriteEdge.writeFilesAndGetQueryExecution:393`.

**Consequence:** the original Issue-1 goal "enable GPU writes" is not actually delivered
by this commit. The module scaffolding, Spark-4.0 API migrations, and CPU fallbacks are
correct; the GPU write path needs a separate Issue 2 that overrides
`writeFilesAndGetQueryExecution` (and/or `writeFilesAndGetExecutedPlan`) in
`GpuOptimisticTransactionBase`.

**Jenkins Delta Lake test skip for `spark400db173` must stay in place** until the new
Issue 2 lands. See [delta-lake-db173-github-issues.md](delta-lake-db173-github-issues.md)
for the revised issue plan.

An additional Databricks-only expression `com.databricks.sql.transaction.tahoe.GenerateIdentityValues`
(emitted by the Delta analyzer for identity columns) is unsupported on GPU and forces
`ProjectExec` to CPU; Issue 2 must address it too.

---

## Summary (historical Issue 1 scope)

Issue 1 implementation initially created `delta-lake/delta-spark400db173/` with 22 Scala
files + pom.xml. **Build-only scaffolding for Databricks 17.3** — every Delta operation
fell back to CPU in that commit. The current post-refactor file layout is listed below.

- **GPU-enabled:** none
- **CPU fallback:** writes (all shapes), DELETE, UPDATE, MERGE, OPTIMIZE, DV reads
- **Jenkins CI:** Delta Lake tests remain skipped for `spark400db173`
  ([test.sh:152-160](jenkins/databricks/test.sh))

Full build succeeds: 172 class files compiled, dist jar produced.

---

## All Files Created/Modified

### A. DB-17.3 Module: `delta-lake/delta-spark400db173/` (post-refactor, 2026-05-06)

```
delta-lake/delta-spark400db173/
  pom.xml
  src/main/java/
    com/nvidia/spark/rapids/delta/
      DeltaBitmapUtils.java             -- DBR bitmap serialization bridge for native cuDF DV
  src/main/scala/
    com/databricks/sql/transaction/tahoe/rapids/
      GpuCreateDeltaTableCommand.scala   -- DB-17.3 adapter over shared base; guards WriteIntoDeltaEdge CTAS/RTAS
      GpuDeleteCommand.scala             -- STUB (~30 LOC, throws on run) — full impl in issue #14597
      GpuDeltaFileFormatWriter.scala     -- NEW (Issue 2): PartitionedTaskAttemptContextImpl wrapper
      GpuOptimisticTransactionBase.scala -- DB-17.3 adapter over shared base; invariant + Spark 4 logical-relation fixes
      GpuOptimisticTransaction.scala     -- DB-17.3 write overrides on shared write base
      GpuUpdateCommand.scala             -- STUB (~35 LOC, throws on run) — full impl in issue #14597
      GpuWriteIntoDelta.scala            -- DB-17.3 NoRowsCopiedTag commit adapter over shared base
    com/nvidia/spark/rapids/delta/
      DeltaProbe.scala                   -- renamed from DB-14.3
      DeltaSpark400DB173Provider.scala   -- renamed + recacheByPlan fix
      GpuDeltaParquetFileFormat.scala    -- fully rewritten for DB-17.3 DV API
      GpuDeltaParquetFileFormatNativeDV.scala -- DBR-17.3 native cuDF DV scanner
      RapidsDeletionVectors.scala        -- DBR DV descriptor/provider lookup and native bitmap loading
    com/nvidia/spark/rapids/delta/shims/
      DeleteCommandMetaShim.scala        -- always falls back to CPU
      MergeIntoCommandMetaShim.scala     -- always CPU; convertToGpu throws (dead code)
      ShimShuffledRowRDD.scala           -- NEW: DB-17.3 ShuffledRowRDD 5/7-arg constructors
      UpdateCommandMetaShim.scala        -- always falls back to CPU
```

**Removed in slim-down** (will be re-added in follow-up issues):
- `GpuMergeIntoCommand.scala` → issue #14598
- `GpuOptimizeExecutor.scala` → issue #14599
- `GpuDoAutoCompaction.scala` → issue #14599

### A2. Shared DB-14.3/DB-17.3 Source Root (added 2026-05-06)

```
delta-lake/common/src/main/db-350db143-400db173/scala/
  com/databricks/sql/transaction/tahoe/rapids/
    GpuCreateDeltaTableCommandBase.scala
    GpuDeltaCatalog.scala
    GpuDeltaCatalogCommon.scala
    GpuOptimisticTransactionBaseCommon.scala
    GpuOptimisticTransactionWriteBase.scala
    GpuWriteIntoDeltaBase.scala
  com/nvidia/spark/rapids/delta/
    GpuDeltaDataSource.scala
  com/nvidia/spark/rapids/delta/shims/
    DeltaLogShim.scala
    InvariantViolationExceptionShim.scala
    MetadataShims.scala
    ShimDeltaUDF.scala
```

### B. New ShimShuffledRowRDD in Older DB Modules (4 files, identical content)

```
delta-lake/delta-spark330db/src/main/scala/com/nvidia/spark/rapids/delta/shims/ShimShuffledRowRDD.scala
delta-lake/delta-spark332db/src/main/scala/com/nvidia/spark/rapids/delta/shims/ShimShuffledRowRDD.scala
delta-lake/delta-spark341db/src/main/scala/com/nvidia/spark/rapids/delta/shims/ShimShuffledRowRDD.scala
delta-lake/delta-spark350db143/src/main/scala/com/nvidia/spark/rapids/delta/shims/ShimShuffledRowRDD.scala
```

### C. Modified Shared Files (backward-compatible)

```
pom.xml                                           -- release400db173 profile: delta-stub -> delta-spark400db173
delta-lake/common/.../GpuCheckDeltaInvariant.scala -- UnaryExprMeta -> ExprMeta, .toMap
delta-lake/common/.../GpuDeltaLog.scala            -- @nowarn on _clock
delta-lake/common/.../GpuIdentityColumn.scala      -- new Column(expr) -> DFUDFShims.exprToColumn(UnresolvedAttribute.quoted(name))
delta-lake/common/.../GpuOptimizeWriteExchangeExec.scala -- removed redundant imports
delta-lake/common/.../OptimizeWriteExchangeExec.scala    -- ShimShuffledRowRDD.create()
```

### D. Generated (by make-scala-version-build-files.sh 2.13)

```
scala2.13/pom.xml                                    -- auto-updated
scala2.13/delta-lake/delta-spark400db173/pom.xml     -- auto-generated
scala2.13/delta-lake/delta-spark350db143/pom.xml     -- synced for shared DB source root
```

---

## Detailed Changes Per File

### pom.xml (root, inside `#if scala-2.13` block, ~lines 645-654)

```diff
-  <rapids.delta.artifactId1>rapids-4-spark-delta-stub</rapids.delta.artifactId1>
+  <rapids.delta.artifactId1>rapids-4-spark-delta-${spark.version.classifier}</rapids.delta.artifactId1>
   <modules>
     <module>shim-deps/databricks</module>
-    <module>delta-lake/delta-stub</module>
+    <module>delta-lake/delta-spark400db173</module>
     <module>iceberg/iceberg-stub</module>
   </modules>
```

### delta-lake/delta-spark400db173/pom.xml (NEW)

Key properties:
- `<rapids.module>../delta-lake/delta-spark400db173</rapids.module>` (critical for scala2.13 source resolution)
- `<artifactId>rapids-4-spark-delta-spark400db173_2.12</artifactId>`
- Parent: `rapids-4-spark-shim-deps-parent_2.12`
- Sources via build-helper: `${spark.rapids.source.basedir}/delta-lake/common/src/main/scala`,
  `.../databricks/scala`, and `.../db-350db143-400db173/scala`

### GpuCheckDeltaInvariant.scala (shared, backward-compatible)

```diff
-import com.nvidia.spark.rapids.{..., UnaryExprMeta}
+import com.nvidia.spark.rapids.{..., ExprMeta}

-    extends UnaryExprMeta[CheckDeltaInvariant](check, conf, parent, rule) {
+    extends ExprMeta[CheckDeltaInvariant](check, conf, parent, rule) {

-  override def convertToGpu(child: Expression): GpuExpression = {
+  override def convertToGpuImpl(): GpuExpression = {
+    val child = childExprs.head.convertToGpu()
     GpuCheckDeltaInvariant(child,
-      wrapped.columnExtractors,
+      wrapped.columnExtractors.toMap,  // .toMap for Spark 4.0 compat
       wrapped.constraint)
```

Why backward-compatible: `ExprMeta` is a superclass of `UnaryExprMeta`. The `.toMap` call is a no-op on `Map` (DB-14.3) and converts `Seq[(K,V)]` to `Map` (DB-17.3).

### GpuDeltaLog.scala (shared)

```diff
+  @scala.annotation.nowarn("msg=never used")
   private lazy implicit val _clock: Clock = deltaLog.clock
```

Why: DB-17.3's `OptimisticTransaction` no longer takes implicit `Clock`. The `_clock` is needed for older DB versions. The `@nowarn` suppresses the unused warning only in DB-17.3.

### GpuIdentityColumn.scala (shared)

```diff
-import org.apache.spark.sql.{Column, Dataset, SparkSession}
+import org.apache.spark.sql.{Dataset, SparkSession}
+import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
+import org.apache.spark.sql.nvidia.DFUDFShims

-      val column = new Column(UnresolvedAttribute.quoted(name))
+      val column = DFUDFShims.exprToColumn(UnresolvedAttribute.quoted(name))
```

Why: `new Column(Expression)` constructor was removed in Spark 4.0, but `col(name)` is
not equivalent for literal column names containing dots. `DFUDFShims.exprToColumn`
provides the Spark-4-compatible column wrapper while preserving
`UnresolvedAttribute.quoted(name)` resolution semantics.

### GpuOptimizeWriteExchangeExec.scala (shared)

Removed redundant explicit imports that were shadowed by `import GpuMetric._` wildcard inside the class:
```diff
-import com.nvidia.spark.rapids.GpuMetric.{OP_TIME_NEW_SHUFFLE_READ, OP_TIME_NEW_SHUFFLE_WRITE}
-import com.nvidia.spark.rapids.GpuMetric.{DESCRIPTION_OP_TIME_NEW_SHUFFLE_READ, ...}
```

### OptimizeWriteExchangeExec.scala (shared)

```diff
+import com.nvidia.spark.rapids.delta.shims.ShimShuffledRowRDD
-import org.apache.spark.sql.execution.{CoalescedPartitionSpec, ShuffledRowRDD, ...}
+import org.apache.spark.sql.execution.{CoalescedPartitionSpec, ...}

-      new ShuffledRowRDD(shuffleDependency, readMetrics)
+      ShimShuffledRowRDD.create(shuffleDependency, readMetrics)
-        new ShuffledRowRDD(shuffleDependency, readMetrics, partitionSpecs.get.toArray)
+        ShimShuffledRowRDD.create(shuffleDependency, readMetrics, partitionSpecs.get.toArray)
```

Why: DB-17.3's `ShuffledRowRDD` requires `PrismMetrics`, `numMappers`, `refHolder` params. Each module provides its own `ShimShuffledRowRDD` with the correct constructor call.

---

## Spark 4.0 API Changes Applied to Module Files

Originally these changes were applied to DB-17.3-local module files. After the
2026-05-06 refactor, the shared portions live in
`delta-lake/common/src/main/db-350db143-400db173/scala` and the DB-17.3 module
keeps only the version-specific adapters/stubs.

### 1. `new Column(expr)` -> `DFUDFShims.exprToColumn(expr)` (14 occurrences)
**Files:** GpuDeleteCommand, GpuUpdateCommand, GpuMergeIntoCommand
**Import added:** `import org.apache.spark.sql.nvidia.DFUDFShims`

### 2. `Column.expr` -> `DFUDFShims.columnToExpr(col)` (3 occurrences)
**File:** GpuMergeIntoCommand (lines ~774, 775, 1017)
**Why:** `Column.expr` deprecated in Spark 4.0, fatal with `-Xfatal-warnings`

### 3. `Dataset.ofRows(spark, plan)` -> `TrampolineConnectShims.createDataFrame(classicSpark, plan)` (15 occurrences)
**Files:** GpuOptimisticTransaction, GpuDeleteCommand, GpuUpdateCommand, GpuMergeIntoCommand, GpuCreateDeltaTableCommand
**Import added:** `import org.apache.spark.sql.rapids.shims.TrampolineConnectShims`
**Cast:** `spark.asInstanceOf[TrampolineConnectShims.SparkSession]`

### 4. `session.sharedState.cacheManager.recacheByPlan(session, plan)` -> use ClassicSparkSession
**Files:** DeltaSpark400DB173Provider, GpuDeleteCommand, GpuUpdateCommand, GpuMergeIntoCommand
**Pattern:** `val classic = TrampolineConnectShims.getActiveSession; classic.sharedState.cacheManager.recacheByPlan(classic, plan)`

### 5. `(implicit clock: Clock)` removed from constructors
**Files:** GpuOptimisticTransactionBase, GpuOptimisticTransaction
**Why:** DB-17.3 `OptimisticTransaction` obtains Clock internally from `DeltaLog.clock()`

### 6. `writeFiles()` signature changed
**GpuOptimisticTransactionBase:** New override for DB-17.3 7-arg signature with `TransactionalWriteOptions`
```scala
override def writeFiles(inputData: Dataset[_],
    writeOptions: TransactionalWriteOptions, isOptimize: Boolean,
    isLiquidClustering: Boolean, additionalConstraints: Seq[Constraint],
    isCDCWritePhase: Boolean, context: Option[String]): Seq[FileAction]
```
Delegates to the existing 3-arg `writeFiles(inputData, writeOptions.deltaOptions, additionalConstraints)`.
**Import added:** `import com.databricks.sql.transaction.tahoe.files.TransactionalWriteOptions`

### 7. `PostCommitHook.run()` signature changed
**GpuDoAutoCompaction:**
```scala
// DB-14.3: run(spark, txn: OptimisticTransactionImpl, version, snapshot, actions)
// DB-17.3: run(spark, committedTxn: CommittedTransaction)
```
Extracts `deltaLog` and `committedActions` from `CommittedTransaction`. Creates `RapidsConf` from `spark.sessionState.conf`.

### 8. `PostCommitHook.handleError()` gained SparkSession param
**GpuDoAutoCompaction:**
```scala
// DB-14.3: handleError(error, version)
// DB-17.3: handleError(spark, error, version)
```

### 9. `LogicalRelation` pattern match arity: 4 -> 7
**Files:** GpuOptimisticTransactionBase (isOptimizeCommand), GpuMergeIntoCommand (buildTargetPlanWithFiles)
**Pattern:** Added wildcards `_, _, _` for new fields; construction preserves new fields.

### 10. `DeltaInvariantCheckerExec` constructor: 2-arg -> 3-arg
**GpuOptimisticTransactionBase:**
```scala
// DB-14.3: DeltaInvariantCheckerExec(cpuPlan, constraints)
// DB-17.3: DeltaInvariantCheckerExec(cpuPlan.session, cpuPlan, constraints)
```

### 11. `TaggedCommitData` now generic + `EMPTY` -> `empty()`
**GpuDeleteCommand:** Return type `DMLUtils.TaggedCommitData` -> `DMLUtils.TaggedCommitData[FileAction]`
**GpuUpdateCommand:** `DMLUtils.TaggedCommitData.EMPTY` -> `DMLUtils.TaggedCommitData.empty[FileAction]`

### 12. `GpuFileFormatWriter.write()` SparkSession type
**GpuOptimisticTransaction:** Cast `spark` to `TrampolineConnectShims.SparkSession` (=`ClassicSparkSession`)

### 13. `RuntimeReplaceable` stats expression handling
**GpuOptimisticTransaction:** Stats expression unwrapped with `.transform { case rr: RuntimeReplaceable => rr.replacement }`

### 14. Unused import cleanup
**GpuDeleteCommand:** Removed `Dataset` (no longer used after `Dataset.ofRows` removal)
**GpuUpdateCommand:** Removed `Dataset`
**DeltaSpark400DB173Provider:** Removed duplicate `SaveMode` import
**GpuDeltaParquetFileFormat:** Removed unused `DeltaColumnMappingMode`

---

## GpuDeltaParquetFileFormat: Historical Minimal DB-17.3 Rewrite

DB-14.3's version used `broadcastDvMap`, `broadcastHadoopConf`, `DeletionVectorDescriptorWithFilterType` — none of which exist in DB-17.3.

DB-17.3 minimal version:
```scala
case class GpuDeltaParquetFileFormat(
    protocol: Protocol,
    metadata: Metadata,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false
  ) extends GpuDeltaParquetFileFormatBase {
  override val columnMappingMode = metadata.columnMappingMode
  override val referenceSchema = metadata.schema
  // ... minimal reader, DV reads blocked via tagSupportForGpuFileSourceScan
}
```

This was the pre-Issue-6 minimal format. As of 2026-05-02, Issue 6 replaced the
DV fallback with `GpuDeltaParquetFileFormatDV` plus DB-17.3-local
`RapidsDeletionVectors` / `RapidsRowIndexFilters`.

`convertToGpu` constructs from `fmt.protocol`, `fmt.metadata`, `fmt.tablePath`, `fmt.isCDCRead`.

---

## DML Command Shims: All Fall Back to CPU (post-slim-down)

For Issue 1, DELETE/UPDATE/MERGE always fall back to CPU via the shim `tagForGpu`:
```scala
// DeleteCommandMetaShim, UpdateCommandMetaShim, MergeIntoCommandMetaShim:
meta.willNotWorkOnGpu("Delta Lake <OP> is not yet supported on GPU for DB-17.3")
```

After the 2026-04-24 slim-down:
- `GpuDeleteCommand` / `GpuUpdateCommand` remain in tree as **30-line stubs** whose
  `run()` throws. The shared `DeleteCommandMeta.convertToGpu` / `UpdateCommandMeta.convertToGpu`
  in `delta-lake/common/src/main/databricks/` constructs these case classes
  unconditionally at compile time, so the types must exist with the right constructor
  signature — the stub body is unreachable because `tagForGpu` forces CPU first.
- `GpuMergeIntoCommand` is **deleted**; `MergeIntoCommandMetaShim.convertToGpu` now
  throws directly. The shim is local to this module, so no shared compile constraint
  forces the class to exist.
- `GpuOptimizeExecutor` + `GpuDoAutoCompaction` are **deleted**; OPTIMIZE and
  auto-compaction both run on CPU. The `registerPostCommitHook(GpuDoAutoCompaction)`
  call in `GpuOptimisticTransaction.gpuWriteFiles` was removed. The 2026-05-11
  review-comment sync registers Databricks `AutoCompact` as a CPU hook instead,
  with RAPIDS disabled around `AutoCompact.run`.

Full implementations return in follow-up issues #14597 / #14598 / #14599 / #14600.

---

## Build Commands

```bash
# Full build on DB-17.3 cluster:
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh

# Skip dependency install for faster rebuilds:
WITH_DEFAULT_UPSTREAM_SHIM=0 SKIP_DEP_INSTALL=1 ./jenkins/databricks/build.sh

# After any pom.xml change:
./build/make-scala-version-build-files.sh 2.13

# Dist jar build only (faster iteration):
mvn -Dmaven.wagon.http.retryHandler.count=3 -B -f scala2.13/pom.xml package -pl dist -am \
    -DskipTests -Dmaven.scaladoc.skip -Ddatabricks -Dbuildver=400db173
```

---

## Issue 2 Delivery (2026-04-23)

Issue 2 (GPU Delta writes on DB-17.3) is implemented. Three surgical DB-17.3-only
changes; no older Databricks shim or OSS module touched.

### Cluster-verified facts

`javap` against the live DB-17.3 JARs showed that `WriteIntoDeltaEdge.write(txn)` is
actually wired to **`writeFilesAndGetExecutedPlan`** (not `writeFilesAndGetQueryExecution`
as the original scope-correction note had assumed; the stack trace in Issue 1 hit that
method via a different path). `OptimisticTransaction implements OptimisticTransactionImplEdge
extends TransactionalWriteEdge`, so overriding default trait methods works via virtual
dispatch. Exact signatures:

```scala
// com.databricks.sql.transaction.tahoe.files.TransactionalWriteEdge
def writeFilesAndGetQueryExecution(
    Dataset[_], TransactionalWriteOptions,
    isOptimize: Boolean, isLiquidClustering: Boolean,
    Seq[Constraint], isCDCWritePhase: Boolean, Option[String], Boolean
  ): (Seq[FileAction], QueryExecution)

def writeFilesAndGetExecutedPlan(              // primary entry point on DB-17.3
    Dataset[_],
    Either[Option[DeltaOptions], TransactionalWriteOptions],
    isOptimize: Boolean, isLiquidClustering: Boolean,
    Seq[Constraint], Option[String], Boolean
  ): (Seq[FileAction], SparkPlan)
```

Both overrides converge on a shared `gpuWriteFiles` helper (refactored body of the DB-14.3
`writeFiles` pipeline) which returns `(Seq[FileAction], QueryExecution)`.

### Files touched

| File | Change |
|------|--------|
| [GpuOptimisticTransaction.scala](delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/GpuOptimisticTransaction.scala) | Extract `gpuWriteFiles(inputData, writeOptions, additionalConstraints, isOptimizeOverride)` helper returning `(Seq[FileAction], QueryExecution)`. Add overrides for `writeFilesAndGetQueryExecution` and `writeFilesAndGetExecutedPlan`. 3-arg `writeFiles` (DML entry point) becomes thin delegator. Route the actual write through new `GpuDeltaFileFormatWriter.write` instead of the generic `GpuFileFormatWriter.write`. |
| [GpuDeltaFileFormatWriter.scala](delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/GpuDeltaFileFormatWriter.scala) — **NEW** | Wrap `TaskAttemptContext` in Delta's `com.databricks.sql.transaction.tahoe.files.DeltaFileFormatWriter.PartitionedTaskAttemptContextImpl` for partitioned writes. Required because DB-17.3's `DelayedCommitProtocol.parsePartitions` hard-casts to this subtype when TIMESTAMP partition columns are present. Mirrors the OSS `delta-33x` / `delta-40x` pattern with Databricks namespace. |
| [GpuWriteIntoDelta.scala](delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/GpuWriteIntoDelta.scala) | Stamp `NoRowsCopiedTag = true` on the commit (plain writes copy no rows; skip when `replaceWhere` is set). Use `DMLUtils.TaggedCommitData` + 3-arg `txn.commit(actions, op, stringTags)`, matching the existing pattern in `GpuDeleteCommand`/`GpuUpdateCommand`. Needed for Delta-log parity with CPU — CPU's `writeFilesAndGetExecutedPlan` default body sets this tag via `recordWriteFilesOperation`, which our override skipped. |

After the 2026-05-06 refactor, the `gpuWriteFiles` helper and shared write
pipeline live in `GpuOptimisticTransactionWriteBase.scala` under
`common/src/main/db-350db143-400db173/scala`; the DB-17.3
`GpuOptimisticTransaction.scala` keeps the DB-17.3-specific overrides and writer
hook.

Explicitly **not** changed:
- `GpuOptimisticTransactionBase.scala` — the 7-arg `writeFiles` override stays as a
  safety net (delegates to 3-arg; dead on the DB-17.3 write path but harmless).
- `DeltaSpark400DB173Provider.scala` — no registration change needed.
- `DatabricksDeltaProviderBase.scala`, `RapidsDeltaUtils.scala`, all DML command files —
  unchanged (DML continues to use the 3-arg `writeFiles`, now routing through the shared
  `gpuWriteFiles` helper).

### What's verified on cluster

- `test_delta_overwrite_by_expression_exec_v1` — 4/4 variants pass.
- `test_delta_write_round_trip_unmanaged[False]` — GPU write runs (`GpuRapidsDeltaWriteExec`
  in plan; ~3.5× CPU speedup); Delta log parity confirmed after the `NoRowsCopiedTag`
  fix.
- Partitioned writes (`test_delta_part_write_round_trip_unmanaged`) unblocked by the
  `PartitionedTaskAttemptContextImpl` fix.

### What was discovered during cluster validation

A distinct pre-existing **DB-17.3 GPU Delta read** bug surfaces once writes are enabled:
`GpuParquetFileFilterHandler.readAndSimpleFilterFooter` throws `FileNotFoundException`
with a relative Parquet path (just a filename, no directory). Triggered by three narrow
scenarios observed:

1. Read-back of a Delta table with multiple files under a global-sort `@ignore_order`
   (global sort samples each file via `RangePartitioner.sketch`).
2. `INSERT OVERWRITE delta.<dst> SELECT * FROM delta.<src>` — write reads source Delta
   files via GPU scan, which fails to resolve paths.
3. Any sort-fallback read-back on a binary-heavy Delta table.

This is not a regression from Issue 2: the same failures would have occurred under Issue
1's CPU-fallback-writes regime if the tests ran, because reads still go through GPU. The
tests are currently skipped in CI (jenkins test.sh). Tracked as Issue 8 below.

An unrelated CPU-side OOM also surfaces in `test_delta_append_data_exec_v1[True-True]`
(Databricks vectorized Parquet reader overflows driver heap on binary data × CDF × DV).
Environment issue — bump `DRIVER_MEMORY` or reduce `spark.sql.parquet.columnarReaderBatchSize`.

### GenerateIdentityValues status

The original scope-correction note listed `com.databricks.sql.transaction.tahoe.GenerateIdentityValues`
as a blocker. We attempted a CPU fallback in `RapidsDeltaUtils.tagForDeltaWrite` but
reverted — it would have regressed DB-14.3/12.2/etc. which currently handle identity
columns via their HWM stats tracker. On DB-17.3 this remains a potential issue for
identity-column tables; deferred to a follow-up (see Issue 9 below).

---

## Issue 8 Delivery (2026-04-24)

The relative-path `FileNotFoundException` documented under Issue 8 was fixed in the same
build-fixes commit (`3b1a1d470`) — two DB-17.3-only shim files. Root cause: DB-17.3
Delta / UC tables store bare filenames in `FilePartition.innerFiles` and rely on
`pathPrefix` for resolution; the GPU shims read `innerFiles` directly, and
`GpuFileSourceScanExec.createNonBucketedReadRDD` recreates partitions via the 2-arg
`FilePartition.getFilePartitions` factory which drops `pathPrefix`.

| File | Change |
|------|--------|
| [Spark400PlusDBShims.scala](sql-plugin/src/main/spark400db173/scala/com/nvidia/spark/rapids/shims/Spark400PlusDBShims.scala) | `getPartitionFiles(partition)` → `partition.filesWithAbsolutePaths.toSeq` (was `partition.innerFiles`). |
| [FilePartitionShims.scala](sql-plugin/src/main/spark400db173/scala/org/apache/spark/sql/execution/rapids/shims/FilePartitionShims.scala) | `getFiles(p)` → `p.filesWithAbsolutePaths` (was `p.innerFiles`). New `withPathPrefixIfNeeded(partitions, relation)` restores `pathPrefix` from `relation.location.rootPaths` (single-root case) for partitions produced by `FilePartition.getFilePartitions`. |

Other shims (`spark330db`, `spark332db`, `spark341db`, `spark350db143`, etc.) keep their
existing `innerFiles` behavior — `withPathPrefixIfNeeded` is DB-17.3 only.

---

## What's Next (Follow-up Issues — updated 2026-05-14 after native DV hardening)

| Issue | Description | Blocker for | Status |
|-------|-------------|-------------|--------|
| Issue 2 | GPU Delta writes — `writeFilesAndGetExecutedPlan` + `writeFilesAndGetQueryExecution` overrides; `GpuDeltaFileFormatWriter`; `NoRowsCopiedTag` commit tag | 3, 4, 5, 6, 7 | **Done** (2026-04-23) |
| Issue 3 | Enable GPU DELETE + UPDATE | 7 | Pending — depends on 2 |
| Issue 4 | Enable GPU MERGE INTO | 7 | Pending — depends on 2 |
| Issue 5 | Enable GPU OPTIMIZE + auto-compaction | 7 | Pending — depends on 2 |
| Issue 6 | GPU Deletion Vector reads — rewrite `GpuDeltaParquetFileFormat` | 7 | **Done** (2026-05-06; hardened 2026-05-14) — DB-17.3 native cuDF-only DV read path |
| Issue 7 | Enable Delta tests in CI — remove jenkins skip | — | Pending — depends on 2-6 (Issue 8 already done) |
| **Issue 8 (NEW)** | GPU Delta **read** path: `FileNotFoundException` with relative Parquet paths on DB-17.3 | — | **Done** (2026-04-24) — `filesWithAbsolutePaths` + `withPathPrefixIfNeeded` in `Spark400PlusDBShims` / `FilePartitionShims`. Issue 7 no longer blocks on this. |
| **Issue 9 (NEW)** | GPU `GenerateIdentityValues` expression for DB-17.3 identity-column writes | 7 | Pending — nice-to-have |

See [delta-lake-db173-github-issues.md](delta-lake-db173-github-issues.md) for full issue
descriptions and dependency graph.

---

## Test Run Status (2026-04-28)

First full integration-test pass of `delta_lake_write_test.py` on a live DB-17.3 cluster
post Issues 2 and 8. Result: **6 failed, 95 passed, 2 skipped, 22 xfailed, 4 xpassed,
44334 deselected** (`run_pyspark_from_build.sh -k delta_lake_write_test.py`).

### Test infra mitigation (committed alongside docs)

Default 2048-row generators with the wide `delta_write_gens` schema (63 columns, 12
complex types) produced ~33 MB serialized `RDDScanExec` task bodies on DB-17.3 and
exhausted the dispatcher-event-loop heap in `TaskSetManager.prepareLaunchingTask`. The
fix is a one-line constant in
[integration_tests/src/main/python/delta_lake_utils.py](integration_tests/src/main/python/delta_lake_utils.py):

```python
delta_db173_wide_schema_gen_length = 128 if is_databricks173_or_later() else 2048
```

8 wide-schema tests in [delta_lake_write_test.py](integration_tests/src/main/python/delta_lake_write_test.py)
pass `length=delta_db173_wide_schema_gen_length` to `gen_df`. On every other shim the
constant resolves to 2048 (no behavior change). On DB-17.3 the serialized task body
shrinks ~33 MB → ~2 MB and the OOM is gone.

Earlier exploratory mitigations (`delta_db173_collect_oom_mitigation_conf` to cap
`spark.sql.parquet.columnarReaderBatchSize`, and an opt-in `_db173_driver_state_cleanup`
fixture that cleared SQL caches + GC'd between tests) were targeting symptoms of the
same root cause and were dropped after the length cap proved sufficient.

### 6 failures from this run — all resolved (2026-04-28)

| Test | Original failure mode | Resolution |
|------|-----------------------|------------|
| `test_delta_write_constraint_check` | planning: arity assertion | **Fixed** via shared `GpuCheckDeltaInvariant.scala` (Group B) |
| `test_delta_write_constraint_check_fallback` | planning: arity assertion | **Fixed** via shared `GpuCheckDeltaInvariant.scala` (Group B) |
| `test_delta_overwrite_mixed_clause[PARTITION (id, p = 2)-STATIC]` | runtime: not-columnar | **xfail** under #11169 (V1 `WriteIntoDeltaCommand`, Group A) |
| `test_delta_overwrite_mixed_clause[PARTITION (p = 2, id)-STATIC]` | runtime: not-columnar | **xfail** under #11169 (V1 `WriteIntoDeltaCommand`, Group A) |
| `test_delta_overwrite_mixed_clause[PARTITION (p = 2)-STATIC]` | runtime: not-columnar | **xfail** under #11169 (V1 `WriteIntoDeltaCommand`, Group A) |
| `test_delta_write_partial_overwrite_replace_where` | runtime: not-columnar | **xfail** under #11169 (V1 `WriteIntoDeltaCommand`, Group A) |

The initial "Group A and Group B share a root cause" hypothesis was wrong — they hit
different code paths and were resolved independently.

**Group B fix — shared `GpuCheckDeltaInvariant.scala`.** The expression rule declared
`ExprChecks.unaryProject` (1 child), but DB-17.3's `CheckDeltaInvariant.children` is
`child +: columnExtractors.map(_._2)` — variable arity. Replaced with
`ExprChecks.projectOnly + paramCheck + repeatingParamCheck` (matches OSS Delta's
`delta-io/.../GpuCheckDeltaInvariant.scala`). One file changed; backward compatible
with older Databricks shims (`UnaryExpression`, single child).

**Group A folded into #11169.** DB-17.3's analyzer routes
`INSERT OVERWRITE TABLE delta.<path> PARTITION (...)` and
`df.write.option("replaceWhere", ...)` through
`com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaCommand` — a
Databricks-only `V1WriteCommand` (OSS Delta has no equivalent). That path runs
`WriteIntoDeltaCommand.run(spark, sparkPlan)` against a pre-planned plan tree, never
invoking our `txn.writeFilesAndGetExecutedPlan` / `writeFilesAndGetQueryExecution`
overrides — so `gpuWriteFiles` never runs and `RapidsDeltaWrite` never enters the
plan. Older Databricks shims resolve the same SQL through V2
(`OverwriteByExpressionExecV1`) which we already intercept on GPU. This is the same
V1 limitation #11169 already tracks for `df.write.saveAsTable(...)`. The 4 tests are
xfailed for `is_databricks173_or_later()` referencing #11169. Captured plan from
the original `test_delta_overwrite_mixed_clause` run (kept for history):

```
Execute WriteIntoDeltaCommand
+- WriteFiles
   +- GpuColumnarToRow
      +- GpuSort [id ASC, p ASC]
         +- GpuRowToColumnar
            +- DeltaInvariantChecker [checkdeltainvariant((p <=> 2), ...)]    ← CPU
               +- GpuColumnarToRow
                  +- GpuProject [id, data, 2 AS p]
                     ...
```

### Updated follow-up table

| Issue | Description | Blocker for | Status |
|-------|-------------|-------------|--------|
| Issue 10 | DB-17.3 `GpuCheckDeltaInvariant` arity meta (Group B) + V1 `WriteIntoDeltaCommand` (Group A) | 7 | **Done** (2026-04-28) — Group B fixed in shared `GpuCheckDeltaInvariant.scala`; Group A subsumed under [#11169](https://github.com/NVIDIA/spark-rapids/issues/11169) |
