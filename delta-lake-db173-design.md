# Design: Delta Lake GPU Support for Databricks 17.3 (DB-17.3)

**Author:** NVIDIA RAPIDS Accelerator Team
**Date:** 2026-03-19
**Scope correction:** 2026-04-23 — see §0.A below.
**Issue 2 delivery:** 2026-04-23 — see §0.B below.
**PR slim-down:** 2026-04-24 — see §0.C below.
**Issue 8 resolution:** 2026-04-24 — see §0.D below.
**Issue 10 resolution + #11169 xfails:** 2026-04-28 — see §0.E below.
**PR review hardening:** 2026-04-30 — see §0.F below.
**Issue 6 DV reads:** 2026-05-02 — see §0.G below.
**Native DV follow-up:** 2026-05-05 — see §0.H below.
**Native-only DV cleanup:** 2026-05-06 — see §0.I below.
**Shared transaction refactor:** 2026-05-06 — see §0.J below.
**DELETE/UPDATE branch:** 2026-05-12 — see §0.K below.
**DELETE/UPDATE PR-ready review:** 2026-05-13 — see §0.K below.
**Status:** Issue 1 + Issue 2 + Issue 6 final DBR-17.3 native cuDF-only DV reads,
Issue 8 + Issue 10, and Issue 3 DELETE/UPDATE are delivered locally. Issue 3 is
PR-ready at `c8a0f87b86c16ce02097ae3501c328f584bb720e`; Issues 4/5/7/9 pending.
`delta_lake_write_test.py`, targeted `delta_lake_test.py`, and targeted
`delta_lake_delete_test.py` / `delta_lake_update_test.py` are green on DB-17.3
for the validated local runs.
DB-14.3/DB-17.3 shared transaction, catalog, data-source, and shim code now
lives under `delta-lake/common/src/main/db-350db143-400db173/scala`.
**Tracking Issue:** https://github.com/NVIDIA/spark-rapids/issues/14015

---

## 0. Scope Correction, Issue 2 Delivery, and PR Slim-down — READ FIRST

### 0.A Original Scope Correction (2026-04-23, before Issue 2 implementation)

The original design in §4 of this document assumed that intercepting the V2
`AppendDataExecV1` / `OverwriteByExpressionExecV1` exec nodes (as DB-14.3 does) would be
sufficient for DB-17.3 Delta writes, with our existing
`GpuOptimisticTransactionBase.writeFiles` override doing the GPU file-write work. Cluster
validation on 2026-04-23 disproved that assumption:

On DB-17.3, `WriteIntoDeltaEdge.write(txn)` does **not** call `txn.writeFiles(...)`. Our
`GpuOptimisticTransactionBase.writeFiles` override is dead code for Delta writes on
DB-17.3. Intercepting the inner `WriteIntoDeltaCommand` via a `DataWritingCommandMeta`
causes `DELTA_ACTIVE_TRANSACTION_ALREADY_SET` because the outer transaction is still
active.

Issue 1 was re-scoped to **module scaffolding + CPU fallback only**. Issue 2 was
introduced to do the actual GPU write work.

### 0.B Issue 2 Delivery (2026-04-23, after implementation + cluster validation)

`javap` of the live DB-17.3 JARs showed that `WriteIntoDeltaEdge.write(txn)` is actually
wired to **`txn.writeFilesAndGetExecutedPlan(...)`** (via `ClusteredWriter.writeFilesWithoutClustering`
→ `WriteIntoDeltaEdge.writeFilesAndGetMaterializationPlans`). The scope-correction note
in §0.A referred to `writeFilesAndGetQueryExecution`; that method also exists on
`TransactionalWriteEdge` and may be reached via CTAS / RTAS internal paths. We override
both for safety.

**Confirmed signatures** (via `javap -p -classpath "/databricks/jars/*"
com.databricks.sql.transaction.tahoe.files.TransactionalWriteEdge`):

```scala
def writeFilesAndGetQueryExecution(
    Dataset[_], TransactionalWriteOptions,
    isOptimize: Boolean, isLiquidClustering: Boolean,
    Seq[Constraint], isCDCWritePhase: Boolean,
    Option[String], Boolean
  ): (Seq[FileAction], QueryExecution)

def writeFilesAndGetExecutedPlan(                // primary entry point on DB-17.3
    Dataset[_],
    Either[Option[DeltaOptions], TransactionalWriteOptions],
    isOptimize: Boolean, isLiquidClustering: Boolean,
    Seq[Constraint], Option[String], Boolean
  ): (Seq[FileAction], SparkPlan)

def writeFiles(Dataset[_], Option[DeltaOptions], Seq[Constraint]): Seq[FileAction]
    // DML entry point — still used by GpuDeleteCommand/Update/Merge/Optimize
```

Class hierarchy confirmed: `OptimisticTransaction implements OptimisticTransactionImplEdge
extends TransactionalWriteEdge`. Overriding the default trait methods dispatches
correctly.

**Original implementation summary** (three files under
`delta-lake/delta-spark400db173/`; shared pieces were later refactored in §0.J):

1. `GpuOptimisticTransaction.scala` — extract `gpuWriteFiles(..., isOptimizeOverride):
   (Seq[FileAction], QueryExecution)` shared helper (refactored body of the DB-14.3
   `writeFiles` GPU pipeline). Add overrides for both new entry points; keep 3-arg
   `writeFiles` as a thin delegator for DML. Route the actual write through the new
   `GpuDeltaFileFormatWriter.write` (see below) instead of the generic
   `GpuFileFormatWriter.write`.
2. `GpuDeltaFileFormatWriter.scala` (new) — overrides `createTaskAttemptContext` to wrap
   in `com.databricks.sql.transaction.tahoe.files.DeltaFileFormatWriter.PartitionedTaskAttemptContextImpl`
   when partition columns are present. Required because DB-17.3's
   `DelayedCommitProtocol.parsePartitions` hard-casts to this subtype for timestamp
   partitions. Mirrors the OSS `delta-33x` / `delta-40x` pattern.
3. `GpuWriteIntoDelta.scala` — stamp `NoRowsCopiedTag = true` on the commit (for
   non-`replaceWhere` writes) via `DMLUtils.TaggedCommitData(actions).withTag(...)` →
   3-arg `txn.commit(actions, op, stringTags)`. Needed for Delta-log parity with CPU —
   CPU's default `writeFilesAndGetExecutedPlan` body sets this tag as a side effect
   (`recordWriteFilesOperation`), and our override skips that body.

**Consequences for the rest of this document:**

- §4.1.3 ("GPU Command Implementations") — the GPU command classes now execute on the
  GPU-write path for plain writes. Issue 3 DELETE/UPDATE is PR-ready separately;
  MERGE/OPTIMIZE still fall back to CPU (Issues 4/5).
- §7.1.6 ("writeFiles() signature completely changed") — the 7-arg `writeFiles`
  override in `GpuOptimisticTransactionBase` is retained as a safety net but is dead on
  the DB-17.3 write path (DB-17.3 routes through `writeFilesAndGetExecutedPlan` instead).
  Both overrides ultimately converge on the shared `gpuWriteFiles` helper.
- §7.1.15 (and §4) still describe the intent; the concrete solution is summarized here
  and in [delta-lake-db173-checkpoint.md](delta-lake-db173-checkpoint.md#issue-2-delivery-2026-04-23).

**GenerateIdentityValues** — the expression-based CPU fallback was attempted but
reverted during Issue 2 implementation because it would have regressed DB-14.3 (whose
identity-column tests currently pass). Filed as Issue 9 (see
[delta-lake-db173-github-issues.md](delta-lake-db173-github-issues.md)).

**New read-path issue surfaced** — Issue 8: GPU Delta reads on DB-17.3 hit
`FileNotFoundException` with relative Parquet paths in narrow scenarios (multi-file
global-sort read-back, `INSERT OVERWRITE ... SELECT FROM delta.<src>`). This is not a
regression from Issue 2 — reads always went through GPU; the tests that expose it were
skipped in CI. Tracked as Issue 8.

The rest of this document is retained as the long-term architecture narrative. Where a
section conflicts with §0 / §7.1.15, the correction takes precedence.

### 0.C PR Slim-down (2026-04-24)

To keep the Issue 1 PR reviewable, the DML/OPTIMIZE command implementations carried in
the initial draft of the build-fixes commit were removed or reduced to compile-only
stubs. The GPU write path (Issue 2) is unaffected. Three kinds of change:

- **Deleted** (no shared-code reference forces the class to exist):
  - `GpuMergeIntoCommand.scala` (1,206 LOC) → returns in follow-up issue #14598
  - `GpuOptimizeExecutor.scala` (420 LOC) → returns in follow-up issue #14599
  - `GpuDoAutoCompaction.scala` (48 LOC) → returns in follow-up issue #14599;
    `registerPostCommitHook(GpuDoAutoCompaction)` also removed from
    `GpuOptimisticTransaction.gpuWriteFiles`

- **Stubbed** (the shared common-code `DeleteCommandMeta.convertToGpu` /
  `UpdateCommandMeta.convertToGpu` directly construct these types — stub-compile is the
  minimum viable form):
  - `GpuDeleteCommand.scala` 382 → ~30 LOC (case class with matching constructor,
    `run()` throws; unreachable because `tagForGpu` already CPU-falls back)
  - `GpuUpdateCommand.scala` 287 → ~35 LOC (same pattern)

- **Trimmed**: `MergeIntoCommandMetaShim.convertToGpu` now throws directly; the
  `GpuMergeIntoCommand` / `GpuDeltaLog` imports are removed.

**Consequences for the design narrative below:**

- §4.1.3 ("GPU Command Implementations") — the DML / OPTIMIZE / auto-compaction command
  files listed are deferred to follow-up issues; only the write-path command files live
  in the module after the slim-down.
- §7.x — no API-level design change; only the set of GPU-accelerated operations shrinks
  to "writes only" for the initial PR.

Net commit size: ~4,597 → ~2,289 lines.

### 0.D Issue 8 Resolution — Delta read absolute-path fix (2026-04-24)

The `FileNotFoundException` on relative Parquet paths described under Issue 8 (and §0.B
above) is **resolved** in commit `3b1a1d470` (the Issue 1 build-fixes commit, which also
carries this read-path fix).

**Root cause.** On DB-17.3, Delta / UC-managed tables store **bare filenames** in
`FilePartition.innerFiles` and rely on `FilePartition.pathPrefix` for absolute path
resolution. Two GPU code paths broke this contract:

1. The DB-17.3 shim methods that hand `PartitionedFile`s to the GPU scan
   (`Spark400PlusDBShims.getPartitionFiles`, `FilePartitionShims.getFiles`) were
   reading `partition.innerFiles` directly — i.e., the unresolved filenames.
2. `GpuFileSourceScanExec.createNonBucketedReadRDD` recreates partitions by calling
   `FilePartition.getFilePartitions(...)`. The 2-arg factory used there does not carry
   `pathPrefix` through, so even if downstream code asked for `filesWithAbsolutePaths`
   it would not resolve correctly.

**Fix.** Two DB-17.3-only shim files:

- [Spark400PlusDBShims.scala](sql-plugin/src/main/spark400db173/scala/com/nvidia/spark/rapids/shims/Spark400PlusDBShims.scala):
  `getPartitionFiles` returns `partition.filesWithAbsolutePaths.toSeq`.
- [FilePartitionShims.scala](sql-plugin/src/main/spark400db173/scala/org/apache/spark/sql/execution/rapids/shims/FilePartitionShims.scala):
  `getFiles` returns `p.filesWithAbsolutePaths`, and a new `withPathPrefixIfNeeded`
  helper restores `pathPrefix` from `relation.location.rootPaths` (single-root case)
  for the partitions produced by `getFilePartitions`. The helper is a no-op on all
  other shims.

**Impact on the issue plan.** Issue 7 (CI enablement) loses its Issue 8 dependency — it
now only blocks on Issues 3–6.

### 0.E Issue 10 resolution + #11169 xfails (2026-04-28)

The 6 `delta_lake_write_test.py` failures filed as Issue 10 split into two groups
with separate root causes; the initial "shared root cause" hypothesis (§7.1.13 era)
turned out to be wrong.

**Group B (2 tests) — fixed.** `test_delta_write_constraint_check{,_fallback}`. The
shared `GpuCheckDeltaInvariant` expression rule declared `ExprChecks.unaryProject`
(1 child), but DB-17.3's `CheckDeltaInvariant.children` is
`child +: columnExtractors.map(_._2)` — variable arity. Replaced with
`ExprChecks.projectOnly + paramCheck + repeatingParamCheck` in
[delta-lake/common/src/main/databricks/scala/com/databricks/sql/transaction/tahoe/rapids/GpuCheckDeltaInvariant.scala](delta-lake/common/src/main/databricks/scala/com/databricks/sql/transaction/tahoe/rapids/GpuCheckDeltaInvariant.scala);
matches the OSS Delta rule at
[delta-lake/common/src/main/delta-io/.../GpuCheckDeltaInvariant.scala](delta-lake/common/src/main/delta-io/scala/org/apache/spark/sql/delta/rapids/GpuCheckDeltaInvariant.scala).
Backward compatible with older Databricks shims (`UnaryExpression`, single child).

**Group A (4 tests) — folded into #11169.**
`test_delta_overwrite_mixed_clause` × 3 STATIC variants and
`test_delta_write_partial_overwrite_replace_where`. DB-17.3's analyzer routes both
SQL forms through `com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaCommand`
(a `V1WriteCommand` — Databricks-only; OSS Delta has no equivalent class). That path
runs `WriteIntoDeltaCommand.run(spark, sparkPlan)` against a pre-planned plan tree,
never invoking our `txn.writeFilesAndGetExecutedPlan` / `writeFilesAndGetQueryExecution`
overrides — so `gpuWriteFiles` never runs and `RapidsDeltaWrite` never enters the
plan. Older Databricks shims resolve the same SQL through V2
(`OverwriteByExpressionExecV1`), which our shared `DatabricksDeltaProviderBase`
intercepts to `GpuOverwriteByExpressionExecV1`. This is the same V1 limitation
upstream [#11169](https://github.com/NVIDIA/spark-rapids/issues/11169) already tracks
for `df.write.saveAsTable(...)`. The 4 tests are xfailed for
`is_databricks173_or_later()` referencing #11169.

**Impact on the issue plan.** Issue 7 (CI enablement) no longer blocks on Issue 10.
The proper code-level fix for the V1 `WriteIntoDeltaCommand` GPU acceleration —
registering a `DataWritingCommandMeta[WriteIntoDeltaCommand]` in
`DeltaSpark400DB173Provider` that routes the V1 write through `gpuWriteFiles`, with
care around the nested-transaction concern from §0.A — rolls under #11169 as a
Group-A follow-up.

### 0.F PR Review Hardening (2026-04-30)

Static review against the live DB-17.3 JARs found two correctness risks in the first PR
surface area. Both are handled conservatively:

- `DeltaSpark400DB173Provider.tagForGpu` now falls back for all DB-17.3 CTAS/RTAS.
  The old GPU `GpuCreateDeltaTableCommand` does not faithfully implement newer CPU
  create-table semantics: row filters, column masks, liquid clustering, auto TTL,
  catalog-owned tables through explicit or default configuration, coordinated-commit
  table properties, coordinated-commit default table properties, and deletion-vector
  enablement through either explicit table properties or default table-property conf.
  Feature-specific TableSpec/property checks remain as defensive tags. Explicit Delta
  property-key checks are case-insensitive, and invalid DV boolean values also fall back
  so Delta's CPU validation reports the error. This prevents GPU table creation from
  silently dropping DB-17.3 create-table semantics. Auto-enable DV triggers are still
  tracked with the broader DB-17.3 create-command port under #14601.
- Shared `GpuIdentityColumn` no longer uses `col(name)`. It now builds the aggregation
  column with `DFUDFShims.exprToColumn(UnresolvedAttribute.quoted(name))`, preserving
  literal identity-column names such as `a.b` while remaining Spark-4-compatible.

### 0.G Issue 6 DV Read Delivery (historical 2026-05-02 checkpoint)

Issue 6 delivered DB-17.3 GPU deletion-vector reads through the
V1/materialized path. The implementation is intentionally DB-17.3-local because
the shared OSS Delta 3.3/4.0 DV sources import `org.apache.spark.sql.delta.*`,
while DB-17.3 exposes the Databricks namespace.

Implemented pieces:
- `GpuDeltaParquetFileFormat.scala` now mirrors the DB-17.3 CPU format flags
  and extends the DV-aware local reader.
- `GpuDeltaParquetFileFormatDV.scala` materializes the DB edge skip-row column,
  uses per-file row-index filters from `PartitionedFile`, and disables
  split/pushdown optimizations for DV-bearing reads.
- `RapidsDeletionVectors.scala` and `RapidsRowIndexFilters.scala` port the
  materialized bitmap and row-index filter helpers with Databricks imports.
- `DeltaSpark400DB173Provider.scala` wires DB-17.3 DV scan detection,
  metadata pruning, provider format recognition, and keeps
  `canPushDVPredicateDownToScan=false` for this initial V1 path.

Validation completed:
- DB-17.3 build passed with
  `SKIP_DEP_INSTALL=1 WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`.
- Focused post-cleanup DV run passed:
  `57 passed, 35 warnings in 182.72s`.
- Full `delta_lake_test.py` and `delta_lake_delete_test.py` runs were green
  before final cleanup; the focused run was repeated after cleanup.

The native-reader items in the original remaining-work list were completed by
the 2026-05-05 follow-up in §0.H, and the intermediate materialized DBR GPU
path was removed by the 2026-05-06 native-only cleanup in §0.I. CDC + DV reads
and Issue 7 CI enablement remain deferred.

### 0.H Native cuDF DV Follow-up Delivery (2026-05-05)

The native DBR-17.3 DV follow-up now matches the OSS Delta-4.0 feature sequence
for the supported read path:

1. Narrow DV predicate pushdown for correctness.
2. PERFILE native cuDF DV scanning.
3. MULTITHREADED native cuDF DV scanning.
4. COALESCING native cuDF DV scanning.

Implemented pieces:
- `DeltaSpark400DB173Provider.scala` gates native DV pushdown on DBR
  metadata-row-index mode plus
  `spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled`.
- `GpuDeltaParquetFileFormatNativeDV.scala` implements the DBR-17.3 native cuDF
  DV scan path for PERFILE, MULTITHREADED, and COALESCING readers.
- `RapidsDeletionVectors.scala` recovers DV descriptors/providers from
  `PartitionedFile`, `RowIndexFilterProvider`, and `TahoeFileIndex`.
- `DeltaBitmapUtils.java` converts DBR `SerializedBitmap` data to cuDF-compatible
  standard portable roaring bitmap bytes.

Validation completed:
- DBR-17.3 dist build passed with
  `mvn -B -f scala2.13/pom.xml -Ddatabricks -Dbuildver=400db173 package -pl dist -am -DskipTests -Dmaven.scaladoc.skip`.
- Focused native PERFILE footer test passed.
- Native footer matrix passed for
  `test_delta_deletion_vector_native_footer_multi_row_group` and
  `test_delta_deletion_vector_native_footer_multi_row_group_count_star`.
- Targeted delete/read and scan-split tests passed:
  `test_delta_deletion_vector`, `test_delta_deletion_vector_read`, and
  `test_delta_scan_split_with_DV_enabled_with_DVs`.

DBR-specific limits:
- Predicate pushdown recognizes the current DBR skip-row predicate shapes only:
  `__delta_internal_is_row_deleted` and
  `_databricks_internal_edge_computed_column_skip_row`.
- Native DV remains gated by metadata-row-index mode and the RAPIDS DV pushdown
  config.
- Only `RowIndexFilterType.IF_CONTAINED` is supported.
- CDC + DV reads, Delta file-in-scan metadata columns, and nullable row-tracking
  metadata fields still fall back or remain unsupported on GPU.

### 0.I Final Native-only DV Cleanup and Squash (2026-05-06)

Team review narrowed the DBR-17.3 DV GPU scope to the native cuDF path only. The
materialized/non-cuDF DV GPU path still exists in OSS Delta modules where it is
supported, but DBR-17.3 should not keep a separate materialized Scala/GPU DV
fallback. If the native DBR gates are not satisfied for a DV scan, the scan
falls back to CPU with a DBR-specific message.

Final squashed commit:

```text
844678a6ebc9cdc7ca02852b0ccd2999ec8e4c46 [databricks] Add native-only Delta DV reads for DBR 17.3
```

The final commit shape is seven files: `DeltaBitmapUtils.java`,
`DeltaSpark400DB173Provider.scala`, `GpuDeltaParquetFileFormat.scala`,
`GpuDeltaParquetFileFormatNativeDV.scala`, `RapidsDeletionVectors.scala`,
`delta_lake_delete_test.py`, and `delta_lake_test.py`. The intermediate DBR
materialized GPU files `GpuDeltaParquetFileFormatDV.scala` and
`RapidsRowIndexFilters.scala` were removed before the squash.

Final gate behavior:
- `DeltaSpark400DB173Provider.canPushDVPredicateDownToScan(conf)` is true only when
  DBR `spark.databricks.delta.deletionVectors.useMetadataRowIndex` and RAPIDS
  `spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled` are both true.
- `useMetadataRowIndex` is effectively true by default through DBR
  `DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX`.
- `getReadFileFormat` selects `GpuDeltaParquetFileFormatNativeDV` whenever the native
  gates are true. When the gates are false, non-DV scans can still use the plain GPU
  Delta format, while DV scans are tagged for CPU fallback before conversion.
- Tagging detects DBR DV scans through CPU Delta format DV flags, requested skip-row
  columns, and the DBR DV plan shape, so disabled-gate DV scans do not accidentally run
  through the plain GPU format.

Final validation:
- `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh` passed.
- Targeted DBR DV read slice 1 passed: 80 selected, 80 passed.
- Targeted DBR DV read slice 2 passed: 120 selected, 108 passed, 12 expected Databricks
  skips.

### 0.J Shared DB-14.3/DB-17.3 Transaction Refactor (2026-05-06)

Commit `d8b1e4309` moved the code shared by `delta-spark350db143` and
`delta-spark400db173` into a dedicated common source root:

```text
delta-lake/common/src/main/db-350db143-400db173/scala
```

Both module poms now add that root in addition to
`common/src/main/scala` and `common/src/main/databricks/scala`, and the
`scala2.13/` mirror poms were regenerated.

The refactor shared these classes/objects:
- `GpuCreateDeltaTableCommandBase`
- `GpuDeltaCatalog`
- `GpuDeltaCatalogCommon`
- `GpuOptimisticTransactionBaseCommon`
- `GpuOptimisticTransactionWriteBase`
- `GpuWriteIntoDeltaBase`
- `GpuDeltaDataSource`
- `DeltaLogShim`, `InvariantViolationExceptionShim`, `MetadataShims`, and `ShimDeltaUDF`

The DB-17.3 module still owns the version-specific adapters:
`GpuCreateDeltaTableCommand`, `GpuOptimisticTransactionBase`,
`GpuOptimisticTransaction`, `GpuWriteIntoDelta`, the local DML adapters/shims,
`GpuDeltaFileFormatWriter`, provider/probe files, and native DV read files.

### 0.K DELETE/UPDATE Follow-on Branch (2026-05-12/13)

Issue 3 is implemented locally on `delta_db173_DELETE_UPDATE_support`:

```text
c8a0f87b86c16ce02097ae3501c328f584bb720e Support Update+DELETE for DB-17.3
```

The branch was intentionally split from the native-DV read PR:
- `25d3e04c9` was dropped from the DELETE/UPDATE branch history.
- Native DV read files and helpers remain in the DV PR.
- The DV metric-scan fallback was moved to the DV PR and squashed into
  `844678a6e [databricks] Add native-only Delta DV reads for DBR 17.3`.

DELETE/UPDATE design shape:
- `GpuDeleteCommandBase` and `GpuUpdateCommandBase` live in
  `common/src/main/db-350db143-400db173/scala` so DB-14.3/DB-17.3 can share the
  command rewrite logic.
- DB-17.3 `GpuDeleteCommand` and `GpuUpdateCommand` are thin adapters over those
  shared bases.
- `DeleteCommandMetaShim` and `UpdateCommandMetaShim` allow GPU execution except
  when persistent deletion-vector writes would be used; those paths remain CPU
  fallback.
- DELETE and UPDATE preserve Delta row-tracking columns during rewrites.
- Whole-table and metadata-only DELETE report metadata-derived affected row counts
  when `spark.databricks.delta.dmlMetricsFromMetadata.enabled` is enabled.
- Runtime DV-write guards backstop the plan-time CPU fallback for persistent
  deletion-vector write paths.
- MERGE and OPTIMIZE remain separate follow-up issues.

Validation:
- `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh` passed.
- Focused DBR-17.3 DELETE/UPDATE regressions passed: 4 tests, 0 failures.
- Focused persistent-DV write fallback coverage passed: 3 tests, 0 failures.
- Earlier broad validation: full `delta_lake_update_test.py` passed; full
  `delta_lake_delete_test.py` had failures only in native DV read cases owned by
  the separate DV PR.

---

## 1. Overview

This document describes the design for adding Delta Lake GPU acceleration support for
Databricks Runtime 17.3 (DBR 17.3, Spark version `4.0.0-databricks-173`) in the RAPIDS
Accelerator for Apache Spark.

At the start of this effort, the `release400db173` Maven profile used
`delta-lake/delta-stub`, which provided a no-op `NoDeltaProvider`. The profile now
builds `delta-lake/delta-spark400db173`; writes and native cuDF DV reads have GPU
coverage, DELETE/UPDATE have a PR-ready follow-on branch, and MERGE, OPTIMIZE,
CDC + DV reads, and CI enablement remain follow-up areas.

---

## 2. Motivation

- DBR 17.3 is a current Databricks LTS-track runtime. Customers running workloads on DBR 17.3
  expect GPU acceleration for Delta Lake operations, consistent with prior supported runtimes
  (DBR 14.3, DBR 13.3, etc.).
- The GPU plugin already fully supports the DBR 17.3 SQL layer (`sql-plugin` shims are
  complete). Delta Lake support is the remaining gap.
- DBR 17.3 is based on Spark 4.0, which is also supported in the OSS `delta-40x` module.
  Many Spark 4.0 API adaptations are already present in `delta-spark350db143` (DBR 14.3),
  simplifying the port.

---

## 3. Background

### 3.1 Delta Lake Module Architecture

The RAPIDS plugin organizes Delta Lake support into version-specific Maven modules under
`delta-lake/`. Each module provides GPU implementations of Delta write/DML operations for
a specific Delta version or runtime pairing.

```
delta-lake/
├── common/                          # Shared source directories (injected via build-helper)
│   └── src/main/
│       ├── scala/                   # Universal: DeltaProviderImplBase, RapidsDeltaWrite, UDFs
│       ├── databricks/scala/        # Databricks-shared: DatabricksDeltaProviderBase,
│                                    #   DeleteCommandMeta, GpuDeltaLog, GpuDeltaCatalogBase, ...
│       ├── db-350db143-400db173/scala
│                                    # Shared DB-14.3/DB-17.3 transaction/catalog/data-source code
│       └── delta-33x-40x/scala      # Shared for OSS Delta 3.3+/4.0+ DV code
├── delta-33x/                       # OSS Delta 3.3.x (Spark 3.5.x)
├── delta-40x/                       # OSS Delta 4.0.x (Spark 4.0.x)
├── delta-spark330db/                # Databricks 11.3
├── delta-spark332db/                # Databricks 12.2
├── delta-spark341db/                # Databricks 13.3
├── delta-spark350db143/             # Databricks 14.3  ← primary reference
├── delta-spark400db173/             # Databricks 17.3
└── delta-stub/                      # No-op fallback for unsupported Delta pairings
```

**Databricks modules do not declare an explicit Delta Lake dependency.** Delta Lake is
bundled with the Databricks Runtime and provided at runtime via the
`com.databricks.sql.transaction.tahoe` package namespace.

### 3.2 Module Entry Point

The plugin discovers Delta providers via the Java SPI in `ShimLoader`. For each supported
Databricks runtime, the corresponding `delta-spark{version}db` module provides:

```
DeltaProbeImpl.getDeltaProvider()  →  DeltaSpark{X}Provider
    extends DatabricksDeltaProviderBase   (common/databricks/scala)
        extends DeltaProviderImplBase     (common/scala)
```

### 3.3 Shared Common Code

All Databricks Delta modules share 17 files from `common/src/main/databricks/scala/`:

| File | Purpose |
|------|---------|
| `DatabricksDeltaProviderBase.scala` | GPU rule registration (Delete/Update/Merge/CreateTable commands) |
| `GpuDeltaLog.scala` | Wrapper around Databricks `DeltaLog` for GPU transaction management |
| `GpuDeltaCatalogBase.scala` | Base class for `GpuDeltaCatalog` |
| `GpuDeltaInvariantCheckerExec.scala` | GPU constraint checker plan node |
| `GpuCheckDeltaInvariant.scala` | Invariant check GPU/CPU dispatch |
| `DeleteCommandMeta.scala` | Spark plan meta for `DeleteCommand` / `DeleteCommandEdge` |
| `UpdateCommandMeta.scala` | Spark plan meta for `UpdateCommand` / `UpdateCommandEdge` |
| `MergeIntoCommandMeta.scala` | Spark plan meta for `MergeIntoCommand` / `MergeIntoCommandEdge` |
| `DeltaWriteUtils.scala` | GPU/CPU conversion helpers |
| `GpuDeltaParquetFileFormatBase.scala` | Base class for Parquet file format with DV support |
| `RapidsDeltaSQLConf.scala` | RAPIDS-specific Delta SQL configs |
| `RapidsDeltaUtils.scala` | Utilities for Delta GPU acceleration |
| `GpuIdentityColumn.scala` | Identity column high-water mark tracking |
| `GpuOptimizeWriteExchangeExec.scala` | Optimize-write exchange node |
| `OptimizeWriteExchangeExec.scala` | CPU fallback for optimize-write exchange |
| `StatsExprShim.scala` | Statistics expression shim |
| `shims/package-shims.scala` | Shim package definition |

DB-14.3 and DB-17.3 additionally share 11 files from
`common/src/main/db-350db143-400db173/scala/`:

| File | Purpose |
|------|---------|
| `GpuCreateDeltaTableCommandBase.scala` | Shared create-table command body |
| `GpuDeltaCatalog.scala` | Shared catalog implementation |
| `GpuDeltaCatalogCommon.scala` | Shared catalog helpers |
| `GpuOptimisticTransactionBaseCommon.scala` | Shared transaction conversion/invariant helpers |
| `GpuOptimisticTransactionWriteBase.scala` | Shared GPU write pipeline |
| `GpuWriteIntoDeltaBase.scala` | Shared `WriteIntoDeltaEdge` transaction wrapper |
| `GpuDeltaDataSource.scala` | Shared V1 data-source write wrapper |
| `DeltaLogShim.scala` | Shared DeltaLog accessors |
| `InvariantViolationExceptionShim.scala` | Shared invariant exception factory |
| `MetadataShims.scala` | Shared Delta statistics metadata accessors |
| `ShimDeltaUDF.scala` | Shared Delta UDF shim |

### 3.4 Scala Version: DB-17.3 is Scala 2.13 Only

**This is a critical architectural difference from all prior Databricks Delta modules.**

All existing Databricks Delta modules (330db, 332db, 341db, 350db143) are built for
**Scala 2.12** (and also optionally Scala 2.13 via the `scala2.13/` mirror). Their Maven
profiles live in the main `pom.xml` and are active by default.

The `release400db173` profile, however, lives **inside the `<!-- #if scala-2.13 --><!--`
... `--><!-- #endif scala-2.13 -->` block** in the root `pom.xml`. This means:

- In the main `pom.xml`, the `release400db173` profile is XML-commented out and **never
  active**.
- In `scala2.13/pom.xml` (generated by `make-scala-version-build-files.sh`), it is
  uncommented and active.
- **All `400db173` builds must use `-f scala2.13`** (e.g.,
  `mvn -f scala2.13 -Dbuildver=400db173 ...`).
- The module artifact suffix is `_2.13` (not `_2.12`).
- No Scala 2.12/2.13 cross-compilation concerns: the code is **Scala 2.13 only**.

This follows the same pattern as `delta-40x` (OSS Spark 4.0), which is also Scala 2.13 only
and lives inside the same `#if scala-2.13` block.

---

## 4. Technical Design

### 4.1 New Module: `delta-lake/delta-spark400db173`

Create a new Maven module following the same structure as `delta-spark350db143` (DBR 14.3).
After the 2026-05-06 refactor, the DB-17.3 module contains 16 local Scala files,
one local Java helper, a `pom.xml`, and the shared DB-14.3/DB-17.3 source root.

#### 4.1.1 `pom.xml` (primary template)

Path: `delta-lake/delta-spark400db173/pom.xml`

Following project convention, the primary `pom.xml` uses `_2.12` suffixes (as a template).
The `scala2.13/` mirror (generated by `make-scala-version-build-files.sh`) automatically
produces the `_2.13` artifact that is actually built and deployed.

```xml
<artifactId>rapids-4-spark-delta-spark400db173_2.12</artifactId>
<name>RAPIDS Accelerator for Apache Spark Databricks 17.3 Delta Lake Support</name>
<parent>
  <artifactId>rapids-4-spark-shim-deps-parent_2.12</artifactId>
  <relativePath>../../shim-deps/pom.xml</relativePath>
</parent>
<dependencies>
  <!-- RoaringBitmap for deletion vector bitsets -->
  <dependency>org.roaringbitmap:RoaringBitmap</dependency>
  <!-- SQL plugin provided at compile-time -->
  <dependency>com.nvidia:rapids-4-spark-sql_${scala.binary.version}
              :${spark.version.classifier}:provided</dependency>
</dependencies>
<build>
  <!-- Inject common Databricks sources -->
  <sources>
    delta-lake/common/src/main/scala
    delta-lake/common/src/main/databricks/scala
    delta-lake/common/src/main/db-350db143-400db173/scala
  </sources>
</build>
```

**After creating this file**, run:
```bash
./build/make-scala-version-build-files.sh 2.13
```
This generates `scala2.13/delta-lake/delta-spark400db173/pom.xml` with:
- `<artifactId>rapids-4-spark-delta-spark400db173_2.13</artifactId>`
- `<parent>rapids-4-spark-shim-deps-parent_2.13</parent>`

The generated `_2.13` pom is what is actually compiled and installed.

#### 4.1.2 Provider and Probe

**`com/nvidia/spark/rapids/delta/DeltaProbe.scala`**

```scala
class DeltaProbeImpl extends DeltaProbe {
  override def getDeltaProvider: DeltaProvider = DeltaSpark400DB173Provider
}
```

**`com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala`**

Extends `DatabricksDeltaProviderBase` (from common code). Overrides:
- `toGpuWrite()` — constructs `GpuWriteIntoDelta(gpuDeltaLog, WriteIntoDeltaEdge(...))`
- `tagForGpu(AtomicCreateTableAsSelectExec / AtomicReplaceTableAsSelectExec)` —
  conservatively keeps DB-17.3 CTAS/RTAS on CPU for this PR
- `convertToGpu(AtomicCreateTableAsSelectExec / AtomicReplaceTableAsSelectExec)` —
  remains implemented for the shared provider interface but should be unreachable while
  DB-17.3 CTAS/RTAS are tagged as CPU fallback

#### 4.1.3 GPU Command Implementations

Version-specific command adapters reside in
`delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/`.
Shared DB-14.3/DB-17.3 transaction/catalog/data-source bodies reside in
`common/src/main/db-350db143-400db173/scala/`.

The Delete/Update/Merge/Optimize descriptions below are the long-term GPU DML
design. In the current DB-17.3 module, `GpuDeleteCommand` and
`GpuUpdateCommand` are compile-only CPU-fallback stubs, while
`GpuMergeIntoCommand`, `GpuOptimizeExecutor`, and `GpuDoAutoCompaction` remain
deferred to follow-up issues.

**`GpuOptimisticTransactionBase.scala`**

DB-17.3 adapter over `GpuOptimisticTransactionBaseCommon`. Key design:
- Extends `OptimisticTransaction(deltaLog, Option.empty[CatalogTable], snapshot)` — the
  `CatalogTable` parameter was added to the Databricks `OptimisticTransaction` constructor
  in DBR 14.3 and must be carried forward.
- Provides GPU/CPU plan conversion helpers (`convertToGpu`, `convertToCpu`)
- Handles `GpuCheckDeltaInvariant` constraint checking
- Manages `GpuOptimizeWriteExchangeExec` for optimize-write

**`GpuOptimisticTransaction.scala`**

Extends `GpuOptimisticTransactionWriteBase`. Keeps the DB-17.3-specific writer
hook and overrides `writeFiles`, `writeFilesAndGetQueryExecution`, and
`writeFilesAndGetExecutedPlan`:
1. Partitions CDC data
2. Normalizes data schema
3. Wraps in `RapidsDeltaWrite` plan node (to prevent AQE transitions)
4. Collects per-file Delta statistics via `GpuStatisticsCollection`
5. Tracks identity column high-water marks via `GpuIdentityColumn`
6. Writes via `GpuDeltaFileFormatWriter.write()` with `GpuParquetFileFormat`

**`GpuDeleteCommand.scala`**

Three execution paths:
1. **Unconditional delete** — truncate table via metadata-only commit
2. **Metadata-only delete** — partition predicate can exclude files without rewriting
3. **Data-driven delete** — scan files, rewrite touched files excluding deleted rows

Includes 3 deletion vector metrics (`numDeletionVectorsAdded/Removed/Updated`), initialized
to 0 (GPU path does not produce deletion vectors; `DeleteCommandMetaShim` falls back to CPU
when persistent deletion vectors are enabled).

**`GpuUpdateCommand.scala`**

Three execution paths (same structure as GpuDeleteCommand). Includes same 3 DV metrics.
Falls back to CPU via `UpdateCommandMetaShim` when `UPDATE_USE_PERSISTENT_DELETION_VECTORS`
is enabled on a table with `deletionVectorsWritable()`.

**`GpuMergeIntoCommand.scala`**

Two-phase merge algorithm:
1. **Phase 1** — Find all files touched by the merge condition
2. **Phase 2** — For each touched file batch: rewrite data applying MATCHED and NOT MATCHED clauses

Key implementation notes:
- `txn.filterFiles(predicates, keepNumRecords = true)` — preserves `numRecords` statistics
  regardless of deletion vector state (required for DBR 14.3+)
- Spark 4.0 encoder construction: `ExpressionEncoder(RowEncoder.encoderFor(schema))` instead
  of `RowEncoder(schema)`
- Schema attributes: `toAttributes(schema)` (via `DataTypeUtils`) instead of
  `schema.toAttributes`
- `notMatchedBySourceClauses` is **not supported** on GPU (see issue #8415); falls back to
  CPU via `MergeIntoCommandMetaShim`

**`GpuWriteIntoDelta.scala`**

Thin DB-17.3 adapter over shared `GpuWriteIntoDeltaBase`: uses
`WriteIntoDeltaEdge`, delegates `write()` to the CPU command inside a GPU
transaction, and stamps `NoRowsCopiedTag` for non-`replaceWhere` commits.

**`GpuDeltaDataSource.scala`**

Shared `GpuCreatableRelationProvider` implementation in
`common/src/main/db-350db143-400db173/scala`. Creates `WriteIntoDeltaEdge` for the
write path and wraps it in `GpuWriteIntoDelta`.

**`GpuDeltaCatalog.scala`**

Shared DB-14.3/DB-17.3 implementation that extends `GpuDeltaCatalogBase`. Key
differences from DBR 13.3 version:
- `getWriter()` method returns `WriteIntoDeltaEdge` (not `WriteIntoDelta`)
- `loadTable(ident, timestamp)` and `loadTable(ident, version)` time-travel overloads

**`GpuDoAutoCompaction.scala`**

`PostCommitHook` that triggers GPU-optimized OPTIMIZE after writes when auto-compaction is
enabled. Creates a new `GpuOptimisticTransaction` and calls `GpuOptimizeExecutor.optimize()`.

**`GpuOptimizeExecutor.scala`**

Implements the OPTIMIZE command with:
- Multi-file, multi-threaded bin packing
- Z-order clustering via `MultiDimClustering`
- Liquid clustering via `ClusteringColumnInfo`

**`GpuCreateDeltaTableCommand.scala`**

DB-17.3 adapter over shared `GpuCreateDeltaTableCommandBase`. DB-17.3 CTAS/RTAS
are tagged as CPU fallback in this PR; the adapter also defensively rejects
`WriteIntoDeltaEdge` so a catalog CTAS/RTAS path cannot silently run outside the
GPU transaction.

#### 4.1.4 Version Shims

Version-specific shim files reside in `delta-spark400db173/.../delta/shims/`.
Shared DB-14.3/DB-17.3 shims reside in
`common/src/main/db-350db143-400db173/scala/com/nvidia/spark/rapids/delta/shims/`.

| Shim | Purpose |
|------|---------|
| `DeleteCommandMetaShim` | Disables GPU DELETE when `deletionVectorsWritable() && DELETE_USE_PERSISTENT_DELETION_VECTORS`; handles both `DeleteCommand` and `DeleteCommandEdge` variants |
| `UpdateCommandMetaShim` | Same guard for UPDATE |
| `MergeIntoCommandMetaShim` | Guards `notMatchedBySourceClauses`; provides `convertToGpu()` for both `MergeIntoCommand` and `MergeIntoCommandEdge` |
| `ShimShuffledRowRDD` | DB-17.3 `ShuffledRowRDD` constructor wrapper |
| `DeltaLogShim` | Shared: `fileFormat(deltaLog)` and `getMetadata(deltaLog)` via `unsafeVolatileSnapshot` |
| `MetadataShims` | Shared: exposes `DeltaStatistics.{NUM_RECORDS, MIN, MAX, NULL_COUNT}` |
| `ShimDeltaUDF` | Shared: `DeltaUDF.stringFromString()` wrapper |
| `InvariantViolationExceptionShim` | Shared: factory for `DeltaInvariantViolationException` |

#### 4.1.5 `GpuDeltaParquetFileFormat.scala`

**⚠ DB-17.3 uses a fundamentally different DV mechanism than DB-14.3.** (Confirmed by
decompiling `spark-sql_2.13-4.0.0-databricks-173.jar`.) The DB-14.3 approach of passing
a `broadcastDvMap: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]]`
**does not exist in DB-17.3** — that constructor parameter was removed.

DB-17.3 uses the **same per-file DV mechanism as OSS Delta 3.3+**:

- DV info is embedded per-file in `PartitionedFile.otherConstantMetadataColumnValues` via
  `DeltaParquetFileFormat.fileConstantMetadataExtractors()`.
- Two public constants on the `DeltaParquetFileFormat` companion object identify the DV
  entries: `FILE_ROW_INDEX_FILTER_ID_ENCODED` and `FILE_ROW_INDEX_FILTER_TYPE`.
- Each file's DV is accessed via a `RowIndexFilterProvider` (new in DB-17.3), which has:
  - `retrieve(conf): RowIndexFilter` — CPU materialization
  - `retrieveSerialized(conf): SerializedBitmap` — raw serialized bitmap bytes
  - `getRowIndexFilterType(): RowIndexFilterType`
- `RowIndexFilterType` values: `IF_CONTAINED`, `IF_NOT_CONTAINED`, `CONSTANT_TRUE_VECTOR_FILTER`,
  `CONSTANT_FALSE_VECTOR_FILTER`, `MOD_SHARD`, `UNKNOWN`

**Blueprint: `GpuDeltaParquetFileFormatBase`** in
`delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/`
implements exactly this per-file DV approach for OSS Delta. The DB-17.3 implementation
follows the same structure, substituting Databricks-namespace classes:

| OSS class | Databricks equivalent (confirmed on cluster) |
|-----------|----------------------------------------------|
| `o.a.s.sql.delta.DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED` | `c.d.s.t.tahoe.DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED` |
| `o.a.s.sql.delta.DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE` | `c.d.s.t.tahoe.DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE` |
| `o.a.s.sql.delta.deletionvectors.RoaringBitmapArray` | `c.d.s.t.tahoe.deletionvectors.RoaringBitmapArray` |
| `o.a.s.sql.delta.deletionvectors.StoredBitmap` | `c.d.s.t.tahoe.deletionvectors.StoredBitmap` |
| `o.a.s.sql.delta.deletionvectors.DropMarkedRowsFilter` | `c.d.s.t.tahoe.deletionvectors.DropMarkedRowsFilter` (same `createInstance` API) |
| `o.a.s.sql.delta.deletionvectors.KeepMarkedRowsFilter` | `c.d.s.t.tahoe.deletionvectors.KeepMarkedRowsFilter` (same `createInstance` API) |
| `o.a.s.sql.delta.storage.dv.HadoopFileSystemDVStore` | `c.d.s.t.tahoe.storage.dv.HadoopFileSystemDVStore` |
| `o.a.s.sql.delta.actions.DeletionVectorDescriptor` | `c.d.s.t.tahoe.actions.DeletionVectorDescriptor` |
| `o.a.s.sql.delta.actions.Protocol` / `Metadata` | `c.d.s.t.tahoe.actions.Protocol` / `Metadata` |
| `o.a.s.sql.delta.sources.DeltaSQLConf` | `c.d.s.t.tahoe.sources.DeltaSQLConf` |
| `o.a.s.sql.delta.schema.SchemaMergingUtils` | `c.d.s.t.tahoe.schema.SchemaMergingUtils` |
| `RowIndexFilter.materializeIntoVectorWithRowIndex` | `c.d.sql.io.RowIndexFilter.materializeIntoVectorWithRowIndex` |
| `RowIndexFilterProvider` | `c.d.sql.io.RowIndexFilterProvider` |
| `PortableRoaringBitmapArraySerializationFormat` | `c.d.s.t.tahoe.deletionvectors.PortableRoaringBitmapArraySerializationFormat` |
| `NativeRoaringBitmapArraySerializationFormat` | `c.d.s.t.tahoe.deletionvectors.NativeRoaringBitmapArraySerializationFormat` |

(`o.a.s` = `org.apache.spark`, `c.d.s.t` = `com.databricks.sql.transaction`)

**⚠ CRITICAL: `delta-33x-40x` common code cannot be compiled into DB-17.3.**

The files in `delta-lake/common/src/main/delta-33x-40x/scala/` import from the OSS
`org.apache.spark.sql.delta.*` namespace, which **does not exist** in DB-17.3. DB-17.3
only has the `com.databricks.sql.transaction.tahoe.*` namespace. Therefore:

- The DB-17.3 `pom.xml` must NOT include `delta-33x-40x` as a source directory.
- The `GpuDeltaParquetFileFormat.scala` for DB-17.3 is written in the
  `delta-spark400db173` module, using the `delta-33x-40x` code as a **pattern/blueprint**
  but with all Databricks-namespace imports.
- The final DBR-17.3 DV implementation ports only the shared pieces still needed by the
  native cuDF path (`RapidsDeletionVectors`). The earlier V1 row-index helper file
  (`RapidsRowIndexFilters`) was an intermediate DBR materialized-path artifact and is
  not in the final squashed commit.

**Note: DB-14.3 has ZERO GPU DV read support.** The DB-14.3 `GpuDeltaParquetFileFormat`
explicitly tags `"deletion vectors are not supported"` when `hasDeletionVectorMap` is true.
DB-17.3 is now the **first Databricks Delta module** with GPU-accelerated DV reads.

**Key design decisions for DB-17.3 `GpuDeltaParquetFileFormat`:**

1. Constructor mirrors `DeltaParquetFileFormat`: takes `protocol`, `metadata`, DV-related
   boolean flags, `tablePath`, and `isCDCRead` — no broadcast fields.
2. `createMultiFileReaderFactory` implemented (enables GPU multi-threaded reader for DV tables).
3. `tagSupportForGpuFileSourceScan` does **not** block GPU for DVs (unlike DB-14.3 which
   fell back to CPU when `hasDeletionVectorMap`). DB-17.3 DV reads ARE GPU-supported.
4. `DeltaSpark400DB173Provider.convertToGpu` constructs this format directly from
   `fmt.protocol`, `fmt.metadata`, and `fmt.tablePath` — no broadcast fields to pass.

**2026-05-05 update:** the DBR-17.3 native cuDF DV path is now implemented locally through
`GpuDeltaParquetFileFormatNativeDV.scala`. DBR `SerializedBitmap` data is converted to
cuDF-compatible standard portable roaring bitmap bytes by `DeltaBitmapUtils.java`; this is
needed because DBR exposes the bitmap serialization helpers in a way that is not directly
callable from Scala in this module.

Note: DV **writes** are not GPU-accelerated (the shims fall back to CPU for DV write paths).
DV **reads** are GPU-accelerated in this file.

---

## 5. Key API Differences

### 5.1 DBR 13.3 → DBR 14.3 (already incorporated in the 350db143 baseline)

| Area | DBR 13.3 | DBR 14.3 |
|------|----------|----------|
| Write command class | `WriteIntoDelta` | `WriteIntoDeltaEdge` |
| `OptimisticTransaction` constructor | `(deltaLog, snapshot)(clock)` | `(deltaLog, Option.empty[CatalogTable], snapshot)` |
| DV metrics in Delete/Update | 2 | 3 (`numDVAdded/Removed/Updated`) |
| `filterFiles` in Merge | `filterFiles(predicates)` | `filterFiles(predicates, keepNumRecords=true)` |
| Encoder construction | `RowEncoder(schema)` | `ExpressionEncoder(RowEncoder.encoderFor(schema))` |
| Schema attributes | `schema.toAttributes` | `toAttributes(schema)` |
| `GpuDeltaCatalog.getWriter()` | Not present | Uses `WriteIntoDeltaEdge` |
| `GpuDeltaParquetFileFormat` | No DV read support (GPU blocks DVs) | DV read support via broadcast map (but GPU explicitly blocks it; falls back to CPU) |
| `GpuLowShuffleMergeCommand` | Present | Removed |

### 5.2 Confirmed API Differences: DBR 14.3 → DBR 17.3

Verified by decompiling DB-17.3 JARs and confirmed on a live DB-17.3 cluster (2026-04-07).

Note: On the cluster, the JAR is named
`----ws_4_0--sql--core--core-hive-2.3__hadoop-3.2_2.13_deploy.jar` (not
`spark-sql_2.13-4.0.0-databricks-173.jar` as originally expected). The catalyst JAR is
`----ws_4_0--sql--catalyst--catalyst-hive-2.3__hadoop-3.2_2.13_deploy.jar`.

#### Deletion Vector API (Major Change)

| Area | DB-14.3 | DB-17.3 |
|------|---------|---------|
| `DeltaParquetFileFormat` constructor | 8 params: `(relation, columnMappingMode, referenceSchema, isSplittable, disablePushDowns, broadcastDvMap, tablePath, broadcastHadoopConf)` | 10 params: `(protocol, metadata, generateRowIndexFilterId, generateRowIndexFilterColumn, generateDeltaFileInScanId, nullableRowTrackingConstantFields, nullableRowTrackingGeneratedFields, optimizationsEnabled, tablePath, isCDCRead)` |
| DV loading mechanism | Broadcast `Map[URI, DeletionVectorDescriptorWithFilterType]` | Per-file via `PartitionedFile.otherConstantMetadataColumnValues` |
| `TahoeFileIndex.rowIndexFilters` type | `Map[String, RowIndexFilterType]` | `Map[String, RowIndexFilterProvider]` |
| `FILE_ROW_INDEX_FILTER_ID_ENCODED` | Not present | **Public** constant on companion |
| `FILE_ROW_INDEX_FILTER_TYPE` | Not present | **Public** constant on companion |
| DV filter provider | `RowIndexFilterType` enum only | `RowIndexFilterProvider` interface (`retrieve`, `retrieveSerialized`, `getRowIndexFilterType`) |
| Raw bitmap bytes | Not exposed | `SerializedBitmap.buffer(): byte[]` via `RowIndexFilterProvider.retrieveSerialized()` |
| GPU materialization method | `materializeIntoVector(start, end, output)` | + `materializeIntoVectorWithRowIndex(numRows, rowIndexCol, output)` |
| `DeletionVectorUtils.deletionVectorsWritable` | `(SnapshotDescriptor): Boolean` | Overloads: `(SnapshotDescriptor, Option[Protocol], Option[Metadata])` and `(Protocol, Metadata)` |
| DV write configs | `DELETE_USE_PERSISTENT_DELETION_VECTORS` ✓, `UPDATE_USE_PERSISTENT_DELETION_VECTORS` ✓ | **Same** — both confirmed present in `DeltaSQLConf` |
| `DropMarkedRowsFilter.createInstance` | `(DeletionVectorDescriptor, Configuration, Option[Path])` | **Same API** — confirmed present |
| `KeepMarkedRowsFilter.createInstance` | `(DeletionVectorDescriptor, Configuration, Option[Path])` | **Same API** — confirmed present |
| `RoaringBitmapArray` | `o.a.s.sql.delta.deletionvectors.RoaringBitmapArray` | `c.d.s.t.tahoe.deletionvectors.RoaringBitmapArray` — confirmed via `javap` (same API: `readFrom(byte[])`, `toArray(): long[]`, `cardinality(): long`) |
| `StoredBitmap.load()` | Returns `RoaringBitmapArray` | Returns `RoaringBitmapArray` — confirmed via `javap` |
| `HadoopFileSystemDVStore.read()` | `(DeletionVectorDescriptor, Path): RoaringBitmapArray` | **Same API** — confirmed present |
| `PortableRoaringBitmapArraySerializationFormat` | `MAGIC_NUMBER`, `serialize`, `deserialize` | **Same API** — confirmed present |
| `NativeRoaringBitmapArraySerializationFormat` | `MAGIC_NUMBER`, `serialize`, `deserialize` | **Same API** — confirmed present |
| `DeletionVectorStore` / `DeletionVectorStoreEdge` | Present | **Confirmed present** — `DeletionVectorStoreEdge.createInstance(conf, cacheConfig)` |

#### Spark 4.0 APIs (Resolved via DB-17.3 Cluster Verification, 2026-04-07)

All 5 items previously listed as unknowns have been verified on a live DB-17.3 cluster:

1. **`SparkSession` aliasing** — **CONFIRMED.** `org.apache.spark.sql.classic.SparkSession`
   is present in the DB-17.3 runtime. The existing `TrampolineConnectShims` in
   `sql-plugin/src/main/spark400/` already includes `{"spark": "400db173"}` in its shim
   header. DB-17.3 command files WILL need to use `TrampolineConnectShims.SparkSession`
   and `TrampolineConnectShims.createDataFrame()` (same as OSS `delta-40x`).

2. **`DFUDFShims`** — **CONFIRMED.** `DFUDFShims` in
   `sql-plugin/src/main/spark400/scala/org/apache/spark/sql/nvidia/DFUDFShims.scala` already
   includes `{"spark": "400db173"}` in its shim header. DB-17.3 WILL need
   `DFUDFShims.exprToColumn()` for UDF invocations in `GpuMergeIntoCommand`.

3. **`GpuFileFormatWriter.write()` signature** — **CONFIRMED.** `GpuFileFormatWriter` in
   `sql-plugin/src/main/spark332db/scala/org/apache/spark/sql/rapids/GpuFileFormatWriter.scala`
   already includes `{"spark": "400db173"}` in its shim header. No signature change needed.

4. **`GpuOptimizeExecutor` clustering APIs** — **CONFIRMED.** On the DB-17.3 cluster:
   `com.databricks.sql.io.skipping.MultiDimClustering` and
   `com.databricks.sql.io.skipping.liquid.ClusteringColumnInfo` are at the same packages
   as DB-14.3. No import changes needed.

5. **`RowTracking` import** — **CONFIRMED.** `com.databricks.sql.transaction.tahoe.RowTracking`
   is present in the DB-17.3 runtime. Import path is unchanged from DB-14.3.

Additional classes confirmed on the DB-17.3 cluster:
- `WriteIntoDelta` and `WriteIntoDeltaEdge` — both present
- `AtomicCreateTableAsSelectExec` / `AtomicReplaceTableAsSelectExec` — confirmed
- `OptimizeExecutor` at `com.databricks.sql.transaction.tahoe.commands.optimize.OptimizeExecutor`
- `DeltaInvariantViolationException` — confirmed
- `DeltaUDF` — confirmed
- `DeltaStatistics` at `com.databricks.sql.transaction.tahoe.stats.DeltaStatistics`
- `DataTypeUtils` at `org.apache.spark.sql.catalyst.types.DataTypeUtils`
- `ExpressionEncoder` — confirmed
- `DeltaSQLConf` at `com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf`

### 5.3 Scala 2.13 Considerations

Since DB-17.3 is **Scala 2.13 only**, some differences from the Scala 2.12 codebase apply:

- **No cross-compilation concerns**: the `delta-spark400db173` module is never compiled with
  Scala 2.12. No need for `scala-2.12`/`scala-2.13` source directory splits or compatibility
  shims within this module.
- **Scala 2.13 collection APIs**: `LazyList` (not `Stream`), `ArraySeq` (not `WrappedArray`),
  etc., may be used freely. However, since the code is copied from the 350db143 baseline
  (which must support both 2.12 and 2.13), Scala 2.13-only APIs should not need to be
  introduced.
- **`-Xsource:2.13` and `-Wconf` flags**: automatically applied via the `scala2.13/`
  parent pom. No manual flag changes needed.
- The OSS `delta-40x` module (also Scala 2.13 only) is a useful reference for any
  compiler-error patterns specific to Scala 2.13.

---

## 6. Build System Changes

### 6.1 Root `pom.xml`

The `release400db173` profile already lives inside the `<!-- #if scala-2.13 --><!--` block.
Update the two property/module values inside that block:

```diff
 <properties>
     <spark.version.classifier>spark400db173</spark.version.classifier>
-    <rapids.delta.artifactId1>rapids-4-spark-delta-stub</rapids.delta.artifactId1>
+    <rapids.delta.artifactId1>rapids-4-spark-delta-spark400db173</rapids.delta.artifactId1>
 </properties>
 <modules>
     <module>shim-deps/databricks</module>
-    <module>delta-lake/delta-stub</module>
+    <module>delta-lake/delta-spark400db173</module>
 </modules>
```

Note: the `_2.12`/`_2.13` suffix is **not** appended to `rapids.delta.artifactId1` in the
profile property — the suffix is appended by the aggregator/dist POM when resolving the
dependency. Verify this matches the pattern used by other profiles (e.g.,
`release350db143` uses `rapids-4-spark-delta-spark350db143` without a suffix).

### 6.2 Generate `scala2.13/` Mirror Files

After all `pom.xml` changes, run:

```bash
./build/make-scala-version-build-files.sh 2.13
```

This script:
1. Generates `scala2.13/delta-lake/delta-spark400db173/pom.xml` from the primary
   `delta-lake/delta-spark400db173/pom.xml` (replacing `_2.12` → `_2.13` throughout)
2. Updates `scala2.13/pom.xml` to reflect the `release400db173` module change

Both the primary pom and the `scala2.13/` mirror must be committed to source control.

### 6.3 No Change to `delta-lake/delta-stub`

The stub module continues to be used for all Databricks runtimes that don't have a dedicated
Delta module (e.g., any future new Databricks versions until their Delta module is created).
The `release400db173` profile simply no longer references it.

---

## 7. Implementation Plan

### Step 1: Scaffold the module

Copy `delta-lake/delta-spark350db143/` to `delta-lake/delta-spark400db173/`.

### Step 2: Mechanical renames

In the new `delta-spark400db173/` directory:
- Rename all filenames containing `350db143` → `400db173`
- Replace all occurrences of `350db143` → `400db173` in class/object names and `pom.xml`
- Replace `DeltaSpark350DB143Provider` → `DeltaSpark400DB173Provider`
- Update copyright years → 2026 where applicable

### Step 3: Update root `pom.xml`

Apply the diff in §6.1 to swap the stub module for the new delta module inside the
`#if scala-2.13` block.

### Step 4: Sync `scala2.13/` mirror

```bash
./build/make-scala-version-build-files.sh 2.13
```

This is **mandatory** after any `pom.xml` change. Commit both the primary pom and the
generated `scala2.13/` pom.

### Step 5: Fix Spark 4.0 API incompatibilities

The DB-14.3 code uses Spark 3.x APIs that are **removed or changed** in Spark 4.0. These
must be fixed before compiling. See §7.1 below for the complete list.

### Step 6: Compile against DBR 17.3

```bash
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests \
    -pl delta-lake/delta-spark400db173 -am
```

Note: `-f scala2.13` is required because the `release400db173` profile is only active in
the Scala 2.13 build. Building without `-f scala2.13` will not find the profile.

Address remaining compile errors by cross-referencing:
- The OSS `delta-lake/delta-40x/` module for Spark 4.0 API patterns
- The Databricks 17.3 JARs for exact API signatures in `com.databricks.sql.transaction.tahoe`

### Step 7: Full build

```bash
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests
```

### Step 8: Run tests

```bash
mvn -f scala2.13 -Dbuildver=400db173 package -pl tests -am \
    -DwildcardSuites="com.nvidia.spark.rapids.delta.*"
```

Integration tests (requires a DBR 17.3 cluster):
```bash
./integration_tests/run_pyspark_from_build.sh -k delta_test.py
```

#### Wide-schema integration test infra (DB-17.3 only, 2026-04-28)

`delta_lake_write_test.py` runs 9 tests through the wide `delta_write_gens` schema
(63 columns, 12 complex types, including maps and arrays of arrays). At the default
`gen_df(..., length=2048)`, the generated rows materialize into ~33 MB serialized
`RDDScanExec` task closures and the DB-17.3 cluster's
`dispatcher-event-loop` thread OOMs in `TaskSetManager.prepareLaunchingTask`.

The minimal mitigation is a single DB-17.3-conditional constant in
[integration_tests/src/main/python/delta_lake_utils.py](integration_tests/src/main/python/delta_lake_utils.py):

```python
delta_db173_wide_schema_gen_length = 128 if is_databricks173_or_later() else 2048
```

The 9 wide-schema tests pass `length=delta_db173_wide_schema_gen_length` to `gen_df`.
On every other shim it resolves to 2048 — no behavior change. On DB-17.3 the closure
shrinks ~33 MB → ~2 MB and the OOM is gone.

#### First post-Issue-2 cluster test run (2026-04-28)

`run_pyspark_from_build.sh -k delta_lake_write_test.py` on DB-17.3:
**6 failed, 95 passed, 2 skipped, 22 xfailed, 4 xpassed**.

All 6 failures are in the `CheckDeltaInvariant` / `DeltaInvariantChecker` path. Two
symptoms (planning-time arity assertion + runtime not-columnar mismatch in plans where
`DeltaInvariantChecker` lands on CPU between GPU operators), likely shared root cause:
the shared `GpuCheckDeltaInvariant` `ExprChecks` registration declares 1 child but
DB-17.3's `CheckDeltaInvariant` is a 3-child expression. Tracked as **Issue 10** —
see [delta-lake-db173-github-issues.md](delta-lake-db173-github-issues.md). 7.1.13
already migrated `columnExtractors` for the type change; the arity claim was missed.

### Step 9: Enable Delta tests in CI

Remove the DB-17.3 skip in `jenkins/databricks/test.sh` (lines 152-160):
```diff
-    if [[ "$SPARK_SHIM_VER" == "spark400db173" ]]; then
-        echo "Skipping Delta Lake tests: not yet supported for DB-17.3 (spark400db173)"
-    else
-        ## Run Delta Lake tests
-        DRIVER_MEMORY="4g" \
-            bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks"  -m "delta_lake" --delta_lake --test_type=$TEST_TYPE
-    fi
+    ## Run Delta Lake tests
+    DRIVER_MEMORY="4g" \
+        bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks"  -m "delta_lake" --delta_lake --test_type=$TEST_TYPE
```

---

## 7.1 Spark 4.0 API Incompatibilities in DB-14.3 Code (Must Fix)

**Verified on DB-17.3 cluster (2026-04-07):** The following Spark 3.x APIs used in the
DB-14.3 command files are removed/changed in Spark 4.0 (DB-17.3). These MUST be fixed
when porting.

### 7.1.1 `new Column(Expression)` → `DFUDFShims.exprToColumn(expr)`

**Impact:** 14 occurrences across 3 files.

In Spark 4.0, `Column` only accepts `ColumnNode`, not `Expression`:
```java
// Spark 4.0 Column constructors (confirmed via javap on DB-17.3):
public Column(ColumnNode)
public Column(String, Option[Object])
public Column(String)
// Column(Expression) is GONE
```

Files affected and approximate occurrence count:
- `GpuMergeIntoCommand.scala` — ~9 occurrences of `new Column(expr)`
- `GpuDeleteCommand.scala` — ~2 occurrences
- `GpuUpdateCommand.scala` — ~3 occurrences

**Fix:** Replace all `new Column(expr)` with `DFUDFShims.exprToColumn(expr)`.
Add `import org.apache.spark.sql.nvidia.DFUDFShims` to each file.

Reference: OSS `delta-40x/GpuMergeIntoCommand.scala` uses
`DFUDFShims.exprToColumn(condition)` for the same pattern.

### 7.1.2 `Dataset.ofRows(spark, plan)` → `TrampolineConnectShims.createDataFrame()`

**Impact:** ~10 occurrences across 3 files.

In Spark 4.0, `SparkSession` is abstract and `Dataset.ofRows()` requires a
`ClassicSparkSession`. The safe wrapper is `TrampolineConnectShims.createDataFrame()`:

```scala
// DB-14.3 (Spark 3.x):
Dataset.ofRows(spark, plan)

// DB-17.3 (Spark 4.0):
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
TrampolineConnectShims.createDataFrame(
  spark.asInstanceOf[TrampolineConnectShims.SparkSession], plan)
```

Files affected:
- `GpuOptimisticTransaction.scala` — 2 occurrences
- `GpuUpdateCommand.scala` — 2 occurrences
- `GpuMergeIntoCommand.scala` — 6 occurrences

Reference: OSS `delta-40x/GpuOptimisticTransaction.scala` imports
`TrampolineConnectShims.SparkSession` and uses `TrampolineConnectShims.createDataFrame()`.

### 7.1.3 `SparkSession.getActiveSession` → `TrampolineConnectShims.getActiveSession`

**Impact:** Used in `GpuDeltaParquetFileFormat.scala` and potentially in command files.

```scala
// DB-14.3:
SparkSession.getActiveSession.get

// DB-17.3:
TrampolineConnectShims.getActiveSession
```

### 7.1.4 `RuntimeReplaceable` expressions in statistics

In Spark 4.0, JSON functions (e.g. `StructsToJson`) are `RuntimeReplaceable` and
unevaluable. Stats expression processing needs:

```scala
// Add to GpuOptimisticTransaction or stats processing:
expr.transform { case rr: RuntimeReplaceable => rr.replacement }
```

Reference: `delta-40x/Delta40xCommandShims.postProcessStatsExpr()`

### 7.1.5 Summary of Required Imports per File

| File | New imports needed |
|------|--------------------|
| `GpuMergeIntoCommand.scala` | `DFUDFShims`, `TrampolineConnectShims` |
| `GpuDeleteCommand.scala` | `DFUDFShims` |
| `GpuUpdateCommand.scala` | `DFUDFShims`, `TrampolineConnectShims` |
| `GpuOptimisticTransaction.scala` | `TrampolineConnectShims`, `RuntimeReplaceable` |
| `GpuDeltaParquetFileFormat.scala` | `TrampolineConnectShims` (for `getActiveSession`) |

### 7.1.6 `writeFiles()` Signature Completely Changed (MAJOR)

**Impact:** `GpuOptimisticTransaction.writeFiles()` override must change.

DB-14.3:
```scala
def writeFiles(inputData: Dataset[_],
    writeOptions: Option[DeltaOptions],
    additionalConstraints: Seq[Constraint]): Seq[FileAction]
```

DB-17.3 (confirmed via `javap`):
```scala
def writeFiles(inputData: Dataset[_],
    writeOptions: TransactionalWriteOptions,   // NEW CLASS
    isOptimize: Boolean,                        // NEW
    isLiquidClustering: Boolean,                // NEW (has default)
    additionalConstraints: Seq[Constraint],     // (has default)
    isCDCWritePhase: Boolean,                   // NEW (has default)
    context: Option[String]): Seq[FileAction]   // NEW (has default)
```

`TransactionalWriteOptions` is a new case class wrapping:
```scala
case class TransactionalWriteOptions(
    optimizeWrite: Option[Boolean],                // all have defaults
    overridingSQLConfs: Map[String, String],
    validateWrite: Option[Boolean],
    deltaOptions: Option[DeltaOptions],
    forcePreserveInputOrder: Boolean)
```

There is also `writeFilesAndGetQueryExecution()` that returns
`(Seq[FileAction], QueryExecution)` instead of just `Seq[FileAction]`.

**Fix:** Update `GpuOptimisticTransaction.writeFiles()` override signature to match.
Construct `TransactionalWriteOptions` wrapping the `DeltaOptions`.

### 7.1.7 Command Edge Constructors Changed (MAJOR)

All Databricks Delta command classes gained new constructor parameters in DB-17.3.
The GPU command classes and the `MergeIntoCommandMetaShim.convertToGpu` methods must
extract and pass these new fields.

**`MergeIntoCommandEdge` / `MergeIntoCommand`** (DB-17.3, confirmed via `javap`):
```scala
(source: LogicalPlan,
 target: LogicalPlan,
 catalogTable: Option[CatalogTable],       // NEW
 targetFileIndex: TahoeFileIndex,           // NEW
 condition: Expression,
 matchedClauses: Seq[DeltaMergeIntoMatchedClause],
 notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
 notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause],
 migratedSchema: Option[StructType],
 trackHighWaterMarks: Set[String],          // NEW
 schemaEvolutionEnabled: Boolean)           // NEW
```

**`DeleteCommandEdge`** (DB-17.3):
```scala
(tahoeFileIndex: TahoeFileIndex,            // NEW (replaces deltaLog in GPU cmd)
 catalogTable: Option[CatalogTable],        // NEW
 target: LogicalPlan,
 condition: Option[Expression])
```

**`UpdateCommandEdge`** (DB-17.3):
```scala
(tahoeFileIndex: TahoeFileIndex,
 catalogTable: Option[CatalogTable],        // NEW
 target: LogicalPlan,
 updateExpressions: Seq[Expression],
 condition: Option[Expression])
```

**Fix:** Update `GpuMergeIntoCommand`, `GpuDeleteCommand`, `GpuUpdateCommand` constructors
to accept and propagate the new fields. Update `MergeIntoCommandMetaShim.convertToGpu`,
`DeleteCommandMeta`, and `UpdateCommandMeta` to extract `catalogTable`, `targetFileIndex`,
`trackHighWaterMarks`, and `schemaEvolutionEnabled` from the CPU command.

### 7.1.8 `WriteIntoDeltaEdge` Constructor Grew (Minor)

DB-17.3 constructor has 13 params (up from ~8). Params 7-13 all have defaults:
```
$lessinit$greater$default$7()  // catalogTable: Option[CatalogTable]
$lessinit$greater$default$8()  // schemaInCatalog: Option[StructType]
$lessinit$greater$default$9()  // clusteringColumns: Option[Seq[String]]  NEW
$lessinit$greater$default$10() // tableAliasOpt: Option[String]           NEW
$lessinit$greater$default$11() // snapshotAtAnalysis: Option[Snapshot]     NEW
$lessinit$greater$default$12() // jobGroupIdAtAnalysis: Option[String]     NEW
$lessinit$greater$default$13() // autoUpdateStatsSchemaOpt: Option[_]      NEW
```

Existing call sites passing 6-8 positional args should compile because new params have
defaults. **Verify during compilation.** If issues arise, explicitly pass `None` for new params.

### 7.1.9 `implicit Clock` Parameter Removed from `OptimisticTransaction`

DB-14.3's `GpuOptimisticTransactionBase` and `GpuOptimisticTransaction` use
`(implicit clock: Clock)` because the DB-14.3 `OptimisticTransaction` constructor
requires it. In DB-17.3 (confirmed via `javap`), the constructor is:
```
OptimisticTransaction(DeltaLog, Option[CatalogTable], Snapshot)
```
**No `implicit Clock` parameter.** Clock is obtained internally from `DeltaLog.clock()`.

**Fix:** Remove `(implicit clock: Clock)` from `GpuOptimisticTransactionBase` and
`GpuOptimisticTransaction` constructors. These are version-specific files (not shared),
so this only affects the DB-17.3 module.

Note: The shared `GpuDeltaLog` has `private lazy implicit val _clock: Clock =
deltaLog.clock`. This implicit won't be consumed by the DB-17.3 constructors, but since
it's `lazy` and type-annotated, it won't trigger unused warnings. **No change needed to
the shared file.**

### 7.1.10 `PostCommitHook.run()` Signature Completely Changed (MAJOR)

DB-14.3:
```scala
def run(spark: SparkSession, txn: OptimisticTransactionImpl,
    committedVersion: Long, postCommitSnapshot: Snapshot,
    committedActions: Seq[Action]): Unit
```

DB-17.3:
```scala
def run(spark: SparkSession, txn: CommittedTransaction): Unit
```

`CommittedTransaction` is a new case class bundling the transaction, committed version,
snapshot, and committed actions.

**Fix:** Rewrite `GpuDoAutoCompaction.run()` to accept `CommittedTransaction` and extract
the individual fields from it. The `txn.asInstanceOf[GpuOptimisticTransaction]` cast must
be updated to extract the transaction from `CommittedTransaction`.

### 7.1.11 `LogicalRelation` Pattern Match Arity Changed (MAJOR)

DB-14.3 `LogicalRelation` unapply returns 4 fields:
```scala
case LogicalRelation(base, output, catalogTbl, isStreaming) =>
```

DB-17.3 `LogicalRelation` unapply returns 7 fields (added `sparkDataStream`,
`injectedConstraints`, `statistics`):
```scala
case LogicalRelation(base, output, catalogTbl, isStreaming, _, _, _) =>
```

Files affected:
- `GpuMergeIntoCommand.scala` (line ~948) — match with 4 fields
- `GpuOptimisticTransactionBase.scala` (line ~178) — match with 4 fields

**Fix:** Add wildcards for the 3 new fields in all pattern matches.

### 7.1.12 `ShuffledRowRDD` Constructor Changed (MAJOR — Shared Code)

**This affects a SHARED file:** `delta-lake/common/src/main/databricks/scala/
org/apache/spark/sql/rapids/delta/OptimizeWriteExchangeExec.scala`

DB-17.3's `ShuffledRowRDD` constructor now requires a `PrismMetrics` parameter that
did not exist in earlier versions. The shared code creates `ShuffledRowRDD` with 2-3 args,
but all DB-17.3 constructors require the `PrismMetrics` parameter.

**Fix:** This shared file CANNOT be used as-is for DB-17.3. Options:
1. Create a version-specific override in `delta-spark400db173` that shadows the shared file
2. Add a shim for `ShuffledRowRDD` construction
3. Pass `null` or a no-op `PrismMetrics` if the parameter is optional

### 7.1.13 `CheckDeltaInvariant.columnExtractors` Type Changed (MAJOR — Shared Code)

**This affects a SHARED file:** `delta-lake/common/src/main/databricks/scala/
com/databricks/sql/transaction/tahoe/rapids/GpuCheckDeltaInvariant.scala`

DB-14.3: `columnExtractors: Map[String, Expression]`
DB-17.3: `columnExtractors: Seq[(String, Expression)]`

The `GpuCheckDeltaInvariantMeta.convertToGpu()` passes `wrapped.columnExtractors` directly
to the `GpuCheckDeltaInvariant` constructor which expects `Map[String, Expression]`.

**Fix:** Either add `.toMap` in the shared code (if backward compatible) or create a
version-specific override in `delta-spark400db173`.

### 7.1.14 `DeletionVectorUtils` Changed from Object to Trait (Minor)

DB-17.3 `DeletionVectorUtils` is now a trait with a companion object. The companion
(`DeletionVectorUtils$`) implements the trait and exposes static-like methods.

The existing code calls `DeletionVectorUtils.deletionVectorsWritable(snapshot)` —
this still works because the companion has the method, and the single-arg overload
is supported via default params (`$default$2()` and `$default$3()` return `None`).

**No code change needed**, but document for awareness.

### 7.1.15 `writeFilesAndGetQueryExecution` Is the DB-17.3 Interception Point (MAJOR — discovered 2026-04-23)

Originally we expected that overriding `writeFiles(Dataset, TransactionalWriteOptions, ...)`
in `GpuOptimisticTransactionBase` (§7.1.6) would be sufficient to GPU-accelerate DB-17.3
Delta writes the same way DB-14.3 does. Cluster validation disproved this.

**Actual DB-17.3 CPU call chain for `WriteIntoDeltaEdge.write(txn)`:**
```
WriteIntoDeltaEdge.write(txn, spark)                                 (unchanged)
  → WriteIntoDeltaLike.write                                         (unchanged shape)
  → WriteIntoDeltaEdge.writeAndReturnCommitData                      (unchanged)
  → WriteIntoDeltaEdge.writeAndReturnCommitDataAndMaterializationPlans (NEW)
  → WriteIntoDeltaEdge.writeFilesAndGetMaterializationPlans          (NEW)
  → ClusteredWriter.runAndReturnMaterializationPlans                 (NEW)
  → ClusteredWriter.runInternal                                       (NEW)
  → ClusteredWriter.writeFilesWithoutClustering                      (NEW)
  → TransactionalWriteEdge.writeFilesAndGetExecutedPlan              (NEW)
  → TransactionalWriteEdge.writeFilesAndGetQueryExecution            (NEW; line 393)
  → SparkPlan.executeCollect  on an internally-built plan            (NEW)
      -- the plan shape is DataWritingCommandExec(WriteIntoDeltaCommand) + WriteFiles + query
```

The old `txn.writeFiles(data, opts, ...)` is **not called** anywhere in this chain. Our
`GpuOptimisticTransactionBase.writeFiles` override (both 3-arg and 7-arg forms) is dead
code on DB-17.3.

**What the fix needs (Issue 2):**

1. Override `TransactionalWriteEdge.writeFilesAndGetQueryExecution` in
   `GpuOptimisticTransactionBase` (delta-spark400db173 version). The override must:
   - Do the GPU pipeline that `writeFiles` does today (RapidsDeltaWrite + GpuFileFormatWriter + stats trackers + identity-column tracking).
   - Return `(Seq[FileAction], QueryExecution)` — the new return shape.
   - Possibly also override `writeFilesAndGetExecutedPlan` (the caller one level up) to
     short-circuit any further CPU machinery.
2. Signatures must be captured from a DB-17.3 cluster:
   ```
   javap -p -classpath "/databricks/jars/*" \
       com.databricks.sql.transaction.tahoe.files.TransactionalWriteEdge
   ```
3. GPU support or write-level CPU-fallback for
   `com.databricks.sql.transaction.tahoe.GenerateIdentityValues` (and
   `PartitionIdentityValueGenerator`). Currently unsupported; when present in the
   write Project, forces `ProjectExec` to CPU, which in turn breaks any further GPU
   tree inside the write.

**Do NOT try to intercept `WriteIntoDeltaCommand` via a `DataWritingCommandMeta`** — we
tried that on 2026-04-23 and hit `DELTA_ACTIVE_TRANSACTION_ALREADY_SET` because the
outer `GpuWriteIntoDelta.run` already opened a transaction. The inner WriteIntoDeltaCommand
is an implementation detail of `writeFilesAndGetQueryExecution`, not a standalone write
entry point.

**Evidence / stack trace** (from test
`test_delta_write_identity_columns_sql` on DB-17.3, 2026-04-23):
```
GpuDeltaDataSource.createRelation
  → GpuWriteIntoDelta.run → GpuDeltaLog.withNewTransaction           (TXN A starts)
  → WriteIntoDeltaEdge.write
  → ... (chain above) ...
  → TransactionalWriteEdge.writeFilesAndGetQueryExecution:393
  → SparkPlan.executeCollect
  → (inner plan) → GpuDataWritingCommandExec(GpuWriteIntoDeltaCommand) [our attempted meta]
  → GpuWriteIntoDelta.run → GpuDeltaLog.withNewTransaction:59        (TXN B — FAIL)
```

---

## 8. Out of Scope

- **Deletion vector writes on GPU** — DV write paths (`DELETE_USE_PERSISTENT_DELETION_VECTORS`,
  `UPDATE_USE_PERSISTENT_DELETION_VECTORS`) continue to fall back to CPU. Tracked in
  https://github.com/NVIDIA/spark-rapids/issues/8654.
- **`notMatchedBySourceClauses` in MERGE** — Continues to fall back to CPU. Tracked in
  https://github.com/NVIDIA/spark-rapids/issues/8415.
- **Iceberg / other table formats** — Not affected by this change.
- **Scala 2.12 support** — DBR 17.3 / Spark 4.0 does not support Scala 2.12. No Scala 2.12
  artifact for this module will be produced or is needed.

---

## 9. Reference Files

| Purpose | Location |
|---------|----------|
| Primary reference module (DBR 14.3) | `delta-lake/delta-spark350db143/` |
| `scala2.13/` mirror of DBR 14.3 | `scala2.13/delta-lake/delta-spark350db143/` |
| Databricks shared common code | `delta-lake/common/src/main/databricks/scala/` |
| DB-14.3/DB-17.3 shared transaction/catalog/data-source code | `delta-lake/common/src/main/db-350db143-400db173/scala/` |
| Universal Delta common code | `delta-lake/common/src/main/scala/` |
| OSS Spark 4.0 Delta (API reference) | `delta-lake/delta-40x/` |
| Spark 4.0 command shims (Spark 4.0 API patterns) | `delta-lake/delta-40x/src/main/scala/.../Delta40xCommandShims.scala` |
| Spark 4.0 runtime shim (reference) | `delta-lake/delta-40x/src/main/scala/.../Delta40xRuntimeShim.scala` |
| `scala2.13/` mirror of OSS 4.0 Delta | `scala2.13/delta-lake/delta-40x/` |
| DB-17.3 SQL plugin shims | `sql-plugin/src/main/spark400db173/` |
| Spark 4.0 shared shims (covers 400db173) | `sql-plugin/src/main/spark400/` |
| `TrampolineConnectShims` (Spark 4.0 session/DF) | `sql-plugin/src/main/spark400/.../TrampolineConnectShims.scala` |
| `DFUDFShims` (Spark 4.0 Column API) | `sql-plugin/src/main/spark400/.../DFUDFShims.scala` |
| `GpuFileFormatWriter` (covers 400db173) | `sql-plugin/src/main/spark332db/.../GpuFileFormatWriter.scala` |
| Root Maven profile | `pom.xml` (inside `#if scala-2.13` block, ~lines 633–654) |
| Scala 2.13 root pom | `scala2.13/pom.xml` |
| Fallback stub module for unsupported Delta pairings | `delta-lake/delta-stub/` |
| Scala version sync script | `build/make-scala-version-build-files.sh` |
| **Jenkins test skip (to remove)** | `jenkins/databricks/test.sh` lines 152–160 |
