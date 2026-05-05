# GitHub Issues for Delta Lake DB-17.3 Support

Parent tracking issue: https://github.com/NVIDIA/spark-rapids/issues/14015

**Scope history:**
- **2026-04-23 (initial):** Issue 1 was originally "enable GPU writes on DB-17.3".
  Cluster validation showed DB-14.3's `txn.writeFiles` interception point was dead code
  on DB-17.3 because `WriteIntoDeltaEdge.write(txn)` had been re-wired. Issue 1 was
  re-scoped to **module scaffolding + CPU fallback only**, and Issue 2 was introduced to
  do the actual GPU write work.
- **2026-04-23 (later):** Issue 2 delivered — see the Issue 2 section for the
  implementation that landed. Additionally, two new follow-up issues were filed that
  surfaced during Issue 2 cluster validation: Issue 8 (GPU Delta read relative-path
  `FileNotFoundException`) and Issue 9 (GPU `GenerateIdentityValues` expression).
- **2026-04-24 (Issue 8 resolved):** The Issue 1 build-fixes commit also carries the
  read-path fix for Issue 8: `Spark400PlusDBShims.getPartitionFiles` and
  `FilePartitionShims.getFiles` now use `FilePartition.filesWithAbsolutePaths`, and a
  new `FilePartitionShims.withPathPrefixIfNeeded` helper restores `pathPrefix` (which
  the 2-arg `FilePartition.getFilePartitions` factory drops) from
  `relation.location.rootPaths`. Issue 7 no longer depends on Issue 8.
- **2026-04-24 (PR slim-down):** The Issue 1 PR was slimmed to write-path only.
  `GpuMergeIntoCommand`, `GpuOptimizeExecutor`, `GpuDoAutoCompaction` were deleted;
  `GpuDeleteCommand` / `GpuUpdateCommand` reduced to stubs (shared common code references
  their constructors at compile time). Full implementations return in follow-up issues
  #14597 / #14598 / #14599. Net PR shrink: ~4,597 → ~2,289 lines.
- **2026-04-28 (Issue 10 resolved):** Of the 6 `delta_lake_write_test.py` failures
  filed as Issue 10, Group B (constraint-check tests, 2 tests) is fixed in shared
  `GpuCheckDeltaInvariant.scala` (`ExprChecks.unaryProject` →
  `ExprChecks.projectOnly + paramCheck + repeatingParamCheck`, matching the OSS Delta
  rule); Group A (4 tests routed through V1 `WriteIntoDeltaCommand`) is folded into
  pre-existing #11169 and xfailed on DB-17.3. `delta_lake_write_test.py` is now green
  on DB-17.3. Issue 7 no longer blocks on Issue 10.
- **2026-04-30 (PR review hardening):** Static review against the live DB-17.3 JARs
  found DB-17.3 create-table semantics that the old GPU `GpuCreateDeltaTableCommand`
  must not silently drop. `DeltaSpark400DB173Provider.tagForGpu` now falls back for all
  DB-17.3 CTAS/RTAS. The feature-specific TableSpec/property checks remain as defensive
  tags for row filters, column masks, liquid clustering, auto TTL, catalog-owned tables,
  coordinated commits, and deletion vectors. Explicit Delta property-key checks are
  case-insensitive, and invalid DV boolean values also fall back so Delta's CPU validation
  reports the error. Shared `GpuIdentityColumn` also preserves literal identity-column
  names by using `DFUDFShims.exprToColumn(UnresolvedAttribute.quoted(name))` instead of
  `col(name)`.
- **2026-05-02 (Issue 6 implemented):** DB-17.3 GPU deletion-vector reads now use the
  V1/materialized path: GPU Parquet read, per-file Databricks DV row-index filter
  materialization, and a GPU skip-row filter above the scan. `delta_lake_test.py` and
  `delta_lake_delete_test.py` were green in local DB-17.3 validation.
- **2026-05-05 (Issue 6 native follow-up implemented):** DB-17.3 now also has the
  OSS Delta-4.0-style native cuDF DV read path for PERFILE, MULTITHREADED, and
  COALESCING, plus narrow DBR-specific DV predicate pushdown behind the DBR
  metadata-row-index and RAPIDS DV pushdown gates. Native footer multi-row-group,
  count-star, delete/read, and scan-split validation passed locally. CDC + DV reads,
  CI enablement, and newly discovered DBR plan shapes remain follow-up work.

---

## Issue 1: [databricks] Scaffold `delta-spark400db173` module (build-only, CPU fallback)

### Description

Create the `delta-lake/delta-spark400db173/` Maven module so the `release400db173` profile
can build without `delta-stub`, and so Delta Lake plan metas exist in a state where every
Delta operation (write, DELETE, UPDATE, MERGE, OPTIMIZE, DV read) falls back cleanly to
CPU. No operation is GPU-accelerated in this issue — this is pure scaffolding.

The 22 Scala files must all be present together because the shared
`DatabricksDeltaProviderBase` registers metas for every command class; any missing class
would be a compile failure.

### Scope

**New module:** `delta-lake/delta-spark400db173/` (after 2026-04-24 slim-down)
- `pom.xml`
- 3 provider/probe files (`DeltaProbe`, `DeltaSpark400DB173Provider`, `GpuDeltaParquetFileFormat`)
- 7 shim files (`DeleteCommandMetaShim`, `UpdateCommandMetaShim`, `MergeIntoCommandMetaShim`,
  `DeltaLogShim`, `MetadataShims`, `ShimDeltaUDF`, `InvariantViolationExceptionShim`,
  `ShimShuffledRowRDD`)
- Write-path command/transaction files (live on GPU):
  `GpuOptimisticTransactionBase`, `GpuOptimisticTransaction`, `GpuWriteIntoDelta`,
  `GpuDeltaFileFormatWriter`, `GpuCreateDeltaTableCommand`, `GpuDeltaDataSource`,
  `GpuDeltaCatalog`
- DML stubs (compile-only, CPU fallback): `GpuDeleteCommand` (~30 LOC),
  `GpuUpdateCommand` (~35 LOC). Required because shared
  `delta-lake/common/src/main/databricks/.../DeleteCommandMeta.scala` /
  `UpdateCommandMeta.scala` directly construct them.
- Deferred to follow-up issues (no file in this PR): `GpuMergeIntoCommand` (→ #14598),
  `GpuOptimizeExecutor` (→ #14599), `GpuDoAutoCompaction` (→ #14599).
- Backward-compatible shared-code patches (`GpuCheckDeltaInvariant`, `GpuDeltaLog`,
  `GpuIdentityColumn`, `OptimizeWriteExchangeExec`)
- `ShimShuffledRowRDD` added to `delta-spark{330db,332db,341db,350db143}` so the shared
  `OptimizeWriteExchangeExec` keeps compiling

**Build system changes:**
- Root `pom.xml`: swap `delta-stub` → `delta-spark400db173` inside the `#if scala-2.13` block
- `scala2.13/` mirror generated via `./build/make-scala-version-build-files.sh 2.13`

**Spark 4.0 / DB-17.3 API fixes applied to all module files:**
- `new Column(expr)` → `DFUDFShims.exprToColumn(expr)`
- `Dataset.ofRows(spark, plan)` → `TrampolineConnectShims.createDataFrame(...)`
- Remove `(implicit clock: Clock)` from transaction constructors
- `writeFiles()` signature rewrite for the new 7-arg `TransactionalWriteOptions` form
- `PostCommitHook.run()` rewrite for `CommittedTransaction`
- `LogicalRelation` pattern match arity 4 → 7
- Command Edge constructor changes (`catalogTable`, `targetFileIndex`,
  `trackHighWaterMarks`, `schemaEvolutionEnabled`)
- `RuntimeReplaceable` stats expression handling
- `DeltaInvariantCheckerExec` 2-arg → 3-arg

**Command shims — all fall back to CPU:**
- `DeleteCommandMetaShim`, `UpdateCommandMetaShim`, `MergeIntoCommandMetaShim` all call
  `willNotWorkOnGpu(...)` unconditionally
- `GpuDeltaParquetFileFormat` blocks GPU for any DV table via
  `tagSupportForGpuFileSourceScan`

**GPU-enabled after Issue 1 + Issue 2 merge (this PR):** DB-17.3 path-style/V1
Delta writes (append / overwrite / partitioned, including path-style `INSERT` /
`INSERT OVERWRITE`). **CPU fallback:** CTAS, RTAS, DELETE, UPDATE, MERGE, OPTIMIZE,
DV reads.

### Key constraint

DB-17.3 is **Scala 2.13 only**. All builds require `-f scala2.13 -Dbuildver=400db173`.

### Tests

Delta Lake integration tests remain **skipped** in CI for `spark400db173` via the existing
guard at [jenkins/databricks/test.sh:152-160](jenkins/databricks/test.sh). The skip stays
in place until Issue 6 (see below).

### Build verification

```bash
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
# or
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests
```

### References

- Copy source: `delta-lake/delta-spark350db143/` (DB-14.3)
- Spark 4.0 API patterns: `delta-lake/delta-40x/.../Delta40xCommandShims.scala`
- Design doc: `delta-lake-db173-design.md`
- Implementation context: `delta-lake-db173-implementation-context.md`
- Checkpoint: `delta-lake-db173-checkpoint.md`

---

## Issue 2: [databricks] GPU Delta writes on DB-17.3 — **DELIVERED 2026-04-23**

### Description

Enable GPU-accelerated path-style/V1 Delta Lake write operations (append, overwrite,
partitioned writes, and path-style `INSERT` / `INSERT OVERWRITE`) on Databricks 17.3.
CTAS/RTAS remain CPU fallback until the DB-17.3 create-table command is ported more
faithfully. Split from Issue 1 once cluster validation showed the DB-14.3 interception
point was dead code on DB-17.3.

### Background — why this is its own issue

DB-14.3 (Spark 3.5) interception pipeline:
```
AppendDataExecV1 / OverwriteByExpressionExecV1
  → GpuAppendDataExecV1 / GpuOverwriteByExpressionExecV1
  → GpuDeltaV1Write.insert(data, overwrite)
  → WriteIntoDeltaEdge(...)
  → GpuWriteIntoDelta.run(spark)
  → WriteIntoDeltaEdge.write(txn)
  → txn.writeFiles(data, ...)           ← DB-14.3 GpuOptimisticTransaction override catches here
```

DB-17.3 (Spark 4.0) actually wires `WriteIntoDeltaEdge.write(txn)` through a different
entry point (confirmed by `javap` of the live JAR's bytecode):
```
SaveIntoDataSourceCommand (V1 save / INSERT OVERWRITE)
  → GpuDeltaDataSource.createRelation / V1 fallback writer
  → GpuWriteIntoDelta.run(spark)
  → WriteIntoDeltaEdge.write(txn)
  → WriteIntoDeltaLike.write → writeAndReturnCommitData
  → WriteIntoDeltaEdge.writeAndReturnCommitDataAndMaterializationPlans
  → WriteIntoDeltaEdge.writeFilesAndGetMaterializationPlans
  → ClusteredWriter.runAndReturnMaterializationPlans / runInternal
  → ClusteredWriter.writeFilesWithoutClustering
  → txn.writeFilesAndGetExecutedPlan(...)      ← primary entry point
```

`txn.writeFilesAndGetQueryExecution(...)` is also exposed and may be reached via CTAS /
RTAS paths; we override both for safety.

### Delivered scope

**Three DB-17.3-only file changes** — no older Databricks shim or OSS module touched.

#### Change 1: [GpuOptimisticTransaction.scala](delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/GpuOptimisticTransaction.scala)

Extract the body of the legacy 3-arg `writeFiles` (the DB-14.3 GPU write pipeline — `RapidsDeltaWrite`
stub + `GpuFileFormatWriter.write` + `GpuDeltaJobStatisticsTracker` + `GpuIdentityColumn`
stats) into a shared `gpuWriteFiles(inputData, writeOptions, additionalConstraints,
isOptimizeOverride): (Seq[FileAction], QueryExecution)` helper. Three entry points
converge on it:
- `writeFilesAndGetExecutedPlan` — primary DB-17.3 entry point. Returns `(actions, qe.executedPlan)`.
- `writeFilesAndGetQueryExecution` — sibling entry point. Returns `(actions, qe)` directly.
- 3-arg `writeFiles` — DML entry point (DELETE/UPDATE/MERGE/OPTIMIZE call this directly).
  Thin delegator discarding the QE.

Both new overrides guard `isLiquidClustering || isCDCWritePhase` and `super.*` to CPU in
those cases.

Also: route the actual write through `GpuDeltaFileFormatWriter.write(...)` (new file,
below) instead of `GpuFileFormatWriter.write(...)`.

#### Change 2: [GpuDeltaFileFormatWriter.scala](delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/GpuDeltaFileFormatWriter.scala) — **NEW**

Overrides `createTaskAttemptContext` on `GpuFileFormatWriterBase` to produce
`com.databricks.sql.transaction.tahoe.files.DeltaFileFormatWriter.PartitionedTaskAttemptContextImpl`
(carries `partitionColToDataType` map) when the write has partition columns, and a plain
`TaskAttemptContextImpl` otherwise. Required because DB-17.3's
`DelayedCommitProtocol.parsePartitions` hard-casts to this subtype when TIMESTAMP
partition columns need UTC handling — without it, partitioned writes fail with
`ClassCastException`. Mirrors the OSS `delta-33x` / `delta-40x` `GpuDeltaFileFormatWriter`
pattern with Databricks namespace substitution.

#### Change 3: [GpuWriteIntoDelta.scala](delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/GpuWriteIntoDelta.scala)

Stamp `NoRowsCopiedTag = true` on the commit (plain writes copy no rows; skip when
`replaceWhere` is set). Uses the existing `DMLUtils.TaggedCommitData(actions).withTag(...)
→ .stringTags → 3-arg txn.commit(actions, op, tags)` pattern from `GpuDeleteCommand`.
Needed for Delta-log parity with CPU — CPU's `writeFilesAndGetExecutedPlan` default body
sets this tag via a private `recordWriteFilesOperation` helper, which our override
bypasses.

### Confirmed DB-17.3 signatures

```scala
// com.databricks.sql.transaction.tahoe.files.TransactionalWriteEdge
def writeFilesAndGetQueryExecution(
    data: Dataset[_], writeOptions: TransactionalWriteOptions,
    isOptimize: Boolean, isLiquidClustering: Boolean,
    additionalConstraints: Seq[Constraint],
    isCDCWritePhase: Boolean, context: Option[String], trailing: Boolean
  ): (Seq[FileAction], QueryExecution)

def writeFilesAndGetExecutedPlan(              // primary DB-17.3 call site
    data: Dataset[_],
    writeOptions: Either[Option[DeltaOptions], TransactionalWriteOptions],
    isOptimize: Boolean, isLiquidClustering: Boolean,
    additionalConstraints: Seq[Constraint],
    context: Option[String], trailing: Boolean
  ): (Seq[FileAction], SparkPlan)

def writeFiles(                                // DML legacy overload — still used
    Dataset[_], Option[DeltaOptions], Seq[Constraint]
  ): Seq[FileAction]
```

Class hierarchy confirmed: `OptimisticTransaction implements OptimisticTransactionImplEdge
extends TransactionalWriteEdge`. Default trait methods are overridable via virtual
dispatch.

### Enabled after this issue

DB-17.3 path-style/V1 Delta writes through `.write.format("delta").save(...)`,
append/overwrite, partitioned writes, and path-style `INSERT` /
`INSERT OVERWRITE delta.\`<path>\``. Partitioned writes work (including timestamp
partitions). CTAS/RTAS remain CPU fallback for now.

### Still on CPU after this issue

DELETE, UPDATE, MERGE, OPTIMIZE (Issues 3/4/5), DV reads (Issue 6). Plus writes to
identity-column tables currently block `ProjectExec` to CPU via `GenerateIdentityValues`
(Issue 9).

### PR review hardening added 2026-04-30

The first PR keeps the DB-17.3 GPU create-table command conservative. The CPU
DB-17.3 `CreateDeltaTableCommand` has newer behavior for row filters, column masks,
liquid clustering, auto TTL, catalog-owned tables, coordinated commits, and default DV
enablement. Until the DB-17.3 create command is ported more faithfully, all DB-17.3
CTAS/RTAS GPU conversion falls back to CPU. The TableSpec/property checks remain as
defensive tags for those individual semantics; explicit Delta property-key checks are
case-insensitive, and invalid DV boolean values also fall back so Delta's CPU validation
reports the error. DV auto-enable triggers driven by Databricks table-shape heuristics
remain tracked under #14601.

### Validated on cluster

- `test_delta_overwrite_by_expression_exec_v1` — 4/4 pass.
- `test_delta_write_round_trip_unmanaged[False]` — write runs on GPU (~3.5× speedup),
  Delta log parity confirmed.
- `test_delta_part_write_round_trip_unmanaged` — unblocked by `PartitionedTaskAttemptContextImpl`.

Three pre-existing GPU Delta **read** issues surfaced once writes are enabled — tracked
separately as Issue 8.

### Commands

```bash
# Build (Scala 2.13 only)
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
# Iterative:
mvn -f scala2.13/pom.xml -Dbuildver=400db173 \
  -pl dist,delta-lake/delta-spark400db173 -am install -DskipTests

# Test
DRIVER_MEMORY="4g" bash integration_tests/run_pyspark_from_build.sh \
  --runtime_env=databricks --delta_lake --test_type=$TEST_TYPE \
  -k test_delta_overwrite_by_expression_exec_v1
```

### Depends on

Issue 1.

---

## Issue 3: [databricks] Delta Lake DB-17.3: Enable GPU DELETE + UPDATE

### Description

Enable GPU-accelerated DELETE and UPDATE commands for Delta Lake on Databricks 17.3.
After the 2026-04-24 slim-down, `GpuDeleteCommand` / `GpuUpdateCommand` exist only as
~30-line stubs (compile-only; `run()` throws); this issue **re-adds the full
implementations** (Spark 4.0 API adapters + DB-17.3-specific runtime fixes) and wires
them into the shims.

### Scope

- Re-add `GpuDeleteCommand.scala` (full body: touched-files discovery, rewrite, DMLUtils
  tagging, CDF path). Port Spark 4.0 patterns from `delta-spark350db143/` + the earlier
  draft: `DFUDFShims.exprToColumn`, `TrampolineConnectShims.createDataFrame`,
  `LogicalRelation` 7-arg pattern match, `DMLUtils.TaggedCommitData[FileAction]`, etc.
- Re-add `GpuUpdateCommand.scala` similarly.
- Update `DeleteCommandMetaShim.scala` to enable the GPU path for DELETE (add the
  DV-write CPU-guard from DB-14.3 and the `convertToGpu(DeleteCommand)` /
  `convertToGpu(DeleteCommandEdge)` methods). DB-17.3's `DeleteCommandEdge` adds a
  `catalogTable` field — propagate it.
- Update `UpdateCommandMetaShim.scala` similarly.
- Remove the Issue-1 unconditional `willNotWorkOnGpu` in both shims.
- Verify on cluster; fix runtime issues as they surface.

Note: DELETE/UPDATE with persistent deletion vectors
(`DELETE_USE_PERSISTENT_DELETION_VECTORS`, `UPDATE_USE_PERSISTENT_DELETION_VECTORS`)
continues to fall back to CPU (existing behavior from DB-14.3,
https://github.com/NVIDIA/spark-rapids/issues/8654).

### Tests

Run on a DB-17.3 cluster:
- `delta_lake_delete_test.py` (non-DV cases)
- `delta_lake_update_test.py`

### Depends on

Issue 2

---

## Issue 4: [databricks] Delta Lake DB-17.3: Enable GPU MERGE INTO

### Description

Enable GPU-accelerated MERGE INTO command for Delta Lake on Databricks 17.3. Largest DML
by code size (~1,206 lines); warrants separate review. After the 2026-04-24 slim-down,
**`GpuMergeIntoCommand.scala` does not exist in tree** — this issue re-adds it.

### Scope

- Re-add `GpuMergeIntoCommand.scala` (copy from `delta-spark350db143/` + Spark 4.0
  adapters: `DFUDFShims.exprToColumn`, `DFUDFShims.columnToExpr`,
  `TrampolineConnectShims.createDataFrame`, `LogicalRelation` 7-arg pattern,
  `recacheByPlan` via classic session). Align constructor to DB-17.3's
  `MergeIntoCommand` / `MergeIntoCommandEdge` fields (including `catalogTable`,
  `targetFileIndex`, `trackHighWaterMarks`, `schemaEvolutionEnabled`).
- Update `MergeIntoCommandMetaShim.scala` to enable the GPU path: remove the
  unconditional `willNotWorkOnGpu` in `tagForGpu` and replace the stub-throwing
  `convertToGpu` with a real conversion that constructs the re-added
  `GpuMergeIntoCommand` via the DB-17.3 signature.
- Verify on cluster.

Note: `notMatchedBySourceClauses` continues to fall back to CPU
(https://github.com/NVIDIA/spark-rapids/issues/8415).

### Tests

Run on a DB-17.3 cluster:
- `delta_lake_merge_test.py`

### Depends on

Issue 2

---

## Issue 5: [databricks] Delta Lake DB-17.3: Enable GPU OPTIMIZE + auto-compaction

### Description

Enable GPU-accelerated OPTIMIZE command and auto-compaction post-commit hook for
Delta Lake on Databricks 17.3. Includes liquid clustering support. After the 2026-04-24
slim-down, **`GpuOptimizeExecutor.scala` and `GpuDoAutoCompaction.scala` do not exist
in tree** — this issue re-adds them.

### Scope

- Re-add `GpuOptimizeExecutor.scala` (copy from `delta-spark350db143/` + Spark 4.0
  adapters).
- Re-add `GpuDoAutoCompaction.scala` — the DB-17.3 `PostCommitHook` signature change is
  `run(spark, CommittedTransaction)` (instead of the DB-14.3 5-arg form) and
  `handleError(spark, error, version)` gains a `SparkSession` param. Extract
  `deltaLog` and `committedActions` from `CommittedTransaction`; build `RapidsConf` from
  `spark.sessionState.conf`.
- Restore `registerPostCommitHook(GpuDoAutoCompaction)` in
  `GpuOptimisticTransaction.gpuWriteFiles` (with the `autoCompactEnabled` guard
  reading `DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED` / `DeltaConfigs.AUTO_COMPACT`).
- Wire OPTIMIZE through the command meta / shim.
- Fix runtime issues as they surface.

### Tests

Run on a DB-17.3 cluster:
- `delta_lake_optimize_table_test.py`
- `delta_lake_liquid_clustering_test.py`

### Depends on

Issue 2

---

## Issue 6: [databricks] Delta Lake DB-17.3: GPU Deletion Vector reads

**Status:** Done locally in two checkpoints: V1/materialized DV reads on
2026-05-02 and native cuDF DV read parity on 2026-05-05. Issue 6 validated the
targeted Delta read/delete, native footer, count-star, and scan-split suites on
a DB-17.3 cluster. See
`delta-lake-db173-issue-6-dv-read-plan.md` for the implementation record.

### Description

Enable GPU-accelerated Deletion Vector (DV) reads for Delta Lake on Databricks 17.3.

DB-17.3 uses a fundamentally different DV mechanism than DB-14.3:
- DB-14.3: Broadcast `Map[URI, DeletionVectorDescriptorWithFilterType]` (GPU explicitly blocked DVs)
- DB-17.3: Per-file DVs via `PartitionedFile.otherConstantMetadataColumnValues` with
  `RowIndexFilterProvider` interface

DB-17.3 is now the **first Databricks module** with GPU-accelerated DV reads.

### Delivered Scope

**DB-17.3-local V1/materialized DV read path:**
- `GpuDeltaParquetFileFormat.scala` now mirrors the DB-17.3 CPU file-format flags and
  extends the DV-aware local reader.
- `GpuDeltaParquetFileFormatDV.scala` uses per-file DV metadata from
  `PartitionedFile.otherConstantMetadataColumnValues`, Databricks
  `RowIndexFilter`/`RowIndexFilterProvider` APIs, and the DB edge skip-row column.
- `createMultiFileReaderFactory` supports the multi-threaded DV reader path.
- The unconditional GPU-unsupported DV tag is removed; CDC + DV and unsupported
  DB metadata paths still fall back.
- `DeltaSpark400DB173Provider` now recognizes the DB-17.3 GPU Delta format, detects
  DV scans, prunes metadata columns, and enables native DV pushdown only when DBR
  metadata-row-index mode and RAPIDS DV predicate pushdown are both enabled.
- `GpuDeltaParquetFileFormatNativeDV.scala` adds native cuDF DV scans for PERFILE,
  MULTITHREADED, and COALESCING.
- `RapidsDeletionVectors.scala` now supports native lookup through `PartitionedFile`,
  `RowIndexFilterProvider`, and `TahoeFileIndex`.
- `DeltaBitmapUtils.java` converts DBR `SerializedBitmap` data to cuDF-compatible
  standard portable roaring bitmap bytes.

**Blueprint:** `delta-lake/common/src/main/delta-33x-40x/scala/.../GpuDeltaParquetFileFormatBase.scala`
(with Databricks namespace substitutions — `delta-33x-40x` code uses OSS imports and cannot
be compiled for DB-17.3)

### Remaining Follow-up Work

- CDC + DV reads
- DB-17.3 CI Delta enablement (Issue 7)
- Review/extend the narrow DBR predicate recognizer if Databricks changes the
  skip-row expression shape
- Revisit native DV lookup if DBR stops exposing DV information through
  `PartitionedFile`, `RowIndexFilterProvider`, or `TahoeFileIndex`

### Tests

Validated on a DB-17.3 cluster:
- Build passed:
  `SKIP_DEP_INSTALL=1 WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`
  (`/tmp/db173-build-final-review-cleanups.log`)
- Focused post-cleanup DV validation passed:
  `delta_lake_delete_test.py::test_delta_deletion_vector_read`
  + `delta_lake_test.py::test_delta_empty_deletion_vector_read`
  (`57 passed, 35 warnings in 182.72s`;
  `/tmp/db173-dv-focused-after-cleanups.log`)
- Full `delta_lake_test.py` passed before final cleanup:
  `180 passed, 40 skipped, 13 xpassed, 34 warnings in 726.67s`
  (`/tmp/db173-delta-lake-full-final2.log`)
- Full `delta_lake_delete_test.py` passed before final cleanup:
  `40 passed, 16 xfailed, 28 warnings in 276.22s`
  (`/tmp/db173-delta-lake-delete-full-final2.log`)
- Native follow-up validation passed:
  `mvn -B -f scala2.13/pom.xml -Ddatabricks -Dbuildver=400db173 package -pl dist -am -DskipTests -Dmaven.scaladoc.skip`,
  `test_delta_deletion_vector_native_footer_multi_row_group`,
  `test_delta_deletion_vector_native_footer_multi_row_group_count_star`,
  `test_delta_deletion_vector`, `test_delta_deletion_vector_read`, and
  `test_delta_scan_split_with_DV_enabled_with_DVs`.

### Depends on

Issue 2. Issue 6 was implemented before Issues 3-5 because customer priority is
read-heavy DV table access. Full CI enablement still waits for the broader Issue
7 matrix.

---

## Issue 8 (NEW): [databricks] DB-17.3 GPU Delta read path: FileNotFoundException on relative paths — **RESOLVED 2026-04-24**

### Description

Surfaced during Issue 2 cluster validation. On DB-17.3, GPU reads of Delta tables
occasionally hit `java.io.FileNotFoundException: File part-00000-<uuid>-c000.snappy.parquet
does not exist` — note the **path has no directory prefix**. The file itself exists on
disk at `<table-root>/part-00000-...`; the read path is not resolving it against the
table root.

### Stack-trace fingerprint

```
java.io.FileNotFoundException: File part-00000-<uuid>-c000.snappy.parquet does not exist
  at org.apache.hadoop.fs.RawLocalFileSystem.deprecatedGetFileStatus
  at org.apache.parquet.hadoop.ParquetFileReader.readFooter
  at com.nvidia.spark.rapids.parquet.GpuParquetFileFilterHandler.readAndSimpleFilterFooter(GpuParquetScan.scala:663)
  at com.nvidia.spark.rapids.parquet.GpuParquetFileFilterHandler.filterBlocks(GpuParquetScan.scala:710)
  at ...AbstractGpuParquetMultiFilePartitionReaderFactory.readBlockMetasForCoalescing
  at ...MultiFilePartitionReaderFactoryBase.createColumnarReader
  at GpuFileSourceScanExec
```

### Triggering scenarios observed

1. **Multi-file Delta table + global-sort read-back.** Test writes 2+ files (e.g.
   `do_update_round_trip_managed`), then reads back with `@ignore_order` (global). CPU
   `RangePartitioner.sketch` samples each partition's file via `GpuFileSourceScanExec`;
   the sampling hits the resolution bug. Tests: `test_delta_overwrite_round_trip_unmanaged`,
   `test_delta_append_round_trip_unmanaged`.

2. **`INSERT OVERWRITE delta.<dst> SELECT * FROM delta.<src>`.** The write task reads the
   source Delta table via GPU; source file resolution fails. Test:
   `test_delta_overwrite_schema_evolution_arrays`.

3. **Sort-fallback read-back on binary-heavy Delta tables.** Any Delta read where
   SortExec falls back to CPU (e.g. because of `BinaryType` on the sort key) and the
   resulting sort-sampling passes relative paths to `GpuFileSourceScanExec`.

### Why this is not an Issue 2 regression

- The earlier passing `test_delta_overwrite_by_expression_exec_v1` also does
  `INSERT OVERWRITE delta.<path> SELECT * FROM <src>`, but `<src>` is a plain Parquet
  temp view, not a Delta table. GPU reads of plain Parquet work.
- Under Issue 1 (writes fell back to CPU), these same tests would have failed the same
  way if they had run — reads already went through GPU. They just didn't run in CI
  because the full Delta test suite is skipped for DB-17.3 ([test.sh:152-160](jenkins/databricks/test.sh)).

### Hypothesis (kept for history)

`GpuParquetFileFilterHandler.readAndSimpleFilterFooter` is being handed a relative path
by the upstream `FileIndex` / listing on DB-17.3's Delta. Either the DB-17.3
`TahoeFileIndex` changed how it materializes absolute paths, or there's a
GPU-scan-specific code path that takes `AddFile.path` (stored relative in the log) and
doesn't resolve it against `deltaLog.dataPath`.

### Confirmed root cause + fix (2026-04-24)

DB-17.3 Delta / UC-managed tables store **bare filenames** in `FilePartition.innerFiles`
and use `FilePartition.pathPrefix` for absolute resolution. Two GPU shim methods were
reading `innerFiles` directly. Additionally, `GpuFileSourceScanExec.createNonBucketedReadRDD`
recreates partitions via `FilePartition.getFilePartitions(...)` — the 2-arg factory drops
`pathPrefix`, so even `filesWithAbsolutePaths` would not resolve correctly without
restoring the prefix.

**Fix (two DB-17.3-only shim files):**

- [Spark400PlusDBShims.scala](sql-plugin/src/main/spark400db173/scala/com/nvidia/spark/rapids/shims/Spark400PlusDBShims.scala) —
  `getPartitionFiles(partition)` returns `partition.filesWithAbsolutePaths.toSeq` (was
  `partition.innerFiles`).
- [FilePartitionShims.scala](sql-plugin/src/main/spark400db173/scala/org/apache/spark/sql/execution/rapids/shims/FilePartitionShims.scala) —
  `getFiles(p)` returns `p.filesWithAbsolutePaths` (was `p.innerFiles`); new
  `withPathPrefixIfNeeded(partitions, relation)` helper restores `pathPrefix` from
  `relation.location.rootPaths` (single-root case) for partitions produced by
  `FilePartition.getFilePartitions`. No-op on all other shims.

Landed in the same commit as Issue 1 build-fixes (`3b1a1d470` on
`databricks_173_delta_lake_issue_1a`).

### Investigation starting points

- [GpuParquetFileFilterHandler.readAndSimpleFilterFooter](sql-plugin/src/main/scala/com/nvidia/spark/rapids/parquet/GpuParquetScan.scala) around line 663 — what `Path` object does it receive?
- How `GpuFileSourceScanExec` converts `PartitionedFile` → absolute path for Delta, compared to Parquet.
- The DB-17.3 `TahoeFileIndex.listFiles(...)` return shape vs. DB-14.3.

### Scope

Either:
- Make `GpuParquetFileFilterHandler` robust to relative paths by resolving against the
  table root (passed via `PartitionedFile.filePath` / containing scan's table location), OR
- Fix the upstream absolute-path materialization for DB-17.3 Delta in `GpuFileSourceScanExec`
  / `GpuDeltaParquetFileFormat`.

### Workaround (no longer needed — fix has landed)

Previously suggested XFAIL of the three affected tests is **not required**; the fix above
addresses the root cause directly.

### Depends on

Independent. Was a blocker for Issue 7; now resolved, so Issue 7 only depends on Issues 3–6.

---

## Issue 9 (NEW): [databricks] GPU `GenerateIdentityValues` expression for DB-17.3

### Description

On DB-17.3, writes to Delta tables with `GENERATED ALWAYS AS IDENTITY` columns inject
`com.databricks.sql.transaction.tahoe.GenerateIdentityValues` (a `LeafExpression with
Nondeterministic` wrapping a `PartitionIdentityValueGenerator`) into the write query's
`ProjectExec`. Our GPU plugin has no meta for this expression, so `ProjectExec` falls back
to CPU, defeating the GPU write path for identity-column tables.

### Not an urgent issue

- DB-14.3 tests for identity columns currently pass — the HWM stats tracker in
  `GpuIdentityColumn` plus a (different) analyzer path on DB-14.3 handle it correctly.
- On DB-17.3 the outcome is graceful: `ProjectExec` runs on CPU, data is still correct,
  but the whole write pipeline regresses to CPU.

### Scope (two options, pick one)

**Option A — GPU expression (preferred long-term):** New `GpuGenerateIdentityValues`
under `delta-spark400db173`, modeled on `GpuMonotonicallyIncreasingID` (cuDF
`ColumnVector.sequence(start, step, n)`). Extract `start` / `step` / `partitionOffset`
from the wrapped `PartitionIdentityValueGenerator` via reflection at meta convert time.
Register in `DeltaSpark400DB173Provider.getExprs`.

**Option B — CPU fallback at tag time:** Detect identity columns in a DB-17.3-only
shim of the write tagging path (NOT in shared `RapidsDeltaUtils.tagForDeltaWrite`, which
would regress DB-14.3). Simple, leaves identity writes on CPU.

### Depends on

Issue 2. Nice-to-have before Issue 7.

---

## Issue 10: [databricks] DB-17.3 `GpuCheckDeltaInvariant` arity meta + `DeltaInvariantChecker` operator GPU conversion — **RESOLVED 2026-04-28**

### Status

The original hypothesis (one shared root cause for both groups) was wrong. The two
groups had separate root causes and were resolved independently:

- **Group B (constraint-check tests)** — fixed in shared
  [GpuCheckDeltaInvariant.scala](delta-lake/common/src/main/databricks/scala/com/databricks/sql/transaction/tahoe/rapids/GpuCheckDeltaInvariant.scala)
  via `ExprChecks.unaryProject` → `ExprChecks.projectOnly + paramCheck +
  repeatingParamCheck` (matches the OSS Delta counterpart at
  [delta-lake/common/src/main/delta-io/scala/org/apache/spark/sql/delta/rapids/GpuCheckDeltaInvariant.scala](delta-lake/common/src/main/delta-io/scala/org/apache/spark/sql/delta/rapids/GpuCheckDeltaInvariant.scala)).
  Backward compatible with older Databricks shims (`UnaryExpression`, single child).
  Tests `test_delta_write_constraint_check{,_fallback}` now pass.
- **Group A (overwrite mixed-clause / replaceWhere tests)** — folded into upstream
  [#11169](https://github.com/NVIDIA/spark-rapids/issues/11169). DB-17.3's analyzer
  routes both SQL forms (`INSERT OVERWRITE TABLE delta.<path> PARTITION (...)` and
  `df.write.option("replaceWhere", ...)`) through the Databricks-only V1
  `WriteIntoDeltaCommand`. That path runs against a pre-planned `SparkPlan` and never
  invokes our `txn.writeFilesAndGetExecutedPlan` overrides, so `gpuWriteFiles` never
  runs and `RapidsDeltaWrite` never enters the plan — the same gap #11169 already
  tracks for `df.write.saveAsTable(...)` (xfailed under #11169 in
  `test_delta_write_round_trip_managed`). Tests
  `test_delta_overwrite_mixed_clause[...-STATIC]` × 3 and
  `test_delta_write_partial_overwrite_replace_where` are xfailed for
  `is_databricks173_or_later()` referencing #11169.

Issue 10 can be closed. The proper code-level GPU acceleration of the V1
`WriteIntoDeltaCommand` path is tracked under #11169 as a Group-A follow-up.

### Description (original, kept for history)

First post-Issue-2 / post-Issue-8 run of `delta_lake_write_test.py` on a live DB-17.3
cluster surfaced 6 test failures, all in the `CheckDeltaInvariant` /
`DeltaInvariantChecker` code path. Two distinct symptoms, likely shared root cause:
the shared `GpuCheckDeltaInvariant` expression meta declares 1 child but DB-17.3's
`CheckDeltaInvariant` is built as a 3-child node.

The `columnExtractors` type change for DB-17.3 (`Map[String, Expression]` →
`Seq[(String, Expression)]`) was already addressed in 7.1.13 of the design doc — the
arity claim in the meta's `ExprChecks` registration was missed.

### Failing tests (2026-04-28 run, 6 of 6 failures in `delta_lake_write_test.py`)

**Group A — runtime `IllegalArgumentException: Part of the plan is not columnar class
DataWritingCommandExec` (4 tests):**

| Test | Variant |
|------|---------|
| `test_delta_overwrite_mixed_clause` | `[PARTITION (id, p = 2)-STATIC]` |
| `test_delta_overwrite_mixed_clause` | `[PARTITION (p = 2, id)-STATIC]` |
| `test_delta_overwrite_mixed_clause` | `[PARTITION (p = 2)-STATIC]` |
| `test_delta_write_partial_overwrite_replace_where` | — |

The CPU `DeltaInvariantChecker` operator is sandwiched between GPU operators in the
captured plan:

```
Execute WriteIntoDeltaCommand
+- WriteFiles
   +- GpuColumnarToRow
      +- GpuSort [id ASC, p ASC]
         +- GpuRowToColumnar
            +- DeltaInvariantChecker [checkdeltainvariant((p <=> 2), ...)]   ← CPU
               +- GpuColumnarToRow
                  +- GpuProject [id, data, 2 AS p]
                     +- GpuRowToColumnar
                        +- Scan ExistingRDD
```

The CPU operator inside the GPU plan trips the `WriteFiles` columnar/row mismatch
assertion at runtime.

**Group B — planning `AssertionError: CheckDeltaInvariant expected 1 but found 3`
(2 tests):**

| Test |
|------|
| `test_delta_write_constraint_check` |
| `test_delta_write_constraint_check_fallback` |

```
Caused by: java.lang.AssertionError: assertion failed: CheckDeltaInvariant expected 1 but found 3
  at scala.Predef$.assert(Predef.scala:279)
  at com.nvidia.spark.rapids.ContextChecks.tagBase(TypeChecks.scala:811)
  at com.nvidia.spark.rapids.ContextChecks.tag(TypeChecks.scala:779)
  at com.nvidia.spark.rapids.ExprChecksImpl.tag(TypeChecks.scala:1032)
  at com.nvidia.spark.rapids.BaseExprMeta.$anonfun$tagSelfForGpu$5(RapidsMeta.scala:1266)
  at com.nvidia.spark.rapids.BaseExprMeta.tagSelfForGpu(RapidsMeta.scala:1266)
  at com.nvidia.spark.rapids.RapidsMeta.tagForGpu(RapidsMeta.scala:353)
  at com.databricks.sql.transaction.tahoe.rapids.GpuCheckDeltaInvariant$
       .$anonfun$maybeConvertToGpu$2(GpuCheckDeltaInvariant.scala:139)
  at com.databricks.sql.transaction.tahoe.rapids.GpuOptimisticTransactionBase
       .addInvariantChecks(GpuOptimisticTransactionBase.scala:71)
  at com.databricks.sql.transaction.tahoe.rapids.GpuOptimisticTransaction
       .gpuWriteFiles(GpuOptimisticTransaction.scala:218)
```

`Check(EXPRESSION(<expr>), <expr>, <orig>)` — three children — versus the meta's
declared one.

### Scope

1. Update `GpuCheckDeltaInvariant`'s `ExprChecks` registration to declare the correct
   child arity for DB-17.3 (3 instead of 1). Older shims (DB-14.3, OSS) keep 1.
2. Audit `DeltaInvariantChecker` operator GPU conversion on DB-17.3 — verify the GPU
   equivalent is selected once the meta arity is fixed (Group A symptom should disappear
   if Group A was just silent fallback caused by Group B).
3. Re-run the 6 failing tests on DB-17.3 cluster to verify.
4. Audit other `delta_lake_*_test.py` suites for similar latent failures.

### Investigation starting points

- `delta-lake/common/src/main/databricks/scala/com/databricks/sql/transaction/tahoe/rapids/GpuCheckDeltaInvariant.scala`
  — line 139 (`maybeConvertToGpu`)
- `delta-lake/delta-spark400db173/.../GpuOptimisticTransactionBase.scala` — line 71
  (`addInvariantChecks`)
- `delta-lake/delta-spark400db173/.../GpuOptimisticTransaction.scala` — line 218
  (`gpuWriteFiles`)
- DB-14.3 baseline: `delta-lake/delta-spark350db143/.../GpuCheckDeltaInvariant.scala`
  for comparison

### Test infra prerequisite (already landed)

`delta_lake_write_test.py` wide-schema tests on DB-17.3 require
`gen_df(..., length=delta_db173_wide_schema_gen_length)` (= 128 on DB-17.3) to avoid a
driver-side task-serialization OOM. The constant lives in
[integration_tests/src/main/python/delta_lake_utils.py](integration_tests/src/main/python/delta_lake_utils.py).
This is the only test-infra mitigation needed for the 9 wide-schema tests; the rest of
the file does not require changes.

### Depends on

Issue 2. Blocks Issue 7.

---

## Issue 7: [databricks] Delta Lake DB-17.3: Enable Delta Lake tests in CI

### Description

Enable the full `@delta_lake` integration test suite for Databricks 17.3 in CI.
Currently, Delta Lake tests are skipped for DB-17.3 in Jenkins via an explicit guard.
The skip should be removed once all GPU Delta Lake functionality (Issues 2-6) is validated.

### Scope

- Remove the Jenkins test skip in `jenkins/databricks/test.sh` (lines 152-159):
  ```diff
  -    if [[ "$SPARK_SHIM_VER" == "spark400db173" ]]; then
  -        echo "Skipping Delta Lake tests: not yet supported for DB-17.3 (spark400db173)"
  -    else
  -        ## Run Delta Lake tests
  -        DRIVER_MEMORY="4g" \
  -            bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" -m "delta_lake" --delta_lake --test_type=$TEST_TYPE
  -    fi
  +    ## Run Delta Lake tests
  +    DRIVER_MEMORY="4g" \
  +        bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" -m "delta_lake" --delta_lake --test_type=$TEST_TYPE
  ```

### Tests

Full `@delta_lake` integration test suite passing in CI on DB-17.3.

### Depends on

Issues 2-6 (all GPU Delta functionality working, including DV reads). Issue 8 is already
resolved.

---

## Dependency Graph (updated 2026-05-02 after Issue 6 delivery)

```
Issue 1: Scaffold delta-spark400db173 (module + CPU fallbacks)   ← DONE
  |
  +---> Issue 2: GPU Delta writes                                ← DONE (2026-04-23)
          |     [writeFilesAndGetExecutedPlan + writeFilesAndGetQueryExecution overrides;
          |      GpuDeltaFileFormatWriter w/ PartitionedTaskAttemptContextImpl;
          |      NoRowsCopiedTag via DMLUtils.TaggedCommitData + 3-arg txn.commit]
          |
          +---> Issue 3: GPU DELETE + UPDATE
          |
          +---> Issue 4: GPU MERGE INTO
          |
          +---> Issue 5: GPU OPTIMIZE + auto-compaction
          |
          +---> Issue 6: GPU Deletion Vector reads                  ← DONE (2026-05-02)
          |
          +---> Issue 9: GPU GenerateIdentityValues expression
          |
          +---> Issue 10: GpuCheckDeltaInvariant arity meta              ← DONE (2026-04-28)
                          (Group B fixed in-tree; Group A folded into #11169)

Issue 8 (independent): GPU Delta read — relative-path FileNotFoundException   ← DONE (2026-04-24)
                                                    |
Issues 3, 4, 5, 6 (and ideally 9, #11169) ----→ Issue 7: Enable Delta tests in CI
```

Issues 3, 4, and 5 can still be developed in parallel. Issue 6 was delivered
ahead of those DML issues because the DB-17.3 DV read path is independently useful
for read-heavy workloads and does not require GPU DML to create the DVs. Issues 8
and 10 are resolved. Issue 7 lands last once the remaining test matrix is clean —
`delta_lake_write_test.py`, `delta_lake_test.py`, and `delta_lake_delete_test.py`
are green in the local DB-17.3 validation runs noted above. The Group-A V1
`WriteIntoDeltaCommand` GPU acceleration is a separate quality-of-coverage follow-up
tracked under #11169.
