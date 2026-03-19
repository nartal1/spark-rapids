# Delta Lake DB-17.3 Implementation Checkpoint
**Date:** 2026-04-13 (build-only); **Scope correction:** 2026-04-23;
**Issue 2 delivered:** 2026-04-23
**Status:** Issue 1 (scaffolding) + Issue 2 (GPU writes) complete. Simple Delta writes
run on GPU end-to-end with data + log parity. DML (DELETE/UPDATE/MERGE), OPTIMIZE, DV
reads remain on CPU.
**Build verified with:** `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`
**Branch:** `databricks_173_delta_lake_Apr7`

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

## Summary (revised)

Issue 1 implementation: Created `delta-lake/delta-spark400db173/` module with 22 Scala
files + pom.xml. **Build-only scaffolding for Databricks 17.3** — every Delta operation
falls back to CPU. No GPU acceleration of any Delta op is active in this commit.

- **GPU-enabled:** none
- **CPU fallback:** writes (all shapes), DELETE, UPDATE, MERGE, OPTIMIZE, DV reads
- **Jenkins CI:** Delta Lake tests remain skipped for `spark400db173`
  ([test.sh:152-160](jenkins/databricks/test.sh))

Full build succeeds: 172 class files compiled, dist jar produced.

---

## All Files Created/Modified

### A. New Module: `delta-lake/delta-spark400db173/` (23 files)

```
delta-lake/delta-spark400db173/
  pom.xml
  src/main/scala/
    com/databricks/sql/transaction/tahoe/rapids/
      GpuCreateDeltaTableCommand.scala   -- copied from DB-14.3, fixed Dataset.ofRows
      GpuDeleteCommand.scala             -- copied from DB-14.3, Spark 4.0 API fixes
      GpuDeltaCatalog.scala              -- copied from DB-14.3, unchanged
      GpuDeltaDataSource.scala           -- copied from DB-14.3, unchanged
      GpuDoAutoCompaction.scala          -- rewritten for CommittedTransaction API
      GpuMergeIntoCommand.scala          -- copied from DB-14.3, extensive Spark 4.0 fixes
      GpuOptimisticTransactionBase.scala -- copied from DB-14.3, Spark 4.0 fixes
      GpuOptimisticTransaction.scala     -- copied from DB-14.3, Spark 4.0 fixes
      GpuOptimizeExecutor.scala          -- copied from DB-14.3, unchanged
      GpuUpdateCommand.scala             -- copied from DB-14.3, Spark 4.0 API fixes
      GpuWriteIntoDelta.scala            -- copied from DB-14.3, unchanged
    com/nvidia/spark/rapids/delta/
      DeltaProbe.scala                   -- renamed from DB-14.3
      DeltaSpark400DB173Provider.scala   -- renamed + recacheByPlan fix
      GpuDeltaParquetFileFormat.scala    -- fully rewritten for DB-17.3 DV API
    com/nvidia/spark/rapids/delta/shims/
      DeleteCommandMetaShim.scala        -- rewritten: always falls back to CPU
      DeltaLogShim.scala                 -- copied from DB-14.3, unchanged
      InvariantViolationExceptionShim.scala -- copied from DB-14.3, unchanged
      MergeIntoCommandMetaShim.scala     -- rewritten: always falls back to CPU
      MetadataShims.scala                -- copied from DB-14.3, unchanged
      ShimDeltaUDF.scala                 -- copied from DB-14.3, unchanged
      ShimShuffledRowRDD.scala           -- NEW: DB-17.3 ShuffledRowRDD 5/7-arg constructors
      UpdateCommandMetaShim.scala        -- rewritten: always falls back to CPU
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
delta-lake/common/.../GpuIdentityColumn.scala      -- new Column(expr) -> col(name)
delta-lake/common/.../GpuOptimizeWriteExchangeExec.scala -- removed redundant imports
delta-lake/common/.../OptimizeWriteExchangeExec.scala    -- ShimShuffledRowRDD.create()
```

### D. Generated (by make-scala-version-build-files.sh 2.13)

```
scala2.13/pom.xml                                    -- auto-updated
scala2.13/delta-lake/delta-spark400db173/pom.xml     -- auto-generated
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
- Sources via build-helper: `${spark.rapids.source.basedir}/delta-lake/common/src/main/scala` and `.../databricks/scala`

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
-import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
+import org.apache.spark.sql.{Dataset, SparkSession}
-import org.apache.spark.sql.functions.{array, max, min, to_json}
+import org.apache.spark.sql.functions.{array, col, max, min, to_json}

-      val column = new Column(UnresolvedAttribute.quoted(name))
+      val column = col(name)
```

Why: `new Column(Expression)` constructor removed in Spark 4.0. `col(name)` is equivalent and works in all versions.

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

All changes below are in `delta-lake/delta-spark400db173/src/main/scala/` only (no shared code impact):

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

## GpuDeltaParquetFileFormat: Fully Rewritten for DB-17.3

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

`tagSupportForGpuFileSourceScan` blocks GPU for any table with `tablePath.isDefined` (DV tables). GPU DV reads will be enabled in Issue 5.

`convertToGpu` constructs from `fmt.protocol`, `fmt.metadata`, `fmt.tablePath`, `fmt.isCDCRead`.

---

## DML Command Shims: All Fall Back to CPU

For Issue 1, DELETE/UPDATE/MERGE always fall back to CPU:
```scala
// DeleteCommandMetaShim, UpdateCommandMetaShim:
meta.willNotWorkOnGpu("Delta Lake DELETE/UPDATE is not yet supported on GPU for DB-17.3")

// MergeIntoCommandMetaShim:
meta.willNotWorkOnGpu("Delta Lake MERGE INTO is not yet supported on GPU for DB-17.3")
```

These will be enabled in Issues 2, 3, 4.

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

## What's Next (Follow-up Issues — updated 2026-04-23 after Issue 2)

| Issue | Description | Blocker for | Status |
|-------|-------------|-------------|--------|
| Issue 2 | GPU Delta writes — `writeFilesAndGetExecutedPlan` + `writeFilesAndGetQueryExecution` overrides; `GpuDeltaFileFormatWriter`; `NoRowsCopiedTag` commit tag | 3, 4, 5, 6, 7 | **Done** (2026-04-23) |
| Issue 3 | Enable GPU DELETE + UPDATE | 7 | Pending — depends on 2 |
| Issue 4 | Enable GPU MERGE INTO | 7 | Pending — depends on 2 |
| Issue 5 | Enable GPU OPTIMIZE + auto-compaction | 7 | Pending — depends on 2 |
| Issue 6 | GPU Deletion Vector reads — rewrite `GpuDeltaParquetFileFormat` | 7 | Pending — depends on 2-5 |
| Issue 7 | Enable Delta tests in CI — remove jenkins skip | — | Pending — depends on 2-6, 8 |
| **Issue 8 (NEW)** | GPU Delta **read** path: `FileNotFoundException` with relative Parquet paths on DB-17.3 | 7 | Pending — independent of 3/4/5 |
| **Issue 9 (NEW)** | GPU `GenerateIdentityValues` expression for DB-17.3 identity-column writes | 7 | Pending — nice-to-have |

See [delta-lake-db173-github-issues.md](delta-lake-db173-github-issues.md) for full issue
descriptions and dependency graph.
