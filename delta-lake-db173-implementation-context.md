# Delta Lake DB-17.3 Implementation Context
**For resuming work across sessions — push this file to git**

**Date compiled:** 2026-03-19
**Scope correction:** 2026-04-23 (§0.A)
**Issue 2 delivered:** 2026-04-23 (§0.B)
**PR slim-down:** 2026-04-24 (§0.C)
**Issue 8 resolved:** 2026-04-24 (§0.D)
**Issue 10 resolved + #11169 xfails:** 2026-04-28 (§0.E)
**PR review hardening:** 2026-04-30 (§0.F)
**Issue 6 DV reads:** 2026-05-02 (§0.G)
**Native DV follow-up:** 2026-05-05 (§0.H)
**Native-only DV cleanup/squash:** 2026-05-06 (§0.I)
**Shared transaction refactor:** 2026-05-06 (§0.J)
**Review-comment sync + DV fallback hardening:** 2026-05-11 (§0.K)
**DML skip-row pushdown guard + DB assertion canary:** 2026-05-14 (§0.L)
**Issue 5 PR review/CI hardening:** 2026-05-21 (§0.C.2)
**Session:** Issues 1, 2, 6, 8, and 10 complete. Issue 6 final state is
DBR-17.3 native cuDF-only GPU DV reads. The 2026-05-02 materialized DBR GPU DV
reader was an intermediate checkpoint and is not in the final squashed commit.
`delta_lake_write_test.py`, targeted `delta_lake_test.py`, and targeted
`delta_lake_delete_test.py`, plus the 2026-05-11 broader Delta read/delete and
auto-compact selections, are green on DB-17.3 for the validated local runs.
DB-14.3/DB-17.3 shared transaction/catalog/data-source/shim code now lives under
`delta-lake/common/src/main/db-350db143-400db173/scala`.
DELETE/UPDATE are now delivered in the base via PR #14810. Remaining follow-up
work is MERGE, liquid clustered OPTIMIZE / `OPTIMIZE FULL`, CDC + DV reads, and
CI enablement. The smaller regular OPTIMIZE + inline auto-compaction PR is in
review as PR #14847 at `f8525abf7`. The 2026-05-14 guard keeps DBR
DML/DELETE fallback plans from losing their DB skip-row predicate and adds a Python canary for DB's missing
row-index-filter assertion wording; GPU DV support in this branch is still DV
reads only, not GPU DELETE with persistent DVs.

---

## 0. Scope Correction, Issue 2 Delivery, and PR Slim-down — READ FIRST

### 0.A Scope Correction (pre-implementation, 2026-04-23)

During cluster validation of `delta_lake_write_test.py`, we discovered DB-17.3 does not
use the DB-14.3 write interception pattern:

- **DB-14.3** `WriteIntoDeltaEdge.write(txn)` → `txn.writeFiles(data, opts, ...)` → our
  `GpuOptimisticTransactionBase.writeFiles` override catches.
- **DB-17.3** `WriteIntoDeltaEdge.write(txn)` → routed via `WriteIntoDeltaLike.write` /
  `WriteIntoDeltaEdge.writeAndReturnCommitData` → `writeFilesAndGetMaterializationPlans`
  → `ClusteredWriter.writeFilesWithoutClustering` → `txn.writeFilesAndGetExecutedPlan(...)`.
  Our 3-arg `writeFiles` override is never invoked.

Attempting to intercept the inner `WriteIntoDeltaCommand` via a `DataWritingCommandMeta`
(2026-04-23) produced `DELTA_ACTIVE_TRANSACTION_ALREADY_SET` because the outer
transaction from `GpuWriteIntoDelta.run` was still active.

Issue 1 was re-scoped to **module-scaffolding only**; Issue 2 was introduced for the
actual write-path work.

### 0.B Issue 2 Delivery (post-implementation, 2026-04-23)

Implemented originally as three DB-17.3-only file changes; validated on cluster
(~3.5× speedup on a simple append,
`test_delta_overwrite_by_expression_exec_v1` 4/4 passing, partitioned writes
unblocked, Delta log parity confirmed). The 2026-05-06 shared transaction
refactor moved the reusable portions of these files into
`common/src/main/db-350db143-400db173/scala`; see §0.J for the current layout.

**Actual entry point on DB-17.3** — `writeFilesAndGetExecutedPlan` (not
`writeFilesAndGetQueryExecution` as §0.A had assumed; that method also exists and is
overridden defensively for CTAS/RTAS paths). The scope-correction note's stack-trace
reference to `writeFilesAndGetQueryExecution:393` was from a different (earlier)
experimental flow; the actual DB-17.3 production flow calls
`writeFilesAndGetExecutedPlan`.

**Confirmed signatures** (via `javap -p -classpath "/databricks/jars/*"
com.databricks.sql.transaction.tahoe.files.TransactionalWriteEdge`):

```scala
def writeFilesAndGetQueryExecution(
    Dataset[_], TransactionalWriteOptions,
    isOptimize: Boolean, isLiquidClustering: Boolean,
    Seq[Constraint], isCDCWritePhase: Boolean,
    Option[String], Boolean
  ): (Seq[FileAction], QueryExecution)

def writeFilesAndGetExecutedPlan(                // ← primary DB-17.3 entry point
    Dataset[_], Either[Option[DeltaOptions], TransactionalWriteOptions],
    isOptimize: Boolean, isLiquidClustering: Boolean,
    Seq[Constraint], Option[String], Boolean
  ): (Seq[FileAction], SparkPlan)

def writeFiles(Dataset[_], Option[DeltaOptions], Seq[Constraint]): Seq[FileAction]
    // Still used by GpuDeleteCommand / GpuUpdateCommand / GpuMergeIntoCommand / GpuOptimizeExecutor
```

Class hierarchy: `OptimisticTransaction implements OptimisticTransactionImplEdge extends
TransactionalWriteEdge`. Default trait methods dispatch correctly to our overrides.

**Original three files touched under `delta-lake/delta-spark400db173/`:**

1. [GpuOptimisticTransaction.scala](delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/GpuOptimisticTransaction.scala):
   - Extract DB-14.3 `writeFiles` body into a shared `gpuWriteFiles(inputData,
     writeOptions, additionalConstraints, isOptimizeOverride): (Seq[FileAction], QueryExecution)`
     helper.
   - Add overrides: `writeFilesAndGetExecutedPlan` (primary) and
     `writeFilesAndGetQueryExecution` (for CTAS/RTAS). Both guard `isLiquidClustering`
     (and `isCDCWritePhase` for the QE variant) → `super.*` CPU fallback.
   - 3-arg `writeFiles` stays — now a thin delegator to `gpuWriteFiles` discarding the
     returned QE. Needed for DML paths.
   - Route actual write through new `GpuDeltaFileFormatWriter.write(...)` instead of
     the generic `GpuFileFormatWriter.write(...)`.

2. [GpuDeltaFileFormatWriter.scala](delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/GpuDeltaFileFormatWriter.scala)
   (new file):
   - `extends GpuFileFormatWriterBase`, overrides `createTaskAttemptContext` to produce
     `com.databricks.sql.transaction.tahoe.files.DeltaFileFormatWriter.PartitionedTaskAttemptContextImpl`
     (carrying `partitionColToDataType`) when the write has partition columns.
   - Required because DB-17.3's `DelayedCommitProtocol.parsePartitions` hard-casts to
     this subtype when handling TIMESTAMP partition columns.
   - Mirrors the OSS `delta-33x` / `delta-40x` `GpuDeltaFileFormatWriter` pattern with
     the Databricks namespace.

3. [GpuWriteIntoDelta.scala](delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/GpuWriteIntoDelta.scala):
   - Stamp `NoRowsCopiedTag = true` on the commit (plain writes copy no existing rows;
     skip when `replaceWhere` is set).
   - Use `DMLUtils.TaggedCommitData(actions).withTag(...)` → `.stringTags` → 3-arg
     `txn.commit(actions, op, stringTags)` — matches the existing pattern in
     `GpuDeleteCommand`. Needed for Delta-log parity: CPU's `writeFilesAndGetExecutedPlan`
     default body sets this tag via a private `recordWriteFilesOperation` helper we
     bypass.

**What did NOT change** (explicit, to resist future refactors):

- `GpuOptimisticTransactionBase.scala` — the 7-arg `writeFiles` override stays as a
  safety net. Dead on DB-17.3's real write path, but harmless.
- `DatabricksDeltaProviderBase.scala`, `RapidsDeltaUtils.scala`, all DML command files
  — unchanged.
- An attempted identity-column CPU fallback in shared `RapidsDeltaUtils.tagForDeltaWrite`
  was reverted because it would have regressed DB-14.3. Tracked as Issue 9.

**Surfaced follow-up issues** (see [delta-lake-db173-github-issues.md](delta-lake-db173-github-issues.md)):

- **Issue 8** — GPU Delta **read** path hits `FileNotFoundException` with relative
  Parquet paths in narrow scenarios (multi-file global-sort read-back, `INSERT OVERWRITE
  ... SELECT FROM delta.<src>`). Pre-existing; not caused by Issue 2 but exposed by
  enabling writes. **Resolved 2026-04-24** — see §0.D for the
  `filesWithAbsolutePaths` + `pathPrefix` restoration fix.
- **Issue 9** — GPU `GenerateIdentityValues` expression for DB-17.3 identity-column
  writes. Currently falls back `ProjectExec` to CPU, defeating GPU acceleration for
  identity writes; correctness is fine.

Jenkins Delta Lake test skip for `spark400db173` stays in place until Issues 3-6 are
resolved and Issue 7 removes it. (Issue 8 is resolved as of 2026-04-24; see §0.D.)

See [delta-lake-db173-checkpoint.md](delta-lake-db173-checkpoint.md) for the
commit-level checkpoint.

### 0.C PR Slim-down (2026-04-24)

To scope the Issue 1 PR cleanly to "build + GPU write path", the DML/OPTIMIZE command
implementations carried in the initial draft of the build-fixes commit were removed
or reduced to compile-only stubs. The GPU write path delivered in Issue 2 is unchanged.

**Deleted entirely** (no shared-code constraint requires the class):
- `GpuMergeIntoCommand.scala` (1,206 LOC) — returns with follow-up issue #14598
- `GpuOptimizeExecutor.scala` (420 LOC) — returns with follow-up issue #14599
- `GpuDoAutoCompaction.scala` (48 LOC) — returns with follow-up issue #14599

Also: `registerPostCommitHook(GpuDoAutoCompaction)` removed from
`GpuOptimisticTransaction.gpuWriteFiles`; Databricks' CPU auto-compact hook runs when
enabled. The 2026-05-11 review-comment sync made that CPU hook explicit by
registering Databricks `AutoCompact` with RAPIDS disabled around its run.
`MergeIntoCommandMetaShim.convertToGpu` now throws directly (dead code —
`tagForGpu` already CPU-falls back), and the import of `GpuMergeIntoCommand` /
`GpuDeltaLog` is dropped.

**Stubbed** (kept because shared `delta-lake/common/src/main/databricks/.../DeleteCommandMeta.scala`
and `UpdateCommandMeta.scala` directly construct these case classes in their
`convertToGpu()`; we cannot edit the shared file without affecting DB-14.3 and
earlier):
- `GpuDeleteCommand.scala` → ~30 LOC stub case class, `run()` throws
- `GpuUpdateCommand.scala` → ~35 LOC stub case class, `run()` throws

Both stubs match the exact constructor signatures used by the shared meta files:
```scala
GpuDeleteCommand(gpuDeltaLog, target: LogicalPlan, condition: Option[Expression])
GpuUpdateCommand(gpuDeltaLog, tahoeFileIndex: TahoeFileIndex,
                 target: LogicalPlan,
                 updateExpressions: Seq[Expression],
                 condition: Option[Expression])
```

The stub body is unreachable because both `DeleteCommandMetaShim.tagForGpu` and
`UpdateCommandMetaShim.tagForGpu` unconditionally call `willNotWorkOnGpu(...)` in this
PR.

**Follow-up issues** (each one re-adds the corresponding file(s) as part of enabling
its GPU path):
- [#14597](https://github.com/NVIDIA/spark-rapids/issues/14597) — DELETE + UPDATE
- [#14598](https://github.com/NVIDIA/spark-rapids/issues/14598) — MERGE INTO
- [#14599](https://github.com/NVIDIA/spark-rapids/issues/14599) — OPTIMIZE + auto-compaction
- [#14600](https://github.com/NVIDIA/spark-rapids/issues/14600) — Deletion Vector reads

Net commit size: ~4,597 → ~2,289 lines. No change to the write-path behavior or the
design described in §3–§8 below, other than the DML/OPTIMIZE impl files being deferred.


### 0.C.1 Issue 5 local follow-up status (2026-05-20)

The Issue 5 follow-up branch `delta_db173_OPTIMIZE_AUTO_COMPACT` now contains a
rewritten local stack for DBR 17.3 OPTIMIZE and auto-compaction:

- `eda0aa172` - regular DBR 17.3 OPTIMIZE support
- `9ae539a3b` - DBR 17.3 auto-compaction support
- `0141241f0` - liquid clustered OPTIMIZE / `OPTIMIZE FULL`
- `a05033865` - liquid clustered write-path completion

For review, use `9ae539a3b` as the cutoff for the smaller OPTIMIZE +
auto-compaction PR. Keep `0141241f0` and `a05033865` for the liquid clustering
follow-up PR. See `delta-lake-db173-optimize-auto-compact-plan.md` for the
current validation checklist and old-to-new hash mapping after autosquash.

Latest Issue 5 PR-prep update: the active smaller PR branch is
`delta_db173_optimize_auto_compaction`; the May 21 review-fix tip is `f8525abf7`.
It contains `37903ee00` (OPTIMIZE), `704568255` (auto-compaction), and
`4bffbd2c0` (follow-up fixes). The follow-up fixes add runtime writable-DV
protection, DBR row-tracking commitInfo tag parity, exception-safe inline
auto-compaction observer cleanup, and DBR's auto-compaction min-file-size
default. Build passed, and the focused OPTIMIZE + auto-compact test run passed
with `19 passed`. The markdown docs remain untracked; the documentation-only
commit `afa0cd4c2` should stay out of the smaller code PR unless docs are
explicitly desired.


### 0.C.2 Issue 5 PR #14847 review and CI status (2026-05-21)

PR #14847 is the active smaller DBR 17.3 regular OPTIMIZE + inline
auto-compaction PR on branch `delta_db173_optimize_auto_compaction`. The current
review-fix tip is `f8525abf7`.

Review and CI work completed on 2026-05-21:

- Fixed scalastyle failures: split the long DB-14.3/DB-17.3 shared transaction
  constructor line and corrected DBR 17.3 import ordering.
- Fixed copyright/license-header CI for the new auto-compact and OPTIMIZE Python
  integration tests.
- Restored unconditional latest Delta-log parity checks for partitioned OPTIMIZE;
  the temporary `compare_delta_logs` escape hatch was removed.
- Replaced `FileSizeStatsWithHistogram.create(...).get` with an explicit
  `getOrElse` failure that reports the unexpected empty stats input.
- Made `GpuOptimizeExecutor.commitAndRetry` return the transaction that actually
  committed, and compute returned OPTIMIZE stats from that committed transaction's
  metadata. This addresses the valid retry-path stale-metadata review concern.
- Added a scoped `ExecutionPlanCaptureCallback` assertion around only the GPU
  `OPTIMIZE` SQL call in `delta_lake_optimize_table_test.py`, verifying that a
  captured plan contains `GpuExecutedCommandExec`. This avoids the broader
  `validate_execs_in_gpu_plan` marker, which also observes Delta metadata plans.
- Explicitly disabled `spark.databricks.delta.autoCompact.enabled` in the manual
  OPTIMIZE test config so the OPTIMIZE parity tests are isolated from
  auto-compaction defaults.

Review comments disposition:

- GPU execution coverage: valid, fixed by the scoped plan-capture assertion.
- Stats staleness after retry: valid for the retry path, fixed by returning the
  committed transaction from `commitAndRetry`; the remaining first-attempt
  concern was not actionable because refreshing after commit could report
  metadata from a later unrelated commit.
- Auto-compaction interference in OPTIMIZE tests: the exact mechanism is unlikely
  because the DBR 17.3 write path suppresses auto-compact registration for
  `isOptimize = true`, but the suggested config is a harmless test isolation
  guard and was incorporated.

Validation completed on DBR 17.3:

```bash
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
```

Result observed: `BUILD SUCCESS`.

```bash
source jenkins/databricks/setup.sh
source jenkins/databricks/common_vars.sh

TEST_PARALLEL=1 TESTS="delta_lake_optimize_table_test.py delta_lake_auto_compact_test.py" \
  bash integration_tests/run_pyspark_from_build.sh \
    --runtime_env=databricks \
    -m "delta_lake" \
    --delta_lake \
    --test_type="$TEST_TYPE"
```

Result observed: `19 passed`, `28 warnings`.

After the GPU execution assertion was added:

```bash
TEST_PARALLEL=1 TESTS="delta_lake_optimize_table_test.py" \
  bash integration_tests/run_pyspark_from_build.sh \
    --runtime_env=databricks \
    -m "delta_lake" \
    --delta_lake \
    --test_type="$TEST_TYPE"
```

Results observed: `5 passed`, `28 warnings` twice: once after the scoped
`GpuExecutedCommandExec` assertion (`184.57s`) and once after the explicit
auto-compact disable (`180.10s`). `git diff --check` passed after the review
edits.

### 0.D Issue 8 Resolved — Delta read absolute-path fix (2026-04-24)

The `FileNotFoundException` on relative Parquet paths described under Issue 8 is **fixed**
in the same commit as the Issue 1 build-fixes (`3b1a1d470`). Root cause confirmed:

On DB-17.3, Delta / UC-managed tables store **bare filenames** in
`FilePartition.innerFiles` and rely on `pathPrefix` for absolute resolution. The GPU
scan paths were calling `partition.innerFiles`, which returns the unresolved relative
filenames. Additionally, `GpuFileSourceScanExec.createNonBucketedReadRDD` recreates
partitions via `FilePartition.getFilePartitions`; the 2-arg factory used there drops
`pathPrefix` (it is not one of the arguments).

**Fix — two DB-17.3-only shim files:**

1. [Spark400PlusDBShims.scala](sql-plugin/src/main/spark400db173/scala/com/nvidia/spark/rapids/shims/Spark400PlusDBShims.scala) —
   `getPartitionFiles` returns `partition.filesWithAbsolutePaths.toSeq` (was
   `partition.innerFiles`).

2. [FilePartitionShims.scala](sql-plugin/src/main/spark400db173/scala/org/apache/spark/sql/execution/rapids/shims/FilePartitionShims.scala) —
   - `getFiles(p)` returns `p.filesWithAbsolutePaths` (was `p.innerFiles`).
   - New `withPathPrefixIfNeeded(partitions, relation)` helper restores the dropped
     `pathPrefix` from `relation.location.rootPaths` (only when there is a single root
     path) so that `filesWithAbsolutePaths` can resolve relative paths correctly. No-op
     on all other shims.
   - The 2026-05-11 review-comment sync also clears `pathPrefix` in
     `copyWithFiles` after storing already-absolute files, so non-bucketed
     scans do not depend on a later absolute-path short-circuit.

This unblocks the multi-file global-sort read-back, the
`INSERT OVERWRITE delta.<dst> SELECT * FROM delta.<src>` pattern, and binary-heavy
sort-fallback read-back scenarios listed in the original Issue 8 write-up. Issue 7
(CI enablement) no longer depends on Issue 8 — only on Issues 3–6.

### 0.E Issue 10 resolution + #11169 xfails (2026-04-28)

Of the 6 `delta_lake_write_test.py` failures filed as Issue 10 (§12), 2 are fixed by
a one-file shared-code change and 4 are folded into the pre-existing #11169 V1
`WriteIntoDeltaCommand` limitation. `delta_lake_write_test.py` is green on DB-17.3.

**Group B — fixed (constraint-check tests).** The shared `GpuCheckDeltaInvariant`
expression rule declared `ExprChecks.unaryProject` (1 child), but DB-17.3's
`CheckDeltaInvariant.children` is `child +: columnExtractors.map(_._2)` — variable
arity. Replaced with `ExprChecks.projectOnly(..., paramCheck = Seq(ParamCheck("input",
...)), repeatingParamCheck = Some(RepeatingParamCheck("extra", ...)))`, which is the
exact shape the OSS Delta counterpart at
[delta-lake/common/src/main/delta-io/scala/org/apache/spark/sql/delta/rapids/GpuCheckDeltaInvariant.scala](delta-lake/common/src/main/delta-io/scala/org/apache/spark/sql/delta/rapids/GpuCheckDeltaInvariant.scala)
already uses for the same reason. Backward compatible with older Databricks shims
(`UnaryExpression`, single child) — the repeating check accepts 1+N children where
N == 0 on those shims. One file touched:

- [delta-lake/common/src/main/databricks/scala/com/databricks/sql/transaction/tahoe/rapids/GpuCheckDeltaInvariant.scala](delta-lake/common/src/main/databricks/scala/com/databricks/sql/transaction/tahoe/rapids/GpuCheckDeltaInvariant.scala)

After this fix `test_delta_write_constraint_check` and
`test_delta_write_constraint_check_fallback` pass (the fallback variant still
correctly takes the CPU path because `GpuCheckDeltaInvariantMeta.tagExprForGpu`
declines when the underlying constraint expression can't be replaced).

**Group A — xfailed under #11169.** The 4 Group A failures
(`test_delta_overwrite_mixed_clause` × 3 STATIC variants +
`test_delta_write_partial_overwrite_replace_where`) hit the V1 `WriteIntoDeltaCommand`
path, not our `gpuWriteFiles` flow. Mechanics confirmed via `javap` against the
DB-17.3 cluster jars:

- DB-17.3's analyzer routes both `INSERT OVERWRITE TABLE delta.<path> PARTITION (...)`
  and `df.write.mode("overwrite").option("replaceWhere", ...)` through
  `com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaCommand` (a
  `V1WriteCommand` — Databricks-only; `org.apache.spark.sql.delta.commands.WriteIntoDeltaCommand`
  does not exist in OSS Delta). Spark's stock V1Writes rule then wraps the query plan
  with `WriteFiles` and Delta's analyzer inserts `DeltaInvariantCheckerExec` to
  enforce the partition / replaceWhere constraint.
- Older Databricks shims resolve the same SQL through the V2 path
  (`OverwriteByExpressionExecV1`), which our shared
  [DatabricksDeltaProviderBase.scala:277](delta-lake/common/src/main/databricks/scala/com/nvidia/spark/rapids/delta/DatabricksDeltaProviderBase.scala)
  intercepts to `GpuOverwriteByExpressionExecV1` — none of the V1 nodes appear in the
  plan. That's why DB-14.3 / 13.3 / 12.2 / OSS Delta-40x do not fail these tests.
- DB-17.3's V1 path runs `WriteIntoDeltaCommand.run(spark, sparkPlan)` against a
  pre-planned plan tree. Our `txn.writeFilesAndGetExecutedPlan` /
  `writeFilesAndGetQueryExecution` overrides are never called, so `gpuWriteFiles`
  never runs and `RapidsDeltaWrite` never enters the plan.

This is the same gap upstream
[#11169](https://github.com/NVIDIA/spark-rapids/issues/11169) already tracks — DB-14.3+
hits the same V1 path for `df.write.saveAsTable(...)`, and
`test_delta_write_round_trip_managed` is xfailed there under #11169. The 4 DB-17.3
failures here are the same family — different SQL forms newly routed through V1 by
DB-17.3's analyzer. They are xfailed for `is_databricks173_or_later()` referencing
#11169:

- `test_delta_overwrite_mixed_clause` — added `@pytest.mark.xfail(is_databricks173_or_later(), reason="…/issues/11169")`
- `test_delta_write_partial_overwrite_replace_where` — same

The `allow_non_gpu_conditional` belt is intentionally NOT added: `xfail` catches
whichever assertion fires first (the JVM-side `assertIsOnTheGpu` walker rejecting
`DataWritingCommandExec`, or the Python-side `assert_rapids_delta_write` not finding
`RapidsDeltaWrite` in any captured plan), and either is the expected failure mode
under #11169.

The proper code-level fix is registering a `DataWritingCommandMeta[WriteIntoDeltaCommand]`
in `DeltaSpark400DB173Provider` that converts the V1 write to a GPU `RunnableCommand`
running through `gpuWriteFiles` (with care around the nested-transaction concern
documented in §0.A). That follow-up rolls under #11169.

**Files touched in this round:**

1. [delta-lake/common/src/main/databricks/scala/com/databricks/sql/transaction/tahoe/rapids/GpuCheckDeltaInvariant.scala](delta-lake/common/src/main/databricks/scala/com/databricks/sql/transaction/tahoe/rapids/GpuCheckDeltaInvariant.scala) —
   `ExprChecks.unaryProject` → `ExprChecks.projectOnly + paramCheck + repeatingParamCheck`
   for variable-arity `CheckDeltaInvariant.children`. Imports `ParamCheck` and
   `RepeatingParamCheck`. Shared across all Databricks shims; backward compatible.
2. [integration_tests/src/main/python/delta_lake_write_test.py](integration_tests/src/main/python/delta_lake_write_test.py) —
   `@pytest.mark.xfail(is_databricks173_or_later(), reason=".../issues/11169")` on
   `test_delta_overwrite_mixed_clause` and `test_delta_write_partial_overwrite_replace_where`.

**Issue 10 status:** Group B fixed in-tree; Group A subsumed under #11169. Issue 10
itself can be closed. Issue 7 (CI enablement) no longer blocks on Issue 10.

### 0.F PR Review Hardening (2026-04-30)

Static review of the first PR against the live DB-17.3 JARs found two areas that needed
conservative hardening before merge:

- `DeltaSpark400DB173Provider.tagForGpu` now falls back for all DB-17.3 CTAS/RTAS.
  The old GPU command port does not faithfully implement newer CPU
  `CreateDeltaTableCommand` semantics: row filters, column masks, liquid clustering,
  auto TTL, catalog-owned tables (explicit or default configuration), coordinated commits
  (explicit table properties or default table-property conf), and deletion vectors
  (explicit table property or default table-property conf). Feature-specific
  TableSpec/property checks remain as defensive tags. Explicit Delta property-key checks
  are case-insensitive, and invalid DV boolean values also fall back so Delta's CPU
  validation reports the error. This avoids silently creating a table with missing
  DB-17.3 metadata or governance semantics.
- `GpuIdentityColumn` now wraps `UnresolvedAttribute.quoted(name)` via
  `DFUDFShims.exprToColumn` instead of using `col(name)`. This keeps Spark-4
  compatibility without changing resolution of identity columns whose names contain
  dots or other multipart-reference characters.

DV auto-enable triggers that depend on Databricks table-shape heuristics are not
implemented by the GPU create-table command and remain tracked with the broader
DB-17.3 create-command port under #14601.

### 0.G Issue 6 DV Read Delivery (historical 2026-05-02 checkpoint)

Issue 6 is implemented for DB-17.3 using the V1/materialized deletion-vector
read path. This was an intermediate checkpoint only. The final 2026-05-06
squashed commit removes the DBR materialized GPU path and keeps DBR-17.3 DV
reads GPU-supported only through native cuDF scanning. The code did not port the
OSS Delta `delta-33x-40x` sources into the DB module; instead it added
DB-17.3-local source files with Databricks namespace imports and used the same
architecture: GPU Parquet read, per-file DV row-index filter materialization,
and a GPU skip-row filter above the scan.

Files added:
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatDV.scala`
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/RapidsDeletionVectors.scala`
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/RapidsRowIndexFilters.scala`

Files updated:
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala`
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala`
- `integration_tests/src/main/python/delta_lake_test.py`
- `integration_tests/src/main/python/delta_lake_delete_test.py`

Implementation notes to remember tomorrow:
- The DB-17.3 skip-row column is
  `_databricks_internal_edge_computed_column_skip_row`; it is allowed and
  generated by the GPU reader. Other unrelated `_databricks_internal*` columns
  still remain unsupported.
- `effectiveOptimizationsEnabled` is disabled for DV-bearing TahoeFileIndex
  reads so split/native scan optimizations do not misalign row indexes.
- Empty-cardinality DVs and empty row-index filters keep the optimized path
  because the skip mask is constant.
- `DeltaSpark400DB173Provider.canPushDVPredicateDownToScan` intentionally
  remained false for the initial V1/materialized delivery. The 2026-05-05
  follow-up enables narrow native DV pushdown only when DBR metadata-row-index
  mode and the RAPIDS DV predicate-pushdown config are both enabled.
- CDC + DV reads remain guarded to CPU.
- The V1 path still handles COALESCING by falling through to the multithreaded
  DV reader path. The native follow-up adds the COALESCING native cuDF path, and
  the final cleanup removes this V1 DBR path from the squashed commit.

Validation:
- Build passed:
  `SKIP_DEP_INSTALL=1 WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`
  (`/tmp/db173-build-final-review-cleanups.log`).
- Focused post-cleanup DV tests passed:
  `delta_lake_delete_test.py::test_delta_deletion_vector_read`
  and `delta_lake_test.py::test_delta_empty_deletion_vector_read`;
  summary `57 passed, 35 warnings in 182.72s`
  (`/tmp/db173-dv-focused-after-cleanups.log`).
- Full `delta_lake_test.py` passed before final cleanup:
  `180 passed, 40 skipped, 13 xpassed, 34 warnings in 726.67s`
  (`/tmp/db173-delta-lake-full-final2.log`).
- Full `delta_lake_delete_test.py` passed before final cleanup:
  `40 passed, 16 xfailed, 28 warnings in 276.22s`
  (`/tmp/db173-delta-lake-delete-full-final2.log`).

### 0.H Native cuDF DV Follow-up Delivery (2026-05-05)

The native DBR-17.3 DV follow-up is implemented locally. It follows the same
sequence used by OSS Delta-4.0 in this repo: narrow predicate pushdown first,
then PERFILE native DV scanning, then MULTITHREADED, then COALESCING.

Files added or updated:
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala`
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/RapidsDeletionVectors.scala`
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatNativeDV.scala`
- `delta-lake/delta-spark400db173/src/main/java/com/nvidia/spark/rapids/delta/DeltaBitmapUtils.java`
- `integration_tests/src/main/python/delta_lake_test.py`

Implementation notes for the next session:
- Native DV pushdown recognizes the current DBR skip-row predicate shapes only:
  `__delta_internal_is_row_deleted` and
  `_databricks_internal_edge_computed_column_skip_row`.
- Native DV scanning is selected only when DBR metadata-row-index mode and
  `spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled` are both
  true. After the 2026-05-06 cleanup, disabled-gate DBR-17.3 DV scans fall back
  to CPU.
- `RapidsDeletionVectors` supports native DV lookup from `PartitionedFile`,
  `RowIndexFilterProvider`, and `TahoeFileIndex`.
- DBR `SerializedBitmap` bytes are converted to cuDF-compatible standard
  portable roaring bitmap bytes through `DeltaBitmapUtils.java`; the OSS
  `RapidsDeletionVectorStore` / `RapidsFileIO` path is not used on DBR-17.3.
- Only `RowIndexFilterType.IF_CONTAINED` is supported on the native path. DBR
  row-index filters with other semantics, including `IF_NOT_CONTAINED`, are
  kept on CPU.
- CDC + DV reads, Delta file-in-scan metadata columns, and nullable row-tracking
  metadata fields remain unsupported on GPU.

Validation:
- Build passed:
  `mvn -B -f scala2.13/pom.xml -Ddatabricks -Dbuildver=400db173 package -pl dist -am -DskipTests -Dmaven.scaladoc.skip`.
- Focused native PERFILE footer test passed:
  `delta_lake_test.py::test_delta_deletion_vector_native_footer_multi_row_group[one_col-NATIVE-PERFILE]`.
- Native footer matrix passed:
  `test_delta_deletion_vector_native_footer_multi_row_group` and
  `test_delta_deletion_vector_native_footer_multi_row_group_count_star`.
- Targeted delete/read and scan-split tests passed:
  `test_delta_deletion_vector`, `test_delta_deletion_vector_read`, and
  `test_delta_scan_split_with_DV_enabled_with_DVs`.

### 0.I Final Native-only DV Cleanup and Squash (2026-05-06)

Team review narrowed DBR-17.3 GPU DV support to the native cuDF path. OSS Delta
still has its materialized/non-cuDF DV support where those modules support it,
but DBR-17.3 now falls back to CPU when the native cuDF DV gates are disabled.

Final squashed commit:

```text
ada2580ea3ff0c558f62b1e503a0f302343e83f9 [databricks] Add native-only Delta DV reads for DBR 17.3
```

Final net files:
- `delta-lake/delta-spark400db173/src/main/java/com/nvidia/spark/rapids/delta/DeltaBitmapUtils.java`
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala`
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormat.scala`
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatNativeDV.scala`
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/RapidsDeletionVectors.scala`
- `integration_tests/src/main/python/delta_lake_delete_test.py`
- `integration_tests/src/main/python/delta_lake_test.py`

The intermediate `GpuDeltaParquetFileFormatDV.scala` and `RapidsRowIndexFilters.scala`
files were removed before the squash because they only supported the DBR materialized
GPU fallback path.

Implementation notes for the next session:
- `DeltaSpark400DB173Provider.canPushDVPredicateDownToScan(conf)` is true only when
  `spark.databricks.delta.deletionVectors.useMetadataRowIndex` and
  `spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled` are both true.
- `useMetadataRowIndex` is effectively true by default through DBR
  `DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX`.
- `getReadFileFormat` selects `GpuDeltaParquetFileFormatNativeDV` whenever the native
  gates are true. When gates are false, non-DV scans can still use the plain GPU Delta
  format, while DV scans are tagged CPU fallback before conversion.
- `GpuDeltaParquetFileFormat.tagSupportForGpuFileSourceScan` also tags DBR
  row-index filters whose type is not `IF_CONTAINED` for CPU fallback. This was
  added after `test_delta_merge_query` produced an `IF_NOT_CONTAINED` provider;
  native cuDF treats DV bitmaps as rows to drop and cannot implement
  keep-marked-row semantics.
- `DeltaSpark400DB173Provider.pushDVPredicateDownToScan` removes DBR skip-row
  filters only when the child plan contains a `GpuFileSourceScanExec` backed by
  `GpuDeltaParquetFileFormatNativeDV`. DBR DML bitmap-writing fallback plans
  can still use hidden skip-row columns and must keep the filter.
- Native-only still uses some JVM-side DBR bitmap interaction for descriptor lookup,
  serialized bitmap conversion, and row-count bookkeeping. What is removed is the
  materialized boolean skip-row GPU filter path.

Validation:
- Build passed: `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`.
- Targeted DBR Delta DV read slice 1 passed: 80 selected, 80 passed.
- Targeted DBR Delta DV read slice 2 passed: 120 selected, 108 passed,
  12 expected Databricks skips.

### 0.J Shared DB-14.3/DB-17.3 Transaction Refactor (2026-05-06)

Commit `d8b1e4309` moved the overlapping DB-14.3 and DB-17.3 Delta
transaction/catalog/data-source/shim code into:

```text
delta-lake/common/src/main/db-350db143-400db173/scala
```

Both `delta-spark350db143` and `delta-spark400db173` now add this source root
from their module poms. The `scala2.13/` mirror poms were regenerated.

Shared files in the new root:
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

DB-17.3 still owns the version-specific adapters and local files:
- `GpuCreateDeltaTableCommand.scala`
- `GpuOptimisticTransactionBase.scala`
- `GpuOptimisticTransaction.scala`
- `GpuWriteIntoDelta.scala`
- `GpuDeltaFileFormatWriter.scala`
- `GpuDeleteCommand.scala` and `GpuUpdateCommand.scala` stubs
- provider/probe files, local command-meta shims, and native DV read files

### 0.K PR Review-comment Sync and 2026-05-11 Validation

The branch now includes review-comment commit `362e9d346` below the amended
native-only DV commit.

Review-comment sync notes:
- `FilePartitionShims.copyWithFiles` stores already-absolute files and clears
  `pathPrefix = None`, avoiding any dependency on downstream absolute-path
  short-circuit behavior for non-bucketed scans.
- The wide-schema row-count mitigation in `delta_lake_utils.py` references
  https://github.com/NVIDIA/spark-rapids/issues/14775.
- `DatabricksDeltaProviderBase.convertToGpu` now throws for unsupported
  CTAS/RTAS conversion, while DBR `tagForGpu` only emits those tags when the
  command meta can be replaced.
- `GpuOptimisticTransaction` registers Databricks `AutoCompact` as a CPU
  post-commit hook with RAPIDS disabled around `AutoCompact.run`.
- Shared `GpuCheckDeltaInvariant` converts only `check.child`; DBR 17.3
  extractor children are CPU error-message helpers.

Validation after the amended DV fallback hardening:
- Build passed: `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`.
- Focused DV selection passed: 194 tests, 0 failures, 0 errors.
- Targeted merge regression passed: `test_delta_merge_query`, 1 test,
  0 failures.
- Broader Delta read/delete selection passed: 289 tests, 0 failures,
  0 errors, 40 skipped.
- `delta_lake_auto_compact_test.py` selection passed: 14 tests, 0 failures.

### 0.L DML Skip-row Pushdown Guard (2026-05-14)

The latest hardening is in `DeltaSpark400DB173Provider.scala`, inside the
`DB173DVPredicatePushdown.pushToScan` helper. The rule still recognizes DBR
skip-row predicates, but it now prunes/removes them only when the filter's child
plan contains a native GPU DV scan:

```scala
GpuFileSourceScanExec
  relation.fileFormat.isInstanceOf[GpuDeltaParquetFileFormatNativeDV]
```

Reason: skip-row removal is correct for native cuDF DV reads because cuDF applies
the deletion vector inside the scan. It is not correct for DBR DELETE/DML
bitmap-writing plans that fall back to CPU while the plugin is enabled; those
plans can still depend on DB's skip-row filter for the deletion-vector
cardinality check.

Regression details:
- Reproduced with
  `delta_lake_delete_test.py::test_delta_delete_twice_with_dv` using
  `SPARK_RAPIDS_TEST_DATAGEN_SEED=1778719081`.
- Failure was
  `DELTA_DELETION_VECTOR_CARDINALITY_MISMATCH` on the second delete.
- The failure reproduced with OOM injection both enabled and disabled, so it was
  not an OOM-retry-only issue.
- The plan logged DELETE as CPU fallback (`DeleteCommandEdge` not supported on
  GPU for DB-17.3), confirming the bug was native-read predicate pushdown
  crossing into a fallback DML plan.

Validation:
- Build passed: `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`.
- Targeted test passed:
  `delta_lake_delete_test.py::test_delta_delete_twice_with_dv` with seed
  `1778719081` and `--test_oom_injection_mode=always`.
- `delta_lake_test.py::test_db173_missing_row_index_filter_assertion_guard`
  protects `MISSING_ROW_INDEX_FILTER_MESSAGE` by comparing it with DB's generated
  missing-row-index-filter assertion message. A temporary mutation of the Scala
  constant made the test fail, then reverting made it pass.

---

## 1. Goal (revised)

Original goal (still valid long-term): full Delta Lake GPU acceleration support for the
Databricks 17.3 shim (`400db173`, Spark `4.0.0-databricks-173`). Reached through Issues
1 (scaffolding — **done**), 2 (writes — **done 2026-04-23**), 3-5 (DML),
6 (DV reads — **done native-only 2026-05-06**),
8 (GPU Delta read relative-path bug), 9 (identity columns), 7 (CI enablement).

Issue 1 deliverable (done in `bd0f6e5f`): Create `delta-lake/delta-spark400db173/`
Maven module with 22 Scala files + pom.xml, following the pattern of
`delta-lake/delta-spark350db143/` (DB-14.3). After the 2026-05-06 refactor, the
current module has 16 local Scala files, one Java helper, and a shared
DB-14.3/DB-17.3 common root. All Spark 4.0 / DB-17.3 API migrations applied;
all Delta DML paths fall back to CPU via `willNotWorkOnGpu`.

Issue 2 deliverable (done 2026-04-23): See §0.B above.

**Tracking issue:** https://github.com/NVIDIA/spark-rapids/issues/14015

---

## 2. Critical Build Fact: Scala 2.13 Only

DB-17.3 / Spark 4.0 is **Scala 2.13 only**. There is no Scala 2.12 artifact.

The `release400db173` Maven profile lives inside the `<!-- #if scala-2.13 --><!--` block
in the root `pom.xml` (~lines 633–654), meaning it is XML-commented out in the main pom
and only active in `scala2.13/pom.xml`.

**Consequence: ALL builds must use `-f scala2.13`:**
```bash
mvn -f scala2.13 -Dbuildver=400db173 ...
```

The primary `pom.xml` for the new module still uses `_2.12` artifact suffix (it's a template);
`./build/make-scala-version-build-files.sh 2.13` generates the actual `scala2.13/` mirror with
`_2.13` suffix. **Always run this script after any pom.xml change.**

---

## 3. Architecture Overview

### 3.1 Delta Lake Module Structure

```
delta-lake/
├── common/src/main/
│   ├── scala/                   # Universal: DeltaProviderImplBase, RapidsDeltaWrite, UDFs
│   ├── databricks/scala/        # Databricks-shared (17 files): DatabricksDeltaProviderBase,
│   │                            #   GpuDeltaLog, DeleteCommandMeta, etc.
│   ├── db-350db143-400db173/scala
│   │                            # DB-14.3/DB-17.3 shared transaction/catalog/data-source code
│   └── delta-33x-40x/scala/    # Shared for OSS Delta 3.3+/4.0+ (DV common code)
├── delta-spark350db143/         # DB-14.3 ← PRIMARY COPY SOURCE
├── delta-spark400db173/         # DB-17.3 local adapters + native DV files
└── delta-stub/                  # No-op fallback for unsupported Delta pairings
```

### 3.2 Entry Point Chain

```
ShimLoader (SPI) → DeltaProbeImpl → DeltaSpark400DB173Provider
    → DatabricksDeltaProviderBase (common/databricks/scala)
        → DB-17.3 local adapters + shared db-350db143-400db173 transaction code
```

### 3.3 DB-17.3 Local Files + Shared DB-14.3/DB-17.3 Root

Current `delta-spark400db173` local files:

**`src/main/java/com/nvidia/spark/rapids/delta/` (1 file):**
- `DeltaBitmapUtils.java` — DBR bitmap serialization bridge for native cuDF DV

**`src/main/scala/com/nvidia/spark/rapids/delta/` (5 files):**
- `DeltaProbe.scala` — SPI entry point, returns the provider
- `DeltaSpark400DB173Provider.scala` — extends `DatabricksDeltaProviderBase`
- `GpuDeltaParquetFileFormat.scala` — DB-17.3 Delta GPU Parquet format
- `GpuDeltaParquetFileFormatNativeDV.scala` — native cuDF DV read support
- `RapidsDeletionVectors.scala` — DBR DV descriptor/provider lookup and bitmap helpers

**`src/main/scala/com/databricks/sql/transaction/tahoe/rapids/` (7 files):**
- `GpuOptimisticTransactionBase.scala`
- `GpuOptimisticTransaction.scala`
- `GpuWriteIntoDelta.scala`
- `GpuDeleteCommand.scala`
- `GpuUpdateCommand.scala`
- `GpuCreateDeltaTableCommand.scala`
- `GpuDeltaFileFormatWriter.scala`

**`src/main/scala/com/nvidia/spark/rapids/delta/shims/` (4 files):**
- `DeleteCommandMetaShim.scala`
- `UpdateCommandMetaShim.scala`
- `MergeIntoCommandMetaShim.scala`
- `ShimShuffledRowRDD.scala`

The current shared DB-14.3/DB-17.3 source root contributes:
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

---

## 4. CRITICAL: DB-17.3 Deletion Vector API — Confirmed via Jar Decompilation

### 4.1 What Was Decompiled / Verified

**Original decompilation (2026-03-19):**
```
/databricks/jars/spark-sql_2.13-4.0.0-databricks-173.jar
/databricks/jars/spark-catalyst_2.13-4.0.0-databricks-173.jar
```

**Live cluster verification (2026-04-07):**

On the DB-17.3 cluster, the JARs use a different naming convention:
```
/databricks/jars/----ws_4_0--sql--core--core-hive-2.3__hadoop-3.2_2.13_deploy.jar      (core + Delta)
/databricks/jars/----ws_4_0--sql--catalyst--catalyst-hive-2.3__hadoop-3.2_2.13_deploy.jar (catalyst)
/databricks/jars/----ws_4_0--sql--api--sql-api-hive-2.3__hadoop-3.2_2.13_deploy.jar      (SQL API)
```

Decompile commands used:
```bash
javap -p -classpath "/databricks/jars/----ws_4_0--sql--core--core-hive-2.3__hadoop-3.2_2.13_deploy.jar" \
  com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat
# For classes in other JARs, use wildcard classpath:
javap -classpath "/databricks/jars/*" com.databricks.sql.transaction.tahoe.deletionvectors.RoaringBitmapArray
```

### 4.2 DB-14.3 vs DB-17.3 DV API Comparison

| Area | DB-14.3 | DB-17.3 (confirmed) |
|------|---------|---------------------|
| `DeltaParquetFileFormat` constructor | `(relation, columnMappingMode, referenceSchema, isSplittable, disablePushDowns, broadcastDvMap, tablePath, broadcastHadoopConf)` — 8 params | `(protocol, metadata, generateRowIndexFilterId, generateRowIndexFilterColumn, generateDeltaFileInScanId, nullableRowTrackingConstantFields, nullableRowTrackingGeneratedFields, optimizationsEnabled, tablePath, isCDCRead)` — 10 params |
| `broadcastDvMap` | Present | **Gone** — does not exist |
| `broadcastHadoopConf` | Present | **Gone** — does not exist |
| DV loading mechanism | Broadcast `Map[URI, DeletionVectorDescriptorWithFilterType]` | Per-file via `PartitionedFile.otherConstantMetadataColumnValues` |
| `TahoeFileIndex.rowIndexFilters` type | `Map[String, RowIndexFilterType]` | `Map[String, RowIndexFilterProvider]` |
| `FILE_ROW_INDEX_FILTER_ID_ENCODED` | Not present | Present as **public** constant on companion |
| `FILE_ROW_INDEX_FILTER_TYPE` | Not present | Present as **public** constant on companion |
| DV filter type accessor | `RowIndexFilterType` enum only | `RowIndexFilterProvider` interface |
| Raw bitmap bytes | Not exposed | `SerializedBitmap.buffer(): byte[]` |
| `RowIndexFilter.materializeIntoVectorWithRowIndex` | Not present | **Present** — new method |
| `DeletionVectorUtils.deletionVectorsWritable` | `(SnapshotDescriptor)` | Overloads: `(SnapshotDescriptor, Option[Protocol], Option[Metadata])` and `(Protocol, Metadata)` |
| `DELETE_USE_PERSISTENT_DELETION_VECTORS` in `DeltaSQLConf` | Present | **Confirmed present** |
| `UPDATE_USE_PERSISTENT_DELETION_VECTORS` in `DeltaSQLConf` | Present | **Confirmed present** |
| `DropMarkedRowsFilter.createInstance` | `(DeletionVectorDescriptor, Configuration, Option[Path])` | **Same API confirmed** |
| `KeepMarkedRowsFilter.createInstance` | `(DeletionVectorDescriptor, Configuration, Option[Path])` | **Same API confirmed** |
| `RoaringBitmapArray` | `o.a.s.sql.delta.deletionvectors.RoaringBitmapArray` | `c.d.s.t.tahoe.deletionvectors.RoaringBitmapArray` — **confirmed via javap** (same API) |
| `StoredBitmap.load()` return type | `RoaringBitmapArray` | `RoaringBitmapArray` — **confirmed** |
| `HadoopFileSystemDVStore.read()` | `(DeletionVectorDescriptor, Path): RoaringBitmapArray` | **Same API confirmed** |
| `PortableRoaringBitmapArraySerializationFormat` | `MAGIC_NUMBER`, `serialize`, `deserialize` | **Same API confirmed** |
| `NativeRoaringBitmapArraySerializationFormat` | `MAGIC_NUMBER`, `serialize`, `deserialize` | **Same API confirmed** |
| `DeletionVectorStoreEdge.createInstance` | N/A | `(Configuration, Option[DeletionVectorCacheConfig])` — **confirmed** |

### 4.3 Confirmed DB-17.3 Companion Object Constants

```
com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat (companion):
  public static java.lang.String FILE_ROW_INDEX_FILTER_ID_ENCODED();  // PUBLIC
  public static java.lang.String FILE_ROW_INDEX_FILTER_TYPE();         // PUBLIC
  public static java.lang.String IS_ROW_DELETED_COLUMN_NAME();
  public static java.lang.String ROW_INDEX_COLUMN_NAME();
```

### 4.4 Confirmed DB-17.3 New Interfaces

```scala
// com.databricks.sql.io.RowIndexFilterProvider
interface RowIndexFilterProvider {
  def retrieve(conf: Configuration): RowIndexFilter
  def retrieveSerialized(conf: Configuration): SerializedBitmap  // NEW
  def getRowIndexFilterType(): RowIndexFilterType
}

// com.databricks.sql.transaction.tahoe.storage.dv.SerializedBitmap
class SerializedBitmap {
  def buffer(): Array[Byte]  // raw serialized bitmap bytes
}

// com.databricks.sql.io.RowIndexFilterType (enum values)
IF_CONTAINED, IF_NOT_CONTAINED, CONSTANT_TRUE_VECTOR_FILTER,
CONSTANT_FALSE_VECTOR_FILTER, MOD_SHARD, UNKNOWN

// com.databricks.sql.io.RowIndexFilter (added method in DB-17.3)
def materializeIntoVectorWithRowIndex(
  numRows: Int,
  rowIndexCol: ColumnVector,
  output: WritableColumnVector
): Unit
```

### 4.5 `DeltaParquetFileFormat` Instance Methods Confirmed

```scala
def fileConstantMetadataExtractors(): Map[String, PartitionedFile => Object]
def copyWithDVInfo(tablePath: String, hasFullDV: Boolean): DeltaParquetFileFormat
def tablePath: Option[String]  // field
```

---

## 5. How to Implement `GpuDeltaParquetFileFormat` for DB-17.3

### 5.1 What NOT to Do

Do **NOT** copy DB-14.3's `GpuDeltaParquetFileFormat` for DB-17.3. The DB-14.3 file:
- Takes `broadcastDvMap: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]]` — **this class/param does not exist in DB-17.3**
- Tags GPU as unsupported when `hasDeletionVectorMap` — **wrong for DB-17.3**
- Has no `createMultiFileReaderFactory` — **DB-17.3 should support multi-threaded reader**

### 5.2 Blueprint: `GpuDeltaParquetFileFormatBase` (OSS)

Use `delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala`
as the **pattern/blueprint** (NOT as compiled source). It implements the per-file DV approach:

- Reads `FILE_ROW_INDEX_FILTER_ID_ENCODED` and `FILE_ROW_INDEX_FILTER_TYPE` from
  `partitionedFile.otherConstantMetadataColumnValues`
- Uses `DropMarkedRowsFilter`/`KeepMarkedRowsFilter` from OSS namespace
- Implements `createMultiFileReaderFactory` → `DeltaMultiFileReaderFactory`
- GPU DV reads are supported (no GPU-unsupported tagging)

**⚠ CRITICAL: Cannot include `delta-33x-40x` sources via pom.xml.**

The `delta-33x-40x` common code imports from `org.apache.spark.sql.delta.*` (OSS namespace),
which **does not exist** in DB-17.3. DB-17.3 only provides the
`com.databricks.sql.transaction.tahoe.*` namespace. Verified on cluster: `jar tf` of the
DB-17.3 core JAR contains zero `org/apache/spark/sql/delta/` classes.

The DB-17.3 pom.xml correctly avoids the OSS `delta-33x-40x` root and includes:
- `delta-lake/common/src/main/scala` (universal)
- `delta-lake/common/src/main/databricks/scala` (Databricks-shared)
- `delta-lake/common/src/main/db-350db143-400db173/scala` (shared DB-14.3/DB-17.3
  transaction/catalog/data-source/shim code)

For DB-17.3, substitute the Databricks namespace equivalents (all confirmed on cluster):

| OSS (`org.apache.spark.sql.delta`) | Databricks (`com.databricks.sql.transaction.tahoe`) |
|------------------------------------|-----------------------------------------------------|
| `DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED` | Same constant, different package |
| `DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE` | Same constant, different package |
| `deletionvectors.RoaringBitmapArray` | `deletionvectors.RoaringBitmapArray` — **confirmed** |
| `deletionvectors.StoredBitmap` | `deletionvectors.StoredBitmap` — **confirmed** |
| `deletionvectors.DropMarkedRowsFilter` | `deletionvectors.DropMarkedRowsFilter` — **confirmed** |
| `deletionvectors.KeepMarkedRowsFilter` | `deletionvectors.KeepMarkedRowsFilter` — **confirmed** |
| `storage.dv.HadoopFileSystemDVStore` | `storage.dv.HadoopFileSystemDVStore` — **confirmed** |
| `actions.DeletionVectorDescriptor` | `actions.DeletionVectorDescriptor` — **confirmed** |
| `RowIndexFilter.materializeIntoVectorWithRowIndex` | `c.d.sql.io.RowIndexFilter.materializeIntoVectorWithRowIndex` — **confirmed** |
| `RowIndexFilterProvider` | `c.d.sql.io.RowIndexFilterProvider` — **confirmed** |
| `PortableRoaringBitmapArraySerializationFormat` | `deletionvectors.PortableRoaringBitmapArraySerializationFormat` — **confirmed** |
| `NativeRoaringBitmapArraySerializationFormat` | `deletionvectors.NativeRoaringBitmapArraySerializationFormat` — **confirmed** |
| `sources.DeltaSQLConf` | `sources.DeltaSQLConf` — **confirmed** |

**Note: DB-14.3 has zero GPU DV read support.** DB-14.3's `GpuDeltaParquetFileFormat`
explicitly tags `"deletion vectors are not supported"` when `hasDeletionVectorMap` is true.
DB-17.3 is now the **first** Databricks module with GPU-accelerated DV reads.

### 5.3 How `DeltaSpark400DB173Provider.convertToGpu` Should Construct the Format

```scala
// DB-14.3 (WRONG for DB-17.3):
case fmt: DeltaParquetFileFormat =>
  GpuDeltaParquetFileFormat(fmt.columnMappingMode, ..., fmt.broadcastDvMap,
                            fmt.tablePath, fmt.broadcastHadoopConf)

// DB-17.3 (CORRECT):
case fmt: DeltaParquetFileFormat =>
  GpuDeltaParquetFileFormat(fmt.protocol, fmt.metadata,
                            fmt.generateRowIndexFilterId,
                            fmt.generateRowIndexFilterColumn,
                            fmt.generateDeltaFileInScanId,
                            fmt.nullableRowTrackingConstantFields,
                            fmt.nullableRowTrackingGeneratedFields,
                            fmt.optimizationsEnabled,
                            fmt.tablePath,
                            fmt.isCDCRead)
```

### 5.4 Multi-Threaded Reader

`GpuDeltaParquetFileFormatBase` already implements `createMultiFileReaderFactory`.
DB-17.3's `GpuDeltaParquetFileFormat` should also implement it (directly or by extending
the base class). This enables the multi-threaded Parquet reader for DV tables.

See: `delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala`

### 5.5 GPU-Native cuDF DV API (Delivered 2026-05-05)

`GpuDeltaParquetFileFormatBase2` (also in `delta-33x-40x`) uses cuDF-native bitmap APIs:
- `DeletionVector.newParquetChunkedReader(HostMemoryBuffer)`
- `MakeParquetTableWithDVProducer`

For DB-17.3, the native follow-up uses DBR `RowIndexFilterProvider` /
`SerializedBitmap` data and converts it to cuDF-compatible standard portable
roaring bitmap bytes through `DeltaBitmapUtils.java`. That Java bridge is used
because the relevant DBR bitmap serialization helpers are not directly callable
from Scala in this module.

---

## 6. Key DB-14.3 → DB-17.3 Command Differences (Non-DV)

These are already in the DB-14.3 baseline (no change needed):

| Area | DB-14.3 (baseline) | DB-17.3 |
|------|--------------------|---------|
| Write command | `WriteIntoDeltaEdge` | Same (but constructor grew to 13 params; new params have defaults) |
| `OptimisticTransaction` constructor | `(deltaLog, Option.empty[CatalogTable], snapshot)` | Same — confirmed |
| `writeFiles()` override | `(Dataset, Option[DeltaOptions], Seq[Constraint])` | **CHANGED**: `(Dataset, TransactionalWriteOptions, Boolean, Boolean, Seq[Constraint], Boolean, Option[String])` |
| `TransactionalWriteOptions` | Does not exist | **NEW** case class wrapping `DeltaOptions` + optimize/validate flags |
| `MergeIntoCommandEdge` constructor | `(source, target, fileIndex, condition, clauses..., migratedSchema)` | **CHANGED**: added `catalogTable`, `targetFileIndex`, `trackHighWaterMarks`, `schemaEvolutionEnabled` |
| `DeleteCommandEdge` constructor | `(fileIndex, target, condition)` | **CHANGED**: added `catalogTable` |
| `UpdateCommandEdge` constructor | `(fileIndex, target, updateExprs, condition)` | **CHANGED**: added `catalogTable` |
| `Column(Expression)` constructor | Present | **REMOVED** — use `DFUDFShims.exprToColumn(expr)` |
| `Dataset.ofRows(spark, plan)` | Present | Needs `TrampolineConnectShims.createDataFrame()` |
| `DeletionVectorUtils` | Object | **Changed to trait+companion** (calls still work via defaults) |
| DV metrics in Delete/Update | 3: `numDVAdded/Removed/Updated` | Same |
| `filterFiles` in Merge | `filterFiles(predicates, keepNumRecords=true)` | Same |
| Encoder construction | `ExpressionEncoder(RowEncoder.encoderFor(schema))` | Same |
| Schema attributes | `toAttributes(schema)` (via `DataTypeUtils`) | Same |
| `GpuDeltaCatalog.getWriter()` | Uses `WriteIntoDeltaEdge` | Same |
| `GpuLowShuffleMergeCommand` | Removed | N/A |

**Previously listed as unknowns — all resolved via DB-17.3 cluster verification (2026-04-07):**

1. **`SparkSession` aliasing** — **CONFIRMED.** `org.apache.spark.sql.classic.SparkSession`
   present in DB-17.3. `TrampolineConnectShims` in `sql-plugin/src/main/spark400/` already
   includes `{"spark": "400db173"}`. DB-17.3 command files WILL use this.
2. **`DFUDFShims`** — **CONFIRMED.** `DFUDFShims` in `sql-plugin/src/main/spark400/` already
   includes `{"spark": "400db173"}`. Needed for `GpuMergeIntoCommand` UDF invocations.
3. **`GpuFileFormatWriter.write()` signature** — **CONFIRMED.** `GpuFileFormatWriter` in
   `sql-plugin/src/main/spark332db/` already includes `{"spark": "400db173"}`. No change needed.
4. **`GpuOptimizeExecutor` clustering APIs** — **CONFIRMED.** Same packages on cluster:
   `com.databricks.sql.io.skipping.MultiDimClustering` and
   `com.databricks.sql.io.skipping.liquid.ClusteringColumnInfo`. No import changes.
5. **`RowTracking` import** — **CONFIRMED.** `com.databricks.sql.transaction.tahoe.RowTracking`
   present on DB-17.3 cluster. Import path unchanged from DB-14.3.

Additional classes confirmed on the DB-17.3 cluster:
- `WriteIntoDelta` AND `WriteIntoDeltaEdge` — both present
- `AtomicCreateTableAsSelectExec` / `AtomicReplaceTableAsSelectExec` — confirmed
- `OptimizeExecutor` at `com.databricks.sql.transaction.tahoe.commands.optimize.OptimizeExecutor`
- `DeltaInvariantViolationException` at `com.databricks.sql.transaction.tahoe.schema`
- `DeltaUDF` at `com.databricks.sql.transaction.tahoe.DeltaUDF`
- `DeltaStatistics` at `com.databricks.sql.transaction.tahoe.stats.DeltaStatistics`
- `DataTypeUtils` at `org.apache.spark.sql.catalyst.types.DataTypeUtils`
- `ExpressionEncoder` / `RowEncoder` — confirmed in catalyst JAR
- `DeltaSQLConf` at `com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf`

---

## 7. Build System Changes Required

### 7.1 Root `pom.xml` (inside `#if scala-2.13` block, ~lines 633–654)

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

### 7.2 New Module `delta-lake/delta-spark400db173/pom.xml`

Copy from `delta-lake/delta-spark350db143/pom.xml`, changing:
- `artifactId`: `rapids-4-spark-delta-spark400db173_2.12`
- `name`: "RAPIDS Accelerator for Apache Spark Databricks 17.3 Delta Lake Support"
- Parent and description: identical to DB-14.3
- Build-helper source dirs: `common/src/main/scala`,
  `common/src/main/databricks/scala`, and
  `common/src/main/db-350db143-400db173/scala`

### 7.3 Generate `scala2.13/` Mirror

```bash
./build/make-scala-version-build-files.sh 2.13
```

Run after any pom.xml change. Both the primary and `scala2.13/` poms must be committed.

---

## 8. Step-by-Step Implementation Plan

```bash
# Step 1: Scaffold
cp -r delta-lake/delta-spark350db143 delta-lake/delta-spark400db173

# Step 2: Rename all 350db143 → 400db173 occurrences
cd delta-lake/delta-spark400db173
find . -name "*350db143*" | while read f; do mv "$f" "${f//350db143/400db173}"; done
grep -rl "350db143" . | xargs sed -i 's/350db143/400db173/g'
grep -rl "DB143\|DB14\.3\|db143" . | xargs sed -i 's/DB143/DB173/g; s/DB14\.3/DB17\.3/g; s/db143/db173/g'
# Update copyright year to 2026 if needed
```

```bash
# Step 3: REDESIGN GpuDeltaParquetFileFormat.scala for DB-17.3
# See Section 5 above — model on GpuDeltaParquetFileFormatBase (delta-33x-40x)
# Do NOT use DB-14.3's broadcastDvMap approach
```

```bash
# Step 4: Update root pom.xml
# Apply diff from Section 7.1 to pom.xml (inside #if scala-2.13 block)
```

```bash
# Step 5: Sync scala2.13/
./build/make-scala-version-build-files.sh 2.13
```

```bash
# Step 6: Compile (must use -f scala2.13)
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests \
    -pl delta-lake/delta-spark400db173 -am \
    -Dmaven.scalastyle.skip=true  # skip for initial compile iteration
```

```bash
# Step 7: Fix Spark 4.0 API incompatibilities (CRITICAL — see design doc §7.1)
# These are NOT unknowns — they are confirmed required changes:
#
# 7a. Replace `new Column(expr)` → `DFUDFShims.exprToColumn(expr)`
#     - GpuMergeIntoCommand.scala (~9 occurrences)
#     - GpuDeleteCommand.scala (~2 occurrences)
#     - GpuUpdateCommand.scala (~3 occurrences)
#     Add: import org.apache.spark.sql.nvidia.DFUDFShims
#
# 7b. Replace `Dataset.ofRows(spark, plan)` → `TrampolineConnectShims.createDataFrame()`
#     - GpuOptimisticTransaction.scala (2 occurrences)
#     - GpuUpdateCommand.scala (2 occurrences)
#     - GpuMergeIntoCommand.scala (6 occurrences)
#     Add: import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
#
# 7c. Replace `SparkSession.getActiveSession` → `TrampolineConnectShims.getActiveSession`
#     - GpuDeltaParquetFileFormat.scala
#
# 7d. Add RuntimeReplaceable handling for stats expressions
#     - GpuOptimisticTransaction.scala
#     Pattern: expr.transform { case rr: RuntimeReplaceable => rr.replacement }
#
# 7e. MAJOR: writeFiles() signature completely changed in DB-17.3
#     DB-14.3: writeFiles(Dataset, Option[DeltaOptions], Seq[Constraint])
#     DB-17.3: writeFiles(Dataset, TransactionalWriteOptions, Boolean, Boolean, Seq[Constraint], Boolean, Option[String])
#     TransactionalWriteOptions wraps DeltaOptions + optimize/validate flags
#     Must rewrite GpuOptimisticTransaction.writeFiles() override
#
# 7f. MAJOR: Command Edge constructors changed in DB-17.3
#     MergeIntoCommandEdge: added catalogTable, targetFileIndex, trackHighWaterMarks, schemaEvolutionEnabled
#     DeleteCommandEdge: added catalogTable (tahoeFileIndex was already there)
#     UpdateCommandEdge: added catalogTable
#     Must update GPU command constructors + shim convertToGpu methods
#
# 7g. WriteIntoDeltaEdge constructor grew to 13 params (params 7-13 have defaults)
#     New: clusteringColumns, tableAliasOpt, snapshotAtAnalysis, jobGroupIdAtAnalysis, autoUpdateStatsSchemaOpt
#     Existing call sites likely compile due to defaults — verify
#
# 7h. DeletionVectorUtils changed from object to trait+companion
#     Existing calls still work via companion + default params — no code change needed
#
# 7i. Remove `(implicit clock: Clock)` from GpuOptimisticTransactionBase and GpuOptimisticTransaction
#     DB-17.3 OptimisticTransaction no longer takes implicit Clock
#     Clock is obtained internally from DeltaLog.clock()
#     The shared GpuDeltaLog._clock implicit is unused but harmless (lazy val)
#
# 7j. MAJOR: PostCommitHook.run() signature completely changed
#     DB-14.3: run(spark, txn, committedVersion, postCommitSnapshot, committedActions)
#     DB-17.3: run(spark, txn: CommittedTransaction)  — new bundled case class
#     Must rewrite GpuDoAutoCompaction.run() to extract fields from CommittedTransaction
#
# 7k. MAJOR: LogicalRelation pattern match arity changed from 4 to 7 fields
#     DB-17.3 added: sparkDataStream, injectedConstraints, statistics
#     Fix: add wildcard `_` for new fields in pattern matches
#     Files: GpuMergeIntoCommand.scala (~line 948), GpuOptimisticTransactionBase.scala (~line 178)
#
# 7l. MAJOR (SHARED CODE): ShuffledRowRDD constructor changed
#     File: common/databricks/scala/.../OptimizeWriteExchangeExec.scala
#     DB-17.3 requires PrismMetrics parameter — shared code passes 2-3 args only
#     Need version-specific override or shim in delta-spark400db173 module
#
# 7m. MAJOR (SHARED CODE): CheckDeltaInvariant.columnExtractors type changed
#     File: common/databricks/scala/.../GpuCheckDeltaInvariant.scala
#     DB-14.3: Map[String, Expression]   DB-17.3: Seq[(String, Expression)]
#     Need .toMap in shared code or version-specific override
#
# Reference: delta-lake/delta-40x/ shows the exact Spark 4.0 patterns
# Reference: design doc §7.1.6-7.1.9 for full details
```

```bash
# Step 8: Fix remaining compile errors
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests \
    -pl delta-lake/delta-spark400db173 -am \
    -Dmaven.scalastyle.skip=true
# Compare against delta-lake/delta-40x/ for any other Spark 4.0 API patterns
```

```bash
# Step 9: Full build
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests
```

```bash
# Step 10: Run tests
mvn -f scala2.13 -Dbuildver=400db173 package -pl tests -am \
    -DwildcardSuites="com.nvidia.spark.rapids.delta.*"
```

```bash
# Step 11: Enable Delta tests in CI
# Remove the skip in jenkins/databricks/test.sh (lines 152-160)
# that says "Skipping Delta Lake tests: not yet supported for DB-17.3"
```

---

## 9. Key Reference File Locations

| Purpose | Path |
|---------|------|
| **Primary copy source (DB-14.3)** | `delta-lake/delta-spark350db143/` |
| `scala2.13/` mirror of DB-14.3 | `scala2.13/delta-lake/delta-spark350db143/` |
| Databricks-shared common code | `delta-lake/common/src/main/databricks/scala/` |
| DB-14.3/DB-17.3 shared transaction/catalog/data-source code | `delta-lake/common/src/main/db-350db143-400db173/scala/` |
| Universal Delta common code | `delta-lake/common/src/main/scala/` |
| **Blueprint for DV file format (OSS)** | `delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala` |
| GPU-native cuDF DV API (delivered locally 2026-05-05) | `delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase2.scala` |
| DV bitmap loading utility | `delta-lake/common/src/main/delta-33x-40x/scala/org/apache/spark/sql/delta/deletionvectors/RapidsDeletionVectorStore.scala` |
| DV RapidsRowIndexFilters (OSS) | `delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsRowIndexFilters.scala` |
| OSS Spark 4.0 Delta (Spark 4.0 API patterns) | `delta-lake/delta-40x/` |
| `scala2.13/` mirror of OSS 4.0 Delta | `scala2.13/delta-lake/delta-40x/` |
| DB-17.3 SQL plugin shims | `sql-plugin/src/main/spark400db173/` |
| Spark 4.0 shared shims (covers 400db173) | `sql-plugin/src/main/spark400/` |
| `TrampolineConnectShims` (covers 400db173) | `sql-plugin/src/main/spark400/scala/org/apache/spark/sql/rapids/shims/TrampolineConnectShims.scala` |
| `DFUDFShims` (covers 400db173) | `sql-plugin/src/main/spark400/scala/org/apache/spark/sql/nvidia/DFUDFShims.scala` |
| `GpuFileFormatWriter` (covers 400db173) | `sql-plugin/src/main/spark332db/scala/org/apache/spark/sql/rapids/GpuFileFormatWriter.scala` |
| **Spark 4.0 command shims (reference)** | `delta-lake/delta-40x/src/main/scala/.../Delta40xCommandShims.scala` |
| **Spark 4.0 runtime shim (reference)** | `delta-lake/delta-40x/src/main/scala/.../Delta40xRuntimeShim.scala` |
| **Jenkins test skip (to remove)** | `jenkins/databricks/test.sh` lines 152–160 |
| Root pom.xml (edit inside #if scala-2.13) | `pom.xml` lines ~633–654 |
| Scala 2.13 root pom | `scala2.13/pom.xml` |
| Scala version sync script | `build/make-scala-version-build-files.sh` |
| **Full design document** | `delta-lake-db173-design.md` |

---

## 10. Completed Research (Do Not Repeat)

### Original research (2026-03-19)
- [x] Explored all `delta-lake/delta-spark*db*/` module structures
- [x] Read all 21 files in `delta-lake/delta-spark350db143/` (primary reference)
- [x] Read `GpuDeltaParquetFileFormatBase.scala` and `GpuDeltaParquetFileFormatBase2.scala`
- [x] Read `RapidsDeletionVectorStore.scala` and `RapidsStoredBitmap.scala`
- [x] Read `delta-lake/delta-40x/` OSS Spark 4.0 wrappers
- [x] Read `delta-lake/common/src/main/databricks/scala/` shared files
- [x] Decompiled `spark-sql_2.13-4.0.0-databricks-173.jar` — confirmed DV API
- [x] Decompiled `spark-catalyst_2.13-4.0.0-databricks-173.jar` — confirmed DeltaSQLConf
- [x] Verified `DELETE/UPDATE_USE_PERSISTENT_DELETION_VECTORS` present in DB-17.3
- [x] Verified `DropMarkedRowsFilter`/`KeepMarkedRowsFilter` API unchanged in DB-17.3
- [x] Updated design doc (`delta-lake-db173-design.md`) with confirmed findings
- [x] Updated plan file with critical DV API differences

### Live DB-17.3 cluster verification (2026-04-07)
- [x] Confirmed actual JAR naming on cluster (`----ws_4_0--sql--core--...` format)
- [x] Verified all 5 "Spark 4.0 Unknowns" are resolved:
  - [x] `SparkSession` aliasing (`classic.SparkSession` exists)
  - [x] `DFUDFShims` (covers `400db173` in `spark400` shim)
  - [x] `GpuFileFormatWriter.write()` (covers `400db173` in `spark332db` shim)
  - [x] `MultiDimClustering`/`ClusteringColumnInfo` (same packages)
  - [x] `RowTracking` (same import path)
- [x] Confirmed `RoaringBitmapArray` exists in DB-17.3 at `c.d.s.t.tahoe.deletionvectors` (via `javap`;
  located in the **catalyst** JAR: `----ws_4_0--sql--catalyst--catalyst-hive-2.3__hadoop-3.2_2.13_deploy.jar`)
- [x] Confirmed `PortableRoaringBitmapArraySerializationFormat` / `NativeRoaringBitmapArraySerializationFormat`
- [x] Confirmed `StoredBitmap`, `DeletionVectorStoredBitmap`, `HadoopFileSystemDVStore`
- [x] Confirmed `DeletionVectorStore` / `DeletionVectorStoreEdge` interfaces
- [x] Confirmed `DeltaSQLConf` at `c.d.s.t.tahoe.sources.DeltaSQLConf`
- [x] Confirmed `delta-33x-40x` common code uses OSS namespace (`org.apache.spark.sql.delta.*`)
  which does NOT exist in DB-17.3 — cannot be included via pom.xml
- [x] Confirmed DB-14.3 has zero GPU DV read support (explicitly tags `"deletion vectors are not supported"`)
- [x] Verified all additional command/utility classes present on cluster:
  `WriteIntoDelta`, `WriteIntoDeltaEdge`, `AtomicCreateTableAsSelectExec`,
  `AtomicReplaceTableAsSelectExec`, `OptimizeExecutor`, `DeltaInvariantViolationException`,
  `DeltaUDF`, `DeltaStatistics`, `DataTypeUtils`, `ExpressionEncoder`, `RowEncoder`

### Critical review — additional findings (2026-04-07)
- [x] `PostCommitHook.run()` signature changed: `(spark, txn, version, snapshot, actions)` →
  `(spark, CommittedTransaction)` — must rewrite `GpuDoAutoCompaction.run()`
- [x] `LogicalRelation` unapply arity changed from 4 to 7 fields — pattern matches in
  `GpuMergeIntoCommand` (~line 948) and `GpuOptimisticTransactionBase` (~line 178) must update
- [x] **SHARED CODE ISSUE**: `ShuffledRowRDD` constructor in DB-17.3 requires `PrismMetrics` —
  `OptimizeWriteExchangeExec.scala` (shared) creates it with 2-3 args, must add override
- [x] **SHARED CODE ISSUE**: `CheckDeltaInvariant.columnExtractors` type changed from
  `Map[String, Expression]` to `Seq[(String, Expression)]` — `GpuCheckDeltaInvariant.scala`
  (shared) must handle this
- [x] `DeleteCommand.deltaLog` / `DeleteCommandEdge.deltaLog` — confirmed accessible via
  `DeltaDMLCommandEdge` parent in DB-17.3 (shared code is compatible)
- [x] `UpdateCommand.tahoeFileIndex` / `UpdateCommandEdge.tahoeFileIndex` — confirmed present
- [x] `MergeIntoCommand(Edge)` fields (source, target, targetFileIndex, clauses, etc.) — all confirmed
- [x] `DeltaStatistics` constants (NUM_RECORDS, MIN, MAX, NULL_COUNT) — confirmed same path
- [x] `DeltaUDF.stringFromString` — confirmed same signature
- [x] `InvariantViolationException` / `DeltaInvariantViolationException` — confirmed compatible
- [x] `DeleteMetric` constructor grew to 59 params (24 original + 35 with defaults) — compatible
- [x] `UpdateMetric` constructor grew to 58 params (11 original + 47 with defaults) — compatible
- [x] `TahoeBatchFileIndex` 6th param type changed Snapshot→SnapshotDescriptor (compatible via inheritance)
- [x] RoaringBitmap version mismatch: pom declares 1.0.6, cluster has 1.2.1 — safe due to shading
- [x] 15 of 17 shared databricks files are compatible; 2 need version-specific overrides

---

## 11. Out of Scope (This PR)

- DV **writes** on GPU — CPU fallback retained (shims already block GPU for DV write configs)
- `notMatchedBySourceClauses` in MERGE — CPU fallback retained (issue #8415)
- GPU-native cuDF DV API (`GpuDeltaParquetFileFormatBase2` pattern) — delivered locally
  in the 2026-05-05 native DV follow-up
- Scala 2.12 support — Spark 4.0 / DB-17.3 does not support it

---

## 12. Test Run Status (2026-04-28)

First full `delta_lake_write_test.py` pass on a live DB-17.3 cluster after Issues 2 and
8 landed. Result: 6 failed, 95 passed, 2 skipped, 22 xfailed, 4 xpassed, 44334
deselected (15-test run took ~14 min on the cluster).

### 12.1 Wide-schema test infra mitigation

The default `gen_df(...)` length of 2048 combined with the wide `delta_write_gens`
schema (63 columns from `parquet_write_gens_list`, includes maps, nested structs, and
arrays of arrays) materializes ~33 MB of generated rows directly into the
`RDDScanExec` attributes baked into the task closure. On DB-17.3 the
`dispatcher-event-loop` thread OOMs while serializing the launch task:

```
Exception in thread "dispatcher-event-loop-2" java.lang.OutOfMemoryError: Java heap space
    at java.io.ByteArrayOutputStream.write(...)
    at org.apache.spark.serializer.JavaSerializerInstance.serialize(...)
    at org.apache.spark.scheduler.TaskSetManager.prepareLaunchingTask(TaskSetManager.scala:925)
    at org.apache.spark.scheduler.TaskSetManager.resourceOffer(TaskSetManager.scala:724)
```

Fix is a one-line constant in
[integration_tests/src/main/python/delta_lake_utils.py](integration_tests/src/main/python/delta_lake_utils.py):

```python
delta_db173_wide_schema_gen_length = 128 if is_databricks173_or_later() else 2048
```

8 wide-schema tests in [delta_lake_write_test.py](integration_tests/src/main/python/delta_lake_write_test.py)
pass `length=delta_db173_wide_schema_gen_length` to `gen_df`:

| Test | Site |
|------|------|
| `test_delta_write_round_trip_managed` | inline `gen_df` |
| `test_delta_write_round_trip_unmanaged` | inline `gen_df` |
| `test_delta_atomic_create_table_as_select` | helper `_atomic_write_table_as_select` |
| `test_delta_atomic_replace_table_as_select` | helper `_atomic_write_table_as_select` |
| `test_delta_ctas_sql` | helper `_atomic_write_table_as_select_sql` |
| `test_delta_rtas_sql` | helper `_atomic_write_table_as_select_sql` |
| `test_delta_append_data_exec_v1` | inline (setup + write) |
| `test_delta_overwrite_by_expression_exec_v1` | inline (`setup_src_table`) |
| `test_delta_write_optimized_supported_types_partitioned` | inline `gen_df` |

The constant resolves to 2048 on every shim other than DB-17.3 — no behavior change off
DB-17.3. Cumulative driver-state pressure across the 4 variants of
`test_delta_append_data_exec_v1` (use_cdf × enable_deletion_vectors) was the original
symptom; reducing the per-task closure body from ~33 MB to ~2 MB also resolves it
without needing per-test cleanup hooks.

Two earlier exploratory mitigations were dropped after the length cap proved
sufficient:
- `delta_db173_collect_oom_mitigation_conf` capping
  `spark.sql.parquet.columnarReaderBatchSize` to 512 — targeted executor-side
  `UnsafeRowBatchUtils.encodeUnsafeRows` pressure during read-back. Trivial at 128 rows.
- `_db173_driver_state_cleanup` opt-in fixture (CLEAR CACHE + `clearCache()` +
  `System.gc()`) — targeted cumulative SQL-cache buildup across variants. Not load-bearing
  once the per-task closure shrank.

### 12.2 6 failures from this run — all resolved (see §0.E)

The 6 failures from this first run, originally filed as Issue 10, broke into two
groups. **Status as of 2026-04-28: all resolved.** See [§0.E](#0e-issue-10-resolution--11169-xfails-2026-04-28)
for the resolution narrative; below is the diagnostic snapshot from the run that
motivated the fix, kept for history.

#### Group A — runtime `IllegalArgumentException: Part of the plan is not columnar class DataWritingCommandExec` (4 tests)

- `test_delta_overwrite_mixed_clause[PARTITION (id, p = 2)-STATIC]`
- `test_delta_overwrite_mixed_clause[PARTITION (p = 2, id)-STATIC]`
- `test_delta_overwrite_mixed_clause[PARTITION (p = 2)-STATIC]`
- `test_delta_write_partial_overwrite_replace_where`

Captured plan fragment (paraphrased):

```
Execute WriteIntoDeltaCommand
+- WriteFiles
   +- GpuColumnarToRow
      +- GpuSort [id ASC, p ASC], com.nvidia.spark.rapids.OutOfCoreSort
         +- GpuRowToColumnar
            +- DeltaInvariantChecker [checkdeltainvariant((p <=> 2), ...)]   ← CPU
               +- GpuColumnarToRow
                  +- GpuProject [id, data, 2 AS p]
                     +- GpuRowToColumnar
                        +- Scan ExistingRDD[...]
```

`DeltaInvariantChecker` (the operator, `org.apache.spark.sql.execution.command.*`) stays
on CPU but has GPU children and a GPU parent. At runtime `WriteFiles` calls into the
columnar pipeline and asserts that the subtree is all-columnar; the CPU operator trips
the assertion.

#### Group B — planning `AssertionError: CheckDeltaInvariant expected 1 but found 3` (2 tests)

- `test_delta_write_constraint_check`
- `test_delta_write_constraint_check_fallback`

```
java.lang.AssertionError: assertion failed: CheckDeltaInvariant expected 1 but found 3
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

`ContextChecks.tagBase` validates the expression's child arity against the meta's
declared shape. The shared `GpuCheckDeltaInvariant` expression meta declares 1 child
(consistent with DB-14.3 / OSS Delta), but DB-17.3's `CheckDeltaInvariant` is built as a
3-child node: `Check(EXPRESSION(<expr>), <expr>, <originalCheck?>)` — the `Check` form
visible in the planner's pretty-print:
`Check(EXPRESSION(('p <=> 2)),('p <=> 2))`.

The 7.1.13 fix already adapted the
shared code's `columnExtractors` access for the DB-17.3 type change (`Map[String,
Expression]` → `Seq[(String, Expression)]`); the per-version arity claim in the
`ExprChecks` registration was missed.

#### Resolution (final, 2026-04-28)

Initial speculation that Group A and Group B shared a root cause was wrong — they hit
two separate code paths and were addressed independently:

- **Group B** turned out to be exactly the arity claim in the meta's `ExprChecks`
  registration. Fixed in shared `GpuCheckDeltaInvariant.scala` by switching to
  `ExprChecks.projectOnly + paramCheck + repeatingParamCheck` (matches OSS Delta).
  See §0.E.
- **Group A** is independent — the affected SQL forms route through the V1
  `WriteIntoDeltaCommand` path on DB-17.3, which never invokes our `gpuWriteFiles`
  flow at all. No `addInvariantChecks` runs for these tests; the `DeltaInvariantCheckerExec`
  operator visible in their captured plan was inserted by Delta's analyzer + Spark's
  V1Writes rule, not by our code. This is the same V1 limitation tracked by upstream
  #11169; the 4 tests are xfailed for `is_databricks173_or_later()` referencing
  #11169. See §0.E.

Issue 10 can be closed. The proper code-level fix for the V1
`WriteIntoDeltaCommand` GPU acceleration follow-up rolls under #11169.
