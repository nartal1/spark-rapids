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

**New module:** `delta-lake/delta-spark400db173/`
- `pom.xml`
- 3 provider/probe files (`DeltaProbe`, `DeltaSpark400DB173Provider`, `GpuDeltaParquetFileFormat`)
- 7 shim files (`DeleteCommandMetaShim`, `UpdateCommandMetaShim`, `MergeIntoCommandMetaShim`,
  `DeltaLogShim`, `MetadataShims`, `ShimDeltaUDF`, `InvariantViolationExceptionShim`,
  `ShimShuffledRowRDD`)
- 11 command/transaction files (`GpuOptimisticTransactionBase`, `GpuOptimisticTransaction`,
  `GpuWriteIntoDelta`, `GpuDeleteCommand`, `GpuUpdateCommand`, `GpuMergeIntoCommand`,
  `GpuCreateDeltaTableCommand`, `GpuDeltaDataSource`, `GpuDeltaCatalog`, `GpuDoAutoCompaction`,
  `GpuOptimizeExecutor`)
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

**GPU-enabled in this PR:** none.
**CPU fallback in this PR:** writes, DELETE, UPDATE, MERGE, OPTIMIZE, DV reads.

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

Enable GPU-accelerated Delta Lake write operations (INSERT, CTAS, RTAS, append, overwrite,
path-style INSERT OVERWRITE) on Databricks 17.3. Split from Issue 1 once cluster
validation showed the DB-14.3 interception point was dead code on DB-17.3.

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

Writes through `.write.format("delta").save/saveAsTable/...`, CTAS, RTAS, `INSERT`,
`INSERT OVERWRITE delta.\`<path>\``. Partitioned writes work (including timestamp
partitions).

### Still on CPU after this issue

DELETE, UPDATE, MERGE, OPTIMIZE (Issues 3/4/5), DV reads (Issue 6). Plus writes to
identity-column tables currently block `ProjectExec` to CPU via `GenerateIdentityValues`
(Issue 9).

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
The `GpuDeleteCommand` / `GpuUpdateCommand` classes are already in-tree from Issue 1
(with Spark 4.0 API adapters applied); this issue wires them into the shims and fixes any
DB-17.3-specific runtime issues.

### Scope

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
by code size (~1000 lines); warrants separate review. The `GpuMergeIntoCommand` class is
already in-tree with Spark 4.0 fixes.

### Scope

- Update `MergeIntoCommandMetaShim.scala` to enable the GPU path (remove the
  unconditional `willNotWorkOnGpu`, fix the existing `convertToGpu` to pass the new
  DB-17.3 fields `catalogTable`, `targetFileIndex`, `trackHighWaterMarks`,
  `schemaEvolutionEnabled` — the dead `convertToGpu` left from Issue 1 uses the old
  DB-14.3 8-arg signature and will not compile once re-enabled).
- Verify `GpuMergeIntoCommand` on DB-17.3.

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
Delta Lake on Databricks 17.3. Includes liquid clustering support. The
`GpuOptimizeExecutor` and `GpuDoAutoCompaction` classes are already in-tree with
`CommittedTransaction` API adapted.

### Scope

- Wire `GpuOptimizeExecutor` so it runs on DB-17.3.
- Wire `GpuDoAutoCompaction` post-commit hook (signature `run(spark, CommittedTransaction)`
  — already adapted in Issue 1).
- Fix runtime issues as they surface.

### Tests

Run on a DB-17.3 cluster:
- `delta_lake_optimize_table_test.py`
- `delta_lake_liquid_clustering_test.py`

### Depends on

Issue 2

---

## Issue 6: [databricks] Delta Lake DB-17.3: GPU Deletion Vector reads

### Description

Enable GPU-accelerated Deletion Vector (DV) reads for Delta Lake on Databricks 17.3.

DB-17.3 uses a fundamentally different DV mechanism than DB-14.3:
- DB-14.3: Broadcast `Map[URI, DeletionVectorDescriptorWithFilterType]` (GPU explicitly blocked DVs)
- DB-17.3: Per-file DVs via `PartitionedFile.otherConstantMetadataColumnValues` with
  `RowIndexFilterProvider` interface

This will be the **first Databricks module** with GPU-accelerated DV reads.

### Scope

**Rewrite `GpuDeltaParquetFileFormat.scala`:**
- Use per-file DV mechanism via `FILE_ROW_INDEX_FILTER_ID_ENCODED` / `FILE_ROW_INDEX_FILTER_TYPE`
  from `PartitionedFile.otherConstantMetadataColumnValues`
- Use `RowIndexFilterProvider.retrieve(conf)` from `c.d.s.t.tahoe.deletionvectors.*`
- Implement `createMultiFileReaderFactory` for GPU multi-threaded reader on DV tables
- Remove GPU-unsupported tag for DVs (DB-14.3 blocked GPU for DVs; DB-17.3 enables it)
- Update `DeltaSpark400DB173Provider.convertToGpu` to construct from DB-17.3 params
  (`protocol`, `metadata`, `tablePath`, etc.)

**Blueprint:** `delta-lake/common/src/main/delta-33x-40x/scala/.../GpuDeltaParquetFileFormatBase.scala`
(with Databricks namespace substitutions — `delta-33x-40x` code uses OSS imports and cannot
be compiled for DB-17.3)

### Out of scope (follow-up PR)

- GPU-native cuDF DV API (`GpuDeltaParquetFileFormatBase2` pattern using
  `DeletionVector.newParquetChunkedReader`) — serialization format compatibility needs
  verification before enabling

### Tests

Run on a DB-17.3 cluster:
- `delta_lake_test.py` (DV read tests: `test_delta_deletion_vector_read`,
  `test_delta_deletion_vector_multithreaded_read`, etc.)
- `delta_lake_delete_test.py` (DV-specific: `test_delta_deletion_vector`,
  `test_delta_deletion_vector_read_drop_row_group`, etc.)

### Depends on

Issues 2-5 (all DML commands working)

---

## Issue 8 (NEW): [databricks] DB-17.3 GPU Delta read path: FileNotFoundException on relative paths

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

### Hypothesis

`GpuParquetFileFilterHandler.readAndSimpleFilterFooter` is being handed a relative path
by the upstream `FileIndex` / listing on DB-17.3's Delta. Either the DB-17.3
`TahoeFileIndex` changed how it materializes absolute paths, or there's a
GPU-scan-specific code path that takes `AddFile.path` (stored relative in the log) and
doesn't resolve it against `deltaLog.dataPath`.

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

### Workaround until this lands

XFAIL the three tests above on DB-17.3:
```python
@pytest.mark.xfail(
    condition=is_databricks173_or_later(),
    reason="https://github.com/NVIDIA/spark-rapids/issues/XXXXX: "
           "GPU Delta read hits FileNotFoundException on relative path"
)
```

### Depends on

Independent. Can be tackled in parallel with Issues 3/4/5. Blocks Issue 7 (CI enablement).

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

Issues 2-6, 8 (all GPU Delta functionality working, including DV reads + relative-path read fix)

---

## Dependency Graph (updated 2026-04-23 after Issue 2 delivery)

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
          +---> Issue 6: GPU Deletion Vector reads (depends on 2-5)
          |
          +---> Issue 9: GPU GenerateIdentityValues expression

Issue 8 (independent): GPU Delta read — relative-path FileNotFoundException
                                                    |
Issues 3, 4, 5, 6, 8 (and ideally 9) ----→ Issue 7: Enable Delta tests in CI
```

Issues 3, 4, 5 can be developed in parallel. Issue 6 depends on all DML paths working for
full DV test coverage. Issue 8 is independent of 3/4/5 and can be picked up in parallel.
Issue 7 lands last once the tests actually pass.
