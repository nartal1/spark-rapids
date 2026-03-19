# GitHub Issues for Delta Lake DB-17.3 Support

Parent tracking issue: https://github.com/NVIDIA/spark-rapids/issues/14015

---

## Issue 1: [databricks] Add Delta Lake GPU write support for Databricks 17.3

### Description

Create the `delta-lake/delta-spark400db173/` Maven module to replace `delta-stub` for the
`release400db173` profile, enabling GPU-accelerated Delta Lake write operations (INSERT,
CREATE TABLE AS SELECT, REPLACE TABLE AS SELECT) on Databricks 17.3.

All 21+ Scala files must be created together (required for compilation, since the shared
`DatabricksDeltaProviderBase` registers all command metas). DML commands
(DELETE, UPDATE, MERGE) should initially fall back to CPU and will be GPU-enabled
in subsequent issues.

### Scope

**New module:** `delta-lake/delta-spark400db173/`
- `pom.xml`
- 3 provider/probe files (`DeltaProbe`, `DeltaSpark400DB173Provider`, `GpuDeltaParquetFileFormat`)
- 7 shim files (DeleteCommandMetaShim, UpdateCommandMetaShim, MergeIntoCommandMetaShim,
  DeltaLogShim, MetadataShims, ShimDeltaUDF, InvariantViolationExceptionShim)
- 11 command/transaction files (GpuOptimisticTransactionBase, GpuOptimisticTransaction,
  GpuWriteIntoDelta, GpuDeleteCommand, GpuUpdateCommand, GpuMergeIntoCommand,
  GpuCreateDeltaTableCommand, GpuDeltaDataSource, GpuDeltaCatalog, GpuDoAutoCompaction,
  GpuOptimizeExecutor)
- 1-2 shared code override files (GpuCheckDeltaInvariant, OptimizeWriteExchangeExec)

**Build system changes:**
- Root `pom.xml`: update `release400db173` profile (inside `#if scala-2.13` block) to
  reference `delta-spark400db173` instead of `delta-stub`
- Run `./build/make-scala-version-build-files.sh 2.13` to sync `scala2.13/` mirror

**Spark 4.0 API fixes in ALL files (required for compilation):**
- `new Column(expr)` -> `DFUDFShims.exprToColumn(expr)` (14 occurrences across 3 files)
- `Dataset.ofRows(spark, plan)` -> `TrampolineConnectShims.createDataFrame()` (12 occurrences)
- Remove `(implicit clock: Clock)` from transaction constructors
- `writeFiles()` signature rewrite for `TransactionalWriteOptions`
- `PostCommitHook.run()` rewrite for `CommittedTransaction`
- `LogicalRelation` pattern match arity 4 -> 7
- Command Edge constructor changes (added `catalogTable`, etc.)
- RuntimeReplaceable stats expression handling

**Spark 4.0 API compatibility fixes required throughout**
 
**GPU-enabled in this PR:** Write path only (INSERT, CTAS, RTAS, append, overwrite)
**CPU fallback in this PR:** DELETE, UPDATE, MERGE, OPTIMIZE (enabled in later PRs)
**GpuDeltaParquetFileFormat:** Minimal version adapted for DB-17.3 constructor, no GPU DV reads

### Key constraint

DB-17.3 is **Scala 2.13 only**. All builds require `-f scala2.13 -Dbuildver=400db173`.
**GPU-enabled:** Write path only (INSERT, CTAS, RTAS, append, overwrite)
**CPU fallback:** DELETE, UPDATE, MERGE, OPTIMIZE (to be enabled in subsequent issues)

### Tests

Run `delta_lake_write_test.py` and basic scan/read tests from `delta_lake_test.py`
on a DB-17.3 cluster.

### Build verification

```bash
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests
```

### References

- Copy source: `delta-lake/delta-spark350db143/` (DB-14.3)
- Spark 4.0 API patterns: `delta-lake/delta-40x/.../Delta40xCommandShims.scala`
- Design doc: `delta-lake-db173-design.md`
- Implementation context: `delta-lake-db173-implementation-context.md`
---

## Issue 2: [databricks] Delta Lake DB-17.3: Enable GPU DELETE + UPDATE

### Description

Enable GPU-accelerated DELETE and UPDATE commands for Delta Lake on Databricks 17.3.
These commands will be introduced in Issue 1 with CPU fallback; this issue enables the GPU path.

### Scope

- Update `DeleteCommandMetaShim.scala` to enable GPU path for DELETE
- Update `UpdateCommandMetaShim.scala` to enable GPU path for UPDATE
- Verify `GpuDeleteCommand.scala` works correctly on DB-17.3
- Verify `GpuUpdateCommand.scala` works correctly on DB-17.3
- Fix any runtime issues discovered during testing

Note: DELETE/UPDATE with persistent deletion vectors (`DELETE_USE_PERSISTENT_DELETION_VECTORS`,
`UPDATE_USE_PERSISTENT_DELETION_VECTORS`) continues to fall back to CPU (existing behavior
from DB-14.3, tracked in https://github.com/NVIDIA/spark-rapids/issues/8654).

### Tests

Run on DB-17.3 cluster:
- `delta_lake_delete_test.py` (non-DV-read tests)
- `delta_lake_update_test.py`

### Depends on

Issue 1

---

## Issue 3: [databricks] Delta Lake DB-17.3: Enable GPU MERGE INTO

### Description

Enable GPU-accelerated MERGE INTO command for Delta Lake on Databricks 17.3.
This is the most complex Delta DML command (~1000 lines) and warrants a separate review.

### Scope

- Update `MergeIntoCommandMetaShim.scala` to enable GPU path for MERGE
- Verify `GpuMergeIntoCommand.scala` works correctly on DB-17.3
- Fix any runtime issues discovered during testing

Note: `notMatchedBySourceClauses` continues to fall back to CPU (tracked in
https://github.com/NVIDIA/spark-rapids/issues/8415).

### Tests

Run on DB-17.3 cluster:
- `delta_lake_merge_test.py`

### Depends on

Issue 1

---

## Issue 4: [databricks] Delta Lake DB-17.3: Enable GPU OPTIMIZE + auto-compaction

### Description

Enable GPU-accelerated OPTIMIZE command and auto-compaction post-commit hook for
Delta Lake on Databricks 17.3. Includes liquid clustering support.

### Scope

- Verify `GpuOptimizeExecutor.scala` works correctly on DB-17.3
- Verify `GpuDoAutoCompaction.scala` works correctly on DB-17.3
  (note: `PostCommitHook.run()` signature changed to `CommittedTransaction` in DB-17.3,
  already adapted in Issue 1)
- Fix any runtime issues discovered during testing

### Tests

Run on DB-17.3 cluster:
- `delta_lake_optimize_table_test.py`
- `delta_lake_liquid_clustering_test.py`

### Depends on

Issue 1

---

## Issue 5: [databricks] Delta Lake DB-17.3: GPU Deletion Vector reads

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

Run on DB-17.3 cluster:
- `delta_lake_test.py` (DV read tests: `test_delta_deletion_vector_read`,
  `test_delta_deletion_vector_multithreaded_read`, etc.)
- `delta_lake_delete_test.py` (DV-specific: `test_delta_deletion_vector`,
  `test_delta_deletion_vector_read_drop_row_group`, etc.)

### Depends on

Issues 1-4 (all DML commands working)

---

## Issue 6: [databricks] Delta Lake DB-17.3: Enable Delta Lake tests in CI

### Description

Enable the full `@delta_lake` integration test suite for Databricks 17.3 in CI.
Currently, Delta Lake tests are skipped for DB-17.3 in Jenkins via an explicit guard.
The skip should be removed once all GPU Delta Lake functionality (Issues 1-5) is validated.

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

Issues 1-5 (all Delta Lake GPU functionality working, including DV reads)

---

## Dependency Graph

```
Issue 1: Add Delta Lake GPU write support for Databricks 17.3
  |
  +---> Issue 2: GPU DELETE + UPDATE
  |
  +---> Issue 3: GPU MERGE INTO
  |
  +---> Issue 4: GPU OPTIMIZE + auto-compaction
  |
  +---> Issue 5: GPU Deletion Vector reads (depends on 1-4)
            |
            +---> Issue 6: Enable Delta Lake tests in CI (depends on 1-5)
```

Issues 2, 3, 4 can be developed and reviewed in parallel after Issue 1 merges.
Issue 5 depends on all DML paths working for full DV test coverage.
Issue 6 should come last once all GPU functionality is validated end-to-end.
