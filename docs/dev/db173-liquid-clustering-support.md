# DBR 17.3 Liquid Clustering Support

Last updated: 2026-07-07

This tracker covers the DBR 17.3 liquid clustering work for
NVIDIA/spark-rapids issue 14599, under the broader DBR 17.3 Delta Lake support
epic 14420.

## Completed Work

### Stage 0: Liquid clustered append and static overwrite

Commit: `8756bc0cd Enable DBR 17.3 liquid clustering writes on GPU`

Changed files:

- `delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/GpuOptimisticTransaction.scala`
- `delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/GpuOptimisticTransactionBase.scala`

Summary:

- Removed the DBR 17.3 liquid-clustering CPU fallback from non-CDC
  `writeFiles` paths.
- Reused the existing DBR 17.3 GPU Delta write path for liquid clustered
  append and static overwrite.
- Kept CDC write phases on the CPU path.
- Preserved the DBR 17.3 skip for dynamic partition overwrite because DBR 17.3
  rejects dynamic partition overwrite on liquid clustered tables.

Validation:

- Build passed:

```bash
SKIP_DEP_INSTALL=1 WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
```

- Focused liquid clustering tests passed:

```text
4 passed, 1 skipped, 8 deselected
```

- Full liquid clustering test file passed:

```text
10 passed, 3 skipped, 32 warnings in 183.70s
```

## Stage 1: replaceWhere

Status: blocked for DBR 17.3 GPU support.

OSS Delta 4.0 status:

- Supported in the OSS Delta 4.0 path.
- The OSS implementation in
  `delta-lake/common/src/main/delta-40x-41x/scala/org/apache/spark/sql/delta/rapids/GpuWriteIntoDeltaBase.scala`
  handles `replaceWhere` predicates, remove-file generation, constraints, CDC
  decisions, and commit metrics.

DBR 17.3 findings:

- The non-liquid DBR 17.3 `replaceWhere` test is already xfailed in
  `integration_tests/src/main/python/delta_lake_write_test.py` because DBR
  17.3 plans the write through V1 `WriteIntoDeltaCommand` plus
  `DeltaInvariantChecker`.
- The liquid SQL and DataFrame `replaceWhere` tests in
  `integration_tests/src/main/python/delta_lake_liquid_clustering_test.py`
  currently pass only because DBR 17.3 CPU Delta write fallback is allowed.
- DBR 17.3 has GPU `DeltaInvariantCheckerExec` support in
  `delta-lake/common/src/main/databricks/scala/com/databricks/sql/transaction/tahoe/rapids/GpuDeltaInvariantCheckerExec.scala`,
  so invariant checking is not the primary blocker.
- The primary blocker is the DBR 17.3 atomic replace/delete side plan. It emits
  row-index-set object pipelines and unsupported object expressions:
  `RowIndexSetData`, `RowIndexSetResult`, `SerializeFromObjectExec`,
  `DeserializeToObjectExec`, `MapPartitionsExec`, `ObjectHashAggregateExec`,
  `StaticInvoke`, `Invoke`, `WrapOption`, `UnwrapOption`, `AssertNotNull`, and
  `BitmapAggregator`.
- The plan also touches hidden Delta metadata and row-tracking fields that are
  not currently covered by the GPU write path.

Config probe:

- The DBR 17.3 runtime exposes these relevant configs:
  - `spark.databricks.delta.insertReplaceWhere.parallel.enabled`
  - `spark.databricks.delta.insertAtomicReplace.parallel.enabled`
  - `spark.databricks.delta.liquid.eagerClustering.insertAtomicReplace.enabled`
  - `spark.databricks.delta.insertReplaceOnOrUsing.liquidDomainMetadata.es1599859.fix.enabled`
- A targeted probe with the first three configs set to `false` still planned
  through the unsupported row-index-set object pipeline.

Probe command:

```bash
source jenkins/databricks/setup.sh
SPARK_CONF="spark.databricks.delta.insertReplaceWhere.parallel.enabled=false,spark.databricks.delta.insertAtomicReplace.parallel.enabled=false,spark.databricks.delta.liquid.eagerClustering.insertAtomicReplace.enabled=false" \
  source jenkins/databricks/common_vars.sh
TESTS=delta_lake_liquid_clustering_test.py bash integration_tests/run_pyspark_from_build.sh \
  --runtime_env=databricks \
  -m "delta_lake" \
  --delta_lake \
  --test_type=$TEST_TYPE \
  -k "replace_where"
```

Probe result:

```text
2 passed, 11 deselected, 28 warnings in 152.99s
```

The tests passed with the existing DBR 17.3 fallback allowance. The diagnostics
still showed the unsupported row-index-set object path, so this is not a narrow
liquid clustering write issue.

What would be required:

- Add GPU support for DBR 17.3 `WriteIntoDeltaCommand` or route around it with
  equivalent DBR semantics.
- Add support for the row-index-set object plan, or provide a DBR-compatible GPU
  replacement for the atomic replace delete path.
- Add support or safe handling for the hidden metadata, row tracking, and
  deletion-vector structures involved in the DBR replace path.

Conclusion:

- `replaceWhere` with liquid clustering cannot be safely enabled on DBR 17.3 by
  removing the test fallback or by toggling the obvious DBR configs.
- The valid reason is that DBR 17.3 uses a broader row-index-set based atomic
  replace path that is currently unsupported by the plugin, and the same general
  DBR `replaceWhere` path is already tracked outside liquid clustering.

## Stage 2: DELETE and UPDATE

Commit message: `Enable DBR 17.3 liquid clustering DML tests`

OSS Delta 4.0 status:

- Supported through the OSS Delta 4.x command rules and GPU command
  implementations when the operation uses the file-rewrite path rather than
  persistent deletion-vector writes.
- OSS Delta 4.0 itself can run DELETE and UPDATE against DV-enabled liquid
  tables on CPU, but spark-rapids does not support GPU persistent DV writes.
- The existing OSS liquid DML tests leave `delta.enableDeletionVectors`
  unspecified instead of forcing DV-on coverage.
- Relevant files include:
  - `delta-lake/common/src/main/delta-33x-41x/scala/org/apache/spark/sql/delta/rapids/GpuDeleteCommandBase.scala`
  - `delta-lake/common/src/main/delta-33x-41x/scala/org/apache/spark/sql/delta/rapids/GpuUpdateCommandBase.scala`
  - `delta-lake/common/src/main/delta-40x-41x/scala/org/apache/spark/sql/delta/rapids/DeleteCommandMeta.scala`
  - `delta-lake/common/src/main/delta-40x-41x/scala/org/apache/spark/sql/delta/rapids/UpdateCommandMeta.scala`

DBR 17.3 current state:

- `test_delta_delete_sql_liquid_clustering` and
  `test_delta_update_sql_liquid_clustering` no longer allow DBR 17.3 CPU Delta
  write fallback.
- The existing DBR 17.3 DELETE and UPDATE GPU command paths can rewrite liquid
  clustered tables through `txn.writeFiles` after the test table opts out of
  persistent deletion vectors.
- DBR 17.3 otherwise defaults these liquid DML tests into persistent DV writes,
  and the plugin already rejects GPU DV writes through the normal DML DV guards.
- The touched-file discovery path still includes a small CPU scan because DBR
  17.3 exposes nullable row-tracking generated fields. This matches the
  existing DBR row-tracking DELETE and UPDATE tests, which already allow that
  nested scan while validating the Delta command/write path on GPU.

Changed files:

- `integration_tests/src/main/python/delta_lake_liquid_clustering_test.py`

Summary:

- Removed the DBR 17.3 CPU Delta write fallback allowance from the liquid
  DELETE and UPDATE tests.
- Created the DBR 17.3 liquid DML test tables with
  `delta.enableDeletionVectors=false` so the tests cover the OSS-equivalent
  non-DV rewrite path.
- Allowed the DBR 17.3 row-tracking metadata scan operators that are already
  accepted by existing row-tracking DML coverage.

Validation:

```bash
TESTS=delta_lake_liquid_clustering_test.py bash integration_tests/run_pyspark_from_build.sh \
  --runtime_env=databricks \
  -m "delta_lake" \
  --delta_lake \
  --test_type=$TEST_TYPE \
  -k "delete_sql_liquid_clustering or update_sql_liquid_clustering"
```

```text
2 passed, 11 deselected, 27 warnings in 94.10s
```

Remaining scope:

- Persistent deletion-vector DELETE/UPDATE writes remain unsupported on GPU.
- The DBR 17.3 row-tracking generated-field scan remains CPU, consistent with
  the existing DBR row-tracking DML tests.

## Stage 3: MERGE

Commit message: `Enable DBR 17.3 liquid clustering MERGE`

OSS Delta 4.0 status:

- Supported through the OSS Delta 4.x MERGE GPU path when the operation uses
  the file-rewrite path rather than persistent deletion-vector writes.
- OSS Delta 4.0 itself can run MERGE against DV-enabled liquid tables on CPU,
  but spark-rapids does not support GPU persistent DV writes.
- The existing OSS liquid MERGE test leaves `delta.enableDeletionVectors`
  unspecified instead of forcing DV-on coverage.
- Relevant files include:
  - `delta-lake/common/src/main/delta-40x-41x/scala/org/apache/spark/sql/delta/rapids/GpuMergeIntoCommand.scala`
  - `delta-lake/common/src/main/delta-40x-41x/scala/org/apache/spark/sql/delta/rapids/MergeIntoCommandMeta.scala`

DBR 17.3 current state:

- `integration_tests/src/main/python/delta_lake_liquid_clustering_test.py`
  no longer expects fallback for MERGE on DBR 17.3.
- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/shims/MergeIntoCommandMetaShim.scala`
  no longer rejects liquid clustered MERGE solely because the table protocol
  supports liquid clustering.
- DBR 17.3 MERGE writes through the same `deltaTxn.writeFiles` path used by
  DELETE, UPDATE, append, and static overwrite, so the Stage 0 liquid clustered
  write support is sufficient for the non-DV MERGE path.
- The existing persistent deletion-vector fallback guard remains in place.
- As with DELETE and UPDATE, the touched-file discovery path still includes a
  DBR 17.3 row-tracking metadata scan on CPU.

Changed files:

- `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/shims/MergeIntoCommandMetaShim.scala`
- `integration_tests/src/main/python/delta_lake_liquid_clustering_test.py`

Summary:

- Removed the DBR 17.3 liquid clustered MERGE veto.
- Converted the DBR 17.3 liquid MERGE test from expected fallback to CPU/GPU
  parity.
- Created the DBR 17.3 liquid MERGE source and target tables with
  `delta.enableDeletionVectors=false` so the test covers the OSS-equivalent
  non-DV MERGE path.
- Kept DBR 17.3 row-tracking metadata scan operators in the allowed CPU
  metadata set.

Validation:

```bash
SKIP_DEP_INSTALL=1 WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
```

```text
BUILD SUCCESS
```

```bash
TESTS=delta_lake_liquid_clustering_test.py bash integration_tests/run_pyspark_from_build.sh \
  --runtime_env=databricks \
  -m "delta_lake" \
  --delta_lake \
  --test_type=$TEST_TYPE \
  -k "merge_sql_liquid_clustering"
```

```text
1 passed, 12 deselected, 26 warnings in 106.57s
```

Remaining scope:

- Persistent deletion-vector MERGE writes remain unsupported on GPU.
- `notMatchedBySourceClauses` remain unsupported on GPU, consistent with the
  existing DBR MERGE guard.
- The DBR 17.3 row-tracking generated-field scan remains CPU, consistent with
  the DELETE and UPDATE liquid DML coverage.

## Clarification: DV Liquid DML and MERGE

The OSS Delta 4.0 runtime and the spark-rapids GPU support matrix are not the
same thing. OSS Delta 4.0 can execute DELETE, UPDATE, and MERGE on DV-enabled
liquid clustered tables on CPU. spark-rapids does not currently support the GPU
path when those operations write persistent deletion vectors.

For spark-rapids, OSS Delta 4.0 and DBR 17.3 are on par for liquid clustered
DELETE, UPDATE, and MERGE:

- Supported on GPU when persistent deletion-vector writes are not used.
- Not supported on GPU when the operation writes persistent deletion vectors.

The common OSS Delta command metadata already rejects persistent DV writes on
GPU through:

- `delta-lake/common/src/main/delta-33x-41x/scala/com/nvidia/spark/rapids/delta/common/DeleteCommandMetaBase.scala`
- `delta-lake/common/src/main/delta-33x-41x/scala/com/nvidia/spark/rapids/delta/common/UpdateCommandMetaBase.scala`
- `delta-lake/common/src/main/delta-33x-41x/scala/com/nvidia/spark/rapids/delta/common/MergeIntoCommandMetaBase.scala`

The DBR 17.3 practical difference is that liquid DML and MERGE enter DV and
row-tracking code paths more readily, so the enabled DBR 17.3 liquid DML/MERGE
tests create test tables with `delta.enableDeletionVectors=false`. Supporting
actual DV liquid DELETE, UPDATE, or MERGE on DBR 17.3 would require
implementing persistent deletion-vector write support on GPU first, not just
removing liquid clustering guards.

## Stage 4: OPTIMIZE

Status: supported on DBR 17.3 for ordinary OPTIMIZE on existing liquid
clustered tables when deletion vectors are disabled.

Implementation commits:

- `00b200afa Add DBR liquid OPTIMIZE GPU write boundary`
- `dc5b80d16 Enable native DBR liquid OPTIMIZE GPU writes`
- `4113bfe91 Fix DBR liquid OPTIMIZE GPU write statistics`
- `f5af1d042 Allow DBR liquid OPTIMIZE metadata aggregation on CPU`
- `ea8f04f0e Use explicit plan capture for DBR liquid OPTIMIZE`

Selected design:

- DBR's native `OptimizeRunner`, `OptimizeExecutor`, liquid batch producer,
  Kd-tree planning, `com.databricks.liquid` domain metadata, transactions,
  metrics, validation, and commit protocol remain authoritative CPU code.
- The GPU replacement is limited to the nested DBR
  `WriteIntoDeltaCommand`, which is the data-plane boundary that reads and
  rewrites the selected files. It reuses the native command's output
  specification, Hadoop configuration, bucket and partition information,
  write options, and `DelayedCommitProtocolEdge` instance.
- Reusing the native committer preserves DBR's liquid AddFile handling,
  including the partition ID tags required by
  `NumberOfRecordsValidator`. Add/remove actions and Kd-tree domain metadata
  continue to be produced and committed by the native optimizer.
- A scoped Spark local property enables the otherwise generic DBR V1
  `WriteIntoDeltaCommand` GPU rule only while native liquid OPTIMIZE is
  running. DBR's `SparkThreadLocalCapturingHelper` captures the property when
  a batch is submitted to its shared worker pool and restores the worker's
  prior properties afterward.
- The command converts DBR's basic and Delta statistics trackers to their GPU
  equivalents, copies per-file recorded statistics back to the native tracker,
  and installs the command-level `GpuWriteJobStatsTracker`. Unsupported tracker
  shapes fail closed to CPU.
- Metadata-only/no-op work remains in the native CPU framework. A productive
  rewrite is verified by captured `GpuDataWritingCommandExec` and
  `GpuWriteFilesExec` plans rather than by treating the outer
  `GpuExecutedCommandExec` alone as proof of a GPU file rewrite.

OSS Delta 4.0 status:

- Supported in the OSS Delta 4.x path for liquid clustered tables.
- The OSS implementation can use the existing GPU optimize executor with
  `OptimizeTableStrategy`, liquid clustering column extraction, clustering-aware
  AddFile tagging, and CPU/GPU Delta log parity tests.

Original DBR 17.3 feasibility findings:

- Liquid clustered OPTIMIZE was explicitly rejected in the DBR 17.3 GPU
  command and meta shim before this stage:
  - `delta-lake/delta-spark400db173/src/main/scala/com/databricks/sql/transaction/tahoe/rapids/GpuOptimizeTableCommand.scala`
  - `delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/shims/OptimizeTableCommandMetaShim.scala`
- DBR 17.3 liquid OPTIMIZE is not equivalent to the OSS/older DBR ZCube-tag
  optimize path.
- DBR 17.3 routes liquid OPTIMIZE through the newer
  `com.databricks.sql.transaction.tahoe.commands.optimize` framework:
  `OptimizeRunner`, `OptimizeExecutor`, `LiquidOptimizeBatchProducerBuilder`,
  `LiquidClusteringOptimizeBatch`, and `IncrementalClusteringTask`.
- That framework writes and updates `domainMetadata` for
  `com.databricks.liquid` with Kd-tree metadata. The older GPU optimize
  executor writes file-level `ZCUBE_*` tags instead, which is not DBR 17.3
  liquid metadata parity.
- A temporary old-executor implementation built successfully, but the focused
  clustered OPTIMIZE test failed because CPU performed a metadata-only liquid
  OPTIMIZE for the one-file test table while GPU rewrote the file and produced
  different Delta log actions.
- A probe with the original DBR command left in place and RAPIDS command
  replacement disabled created a 128-file liquid clustered table and showed
  DBR's own liquid OPTIMIZE rewrite still running through CPU-only internals:
  `ExecutedCommandExec`, `DataWritingCommandExec`, `WriteIntoDeltaCommand`, and
  `WriteFilesExec`.
- The probe diagnostics showed the DBR liquid path depends on currently
  unsupported pieces: nullable row-tracking generated fields, hidden metadata
  columns, `TableCacheQueryStageExec`, `InMemoryTableScanExec`, Kd-tree metadata
  object serialization (`SerializeFromObjectExec`, `MapObjects`,
  `StaticInvoke`, `Invoke`), and disabled JSON serialization for metadata
  (`StructsToJson`).

### 2026-07-07 rejected transaction-level safe-boundary probe

A second probe tested the narrower boundary of keeping DBR's native liquid
planning, Kd-tree/domain-metadata generation, batching, metrics, and commit on
CPU while supplying a `GpuOptimisticTransaction` only for the virtual
`writeFiles` call. This avoided the semantic error of the old executor and
preserved the native `OptimizeRunner` path.

DBR's private `OptimizeBatch.execute` splits the command transaction before a
rewrite batch. Its `OptimisticTransaction.split` implementation constructs a
plain `OptimisticTransaction`, so the probe also implemented an exact state-copy
override that retained the GPU subtype. That was still insufficient for a real
multi-file liquid rewrite:

- A table created with `CLUSTER BY (a)`, deletion vectors disabled, and eight
  multi-file appends forced the rewrite path.
- The actual batch reached DBR's base `OptimisticTransaction.writeFiles`, not
  `GpuOptimisticTransaction.writeFiles`.
- The captured plan had a GPU scan/project below a CPU
  `DataWritingCommandExec` / `WriteIntoDeltaCommand` / `WriteFiles` boundary.
  Strict GPU validation stopped the command before commit with:

```text
java.lang.IllegalArgumentException: Part of the plan is not columnar
class org.apache.spark.sql.execution.command.DataWritingCommandExec
Execute WriteIntoDeltaCommand ...
+- WriteFiles
   +- DeltaInvariantChecker ...
      +- GpuColumnarToRow
         +- GpuProject ...
```

- DBR exposes
  `spark.databricks.delta.liquid.lazyClustering.backfillStats`; bytecode shows
  its default is `false`. Explicitly pinning it to `false` produced the same
  plain-transaction write boundary, so the loss of the GPU transaction is not
  avoided by disabling lazy stats backfill.
- The rewrite plan also carried DBR row-tracking fields (`row_id`,
  `base_row_id`, `row_commit_version`, and `default_row_commit_version`), even
  though the test explicitly disabled deletion vectors.

This proves there is no stable transaction-level seam in the exposed DBR 17.3
framework where only file I/O can be accelerated. Merely overriding `split`,
disabling stats backfill, or removing the liquid guards is not enough. The
selected implementation therefore follows the other safe option identified by
the probe: narrowly scoped, first-class GPU support for the nested DBR
`WriteIntoDeltaCommand` while leaving the private transaction and optimizer
framework unchanged.

Validation and probe results:

- The temporary old-executor implementation passed the DBR 17.3 build:

```bash
SKIP_DEP_INSTALL=1 WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
```

```text
BUILD SUCCESS
```

- The focused clustered OPTIMIZE test then failed for the non-DV case and
  passed only the expected DV fallback case:

```bash
TESTS=delta_lake_optimize_table_test.py bash integration_tests/run_pyspark_from_build.sh \
  --runtime_env=databricks \
  -m "delta_lake" \
  --delta_lake \
  --test_type=$TEST_TYPE \
  -k "test_delta_optimize_clustered_table"
```

```text
1 failed, 1 passed, 6 deselected, 27 warnings in 116.43s
```

- The failure was a Delta log semantic mismatch. CPU wrote only an `OPTIMIZE`
  commit plus `com.databricks.liquid` domain metadata for the one-file table.
  GPU rewrote the file through the old executor, producing `add`/`remove`
  actions and `ZCUBE_*` file tags instead.

- The native-runner safe-boundary probe built successfully as a full Scala
  2.13 CUDA 12 distribution, but its forced multi-file test failed before
  commit at the CPU write-command boundary:

```text
1 failed, 8 deselected, 27 warnings in 134.96s
```

The temporary transaction-split changes were backed out after the probe. The
selected implementation keeps the native runner but replaces only its nested
file-writing command under the scoped liquid-OPTIMIZE marker.

Validated support and fallback boundaries:

- Supported: existing DBR 17.3 liquid clustered tables, deletion vectors
  disabled, and ordinary `OPTIMIZE` without predicates.
- Supported: productive multi-file rewrites through the GPU
  `WriteIntoDeltaCommand` data plane while DBR retains native planning and
  commit semantics.
- Supported: repeated `OPTIMIZE` with CPU/GPU data and Delta-log semantic
  parity after each invocation.
- Supported: metadata-only liquid `OPTIMIZE`, including exact action and
  `com.databricks.liquid` domain-metadata parity after normalization of only
  generated metadata and revision IDs.
- Covered: liquid tables with row tracking enabled. DBR-owned row-tracking
  metadata work may remain on CPU; the selected file rewrite is eligible for
  GPU execution when the tracker and schema guards accept it.
- CPU fallback: liquid tables with deletion vectors enabled or with existing
  deletion vectors, persistent DV writes or cleanup, `ZORDER`, `REORG`, `FULL`,
  liquid partition predicates, unsupported write-statistics shapes, and the
  broader catalog/table features listed below.
- Ordinary non-liquid DBR 17.3 OPTIMIZE continues to use the existing GPU
  executor; the native-runner/write-command boundary is limited to liquid
  tables.

Correctness validation:

- Focused forced-rewrite and repeated-OPTIMIZE tests passed together:

```text
2 passed
```

- The exact metadata-only liquid OPTIMIZE parity test passed:

```text
1 passed
```

- The focused tests compare CPU/GPU table data and the latest Delta-log action
  set after the first productive OPTIMIZE and again after the repeat. The log
  comparison includes add/remove actions and `com.databricks.liquid`
  `domainMetadata`; only nondeterministic file values, generated liquid IDs,
  and established nondeterministic tags are normalized.
- Plan capture requires both `GpuDataWritingCommandExec` and
  `GpuWriteFilesExec` for productive liquid rewrites. The metadata-only test
  does not incorrectly require a GPU file writer.
- DBR Delta module compilation, integration-test packaging, and a full Scala
  2.13 CUDA 12 distribution build completed with `BUILD SUCCESS`. The built JAR
  is:

```text
/home/ubuntu/spark-rapids-liquid-optimize-solution/scala2.13/dist/target/
rapids-4-spark_2.13-26.08.0-SNAPSHOT-cuda12.jar
```

- The final consolidated DBR 17.3 OPTIMIZE integration-test file passed:

```text
delta_lake_optimize_table_test.py: 11 passed, 28 warnings in 303.49s
```

- Regression validation of the supported non-DV liquid DML scenarios passed:

```text
delta_lake_liquid_clustering_test.py (MERGE/DELETE/UPDATE selectors):
3 passed, 10 deselected, 27 warnings in 110.02s
```

Benchmark validation:

- A matched DBR 17.3 benchmark used one immutable non-DV `CLUSTER BY (a)`
  snapshot with 33,554,432 rows, eight fragmented input files, and
  9,011,604,429 input bytes. Every CPU and GPU run shallow-cloned that same
  snapshot. The measured command was only `OPTIMIZE`; clone setup and
  correctness checks were excluded.
- Configuration was `RUNS=3`, `ITERATIONS=1`, and `WARMUP_ITERATIONS=0`.
  All three CPU runs and all three GPU runs passed exact data comparisons and
  productive add/remove-file assertions.
- Captured event logs contain one productive nested GPU writer execution in
  every GPU run. Each adaptive plan contains `Execute
  GpuWriteIntoDeltaCommand` and `GpuWriteFiles`. The DBR hidden-field and
  nullable row-tracking source `Scan parquet` remains CPU, as expected.
- The final CPU and GPU commits both contain eight removes, eight adds, one
  commitInfo action, and two domainMetadata actions. Stable semantics match
  after normalizing generated liquid IDs, revision IDs, timings, cluster/job
  identity, and physical file names: operation parameters and counts, liquid
  and row-tracking domains, aggregate AddFile row counts/global stats/null
  counts, stable tags, and row-ID coverage are equal.
- Individual AddFile boundaries are not byte-for-byte equal: the GPU writer
  produces different per-file row ranges, file sizes, baseRowId starts, and
  file-level statistics. Exact table data and aggregate/stable Delta semantics
  are equal. This physical layout difference is distinct from the rejected
  implementation that produced metadata-only CPU actions versus GPU ZCUBE
  actions.
- Timed OPTIMIZE results were:

| Run | CPU (seconds) | GPU (seconds) | CPU/GPU |
| ---: | ---: | ---: | ---: |
| 1 | 109.308 | 105.820 | 1.033x |
| 2 | 46.268 | 56.629 | 0.817x |
| 3 | 38.983 | 53.563 | 0.728x |
| Median | 46.268 | 56.629 | 0.817x |

The GPU median is 22.39% slower for this boundary-heavy workload. The result is
consistent with keeping DBR planning, Kd-tree/domain metadata, the hidden-field
source scan, validation, and commit on CPU while accelerating only the rewrite
data plane after the CPU scan.

Conclusion:

- DBR 17.3 liquid clustered OPTIMIZE is enabled only at the typed native write
  boundary; it does not reuse the incompatible older ZCube executor.
- DBR remains responsible for Kd-tree planning, domain metadata, transaction
  splitting, validation, metrics, and commit semantics, which is why CPU/GPU
  Delta-log parity can be maintained.
- Correctness is established for productive multi-file, repeated, and exact
  metadata-only cases. The matched scale benchmark also proves productive GPU
  execution and stable Delta semantic parity, but it does not show a speedup
  for the current CPU-scan/GPU-write boundary.

## Remaining Work

- `replaceWhere` for DBR 17.3 remains blocked on the DBR atomic replace
  row-index-set path.
- Persistent deletion-vector DELETE, UPDATE, MERGE, and OPTIMIZE writes remain
  unsupported on GPU.
- Investigate GPU performance with the DBR hidden metadata/row-tracking scan
  boundary; the completed scale benchmark has a 0.817x median CPU/GPU ratio.
- Expand liquid OPTIMIZE support only after independent semantic proof for
  `ZORDER`, `REORG`, `FULL`, deletion-vector cleanup/persistent DV writes,
  predicate variants, and broader catalog/table features.
- The DBR 17.3 row-tracking generated-field scan is still CPU for supported
  non-DV DELETE, UPDATE, and MERGE paths.

## DBR 17.3 Exclusive Items To Track

- Runtime-specific liquid `domainMetadata` and clustering operation parameters.
  The OPTIMIZE parity helper now compares DBR 17.3 Delta-log actions and liquid
  domain metadata while normalizing only generated metadata/revision IDs.
- Late-stage/eager clustered writes:
  - `AQELateStageClusteredWrite`
  - `DeltaLateStageClusteredWriteRepartition`
  - `DeltaLateStageClusteredWritePartitioning`
  - `DeltaLateStageClusteredWriterStrategy`
- `spark.databricks.delta.liquid.eagerClustering.insertAtomicReplace.enabled`.
- Native deletion-vector and row-tracking paths, especially hidden metadata
  columns and nullable row-tracking generated fields.
- Catalog-owned or coordinated commits, row filters, column masks, auto TTL, and
  CTAS/RTAS V2 atomic create/replace paths remain broader DBR 17.3 exclusions
  unless implemented separately.

## Current Support Matrix

| Feature | OSS Delta 4.0 GPU state | DBR 17.3 GPU state | Notes |
| --- | --- | --- | --- |
| Append to liquid table | Supported | Supported | Implemented in `8756bc0cd`. |
| Static overwrite of liquid table | Supported | Supported | Implemented in `8756bc0cd`. |
| Dynamic overwrite of liquid table | Supported where runtime permits | Not applicable | DBR 17.3 rejects dynamic partition overwrite for liquid clustering. |
| replaceWhere on liquid table | Supported | Blocked | DBR 17.3 row-index-set atomic replace path unsupported. |
| DELETE on liquid table | Supported without persistent DV writes | Supported without persistent DV writes | Implemented in `249db991e`; DBR row-tracking metadata scan remains CPU. |
| UPDATE on liquid table | Supported without persistent DV writes | Supported without persistent DV writes | Implemented in `249db991e`; DBR row-tracking metadata scan remains CPU. |
| MERGE on liquid table | Supported without persistent DV writes | Supported without persistent DV writes | Implemented in `23be6440f`; `notMatchedBySourceClauses` remain unsupported. |
| DELETE/UPDATE/MERGE on DV liquid table with persistent DV writes | Not supported on GPU | Not supported on GPU | Delta runtimes can execute these paths on CPU, but spark-rapids rejects persistent DV writes. |
| OPTIMIZE liquid table without DVs | Supported | Supported for ordinary OPTIMIZE | DBR native `OptimizeRunner` owns Kd-tree/domain metadata and commit semantics; the nested `WriteIntoDeltaCommand` file rewrite runs on GPU. |
| OPTIMIZE liquid table with DVs | Blocked by existing DV support | Blocked by existing DV support | Persistent DV writes and DV cleanup remain CPU fallback boundaries. |
