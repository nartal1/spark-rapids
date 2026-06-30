# DBR 17.3 Liquid Clustering Support

Last updated: 2026-06-25

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

Status: blocked for DBR 17.3 GPU support.

OSS Delta 4.0 status:

- Supported in the OSS Delta 4.x path for liquid clustered tables.
- The OSS implementation can use the existing GPU optimize executor with
  `OptimizeTableStrategy`, liquid clustering column extraction, clustering-aware
  AddFile tagging, and CPU/GPU Delta log parity tests.

DBR 17.3 findings:

- Liquid clustered OPTIMIZE remains explicitly rejected in the DBR 17.3 GPU
  command and meta shim:
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

What would be required:

- Integrate the GPU path with DBR 17.3's new liquid optimize framework rather
  than reusing the old OSS/DBR optimize executor directly.
- Preserve and update DBR `com.databricks.liquid` Kd-tree domain metadata with
  the same semantics as the DBR CPU command.
- Add GPU support or safe CPU/GPU boundaries for the DBR liquid optimize
  internals listed above, especially row-tracking generated-field scans,
  Kd-tree metadata object plans, table-cache stages, and the DBR
  `WriteIntoDeltaCommand` / `WriteFilesExec` rewrite path.
- Add multi-file liquid OPTIMIZE tests once the metadata and rewrite path are
  supported. The current clustered OPTIMIZE test setup can produce only one
  file on DBR 17.3, which exercises metadata-only OPTIMIZE instead of file
  clustering unless the setup is adjusted.

Conclusion:

- Do not enable DBR 17.3 liquid clustered OPTIMIZE tests yet.
- The valid reason is DBR 17.3's liquid OPTIMIZE uses a private/newer Kd-tree
  domain-metadata optimizer and CPU-only rewrite internals that the current GPU
  optimize executor cannot reproduce safely.
- The temporary old-executor OPTIMIZE attempt was backed out before commit.

## Remaining Work

- `replaceWhere` for DBR 17.3 remains blocked on the DBR atomic replace
  row-index-set path.
- Persistent deletion-vector DELETE, UPDATE, MERGE, and OPTIMIZE writes remain
  unsupported on GPU.
- Liquid clustered OPTIMIZE on DBR 17.3 remains blocked on DBR-specific
  Kd-tree domain metadata and the new optimize framework.
- The DBR 17.3 row-tracking generated-field scan is still CPU for supported
  non-DV DELETE, UPDATE, and MERGE paths.

## DBR 17.3 Exclusive Items To Track

- Runtime-specific liquid `domainMetadata` and clustering operation parameters.
  The current liquid test helper skips Delta log equivalence for DBR 17.3.
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
| OPTIMIZE liquid table without DVs | Supported | Blocked | DBR 17.3 uses Kd-tree `com.databricks.liquid` domain metadata and new CPU-only optimize internals. |
| OPTIMIZE liquid table with DVs | Blocked by existing DV support | Blocked | Existing DV optimize guard plus DBR liquid optimize blocker. |
