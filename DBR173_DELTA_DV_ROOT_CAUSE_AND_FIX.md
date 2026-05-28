# DBR 17.3 Delta Deletion Vector Root Cause and Fix

## Root Cause

DBR 17.3 adds a Delta deletion-vector read path that uses Databricks internal metadata columns to filter deleted rows. One of those columns is:

```text
_databricks_internal_edge_computed_column_skip_row
```

Databricks injects a guard around this column. The guard expects the reader to fill the column. If the column is requested but remains unfilled, Databricks raises:

```text
[DELTA_SKIP_ROW_COLUMN_NOT_FILLED] The Skip Row Column was requested but not filled by the reader.
```

The RAPIDS native DBR 17.3 DV path pushes deletion-vector filtering into the GPU Parquet reader. That means the native reader can remove deleted rows directly, without needing downstream Spark filters to evaluate the skip-row column.

The original assumption was that after pushing DV filtering into the scan, the planner would remove the Databricks skip-row predicate and prune the skip-row metadata column from the scan output.

That assumption was incomplete.

In query1, Spark produced a mixed CPU/GPU plan. The native GPU DV scan filtered deleted rows internally, but a CPU `FilterExec` remained above the scan. That CPU filter still contained Databricks' skip-row guard and still referenced `_databricks_internal_edge_computed_column_skip_row`.

The native GPU DV reader had already applied the deletion vector but did not materialize the skip-row metadata column. Therefore, when the CPU filter evaluated the skip-row guard, Databricks detected that the requested skip-row column was not filled and threw `DELTA_SKIP_ROW_COLUMN_NOT_FILLED`.

## Why The Previous Planner-Only Fix Was Not Enough

The planner-side fix handled several common shapes by removing the skip-row predicate and pruning the skip-row column from GPU scan/project/filter nodes. It was later extended to handle CPU `FilterExec`, CPU `ProjectExec`, and CPU `FileSourceScanExec` around the native DV scan.

However, relying only on plan rewriting is fragile for DBR 17.3 because Spark can leave mixed plans such as:

```text
GpuFileGpuScan
  -> GpuColumnarToRow
    -> CPU FilterExec
      -> GpuRowToColumnar
```

This can happen when another expression in the filter is not on GPU. In query1, the CPU filter was caused by additional CPU-only filter logic in the query plan. The result was a CPU filter that still referenced the Databricks skip-row guard.

A planner-only approach must remove every possible surviving skip-row consumer. If any mixed-plan shape is missed, the native reader can still return data without filling the requested skip-row metadata column, and DBR can fail at runtime.

## Fix Strategy

The fix has two parts.

## 1. Planner-side pruning

The DBR 17.3 DV predicate-pushdown rule was extended to handle CPU and GPU plan nodes.

The rule now prunes the deletion-vector skip-row column from:

- `GpuProjectExec`
- CPU `ProjectExec`
- `GpuFileSourceScanExec`
- CPU `FileSourceScanExec`

It also removes the skip-row-only predicate from:

- `GpuFilterExec`
- CPU `FilterExec`

The CPU and GPU filter rewrites share a single `rewriteFilter` helper. That helper partitions the
filter conjuncts into Databricks skip-row-only predicates and all other predicates, refuses to prune
if another surviving predicate still reads the skip-row column, verifies that the child contains a
native GPU DV scan, and then rebuilds the CPU or GPU filter only when non-DV predicates remain.

The scan rewrite also removes the skip-row predicate from `dataFilters`. This matters for AQE stage
reuse because AQE expects scan data filters to reference attributes that still exist in the scan
output after pruning.

This preserves the intended optimized plan when the skip-row guard can be safely removed. For query1
after the fix, the final plan no longer contains `_databricks_internal_edge_computed_column_skip_row`
in the scan output or filter condition.

Relevant file:

```text
delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala
```

## 2. Reader-side hardening

The native DBR 17.3 DV reader was hardened so that if a skip-row metadata column is still requested in `readDataSchema`, the reader materializes it as a boolean column of `false` values.

This is semantically correct for the native DV path because the reader has already applied the deletion vectors internally. Every row returned by the reader is a row that should not be skipped, so the skip-row value for every returned row is `false`.

This is important as a safety net. If a future plan shape leaves the skip-row column in the read schema, the reader can still satisfy the Databricks column contract instead of returning an unfilled metadata column.

The helper recognizes both supported DBR Delta deletion-vector metadata column names:

```text
__delta_internal_is_row_deleted
_databricks_internal_edge_computed_column_skip_row
```

and replaces those requested columns with a constant `false` cudf column after native DV filtering.

The reader-side hardening is intentionally cheap on the normal path. The planner-side rule should
remove the skip-row column from `readDataSchema`; when that happens
`materializeDeletionVectorSkipRowColumnsAsFalseIfNeeded` returns the original table unchanged and
does not rebuild the cudf table or increment column references.

When the safety net does need to rebuild the cudf table, pass-through columns are handed to the new
`Table` without an extra manual `incRefCount`; the `Table` constructor takes its own references.
Only newly created constant-`false` replacement columns are tracked and closed after the returned
`Table` has been constructed.

For the chunked Parquet reader, the `false` materialization must run after the common Parquet reader
post-processing has completed schema evolution and date/timestamp rebasing. Doing it in
`postProcessChunk` is too early because missing metadata columns can be introduced by schema
evolution after the raw cuDF chunk has been decoded. The chunked path now only drops cuDF's prepended
index column in `postProcessChunk`, then applies the skip-row materialization in `next`. Its skip-row
indexes are computed lazily in the chunked reader, while the non-chunked path computes them only in
the non-chunked branch before materializing the final output table.

Relevant file:

```text
delta-lake/delta-spark400db173/src/main/scala/com/nvidia/spark/rapids/delta/GpuDeltaParquetFileFormatNativeDV.scala
```

## CPU Implementation Reference

The DBR 17.3 CPU implementation in `/databricks/jars` confirms the expected semantics. The relevant
classes are in:

```text
/databricks/jars/----ws_4_0--sql--core--core-hive-2.3__hadoop-3.2_2.13_deploy.jar
```

The decompiled `DatabricksSpecificParquetRecordReaderBase.SkipRowGenerator.populateSkipRow` fills the
skip-row vector from the row-index filter when one is present. If no row-index filter is present, it
fills the requested skip-row vector with `false`. This matches the native GPU DV path after cuDF has
already filtered deleted rows.

## Why Filling `false` Is Correct

The native GPU DV reader applies deletion vectors before returning rows. Therefore:

- Deleted rows are already removed from the output table.
- Rows that remain are not deleted.
- Databricks' skip-row guard expects `false` for rows that should be kept.

So when the skip-row column is still requested, materializing it as all `false` matches the post-DV-filtered output.

This should only be done in the native DV reader path. Unknown Databricks internal metadata columns should still be treated conservatively and fall back to CPU unless explicitly supported.

## Relation To `tablePath.isDefined`

DBR's `DeltaParquetFileFormat.hasTablePath()` is implemented as `tablePath.isDefined`, and `copyWithDVInfo` sets `tablePath`. So `tablePath.isDefined` is a signal that DBR prepared the scan with DV-related table-path information.

However, `tablePath.isDefined` is not a complete fix for this bug.

It can help identify some DV-read contexts, but query1 failed because a requested skip-row metadata column survived above the native GPU DV scan and was not filled. Whether `tablePath.isDefined` is used more or less conservatively does not by itself satisfy the Databricks skip-row column contract.

The required correctness fix is that the planner removes the skip-row consumer when possible, and the native reader fills the known skip-row metadata column when it is still requested.

## Tests Added

A focused regression test was added to:

```text
integration_tests/src/main/python/delta_lake_test.py
```

Test name:

```text
test_delta_dv_cpu_filter_after_native_scan
```

The test creates a Delta table with persistent deletion vectors, enables DBR 17.3 native DV predicate pushdown, and disables GPU `In`/`InSet` expression support. That forces a CPU filter above the native GPU Delta scan, matching the important part of the query1 failure shape.

The test also forces:

```text
spark.rapids.sql.format.parquet.reader.type=MULTITHREADED
spark.rapids.sql.reader.chunked=true
```

This exercises the default chunked reader path where the ordering of skip-row materialization matters.

The test verifies:

- CPU and GPU results match.
- A GPU Delta scan is used.
- A CPU `FilterExec` remains above the native GPU scan.
- The executed plan does not contain `_databricks_internal_edge_computed_column_skip_row`.
- The query completes without the runtime `DELTA_SKIP_ROW_COLUMN_NOT_FILLED` failure.

A review cleanup removed an earlier assertion that
`DELTA_SKIP_ROW_COLUMN_NOT_FILLED` was absent from the explain string. That error is raised at
runtime, not printed in a normal executed plan, so the useful checks are the plan-shape assertions
above plus successful CPU/GPU result comparison.

The focused test was run and passed:

```text
1 passed, 39942 deselected
```

The pre-conversion explain output showed the intended regression trigger: a CPU `FilterExec` containing the Databricks skip-row guard plus a disabled `In` expression.

## Validation

Historical validation of the original fix:

1. Existing focused metadata-column test passed:
   - `test_delta_filter_out_metadata_col`: `2 passed`
2. AB benchmark rerun with the latest jar completed successfully:
   - query1: completed, no exceptions
   - query72: completed, no exceptions
   - predicate pushdown enabled
   - final plans contained `GpuFileGpuScan` and no skip-row metadata column references

Latest local validation after the reader-side and planner cleanup:

1. Full DBR 17.3 build completed successfully:
   - `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`
2. Focused regression test passed:
   - `test_delta_dv_cpu_filter_after_native_scan`: `1 passed, 39942 deselected`
3. Diff whitespace check passed:
   - `git diff --check`
