# DBR 17.3 Delta Deletion Vector Query Failure

## Summary

A performance run of the NDS Delta benchmark on Databricks Runtime 17.3 exposed a failure in query1 when Delta deletion-vector predicate pushdown was enabled in the RAPIDS Accelerator.

The failure was reported while testing Delta deletion-vector support added for DBR 17.3. Query72 completed successfully in the same run, but query1 failed with a Databricks Delta skip-row metadata error.

## Reproduction Context

The issue was reproduced with the NDS benchmark on Delta SF3000 data:

- Dataset: `s3://ndsv2-data/delta_sf3000/`
- Runtime: Databricks 17.3 GPU ML, Scala 2.13
- Query that failed: `query1`
- Query that passed: `query72`
- Important configs:
  - `spark.databricks.delta.properties.defaults.enableDeletionVectors=false`
  - `spark.databricks.delta.deletionVectors.useMetadataRowIndex=true`
  - `spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled=true`

The original failure artifacts were under paths such as:

- `/dbfs/nartal/ab-result/delta_nds_fix1/query1`
- `/dbfs/nartal/ab-result/delta_nds_fix1/query72`

With the previous jar, query1 failed while query72 passed.

## Failure

Query1 failed with:

```text
[DELTA_SKIP_ROW_COLUMN_NOT_FILLED] The Skip Row Column was requested but not filled by the reader. SQLSTATE: XX000
```

The executor stack showed the exception coming from generated CPU filter code above a columnar scan path:

```text
org.apache.spark.SparkRuntimeException: [DELTA_SKIP_ROW_COLUMN_NOT_FILLED]
  at GeneratedIteratorForCodegenStage1.processNext
  at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory...
  at com.nvidia.spark.rapids.InternalRowToColumnarBatchIterator.hasNext
  at com.nvidia.spark.rapids.GpuOpTimeTrackingRDD...
```

The physical plan for query1 included a Delta scan reading the Databricks internal skip-row metadata column:

```text
Scan parquet spark_catalog.default.store_returns
Output: [..., _databricks_internal_edge_computed_column_skip_row, ...]
ReadSchema: struct<..., _databricks_internal_edge_computed_column_skip_row:boolean>
```

and a CPU filter condition containing the Databricks skip-row guard:

```text
if (isnotnull(_databricks_internal_edge_computed_column_skip_row))
  (_databricks_internal_edge_computed_column_skip_row = false)
else
  isnotnull(raise_error(DELTA_SKIP_ROW_COLUMN_NOT_FILLED, ...))
```

## Why Predicate Pushdown Disabled Passed

When running query1 with:

```text
spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled=false
```

the query completed, but the Delta scans fell back to CPU. In that mode, Databricks' CPU Delta/Parquet reader fills the skip-row metadata column, so the Databricks skip-row guard can be evaluated safely.

That was useful confirmation that the table/data were valid and that the failure was specific to the native GPU deletion-vector read path.

## Why Query72 Did Not Fail

Query72 passed because its final plan did not leave the same problematic CPU filter over a native GPU DV scan. Query72's scans and filters stayed in a shape where the internal skip-row metadata column did not survive as an unfilled CPU-filter input.

In the successful fixed run, both query1 and query72 had final plans with `GpuFileGpuScan` and no `_databricks_internal_edge_computed_column_skip_row` references.

## Validation After Fix

After applying the fix and rebuilding the DBR 17.3 jar, the latest AB run completed successfully:

- Result directory: `/dbfs/nartal/ab-result/delta_nds_fix2/`
- Query1: `Completed`, no exceptions, runtime about `37.57s`
- Query72: `Completed`, no exceptions, runtime about `36.747s`
- Predicate pushdown remained enabled:
  - `spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled=true`

The DBFS jar used by the AB run matched the locally rebuilt jar by SHA-256.

The final query1 plan showed GPU Delta scans and no skip-row metadata column in the scan output or filter condition.

## Latest Local Review Update

A follow-up review compared the fix with the DBR 17.3 CPU implementation under `/databricks/jars`
and confirmed that filling surviving skip-row metadata columns with `false` is the right native-DV
reader behavior after cuDF has already removed deleted rows.

The follow-up code changes were cleanup and risk-reduction items, not a change in the core
semantics:

- CPU and GPU `FilterExec` handling now share the same `rewriteFilter` helper. The helper separates
  the Databricks skip-row-only predicate from the remaining predicates, verifies that the child
  contains a native GPU DV scan, prunes the skip-row column from the child subtree, and rebuilds the
  CPU or GPU filter only when non-DV predicates remain.
- The pruning path removes matching skip-row expressions from scan `dataFilters` as well as scan
  output and required schema. This avoids AQE stage-reuse problems after the skip-row column has
  been removed from the scan output.
- The reader-side safety net now avoids extra work when planner pruning succeeds. If the skip-row
  metadata column is absent from `readDataSchema`, `materializeDeletionVectorSkipRowColumnsAsFalseIfNeeded`
  returns the original cudf table unchanged.
- When the safety net does need to rebuild the table, pass-through columns are handed to the new
  `Table` without redundant manual `incRefCount` calls; the `Table` constructor takes its own
  references. Only newly created constant-`false` replacement columns are closed after construction.
- The non-chunked reader computes skip-row indexes only in the non-chunked branch. The chunked reader
  owns its own lazy skip-row index computation.
- The chunked-reader path drops only cuDF's prepended index column in `postProcessChunk` and applies
  skip-row materialization in `next`, after common schema evolution and date/timestamp rebasing.
- The focused integration test now checks the meaningful conditions only: GPU scan is present, a CPU
  `FilterExec` remains, the skip-row column is absent from the executed plan, and CPU/GPU results
  match. The earlier assertion that the runtime error string was absent from an explain plan was
  removed because it did not exercise the failure mode.

Latest local validation after these refinements:

- `WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh`: passed
- `test_delta_dv_cpu_filter_after_native_scan`: `1 passed, 39942 deselected`
- `git diff --check`: passed
