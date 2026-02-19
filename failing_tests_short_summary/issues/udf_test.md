## [DBR 17.3] Integration test failures in udf_test.py

**Describe the bug**
36 integration test(s) failed in `udf_test.py` when running on Databricks Runtime 17.3 (spark400db173 shim).

```
= 36 failed, 91 passed, 1 skipped, 9 xfailed, 28 warnings in 400.16s (0:06:40) =
```

<details>
<summary>Failing Tests (first 50 lines)</summary>

```
=========================== short test summary info ============================
FAILED ../../../../integration_tests/src/main/python/udf_test.py::test_window_aggregate_udf[No_Partition-Byte][DATAGEN_SEED=1771010990, TZ=UTC, INJECT_OOM, IGNORE_ORDER] - py4j.protocol.Py4JJavaError: An error occurred while calling o12184.collectToPython.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 185.0 failed 1 times, most recent failure: Lost task 0.0 in stage 185.0 (TID 621) (ip-10-59-240-145.us-west-2.compute.internal executor driver): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/databricks/spark/python/pyspark/worker.py", line 3267, in main
    func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
                                               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/spark/python/pyspark/worker.py", line 3134, in read_udfs
    read_single_udf(
  File "/databricks/spark/python/pyspark/worker.py", line 1316, in read_single_udf
    return wrap_window_agg_pandas_udf(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/spark/python/pyspark/worker.py", line 923, in wrap_window_agg_pandas_udf
    window_bound_type = [t.strip().lower() for t in window_bound_types_str.split(",")][udf_index]
                                                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'NoneType' object has no attribute 'split'

	at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:979)
	at org.apache.spark.sql.rapids.execution.python.shims.GpuArrowPythonOutput$$anon$1.read(GpuArrowPythonOutput.scala:84)
	at org.apache.spark.sql.rapids.execution.python.shims.GpuArrowPythonOutput$$anon$1.read(GpuArrowPythonOutput.scala:53)
	at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:928)
	at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
	at org.apache.spark.sql.rapids.execution.python.CombiningIterator.hasNext(BatchGroupUtils.scala:423)
	at scala.collection.Iterator$$anon$9.hasNext(Iterator.scala:583)
	at com.nvidia.spark.rapids.GpuOpTimeTrackingRDD$$anon$1.$anonfun$hasNext$1(GpuExec.scala:68)
	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.scala:17)
	at com.nvidia.spark.rapids.GpuMetric.ns(GpuMetrics.scala:462)
	at com.nvidia.spark.rapids.GpuOpTimeTrackingRDD$$anon$1.hasNext(GpuExec.scala:68)
	at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:601)
	at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:601)
	at com.nvidia.spark.rapids.GpuOutOfCoreSortIterator.hasNext(GpuSortExec.scala:323)
	at com.nvidia.spark.rapids.GpuOpTimeTrackingRDD$$anon$1.$anonfun$hasNext$1(GpuExec.scala:68)
	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.scala:17)
	at com.nvidia.spark.rapids.GpuMetric.ns(GpuMetrics.scala:462)
	at com.nvidia.spark.rapids.GpuOpTimeTrackingRDD$$anon$1.hasNext(GpuExec.scala:68)
	at com.nvidia.spark.rapids.ColumnarToRowIterator.$anonfun$fetchNextBatch$3(GpuColumnarToRowExec.scala:290)
	at com.nvidia.spark.rapids.NvtxId.apply(NvtxRangeWithDoc.scala:84)
	at com.nvidia.spark.rapids.NvtxIdWithMetrics$.apply(NvtxWithMetrics.scala:67)
	at com.nvidia.spark.rapids.ColumnarToRowIterator.fetchNextBatch(GpuColumnarToRowExec.scala:289)
	at com.nvidia.spark.rapids.ColumnarToRowIterator.loadNextBatch(GpuColumnarToRowExec.scala:259)
	at com.nvidia.spark.rapids.ColumnarToRowIterator.hasNext(GpuColumnarToRowExec.scala:306)
	at scala.collection.Iterator$$anon$9.hasNext(Iterator.scala:583)
	at org.apache.spark.sql.execution.collect.UnsafeRowBatchUtils$.$anonfun$encodeUnsafeRows$5(UnsafeRowBatchUtils.scala:88)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
	at com.databricks.spark.util.ExecutorFrameProfiler$.record(ExecutorFrameProfiler.scala:110)
	at org.apache.spark.sql.execution.collect.UnsafeRowBatchUtils$.$anonfun$encodeUnsafeRows$3(UnsafeRowBatchUtils.scala:88)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
	at com.databricks.spark.util.ExecutorFrameProfiler$.record(ExecutorFrameProfiler.scala:110)
	at org.apache.spark.sql.execution.collect.UnsafeRowBatchUtils$.$anonfun$encodeUnsafeRows$1(UnsafeRowBatchUtils.scala:68)
	at com.databricks.spark.util.ExecutorFrameProfiler$.record(ExecutorFrameProfiler.scala:110)
	at org.apache.spark.sql.execution.collect.UnsafeRowBatchUtils$.encodeUnsafeRows(UnsafeRowBatchUtils.scala:62)
```

</details>

**Steps/Code to reproduce bug**
1. Checkout branch from fork:
   ```
   git remote add nartal1 https://github.com/nartal1/spark-rapids.git  # if not already added
   git fetch nartal1
   git checkout nartal1/databricks_173_support
   ```
2. Build:
   ```
   WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
   ```
3. Run failing tests:
   ```
   TESTS=udf_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
   ```

**Expected behavior**
All tests should pass as they do on other Spark versions.

**Environment details (please complete the following information)**
- Environment location: Databricks (DBR 17.3)
- Branch: `databricks_173_support`
- Shim: `spark400db173`
- Scala Version: 2.13

**Additional context**
This is part of the DBR 17.3 integration effort. Root cause needs investigation.
