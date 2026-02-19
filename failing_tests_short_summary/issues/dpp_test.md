## [DBR 17.3] Integration test failures in dpp_test.py

**Describe the bug**
32 integration test(s) failed in `dpp_test.py` when running on Databricks Runtime 17.3 (spark400db173 shim).

```
============ 32 failed, 73 passed, 29 warnings in 683.22s (0:11:23) ============
```

<details>
<summary>Failing Tests (first 50 lines)</summary>

```
=========================== short test summary info ============================
FAILED ../../../../integration_tests/src/main/python/dpp_test.py::test_dpp_reuse_broadcast_exchange[false-0-parquet][DATAGEN_SEED_OVERRIDE=0, TZ=UTC, INJECT_OOM, IGNORE_ORDER, ALLOW_NON_GPU(CollectLimitExec)] - py4j.protocol.Py4JJavaError: An error occurred while calling o531.collectToPython.
: org.apache.spark.SparkException: Exception thrown in awaitResult: java.util.concurrent.ExecutionException: java.lang.AssertionError: assertion failed
	at org.apache.spark.util.ThreadUtils$.awaitResult(ThreadUtils.scala:554)
	at org.apache.spark.sql.execution.SubqueryBroadcastExec.executeCollect(SubqueryBroadcastExec.scala:158)
	at org.apache.spark.sql.execution.InSubqueryExec.updateResult(subquery.scala:197)
	at org.apache.spark.sql.execution.SparkPlan.$anonfun$waitForSubqueries$2(SparkPlan.scala:470)
	at org.apache.spark.sql.execution.SparkPlan.$anonfun$waitForSubqueries$2$adapted(SparkPlan.scala:469)
	at scala.collection.IterableOnceOps.foreach(IterableOnce.scala:619)
	at scala.collection.IterableOnceOps.foreach$(IterableOnce.scala:617)
	at scala.collection.AbstractIterable.foreach(Iterable.scala:935)
	at org.apache.spark.sql.execution.SparkPlan.$anonfun$waitForSubqueries$1(SparkPlan.scala:469)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
	at com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)
	at com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:200)
	at com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)
	at org.apache.spark.sql.execution.SparkPlan.waitForSubqueries(SparkPlan.scala:467)
	at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeQuery$1(SparkPlan.scala:421)
	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:165)
	at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:418)
	at org.apache.spark.sql.execution.SparkPlan.executeColumnar(SparkPlan.scala:388)
	at com.nvidia.spark.rapids.GpuFilterExec.internalDoExecuteColumnar(basicPhysicalOperators.scala:1266)
	at com.nvidia.spark.rapids.GpuExec.doExecuteColumnar(GpuExec.scala:341)
	at com.nvidia.spark.rapids.GpuExec.doExecuteColumnar$(GpuExec.scala:338)
	at com.nvidia.spark.rapids.GpuFilterExec.doExecuteColumnar(basicPhysicalOperators.scala:1221)
	at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeColumnarRDD$1(SparkPlan.scala:378)
	at scala.util.Try$.apply(Try.scala:217)
	at org.apache.spark.util.Utils$.doTryWithCallerStacktrace(Utils.scala:1684)
	at org.apache.spark.util.LazyTry.tryT$lzycompute(LazyTry.scala:60)
	at org.apache.spark.util.LazyTry.tryT(LazyTry.scala:59)
	at org.apache.spark.util.LazyTry.get(LazyTry.scala:75)
	at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeColumnar$1(SparkPlan.scala:392)
	at org.apache.spark.sql.execution.SparkPlan$.org$apache$spark$sql$execution$SparkPlan$$withExecuteQueryLogging(SparkPlan.scala:138)
	at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeQuery$1(SparkPlan.scala:422)
	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:165)
	at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:418)
	at org.apache.spark.sql.execution.SparkPlan.executeColumnar(SparkPlan.scala:388)
	at com.nvidia.spark.rapids.GpuCoalesceBatches.internalDoExecuteColumnar(GpuCoalesceBatches.scala:954)
	at com.nvidia.spark.rapids.GpuExec.doExecuteColumnar(GpuExec.scala:341)
	at com.nvidia.spark.rapids.GpuExec.doExecuteColumnar$(GpuExec.scala:338)
	at com.nvidia.spark.rapids.GpuCoalesceBatches.doExecuteColumnar(GpuCoalesceBatches.scala:899)
	at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeColumnarRDD$1(SparkPlan.scala:378)
	at scala.util.Try$.apply(Try.scala:217)
	at org.apache.spark.util.Utils$.doTryWithCallerStacktrace(Utils.scala:1684)
	at org.apache.spark.util.LazyTry.tryT$lzycompute(LazyTry.scala:60)
	at org.apache.spark.util.LazyTry.tryT(LazyTry.scala:59)
	at org.apache.spark.util.LazyTry.get(LazyTry.scala:75)
	at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeColumnar$1(SparkPlan.scala:392)
	at org.apache.spark.sql.execution.SparkPlan$.org$apache$spark$sql$execution$SparkPlan$$withExecuteQueryLogging(SparkPlan.scala:138)
	at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeQuery$1(SparkPlan.scala:422)
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
   TESTS=dpp_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
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
