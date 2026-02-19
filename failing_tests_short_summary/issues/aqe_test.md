## [DBR 17.3] Integration test failures in aqe_test.py

**Describe the bug**
2 integration test(s) failed in `aqe_test.py` when running on Databricks Runtime 17.3 (spark400db173 shim).

```
======= 2 failed, 14 passed, 5 skipped, 28 warnings in 113.73s (0:01:53) =======
```

<details>
<summary>Failing Tests (first 50 lines)</summary>

```
=========================== short test summary info ============================
FAILED ../../../../integration_tests/src/main/python/aqe_test.py::test_aqe_join_with_dpp[DATAGEN_SEED=1771000058, TZ=UTC, IGNORE_ORDER({'local': True}), ALLOW_NON_GPU(FilterExec,CollectLimitExec)] - py4j.protocol.Py4JJavaError: An error occurred while calling o4561.collectToPython.
: org.apache.spark.SparkException: [INTERNAL_ERROR] The "collectToPython" action failed. You hit a bug in Spark or the Spark plugins you use. Please, report this bug to the corresponding communities or vendors, and provide the full stack trace. SQLSTATE: XX000
	at org.apache.spark.SparkException$.internalError(SparkException.scala:117)
	at org.apache.spark.sql.execution.QueryExecution$.toInternalError(QueryExecution.scala:1609)
	at org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:1622)
	at org.apache.spark.sql.classic.Dataset.$anonfun$withAction$2(Dataset.scala:2764)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId0$18(SQLExecution.scala:602)
	at com.databricks.sql.util.MemoryTrackerHelper.withMemoryTracking(MemoryTrackerHelper.scala:111)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId0$16(SQLExecution.scala:517)
	at org.apache.spark.sql.execution.SQLExecution$.withSessionTagsApplied(SQLExecution.scala:934)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId0$15(SQLExecution.scala:438)
	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:97)
	at org.apache.spark.sql.artifact.ArtifactManager.$anonfun$withResources$1(ArtifactManager.scala:124)
	at org.apache.spark.sql.artifact.ArtifactManager.withClassLoaderIfNeeded(ArtifactManager.scala:118)
	at org.apache.spark.sql.artifact.ArtifactManager.withResources(ArtifactManager.scala:123)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId0$14(SQLExecution.scala:438)
	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:969)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId0$1(SQLExecution.scala:437)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:860)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId0(SQLExecution.scala:255)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:887)
	at org.apache.spark.sql.classic.Dataset.withAction(Dataset.scala:2763)
	at org.apache.spark.sql.classic.Dataset.collectToPython(Dataset.scala:2487)
	at jdk.internal.reflect.GeneratedMethodAccessor288.invoke(Unknown Source)
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:397)
	at py4j.Gateway.invoke(Gateway.java:306)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:197)
	at py4j.ClientServerConnection.run(ClientServerConnection.java:117)
	at java.base/java.lang.Thread.run(Thread.java:840)
Caused by: java.lang.AssertionError: Unexpected child exec in AdaptiveSparkPlan: org.apache.spark.sql.execution.CollectLimitExec
	at org.apache.spark.sql.rapids.execution.GpuSubqueryBroadcastMetaBase.tagPlanForGpu(GpuSubqueryBroadcastExec.scala:132)
	at com.nvidia.spark.rapids.SparkPlanMeta.tagSelfForGpu(RapidsMeta.scala:856)
	at com.nvidia.spark.rapids.RapidsMeta.tagForGpu(RapidsMeta.scala:353)
	at com.nvidia.spark.rapids.GpuOverrides$.wrapAndTagPlan(GpuOverrides.scala:4712)
	at com.nvidia.spark.rapids.shims.FileSourceScanExecMeta.com$nvidia$spark$rapids$shims$FileSourceScanExecMeta$$convertBroadcast(FileSourceScanExecMeta.scala:51)
	at com.nvidia.spark.rapids.shims.FileSourceScanExecMeta$$anonfun$$nestedInanonfun$convertDynamicPruningFilters$1$1.applyOrElse(FileSourceScanExecMeta.scala:97)
	at com.nvidia.spark.rapids.shims.FileSourceScanExecMeta$$anonfun$$nestedInanonfun$convertDynamicPruningFilters$1$1.applyOrElse(FileSourceScanExecMeta.scala:93)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:543)
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:121)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:543)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:519)
	at com.nvidia.spark.rapids.shims.FileSourceScanExecMeta.$anonfun$convertDynamicPruningFilters$1(FileSourceScanExecMeta.scala:93)
	at scala.collection.immutable.List.map(List.scala:251)
	at scala.collection.immutable.List.map(List.scala:79)
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
   TESTS=aqe_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
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
