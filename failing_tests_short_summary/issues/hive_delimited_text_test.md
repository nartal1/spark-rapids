## [DBR 17.3] Integration test failures in hive_delimited_text_test.py

**Describe the bug**
27 integration test(s) failed in `hive_delimited_text_test.py` when running on Databricks Runtime 17.3 (spark400db173 shim).

```
===== 27 failed, 115 passed, 6 xfailed, 189 warnings in 251.12s (0:04:11) ======
```

<details>
<summary>Failing Tests (first 50 lines)</summary>

```
=========================== short test summary info ============================
FAILED ../../../../integration_tests/src/main/python/hive_delimited_text_test.py::test_basic_hive_text_write[hive-delim-text/simple-boolean-values-StructType([StructField('number', BooleanType(), True)])-{}-TableWriteMode.CTAS][DATAGEN_SEED=1771010128, TZ=UTC, IGNORE_ORDER({'local': True}), APPROXIMATE_FLOAT] - py4j.protocol.Py4JJavaError: An error occurred while calling o75.sql.
: java.util.ServiceConfigurationError: org.apache.spark.sql.sources.DataSourceRegister: com.google.cloud.spark.bigquery.BigQueryRelationProvider Unable to get public no-arg constructor
	at java.base/java.util.ServiceLoader.fail(ServiceLoader.java:586)
	at java.base/java.util.ServiceLoader.getConstructor(ServiceLoader.java:679)
	at java.base/java.util.ServiceLoader$LazyClassPathLookupIterator.hasNextService(ServiceLoader.java:1240)
	at java.base/java.util.ServiceLoader$LazyClassPathLookupIterator.hasNext(ServiceLoader.java:1273)
	at java.base/java.util.ServiceLoader$2.hasNext(ServiceLoader.java:1309)
	at java.base/java.util.ServiceLoader$3.hasNext(ServiceLoader.java:1393)
	at scala.collection.convert.JavaCollectionWrappers$JIteratorWrapper.hasNext(JavaCollectionWrappers.scala:46)
	at scala.collection.StrictOptimizedIterableOps.filterImpl(StrictOptimizedIterableOps.scala:225)
	at scala.collection.StrictOptimizedIterableOps.filterImpl$(StrictOptimizedIterableOps.scala:222)
	at scala.collection.convert.JavaCollectionWrappers$JIterableWrapper.filterImpl(JavaCollectionWrappers.scala:83)
	at scala.collection.StrictOptimizedIterableOps.filter(StrictOptimizedIterableOps.scala:218)
	at scala.collection.StrictOptimizedIterableOps.filter$(StrictOptimizedIterableOps.scala:218)
	at scala.collection.convert.JavaCollectionWrappers$JIterableWrapper.filter(JavaCollectionWrappers.scala:83)
	at org.apache.spark.sql.rapids.GpuDataSourceBase$.lookupDataSource(GpuDataSourceBase.scala:396)
	at org.apache.spark.sql.rapids.GpuDataSourceBase$.lookupDataSourceWithFallback(GpuDataSourceBase.scala:363)
	at com.nvidia.spark.rapids.shims.CreateDataSourceTableAsSelectCommandMeta.tagSelfForGpu(CreateDataSourceTableAsSelectCommandMetaShims.scala:70)
	at com.nvidia.spark.rapids.RapidsMeta.tagForGpu(RapidsMeta.scala:353)
	at com.nvidia.spark.rapids.RapidsMeta.$anonfun$tagForGpu$5(RapidsMeta.scala:331)
	at com.nvidia.spark.rapids.RapidsMeta.$anonfun$tagForGpu$5$adapted(RapidsMeta.scala:331)
	at scala.collection.immutable.List.foreach(List.scala:334)
	at com.nvidia.spark.rapids.RapidsMeta.tagForGpu(RapidsMeta.scala:331)
	at com.nvidia.spark.rapids.GpuOverrides$.wrapAndTagPlan(GpuOverrides.scala:4712)
	at com.nvidia.spark.rapids.GpuOverrides.applyOverrides(GpuOverrides.scala:5047)
	at com.nvidia.spark.rapids.GpuOverrides.$anonfun$applyWithContext$3(GpuOverrides.scala:4924)
	at com.nvidia.spark.rapids.GpuOverrides$.logDuration(GpuOverrides.scala:480)
	at com.nvidia.spark.rapids.GpuOverrides.$anonfun$applyWithContext$1(GpuOverrides.scala:4920)
	at com.nvidia.spark.rapids.GpuOverrideUtil$.$anonfun$tryOverride$1(GpuOverrides.scala:4886)
	at com.nvidia.spark.rapids.GpuOverrides.applyWithContext(GpuOverrides.scala:4942)
	at com.nvidia.spark.rapids.GpuOverrides.apply(GpuOverrides.scala:4913)
	at com.nvidia.spark.rapids.GpuOverrides.apply(GpuOverrides.scala:4909)
	at org.apache.spark.sql.execution.ApplyColumnarRulesAndInsertTransitions.$anonfun$apply$1(Columnar.scala:654)
	at org.apache.spark.sql.execution.ApplyColumnarRulesAndInsertTransitions.$anonfun$apply$1$adapted(Columnar.scala:654)
	at scala.collection.immutable.List.foreach(List.scala:334)
	at org.apache.spark.sql.execution.ApplyColumnarRulesAndInsertTransitions.apply(Columnar.scala:654)
	at org.apache.spark.sql.execution.ApplyColumnarRulesAndInsertTransitions.apply(Columnar.scala:553)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$17(RuleExecutor.scala:510)
	at org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule(RuleExecutor.scala:664)
	at org.apache.spark.sql.catalyst.rules.RecoverableRuleExecutionHelper.processRule$(RuleExecutor.scala:648)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.processRule(RuleExecutor.scala:144)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$16(RuleExecutor.scala:510)
	at com.databricks.spark.util.MemoryTracker$.withThreadAllocatedBytes(MemoryTracker.scala:51)
	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.measureRule(QueryPlanningTracker.scala:350)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$15(RuleExecutor.scala:508)
	at com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)
	at com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:200)
	at com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$14(RuleExecutor.scala:507)
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
   TESTS=hive_delimited_text_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
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
