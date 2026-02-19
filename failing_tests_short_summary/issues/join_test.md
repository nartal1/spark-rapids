## [DBR 17.3] Integration test failures in join_test.py

**Describe the bug**
7 integration test(s) failed in `join_test.py` when running on Databricks Runtime 17.3 (spark400db173 shim).

```
= 7 failed, 4450 passed, 54 skipped, 24 xpassed, 2670 warnings in 7904.34s (2:11:44) =
```

<details>
<summary>Failing Tests (first 50 lines)</summary>

```
FAILED ../../../../integration_tests/src/main/python/join_test.py::test_broadcast_hash_join_constant_keys[true-LeftAnti][DATAGEN_SEED=1771018475, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.EmptyRelationExec
EmptyRelation [plan_id=31879]
+- Join LeftAnti, rightHint=(strategy=broadcast), joinId=368
   :- AggregatePart [partial_count(1) AS count#2627L], false
   :  +- Project
   :     +- Range (0, 10, step=1, splits=Some(4))
   +- LogicalQueryStage LocalLimit 1, ShuffleQueryStage 0, Statistics(sizeInBytes=112.0 B, rowCount=4, ColumnStat: N/A, isRuntime=true)
      +- LocalLimit 1
         +- Project
            +- Range (0, 10000, step=1, splits=Some(4))
FAILED ../../../../integration_tests/src/main/python/join_test.py::test_broadcast_hash_join_constant_keys[false-LeftAnti][DATAGEN_SEED=1771018475, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.EmptyRelationExec
EmptyRelation [plan_id=33608]
+- Join LeftAnti, rightHint=(strategy=broadcast), joinId=376
   :- AggregatePart [partial_count(1) AS count#2793L], false
   :  +- Project
   :     +- Range (0, 10, step=1, splits=Some(4))
   +- LogicalQueryStage LocalLimit 1, ShuffleQueryStage 0, Statistics(sizeInBytes=0.0 B, rowCount=4, ColumnStat: N/A, isRuntime=true)
      +- LocalLimit 1
         +- Project
            +- Range (0, 10000, step=1, splits=Some(4))
FAILED ../../../../integration_tests/src/main/python/join_test.py::test_empty_right_outer_side_with_limit[DATAGEN_SEED=1771018475, TZ=UTC, INJECT_OOM, ALLOW_NON_GPU(CollectLimitExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/t1..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<>
FAILED ../../../../integration_tests/src/main/python/join_test.py::test_join_bucketed_table[true-true][DATAGEN_SEED=1771018475, TZ=UTC, INJECT_OOM, IGNORE_ORDER, ALLOW_NON_GPU(DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec)] - py4j.protocol.Py4JJavaError: An error occurred while calling o943212.saveAsTable.
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
   TESTS=join_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
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
