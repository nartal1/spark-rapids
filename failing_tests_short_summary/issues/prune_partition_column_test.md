## [DBR 17.3] Integration test failures in prune_partition_column_test.py

**Describe the bug**
28 integration test(s) failed in `prune_partition_column_test.py` when running on Databricks Runtime 17.3 (spark400db173 shim).

```
= 28 failed, 72 passed, 4 skipped, 28 xfailed, 28 warnings in 258.59s (0:04:18) =
```

<details>
<summary>Failing Tests (first 50 lines)</summary>

```
=========================== short test summary info ============================
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_project[csv-False][DATAGEN_SEED=1771003810, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#102,b#103,c#104L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_project[csv-True][DATAGEN_SEED=1771003810, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#124,b#125,c#126L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_project[csv-False][DATAGEN_SEED=1771003810, TZ=UTC, ALLOW_NON_GPU(ProjectExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#274,b#275,c#276L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_project[csv-True][DATAGEN_SEED=1771003810, TZ=UTC, ALLOW_NON_GPU(ProjectExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#296,b#297,c#298L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_filter_project[a-csv-False][DATAGEN_SEED=1771003810, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#446,b#447,c#448L] Batched: false, DataFilters: [isnotnull(a#446), (a#446 > 0)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [], PushedFilters: [IsNotNull(a), GreaterThan(a,0)], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_filter_project[a-csv-True][DATAGEN_SEED=1771003810, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#468,b#469,c#470L] Batched: false, DataFilters: [isnotnull(a#468), (a#468 > 0)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [], PushedFilters: [IsNotNull(a), GreaterThan(a,0)], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_filter_project[b-csv-False][DATAGEN_SEED=1771003810, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#618,b#619,c#620L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [isnotnull(b#619), (b#619 > 0)], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_filter_project[b-csv-True][DATAGEN_SEED=1771003810, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#640,b#641,c#642L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [isnotnull(b#641), (b#641 > 0)], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_filter_project[c-csv-False][DATAGEN_SEED=1771003810, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#790,b#791,c#792L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [isnotnull(c#792L), (c#792L > 0)], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_filter_project[c-csv-True][DATAGEN_SEED=1771003810, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#812,b#813,c#814L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [isnotnull(c#814L), (c#814L > 0)], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_filter_and_project[a-csv-False][DATAGEN_SEED=1771003810, TZ=UTC, ALLOW_NON_GPU(ProjectExec,FilterExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#962,b#963,c#964L] Batched: false, DataFilters: [isnotnull(a#962), (a#962 > 0)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [], PushedFilters: [IsNotNull(a), GreaterThan(a,0)], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_filter_and_project[a-csv-True][DATAGEN_SEED=1771003810, TZ=UTC, INJECT_OOM, ALLOW_NON_GPU(ProjectExec,FilterExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#984,b#985,c#986L] Batched: false, DataFilters: [isnotnull(a#984), (a#984 > 0)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [], PushedFilters: [IsNotNull(a), GreaterThan(a,0)], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_filter_and_project[b-csv-False][DATAGEN_SEED=1771003810, TZ=UTC, INJECT_OOM, ALLOW_NON_GPU(ProjectExec,FilterExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#1134,b#1135,c#1136L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [isnotnull(b#1135), (b#1135 > 0)], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_filter_and_project[b-csv-True][DATAGEN_SEED=1771003810, TZ=UTC, ALLOW_NON_GPU(ProjectExec,FilterExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#1156,b#1157,c#1158L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [isnotnull(b#1157), (b#1157 > 0)], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_filter_and_project[c-csv-False][DATAGEN_SEED=1771003810, TZ=UTC, ALLOW_NON_GPU(ProjectExec,FilterExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#1306,b#1307,c#1308L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [isnotnull(c#1308L), (c#1308L > 0)], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_filter_and_project[c-csv-True][DATAGEN_SEED=1771003810, TZ=UTC, INJECT_OOM, ALLOW_NON_GPU(ProjectExec,FilterExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#1328,b#1329,c#1330L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [isnotnull(c#1330L), (c#1330L > 0)], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_filter_project[a-csv-False][DATAGEN_SEED=1771003810, TZ=UTC, INJECT_OOM, ALLOW_NON_GPU(FilterExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#1478,b#1479,c#1480L] Batched: false, DataFilters: [isnotnull(a#1478), (a#1478 > 0)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [], PushedFilters: [IsNotNull(a), GreaterThan(a,0)], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_filter_project[a-csv-True][DATAGEN_SEED=1771003810, TZ=UTC, ALLOW_NON_GPU(FilterExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#1500,b#1501,c#1502L] Batched: false, DataFilters: [isnotnull(a#1500), (a#1500 > 0)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [], PushedFilters: [IsNotNull(a), GreaterThan(a,0)], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_filter_project[b-csv-False][DATAGEN_SEED=1771003810, TZ=UTC, INJECT_OOM, ALLOW_NON_GPU(FilterExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#1650,b#1651,c#1652L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [isnotnull(b#1651), (b#1651 > 0)], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_filter_project[b-csv-True][DATAGEN_SEED=1771003810, TZ=UTC, ALLOW_NON_GPU(FilterExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#1672,b#1673,c#1674L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [isnotnull(b#1673), (b#1673 > 0)], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_filter_project[c-csv-False][DATAGEN_SEED=1771003810, TZ=UTC, ALLOW_NON_GPU(FilterExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#1822,b#1823,c#1824L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [isnotnull(c#1824L), (c#1824L > 0)], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_fallback_filter_project[c-csv-True][DATAGEN_SEED=1771003810, TZ=UTC, ALLOW_NON_GPU(FilterExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#1844,b#1845,c#1846L] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [isnotnull(c#1846L), (c#1846L > 0)], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_filter_fallback_project[a-csv-False][DATAGEN_SEED=1771003810, TZ=UTC, ALLOW_NON_GPU(ProjectExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#1994,b#1995,c#1996L] Batched: false, DataFilters: [isnotnull(a#1994), (a#1994 > 0)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [], PushedFilters: [IsNotNull(a), GreaterThan(a,0)], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_filter_fallback_project[a-csv-True][DATAGEN_SEED=1771003810, TZ=UTC, INJECT_OOM, ALLOW_NON_GPU(ProjectExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan csv [a#2016,b#2017,c#2018L] Batched: false, DataFilters: [isnotnull(a#2016), (a#2016 > 0)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-9207..., PartitionFilters: [], PushedFilters: [IsNotNull(a), GreaterThan(a,0)], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/prune_partition_column_test.py::test_prune_partition_column_when_filter_fallback_project[b-csv-False][DATAGEN_SEED=1771003810, TZ=UTC, ALLOW_NON_GPU(ProjectExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
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
   TESTS=prune_partition_column_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
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
