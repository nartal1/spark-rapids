## [DBR 17.3] Integration test failures in json_test.py

**Describe the bug**
4462 integration test(s) failed in `json_test.py` when running on Databricks Runtime 17.3 (spark400db173 shim).

```
= 4462 failed, 2742 passed, 32 skipped, 413 xfailed, 234 xpassed, 5048 warnings in 5093.05s (1:24:53) =
```

<details>
<summary>Failing Tests (first 50 lines)</summary>

```
=========================== short test summary info ============================
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-String(not_null)][DATAGEN_SEED=1771090713, TZ=UTC, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#386] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:string>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-String0][DATAGEN_SEED=1771090713, TZ=UTC, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#390] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:string>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-String1][DATAGEN_SEED=1771090713, TZ=UTC, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#394] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:string>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-String2][DATAGEN_SEED=1771090713, TZ=UTC, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#398] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:string>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-Byte][DATAGEN_SEED=1771090713, TZ=UTC, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#402] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:tinyint>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-Short][DATAGEN_SEED=1771090713, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#406] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:smallint>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-Integer][DATAGEN_SEED=1771090713, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#410] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:int>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-Long][DATAGEN_SEED=1771090713, TZ=UTC, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#414L] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:bigint>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-Boolean][DATAGEN_SEED=1771090713, TZ=UTC, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#418] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:boolean>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-Double0][DATAGEN_SEED=1771090713, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#422] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:double>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-Float0][DATAGEN_SEED=1771090713, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#426] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:float>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-Float1][DATAGEN_SEED=1771090713, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#430] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:float>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_round_trip[json-Double1][DATAGEN_SEED=1771090713, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#434] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:double>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_input_meta[json][DATAGEN_SEED=1771090713, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [b#492L, input_file_name() AS input_file_name()#496, input_file_block_start() AS input_file_block_start()#497L, input_file_block_length() AS input_file_block_length()#498L]
+- Filter (isnotnull(b#492L) AND (b#492L > 0))
   +- FileScan json [b#492L,key#494] Batched: false, DataFilters: [isnotnull(b#492L), (b#492L > 0)], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [IsNotNull(b), GreaterThan(b,0)], ReadSchema: struct<b:bigint>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_date_formats_round_trip[json-None][DATAGEN_SEED=1771090713, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#520] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:date>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_date_formats_round_trip[json-yyyy-MM-dd][DATAGEN_SEED=1771090713, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#524] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:date>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_ts_formats_round_trip[json-None][DATAGEN_SEED=1771090713, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#1257] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:timestamp>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_ts_formats_round_trip[json-yyyy-MM-dd][DATAGEN_SEED=1771090713, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#1261] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:timestamp>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_ts_formats_round_trip[json-yyyy-MM-dd'T'HH:mm:ss.SSSXXX][DATAGEN_SEED=1771090713, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#1265] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:timestamp>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_ts_formats_round_trip[json-yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]][DATAGEN_SEED=1771090713, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#1269] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:timestamp>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_ts_formats_round_trip[json-yyyy-MM-dd'T'HH:mm:ss.SSS][DATAGEN_SEED=1771090713, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#1273] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:timestamp>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_ts_formats_round_trip[json-yyyy-MM-dd'T'HH:mm:ss[.SSS]][DATAGEN_SEED=1771090713, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#1277] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:timestamp>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_ts_formats_round_trip[json-yyyy-MM-dd'T'HH:mm:ss][DATAGEN_SEED=1771090713, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [a#1281] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/tmp/pyspark_tests/1210-231823-ex2q287m-10-59-224-45-master-10062..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:timestamp>
FAILED ../../../../integration_tests/src/main/python/json_test.py::test_json_ts_formats_round_trip[json-yyyy-MM-dd'T'HH:mm[:ss]][DATAGEN_SEED=1771090713, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
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
   TESTS=json_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
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
