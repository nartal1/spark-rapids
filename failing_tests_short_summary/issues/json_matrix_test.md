## [DBR 17.3] Integration test failures in json_matrix_test.py

**Describe the bug**
377 integration test(s) failed in `json_matrix_test.py` when running on Databricks Runtime 17.3 (spark400db173 shim).

```
= 377 failed, 526 passed, 106 xfailed, 26 xpassed, 70 warnings in 587.61s (0:09:47) =
```

<details>
<summary>Failing Tests (first 50 lines)</summary>

```
=========================== short test summary info ============================
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_comments_off[read_json_df][DATAGEN_SEED=1771005901, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [str#48] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<str:string>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_comments_off[read_json_sql][DATAGEN_SEED=1771005901, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json spark_catalog.default.tmp_table_master_235104671_1[str#56] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<str:string>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_single_quotes_on[read_json_df][DATAGEN_SEED=1771005901, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [str#158] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<str:string>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_single_quotes_on[read_json_sql][DATAGEN_SEED=1771005901, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json spark_catalog.default.tmp_table_master_1083750207_1[str#166] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<str:string>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_unquoted_field_names_off[read_json_df][DATAGEN_SEED=1771005901, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [str#268] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<str:string>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_unquoted_field_names_off[read_json_sql][DATAGEN_SEED=1771005901, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json spark_catalog.default.tmp_table_master_1720292565_1[str#276] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<str:string>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_numeric_leading_zeros_on[read_json_df][DATAGEN_SEED=1771005901, TZ=UTC, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [byte#338,int#339,float#340,decimal#341] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<byte:tinyint,int:int,float:float,decimal:decimal(10,3)>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_numeric_leading_zeros_on[read_json_sql][DATAGEN_SEED=1771005901, TZ=UTC, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json spark_catalog.default.tmp_table_master_634149658_1[byte#361,int#362,float#363,decimal#364] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<byte:tinyint,int:int,float:float,decimal:decimal(10,3)>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_numeric_leading_zeros_off[read_json_df][DATAGEN_SEED=1771005901, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [byte#406,int#407,float#408,decimal#409] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<byte:tinyint,int:int,float:float,decimal:decimal(10,3)>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_numeric_leading_zeros_off[read_json_sql][DATAGEN_SEED=1771005901, TZ=UTC, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json spark_catalog.default.tmp_table_master_2146446762_1[byte#429,int#430,float#431,decimal#432] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<byte:tinyint,int:int,float:float,decimal:decimal(10,3)>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_nonnumeric_numbers_off[read_json_df][DATAGEN_SEED=1771005901, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [float#503,double#504] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<float:float,double:double>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_nonnumeric_numbers_off[read_json_sql][DATAGEN_SEED=1771005901, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json spark_catalog.default.tmp_table_master_1339224492_1[float#516,double#517] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<float:float,double:double>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_nonnumeric_numbers_on[read_json_df][DATAGEN_SEED=1771005901, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [float#555,double#556] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<float:float,double:double>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_nonnumeric_numbers_on[read_json_sql][DATAGEN_SEED=1771005901, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json spark_catalog.default.tmp_table_master_651656730_1[float#568,double#569] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<float:float,double:double>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_backslash_escape_any_off[read_json_df][DATAGEN_SEED=1771005901, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [str#630] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<str:string>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_unquoted_control_chars_off[read_json_df][DATAGEN_SEED=1771005901, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [str#722] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<str:string>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_unquoted_control_chars_off[read_json_sql][DATAGEN_SEED=1771005901, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json spark_catalog.default.tmp_table_master_16720233_1[str#730] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<str:string>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_unquoted_control_chars_on[read_json_df][DATAGEN_SEED=1771005901, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [str#766] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<str:string>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_allow_unquoted_control_chars_on[read_json_sql][DATAGEN_SEED=1771005901, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json spark_catalog.default.tmp_table_master_148865595_1[str#774] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/wi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<str:string>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_dec_locale_US[read_json_df][DATAGEN_SEED=1771005901, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [data#830] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/de..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<data:decimal(10,5)>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_bytes[read_json_df-int_formatted.json][DATAGEN_SEED=1771005901, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [data#1652] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/in..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<data:tinyint>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_bytes[read_json_df-float_formatted.json][DATAGEN_SEED=1771005901, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [data#1658] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/fl..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<data:tinyint>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_bytes[read_json_df-sci_formatted.json][DATAGEN_SEED=1771005901, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [data#1664] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/sc..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<data:tinyint>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_bytes[read_json_df-int_formatted_strings.json][DATAGEN_SEED=1771005901, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
FileScan json [data#1670] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/home/ubuntu/spark-rapids/integration_tests/src/test/resources/in..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<data:tinyint>
FAILED ../../../../integration_tests/src/main/python/json_matrix_test.py::test_scan_json_bytes[read_json_df-float_formatted_strings.json][DATAGEN_SEED=1771005901, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec
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
   TESTS=json_matrix_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
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
