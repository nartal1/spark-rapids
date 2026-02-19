## [DBR 17.3] Integration test failures in date_time_test.py

**Describe the bug**
42 integration test(s) failed in `date_time_test.py` when running on Databricks Runtime 17.3 (spark400db173 shim).

```
= 42 failed, 2936 passed, 4 xfailed, 4 xpassed, 141 warnings in 1985.26s (0:33:05) =
```

<details>
<summary>Failing Tests (first 50 lines)</summary>

```
=========================== short test summary info ============================
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timesub[(-584, 1563)][DATAGEN_SEED=1771016245, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#2 + INTERVAL '583 23:33:57' DAY TO SECOND AS a - INTERVAL '-583 23:33:57' DAY TO SECOND#3]
+- Scan ExistingRDD[a#2]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timesub[(1943, 1101)][DATAGEN_SEED=1771016245, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#6 + INTERVAL '-1943 00:18:21' DAY TO SECOND AS a - INTERVAL '1943 00:18:21' DAY TO SECOND#7]
+- Scan ExistingRDD[a#6]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timesub[(2693, 2167)][DATAGEN_SEED=1771016245, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#10 + INTERVAL '-2693 00:36:07' DAY TO SECOND AS a - INTERVAL '2693 00:36:07' DAY TO SECOND#11]
+- Scan ExistingRDD[a#10]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timesub[(2729, 0)][DATAGEN_SEED=1771016245, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#14 + INTERVAL '-2729 00:00:00' DAY TO SECOND AS a - INTERVAL '2729 00:00:00' DAY TO SECOND#15]
+- Scan ExistingRDD[a#14]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timesub[(44, 1534)][DATAGEN_SEED=1771016245, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#18 + INTERVAL '-44 00:25:34' DAY TO SECOND AS a - INTERVAL '44 00:25:34' DAY TO SECOND#19]
+- Scan ExistingRDD[a#18]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timesub[(2635, 3319)][DATAGEN_SEED=1771016245, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#22 + INTERVAL '-2635 00:55:19' DAY TO SECOND AS a - INTERVAL '2635 00:55:19' DAY TO SECOND#23]
+- Scan ExistingRDD[a#22]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timesub[(1885, -2828)][DATAGEN_SEED=1771016245, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#26 + INTERVAL '-1884 23:12:52' DAY TO SECOND AS a - INTERVAL '1884 23:12:52' DAY TO SECOND#27]
+- Scan ExistingRDD[a#26]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timesub[(0, 2463)][DATAGEN_SEED=1771016245, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#30 + INTERVAL '-0 00:41:03' DAY TO SECOND AS a - INTERVAL '0 00:41:03' DAY TO SECOND#31]
+- Scan ExistingRDD[a#30]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timesub[(932, 2286)][DATAGEN_SEED=1771016245, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#34 + INTERVAL '-932 00:38:06' DAY TO SECOND AS a - INTERVAL '932 00:38:06' DAY TO SECOND#35]
+- Scan ExistingRDD[a#34]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timesub[(0, 0)][DATAGEN_SEED=1771016245, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#38 + INTERVAL '0 00:00:00' DAY TO SECOND AS a - INTERVAL '0 00:00:00' DAY TO SECOND#39]
+- Scan ExistingRDD[a#38]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timeadd[(-584, 1563)][DATAGEN_SEED=1771016245, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#42 + INTERVAL '-583 23:33:57' DAY TO SECOND AS a + INTERVAL '-583 23:33:57' DAY TO SECOND#43]
+- Scan ExistingRDD[a#42]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timeadd[(1943, 1101)][DATAGEN_SEED=1771016245, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#46 + INTERVAL '1943 00:18:21' DAY TO SECOND AS a + INTERVAL '1943 00:18:21' DAY TO SECOND#47]
+- Scan ExistingRDD[a#46]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timeadd[(2693, 2167)][DATAGEN_SEED=1771016245, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#50 + INTERVAL '2693 00:36:07' DAY TO SECOND AS a + INTERVAL '2693 00:36:07' DAY TO SECOND#51]
+- Scan ExistingRDD[a#50]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timeadd[(2729, 0)][DATAGEN_SEED=1771016245, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#54 + INTERVAL '2729 00:00:00' DAY TO SECOND AS a + INTERVAL '2729 00:00:00' DAY TO SECOND#55]
+- Scan ExistingRDD[a#54]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timeadd[(44, 1534)][DATAGEN_SEED=1771016245, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#58 + INTERVAL '44 00:25:34' DAY TO SECOND AS a + INTERVAL '44 00:25:34' DAY TO SECOND#59]
+- Scan ExistingRDD[a#58]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timeadd[(2635, 3319)][DATAGEN_SEED=1771016245, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
Project [a#62 + INTERVAL '2635 00:55:19' DAY TO SECOND AS a + INTERVAL '2635 00:55:19' DAY TO SECOND#63]
+- Scan ExistingRDD[a#62]
FAILED ../../../../integration_tests/src/main/python/date_time_test.py::test_timeadd[(1885, -2828)][DATAGEN_SEED=1771016245, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec
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
   TESTS=date_time_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
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
