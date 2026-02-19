## [DBR 17.3] Integration test failures in array_test.py

**Describe the bug**
13 integration test(s) failed in `array_test.py` when running on Databricks Runtime 17.3 (spark400db173 shim).

```
= 13 failed, 629 passed, 73 skipped, 2 xfailed, 28 warnings in 1207.63s (0:20:07) =
```

<details>
<summary>Failing Tests (first 50 lines)</summary>

```
=========================== short test summary info ============================
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_array_item_ansi_fail_invalid_index[-2][DATAGEN_SEED=1771000777, TZ=UTC] - AssertionError: Expected error 'SparkArrayIndexOutOfBoundsException' did not appear in 'pyspark.errors.exceptions.captured.ArrayIndexOutOfBoundsException: [INVALID_ARRAY_INDEX] The index -2 is out of bounds. The array has 0 elements. Use the SQL function `get()` to tolerate accessing element at invalid index and return NULL instead. SQLSTATE: 22003
== DataFrame ==
"__getitem__" was called from
/home/ubuntu/spark-rapids/integration_tests/src/main/python/array_test.py:158'
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_array_item_ansi_fail_invalid_index[100][DATAGEN_SEED=1771000777, TZ=UTC] - AssertionError: Expected error 'SparkArrayIndexOutOfBoundsException' did not appear in 'pyspark.errors.exceptions.captured.ArrayIndexOutOfBoundsException: [INVALID_ARRAY_INDEX] The index 100 is out of bounds. The array has 13 elements. Use the SQL function `get()` to tolerate accessing element at invalid index and return NULL instead. SQLSTATE: 22003
== DataFrame ==
"__getitem__" was called from
/home/ubuntu/spark-rapids/integration_tests/src/main/python/array_test.py:158'
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_array_item_ansi_fail_invalid_index[Integer0][DATAGEN_SEED=1771000777, TZ=UTC, INJECT_OOM] - AssertionError: Expected error 'SparkArrayIndexOutOfBoundsException' did not appear in 'pyspark.errors.exceptions.captured.ArrayIndexOutOfBoundsException: [INVALID_ARRAY_INDEX] The index -2 is out of bounds. The array has 17 elements. Use the SQL function `get()` to tolerate accessing element at invalid index and return NULL instead. SQLSTATE: 22003
== SQL (line 1, position 1) ==
a[b]
^^^^'
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_array_item_ansi_fail_invalid_index[Integer1][DATAGEN_SEED=1771000777, TZ=UTC, INJECT_OOM] - AssertionError: Expected error 'SparkArrayIndexOutOfBoundsException' did not appear in 'pyspark.errors.exceptions.captured.ArrayIndexOutOfBoundsException: [INVALID_ARRAY_INDEX] The index 87 is out of bounds. The array has 5 elements. Use the SQL function `get()` to tolerate accessing element at invalid index and return NULL instead. SQLSTATE: 22003
== SQL (line 1, position 1) ==
a[b]
^^^^'
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_sql_array_scalars[sequence(1, 5) as s][DATAGEN_SEED=1771000777, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.OneRowRelationExec
Scan OneRowRelation[]
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_sql_array_scalars[array(1, 2, 3) as a][DATAGEN_SEED=1771000777, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.OneRowRelationExec
Scan OneRowRelation[]
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_sql_array_scalars[array(sequence(1, 5), sequence(2, 7)) as a_a][DATAGEN_SEED=1771000777, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.OneRowRelationExec
Scan OneRowRelation[]
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_sql_array_scalars[array(map(1, "a", 2, "b")) as a_m][DATAGEN_SEED=1771000777, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.OneRowRelationExec
Scan OneRowRelation[]
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_sql_array_scalars[array(map_from_arrays(sequence(1, 2), array("1", "2"))) as a_m][DATAGEN_SEED=1771000777, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.OneRowRelationExec
Scan OneRowRelation[]
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_sql_array_scalars[array(struct(1 as a, 2 as b), struct(3 as a, 4 as b)) as a_s][DATAGEN_SEED=1771000777, TZ=UTC] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.OneRowRelationExec
Scan OneRowRelation[]
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_sql_array_scalars[array(struct(1 as a, sequence(1, 5) as b), struct(3 as a, sequence(2, 7) as b)) as a_s_a][DATAGEN_SEED=1771000777, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.OneRowRelationExec
Scan OneRowRelation[]
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_sql_array_scalars[array(array(struct(1 as a, 2 as b), struct(3 as a, 4 as b))) as a_a_s][DATAGEN_SEED=1771000777, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.OneRowRelationExec
Scan OneRowRelation[]
FAILED ../../../../integration_tests/src/main/python/array_test.py::test_array_max_q1[DATAGEN_SEED=1771000777, TZ=UTC, INJECT_OOM] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.OneRowRelationExec
Scan OneRowRelation[]
= 13 failed, 629 passed, 73 skipped, 2 xfailed, 28 warnings in 1207.63s (0:20:07) =
org.apache.spark.SparkUserAppException: User application exited with 1
	at org.apache.spark.deploy.PythonRunner$.main(PythonRunner.scala:127)
	at org.apache.spark.deploy.PythonRunner.main(PythonRunner.scala)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
	at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:1024)
	at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:198)
	at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:221)
	at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:95)
	at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1140)
	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1149)
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
   TESTS=array_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
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
