## [DBR 17.3] Integration test failures in generate_expr_test.py

**Describe the bug**
1 integration test(s) failed in `generate_expr_test.py` when running on Databricks Runtime 17.3 (spark400db173 shim).

```
=========== 1 failed, 403 passed, 244 warnings in 3073.03s (0:51:13) ===========
```

<details>
<summary>Failing Tests (first 50 lines)</summary>

```
=========================== short test summary info ============================
FAILED ../../../../integration_tests/src/main/python/generate_expr_test.py::test_generate_outer_fallback[DATAGEN_SEED=1771010506, TZ=UTC, INJECT_OOM, IGNORE_ORDER({'local': True}), ALLOW_NON_GPU(GenerateExec,ShuffleExchangeExec)] - pyspark.errors.exceptions.captured.IllegalArgumentException: Part of the plan is not columnar class org.apache.spark.sql.execution.OneRowRelationExec
Scan OneRowRelation[]
=========== 1 failed, 403 passed, 244 warnings in 3073.03s (0:51:13) ===========
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
	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
2026-02-13 20:13:46,831 [shutdown-hook-0] WARN  org.apache.spark.SparkContext - Requesting executors is not supported by current scheduler.
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
   TESTS=generate_expr_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
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
