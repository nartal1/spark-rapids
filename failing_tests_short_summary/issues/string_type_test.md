## [DBR 17.3] Integration test failures in string_type_test.py

**Describe the bug**
1 integration test(s) failed in `string_type_test.py` when running on Databricks Runtime 17.3 (spark400db173 shim).

```
============= 1 failed, 17 passed, 27 warnings in 73.73s (0:01:13) =============
```

<details>
<summary>Failing Tests (first 50 lines)</summary>

```
=========================== short test summary info ============================
FAILED ../../../../integration_tests/src/main/python/string_type_test.py::test_constraint_char_preserve_disabled_fallback[DATAGEN_SEED=1771009189, TZ=UTC, ALLOW_NON_GPU(ProjectExec)] - py4j.protocol.Py4JJavaError: An error occurred while calling z:org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertDidFallBack.
: java.lang.AssertionError: assertion failed: Could not find StaticInvoke in the GPU plans:
GpuColumnarToRow false, [loreId=4]
+- GpuProject [gpucontains(char_col#208, a) AS contains(char_col, a)#209], true, [loreId=3]
   +- GpuFileGpuScan parquet [char_col#208] Batched: true, DataFilters: [], Eager_IO_Prefetch: false, Format: Parquet, Location: InMemoryFileIndex[file:/tmp/pyspark_tests/0211-040007-7q3e991u-10-59-240-145-master-599964-143723..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<char_col:string>

	at scala.Predef$.assert(Predef.scala:279)
	at org.apache.spark.sql.rapids.ShimmedExecutionPlanCaptureCallbackImpl.assertDidFallBack(ShimmedExecutionPlanCaptureCallbackImpl.scala:153)
	at org.apache.spark.sql.rapids.ShimmedExecutionPlanCaptureCallbackImpl.assertDidFallBack(ShimmedExecutionPlanCaptureCallbackImpl.scala:165)
	at org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback$.assertDidFallBack(ExecutionPlanCaptureCallback.scala:92)
	at org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertDidFallBack(ExecutionPlanCaptureCallback.scala)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)
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
============= 1 failed, 17 passed, 27 warnings in 73.73s (0:01:13) =============
--- Logging error ---
Traceback (most recent call last):
  File "/usr/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/databricks/spark/python/lib/py4j-0.10.9.9-src.zip/py4j/clientserver.py", line 673, in __del__
    self.close()
  File "/databricks/spark/python/lib/py4j-0.10.9.9-src.zip/py4j/clientserver.py", line 570, in close
    logger.info("Closing down clientserver connection")
Message: 'Closing down clientserver connection'
Arguments: ()
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
   TESTS=string_type_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
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
