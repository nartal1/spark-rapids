/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*** spark-rapids-shim-json-lines
{"spark": "410db183"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.util.Collections

import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class GpuPythonRunnerCommandSuite extends AnyFunSuite {
  private val schema = StructType(Seq(StructField("a", IntegerType)))
  private val offsets = Array(Array(0))
  private val conf = Map("spark.sql.session.timeZone" -> "UTC")
  private val functions = Seq(ChainedPythonFunctions(Seq(new SimplePythonFunction(
    Array[Byte](1, 2), Collections.emptyMap[String, String](),
    Collections.emptyList[String](), "python", "3", Collections.emptyList(), null))) -> 42L)

  private def runners(): Seq[GpuBasePythonRunner[_]] = Seq(
    new GpuArrowPythonRunner(functions, PythonEvalType.SQL_SCALAR_PANDAS_UDF,
      offsets, schema, "UTC", conf, 1024, schema, udfLogMaxEntries = 17,
      udfLogLevel = "ERROR"),
    new GpuWindowArrowPythonRunner(functions, PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF,
      offsets, schema, "UTC", conf, 1024, schema, udfLogMaxEntries = 17,
      udfLogLevel = "ERROR"),
    new GpuGroupUDFArrowPythonRunner(functions, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
      offsets, schema, "UTC", conf, 1024, schema, udfLogMaxEntries = 17,
      udfLogLevel = "ERROR"),
    new GpuCoGroupedArrowPythonRunner(functions, PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
      offsets, schema, schema, "UTC", conf, 1024, schema, udfLogMaxEntries = 17,
      udfLogLevel = "ERROR"))

  test("DBR 18.3 runners expose Arrow settings to BasePythonRunner") {
    withTestSparkEnv {
      runners().foreach { runner =>
        val runnerConf = runner.getClass.getDeclaredMethod("runnerConf")
        runnerConf.setAccessible(true)
        val settings = runnerConf.invoke(runner).asInstanceOf[Map[String, String]]
        assert(settings("spark.sql.session.timeZone") == "UTC")
      }
    }
  }

  test("DBR 18.3 writer commands contain only native UDF metadata, not another conf block") {
    withTestSparkEnv {
      val expected = new ByteArrayOutputStream()
      PythonUDFRunner.writeUDFs(new DataOutputStream(expected), functions, offsets, 17, "ERROR")
      runners().foreach { runner =>
        val newWriter = runner.getClass.getDeclaredMethod("newWriter", classOf[SparkEnv],
          classOf[PythonWorker], classOf[Iterator[_]], Integer.TYPE, classOf[TaskContext])
        newWriter.setAccessible(true)
        val writer = newWriter.invoke(runner, null, null, Iterator.empty, Int.box(0), null)
        val writeCommand = writer.getClass.getDeclaredMethod("writeCommand",
          classOf[DataOutputStream])
        writeCommand.setAccessible(true)
        val actual = new ByteArrayOutputStream()
        writeCommand.invoke(writer, new DataOutputStream(actual))
        assert(actual.toByteArray.sameElements(expected.toByteArray),
          s"${runner.getClass.getSimpleName} duplicated or corrupted the command header")
      }
    }
  }

  private def withTestSparkEnv(f: => Unit): Unit = {
    val previous = SparkEnv.get
    val env = mock[SparkEnv]
    when(env.conf).thenReturn(new SparkConf(loadDefaults = false))
    SparkEnv.set(env)
    try {
      f
    } finally {
      SparkEnv.set(previous)
    }
  }
}
