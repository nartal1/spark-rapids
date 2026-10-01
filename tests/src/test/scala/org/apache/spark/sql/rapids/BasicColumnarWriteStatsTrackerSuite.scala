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

package org.apache.spark.sql.rapids

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.mockito.Mockito.{never, spy, verify}
import org.scalatest.funsuite.AnyFunSuite

class BasicColumnarWriteStatsTrackerSuite extends AnyFunSuite {
  test("positive visible file size can be shared without changing either tracker stats") {
    val file = Files.createTempFile("rapids-write-stats", ".parquet")
    try {
      Files.write(file, Array[Byte](1, 2, 3, 4))
      val conf = new Configuration()
      val basic = spy(new BasicColumnarWriteTaskStatsTracker(conf, None))
      val gpu = spy(new GpuWriteTaskStatsTracker(conf, Map.empty))
      val path = file.toUri.toString
      basic.newFile(path)
      gpu.newFile(path)

      GpuFileFormatDataWriter.closeFileStatsTrackers(Seq(basic, gpu), path)
      verify(basic).closeFileAndGetReusableStatusLength(path)
      verify(gpu).closeFileWithStatusLength(path, 4L)
      verify(gpu, never()).closeFile(path)

      val basicStats = basic.getFinalStats(0L).asInstanceOf[BasicColumnarWriteTaskStats]
      val gpuStats = gpu.getFinalStats(0L).asInstanceOf[BasicColumnarWriteTaskStats]
      assert(basicStats.numFiles == 1 && basicStats.numBytes == 4L)
      assert(gpuStats.numFiles == 1 && gpuStats.numBytes == 4L)
    } finally {
      Files.deleteIfExists(file)
    }
  }

  test("zero-length and missing files are not reused") {
    val file = Files.createTempFile("rapids-write-stats-zero", ".parquet")
    try {
      val conf = new Configuration()
      val basic = new BasicColumnarWriteTaskStatsTracker(conf, None)
      val zeroPath = file.toUri.toString
      basic.newFile(zeroPath)
      assert(basic.closeFileAndGetReusableStatusLength(zeroPath).isEmpty)
      val zeroStats = basic.getFinalStats(0L).asInstanceOf[BasicColumnarWriteTaskStats]
      assert(zeroStats.numFiles == 1 && zeroStats.numBytes == 0L)

      val absent = file.resolveSibling(file.getFileName.toString + ".missing")
      val absentPath = absent.toUri.toString
      val missing = new BasicColumnarWriteTaskStatsTracker(conf, None)
      missing.newFile(absentPath)
      assert(missing.closeFileAndGetReusableStatusLength(absentPath).isEmpty)
      val missingStats = missing.getFinalStats(0L).asInstanceOf[BasicColumnarWriteTaskStats]
      assert(missingStats.numFiles == 0 && missingStats.numBytes == 0L)
    } finally {
      Files.deleteIfExists(file)
    }
  }

  test("file size is not shared across Hadoop configuration instances") {
    val file = Files.createTempFile("rapids-write-stats-scope", ".parquet")
    try {
      Files.write(file, Array[Byte](1, 2, 3))
      val basic = new BasicColumnarWriteTaskStatsTracker(new Configuration(), None)
      val gpu = spy(new GpuWriteTaskStatsTracker(new Configuration(), Map.empty))
      val path = file.toUri.toString
      basic.newFile(path)
      gpu.newFile(path)

      GpuFileFormatDataWriter.closeFileStatsTrackers(Seq(basic, gpu), path)
      verify(gpu).closeFile(path)
      verify(gpu, never()).closeFileWithStatusLength(path, 3L)
      assert(gpu.getFinalStats(0L).asInstanceOf[BasicColumnarWriteTaskStats].numBytes == 3L)
    } finally {
      Files.deleteIfExists(file)
    }
  }
}
