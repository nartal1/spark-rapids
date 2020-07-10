/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shuffle

import com.nvidia.spark.rapids.{RapidsBuffer, ShuffleReceivedBufferId}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import org.apache.spark.TaskContext
import org.apache.spark.shuffle.RapidsShuffleFetchFailedException
import org.apache.spark.sql.rapids.ShuffleMetricsUpdater
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class RapidsShuffleIteratorSuite extends RapidsShuffleTestHelper {
  test("inability to get a client raises a fetch failure") {
    val blocksByAddress = RapidsShuffleTestHelper.getBlocksByAddress

    val cl = new RapidsShuffleIterator(
      RapidsShuffleTestHelper.makeMockBlockManager("1", "1"),
      null,
      mockTransport,
      blocksByAddress,
      null)

    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Error)

    when(mockTransport.makeClient(any(), any())).thenThrow(new IllegalStateException("Test"))

    assert(cl.hasNext)
    assertThrows[RapidsShuffleFetchFailedException](cl.next())
  }

  test("a transport error/cancellation raises a fetch failure") {
    Seq(TransactionStatus.Error, TransactionStatus.Cancelled).foreach { status =>
      when(mockTransaction.getStatus).thenReturn(status)

      val blocksByAddress = RapidsShuffleTestHelper.getBlocksByAddress

      val cl = new RapidsShuffleIterator(
        RapidsShuffleTestHelper.makeMockBlockManager("1", "1"),
        null,
        mockTransport,
        blocksByAddress,
        null)

      val ac = ArgumentCaptor.forClass(classOf[RapidsShuffleFetchHandler])
      when(mockTransport.makeClient(any(), any())).thenReturn(client)
      doNothing().when(client).doFetch(any(), ac.capture(), any())
      cl.start()

      val handler = ac.getValue.asInstanceOf[RapidsShuffleFetchHandler]
      handler.transferError("Test")

      assert(cl.hasNext)
      assertThrows[RapidsShuffleFetchFailedException](cl.next())

      newMocks()
    }
  }

  test("a new good batch is queued") {
    val blocksByAddress = RapidsShuffleTestHelper.getBlocksByAddress

    val mockMetrics = mock[ShuffleMetricsUpdater]

    val cl = new RapidsShuffleIterator(
      RapidsShuffleTestHelper.makeMockBlockManager("1", "1"),
      null,
      mockTransport,
      blocksByAddress,
      mockMetrics,
      mockCatalog)

    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Error)

    val ac = ArgumentCaptor.forClass(classOf[RapidsShuffleFetchHandler])
    when(mockTransport.makeClient(any(), any())).thenReturn(client)
    doNothing().when(client).doFetch(any(), ac.capture(), any())
    val bufferId = ShuffleReceivedBufferId(1)
    val mockBuffer = mock[RapidsBuffer]

    val cb = new ColumnarBatch(Seq[ColumnVector]().toArray, 10)

    when(mockBuffer.getColumnarBatch).thenReturn(cb)
    when(mockCatalog.acquireBuffer(any[ShuffleReceivedBufferId]())).thenReturn(mockBuffer)
    doNothing().when(mockCatalog).removeBuffer(any())
    cl.start()

    val handler = ac.getValue.asInstanceOf[RapidsShuffleFetchHandler]
    handler.start(1)
    handler.batchReceived(bufferId)

    assert(cl.hasNext)
    assertResult(cb)(cl.next())
  }
}
