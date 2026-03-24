# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from spark_init_internal import get_spark_i_know_what_i_am_doing
from spark_session import is_spark_358_or_later


@pytest.mark.skipif(not is_spark_358_or_later(),
    reason="initMetricsValues() only exists in Spark 3.5.8 (fix versions: 3.5.8, 4.0.3, 4.1.2, 4.2.0)")
def test_gpu_data_source_rdd_init_metrics_propagation():
    """
    Verifies that GpuDataSourceRDD (spark358+ shim) propagates custom metrics between
    grouped partition readers via initMetricsValues(), closing the gap identified in
    SPARK-55302.

    The test groups two input partitions into one output split (simulating
    KeyGroupedPartitioning). Partition 0's reader reports currentMetricsValues()
    = [nextRowId=3]. After our fix, GpuDataSourceRDD calls
      batchReader.initMetricsValues(prev.currentMetricsValues())
    on partition 1's reader, which stores the received nextRowId in shared state.

    Expected:  received nextRowId == 3  (fix present)
    Without fix: received nextRowId == -1 (initMetricsValues never called)
    """
    spark = get_spark_i_know_what_i_am_doing()
    jvm = spark._jvm
    sc = spark._jsc.sc()

    helper = jvm.com.nvidia.spark.rapids.tests.datasourcev2.KeyGroupedMetricsTestHelper
    received = helper.runAndGetReceivedNextRowId(sc)

    assert received == 3, (
        f"GpuDataSourceRDD should propagate custom metrics via initMetricsValues() "
        f"(SPARK-55302 fix). Expected nextRowId=3 but received {received}. "
        f"If received=-1, initMetricsValues() was never called on the second grouped reader."
    )
