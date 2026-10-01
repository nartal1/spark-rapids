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

from asserts import assert_gpu_and_cpu_are_equal_collect
from delta_lake_utils import assert_rapids_delta_write, delta_meta_allow, is_oss_delta_lake_42
from marks import allow_non_gpu, delta_lake, ignore_order
from spark_session import with_cpu_session


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_oss_delta_lake_42(), reason="Experimental OSS Delta 4.2 scan")
@pytest.mark.parametrize("reader", ["PERFILE", "MULTITHREADED", "COALESCING"])
@pytest.mark.parametrize("chunked", ["true", "false"])
@pytest.mark.parametrize("materialized", [False, True])
def test_delta_row_tracking_native_scan(spark_tmp_path, reader, chunked, materialized):
    path = spark_tmp_path + "/row_tracking"
    conf = {
        "spark.sql.adaptive.enabled": "true",
        "spark.rapids.sql.format.parquet.reader.type": reader,
        "spark.rapids.sql.reader.chunked": chunked,
        "spark.rapids.sql.reader.batchSizeRows": "512",
        "spark.sql.files.maxPartitionBytes": "32768",
        "spark.databricks.delta.optimizeWrite.enabled": "false",
        "spark.databricks.delta.autoCompact.enabled": "false",
        "spark.databricks.delta.deletionVectors.useMetadataRowIndex": "true",
        "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": "true",
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
        "spark.databricks.delta.update.deletionVectors.persistent": "true",
    }

    def setup(spark):
        (spark.range(20000).selectExpr(
            "id", "CAST(id % 4 AS INT) p", "sha2(CAST(id AS STRING), 256) value",
            "CAST(CASE WHEN id % 13 = 0 THEN NULL ELSE id % 100 END AS DECIMAL(7,2)) amount",
            "-id AS base_row_id")
            .coalesce(1).write.format("delta").partitionBy("p")
            .option("parquet.block.size", "16384")
            .option("delta.enableRowTracking", "true")
            .option("delta.enableDeletionVectors", "true").save(path))
        if materialized:
            spark.sql(f"UPDATE delta.`{path}` SET value = 'updated' WHERE id % 5 = 0").collect()
            spark.sql(f"DELETE FROM delta.`{path}` WHERE id % 11 = 0").collect()

    with_cpu_session(setup, conf=conf)

    def read(spark):
        # Deliberately omit the partition column: metadata must survive partition pruning.
        df = spark.sql(f"""SELECT id, value, amount, base_row_id,
            _metadata.row_id AS tracked_id,
            _metadata.row_commit_version AS tracked_version,
            _metadata.file_name AS file_name,
            _metadata.file_size AS file_size
            FROM delta.`{path}` WHERE id BETWEEN 100 AND 19800""")
        if str(spark.conf.get("spark.rapids.sql.enabled", "false")).lower() == "true":
            plan = df._jdf.queryExecution().executedPlan()
            callback = spark._sc._jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
            assert callback.contains(plan, "GpuFileSourceScanExec"), str(plan)
            assert not callback.contains(plan, "HostColumnarToGpu"), str(plan)
        return df

    assert_gpu_and_cpu_are_equal_collect(read, conf=conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(not is_oss_delta_lake_42(), reason="Experimental OSS Delta 4.2 scan")
@pytest.mark.parametrize("reader", ["PERFILE", "MULTITHREADED", "COALESCING"])
def test_delta_row_tracking_native_update(spark_tmp_path, reader):
    paths = [spark_tmp_path + "/cpu", spark_tmp_path + "/gpu"]
    conf = {
        "spark.sql.adaptive.enabled": "true",
        "spark.rapids.sql.format.parquet.reader.type": reader,
        "spark.rapids.sql.format.delta.write.enabled": "true",
        "spark.rapids.sql.command.UpdateCommand": "true",
        "spark.databricks.delta.deletionVectors.useMetadataRowIndex": "true",
        "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": "true",
        "spark.databricks.delta.update.deletionVectors.persistent": "true",
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "false",
        "spark.databricks.delta.autoCompact.enabled": "false",
    }

    def setup(spark):
        for path in paths:
            (spark.range(2000).selectExpr("id", "CAST(id AS STRING) value").coalesce(1)
             .write.format("delta").option("delta.enableRowTracking", "true")
             .option("delta.enableDeletionVectors", "true").save(path))

    def snapshot(spark, path):
        return spark.sql(f"""SELECT id, value, _metadata.row_id,
            _metadata.row_commit_version FROM delta.`{path}` ORDER BY id""").collect()

    with_cpu_session(setup, conf=conf)
    before = [with_cpu_session(lambda s: snapshot(s, path), conf=conf) for path in paths]
    assert before[0] == before[1]
    original_ids = {r.id: r.row_id for r in before[0]}

    for divisor in (5, 7):
        def update(spark, path):
            return spark.sql(f"UPDATE delta.`{path}` SET value = 'updated-{divisor}' "
                             f"WHERE id % {divisor} = 0").collect()
        cpu_result = with_cpu_session(lambda s: update(s, paths[0]), conf=conf)
        gpu_result = assert_rapids_delta_write(
            lambda s: update(s, paths[1]), conf=conf,
            required_gpu_classes=["GpuRapidsDeltaWriteExec", "GpuFileSourceScanExec"],
            require_same_plan=True, require_non_empty=True)
        assert cpu_result == gpu_result
        after = [with_cpu_session(lambda s: snapshot(s, path), conf=conf) for path in paths]
        assert after[0] == after[1]
        assert all(r.row_id == original_ids[r.id] for r in after[1])
        if divisor == 5:
            # The second GPU update reads both materialized IDs and existing deletion vectors.
            for path in paths:
                with_cpu_session(lambda s: s.sql(
                    f"DELETE FROM delta.`{path}` WHERE id % 11 = 0").collect(), conf=conf)
