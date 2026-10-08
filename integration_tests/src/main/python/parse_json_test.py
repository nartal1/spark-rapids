# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

import pytest

from asserts import (assert_cpu_and_gpu_are_equal_collect_with_capture,
                     assert_gpu_and_cpu_error, run_with_cpu_and_gpu)
from conftest import spark_jvm
from marks import allow_non_gpu, incompat
from spark_session import is_before_spark_400, is_spark_420_or_later, with_cpu_session

pytestmark = pytest.mark.skipif(is_before_spark_400(), reason='VARIANT requires Spark 4.0+')

_conf = {'spark.sql.adaptive.enabled': 'false',
         'spark.sql.variant.allowDuplicateKeys': 'false',
         'spark.sql.variant.validateUnicodeInJsonParsing': 'false',
         'spark.rapids.sql.expression.cpuBridge.enabled': 'true',
         'spark.rapids.sql.metrics.level': 'DEBUG'}


def _input(spark, documents):
    # Keep heterogeneous rows in one task/batch, so a failed row exercises expression-batch
    # replay rather than accidentally testing separate all-valid and all-invalid partitions.
    return spark.createDataFrame(list(enumerate(documents)), 'id LONG, json STRING').coalesce(1)


def _cpu_bridge_time(plan):
    total = 0
    metrics = plan.metrics().iterator()
    while metrics.hasNext():
        entry = metrics.next()
        if entry._1() == 'cpuBridgeProcessingTime':
            total += entry._2().value()
    children = plan.children().iterator()
    while children.hasNext():
        total += _cpu_bridge_time(children.next())
    return total


def _assert_variant_bytes(func, conf=None, native=True, fallback=None):
    cpu, gpu = run_with_cpu_and_gpu(func, 'COLLECT_WITH_DATAFRAME',
                                   conf={**_conf, **(conf or {})})
    cpu_rows, _ = cpu
    gpu_rows, gpu_df = gpu
    # Sorting a projection that still contains VARIANT is a separate unsupported operator.
    # Compare by the stable input id on the host instead of introducing a CPU SortExec.
    cpu_rows = sorted(cpu_rows, key=lambda row: row.id)
    gpu_rows = sorted(gpu_rows, key=lambda row: row.id)
    assert len(cpu_rows) == len(gpu_rows)
    for expected, actual in zip(cpu_rows, gpu_rows):
        assert expected.id == actual.id
        if expected.v is None:
            assert actual.v is None
        else:
            assert actual.v is not None
            assert bytes(expected.v.value) == bytes(actual.v.value)
            assert bytes(expected.v.metadata) == bytes(actual.v.metadata)
    callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    if native:
        callback.assertContains(gpu_df._jdf, 'GpuParseJson')
    else:
        callback.assertNotContain(gpu_df._jdf, 'GpuParseJson')
    if fallback is not None:
        bridge_time = _cpu_bridge_time(gpu_df._jdf.queryExecution().executedPlan())
        if fallback:
            assert bridge_time > 0, 'Expected parser CPU replay to be exercised'
        else:
            assert bridge_time == 0, 'Expected native parsing without CPU replay'


@allow_non_gpu('RDDScanExec')
@pytest.mark.parametrize('function', ['parse_json', 'try_parse_json'])
def test_parse_json_native_encoding(function):
    documents = [None, 'null', '"null"', 'true', 'false', '0', '-129', '32768',
                 '9223372036854775807', '1.25', '-0.0', '1e3', '1e-300',
                 '[]', '{}', '[1,"x",null,{"a":true}]',
                 '{"":1,"a.b":2,"a[0]":3,"quote\\\"":4,"slash\\\\":5}',
                 '{"z":1,"a":{"items":[{"sku":"first"},2]},"b":true}']
    _assert_variant_bytes(lambda spark: _input(spark, documents).selectExpr(
        'id', f'{function}(json) AS v'), fallback=False)


@allow_non_gpu('RDDScanExec')
def test_try_parse_json_invalid_and_unsupported():
    documents = [None, 'null', '{', 'nonsense', '{"a":1,}', '{"a":1 "b":2}',
                 '{"a":1,"a":2}', '{"a":"x\\qy"}', '{"a":[1,2}',
                 '{"a":1} junk', '{"a":1}{"b":2}',
                 '[' * 128 + '1' + ']' * 128, '[' * 1001 + '1' + ']' * 1001]
    _assert_variant_bytes(lambda spark: _input(spark, documents).selectExpr(
        'id', 'try_parse_json(json) AS v'), fallback=True)


@allow_non_gpu('RDDScanExec')
@pytest.mark.parametrize('document', ['{', '{"a":1,"a":2}', '{"a":"x\\qy"}',
                                      '[' * 1001 + '1' + ']' * 1001])
def test_parse_json_strict_errors(document):
    assert_gpu_and_cpu_error(
        lambda spark: _input(spark, [document]).selectExpr('parse_json(json)').collect(),
        conf=_conf, error_message='MALFORMED_RECORD_IN_PARSING')


@allow_non_gpu('RDDScanExec')
def test_parse_json_strict_valid_unsupported_depth():
    document = '[' * 128 + '1' + ']' * 128
    _assert_variant_bytes(lambda spark: _input(spark, ['{"a":1}', document, None])
        .selectExpr('id', 'parse_json(json) AS v'), fallback=True)


@allow_non_gpu('RDDScanExec')
def test_parse_json_strict_mixed_error_batch():
    documents = ['{"a":1}', '[' * 128 + '1' + ']' * 128, '{', None]
    assert_gpu_and_cpu_error(
        lambda spark: _input(spark, documents).coalesce(1).selectExpr('parse_json(json)')
            .collect(), conf=_conf, error_message='MALFORMED_RECORD_IN_PARSING')


@allow_non_gpu('RDDScanExec', 'ProjectExec', 'StaticInvoke')
def test_parse_json_sql_null_and_json_null_distinct():
    # is_variant_null is outside this parser change; the upper CPU projection is expected.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _input(spark, [None, 'null', '"null"', '{}'])
            .selectExpr('id', 'parse_json(json) AS v').selectExpr('id',
                'v IS NULL AS sql_null', 'is_variant_null(v) AS variant_null').orderBy('id'),
        exist_classes='GpuParseJson', conf=_conf)


@allow_non_gpu('RDDScanExec')
@incompat
def test_parse_json_sql_null_then_native_extraction():
    # Parser SQL nulls have a null parent struct but empty non-null metadata children.
    # Metadata eligibility must honor the parent mask instead of replaying the whole batch.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _input(spark, [None, '{"a":2}', 'null']).selectExpr(
            'id', 'parse_json(json) AS v').selectExpr('id',
                "try_variant_get(v, '$.a', 'bigint') AS a").orderBy('id'),
        exist_classes='GpuParseJson,GpuVariantGet', conf=_conf,
        gpu_plan_assertion=lambda cpu_plan, gpu_plan: _assert_no_cpu_bridge(gpu_plan))


@allow_non_gpu('RDDScanExec', 'IsNull')
def test_parse_json_static_invoke_preserves_string_decode():
    def query(spark):
        return spark.createDataFrame([(0, '{"a":1}', bytearray(b'abc'))],
            'id LONG, json STRING, bytes BINARY').selectExpr('id',
                "decode(bytes, 'GBK') AS decoded", 'parse_json(json) AS v').selectExpr(
                    'id', 'decoded', 'v IS NULL AS parsed_null')
    assert_cpu_and_gpu_are_equal_collect_with_capture(query,
        exist_classes='GpuParseJson,GpuStringDecode',
        conf={**_conf, 'spark.sql.legacy.javaCharsets': 'true'})


@allow_non_gpu('RDDScanExec')
@pytest.mark.parametrize('function', ['parse_json', 'try_parse_json'])
def test_parse_json_unicode_guard(function):
    documents = ['{"é":"λ"}', '{"\\uD800":1,"\\uD801":2}',
                 '{"\\uE000":1,"\\uD800\\uDC00":2}',
                 json.dumps({'\ue000': 1, '\U00010000': 2}, ensure_ascii=False)]
    _assert_variant_bytes(lambda spark: _input(spark, documents).selectExpr(
        'id', f'{function}(json) AS v'), fallback=True)


@allow_non_gpu('RDDScanExec', 'ProjectExec', 'StaticInvoke')
@pytest.mark.parametrize('function', ['parse_json', 'try_parse_json'])
def test_parse_json_allowed_duplicates_planner_fallback(function):
    # Duplicate-allowed parsing deliberately remains a CPU StaticInvoke/ProjectExec.
    _assert_variant_bytes(lambda spark: _input(spark, ['{"a":1,"a":2}',
        '{"outer":{"a":1,"a":2}}']).selectExpr('id', f'{function}(json) AS v'),
        conf={'spark.sql.variant.allowDuplicateKeys': 'true'}, native=False)


@allow_non_gpu('RDDScanExec')
@pytest.mark.skipif(not is_spark_420_or_later(), reason='Unicode validation requires Spark 4.2+')
@pytest.mark.parametrize('function', ['parse_json', 'try_parse_json'])
def test_parse_json_unicode_validation_native_ascii(function):
    _assert_variant_bytes(lambda spark: _input(spark, ['{"a":1}', 'null', None,
        '["valid ASCII",true]']).selectExpr('id', f'{function}(json) AS v'),
        conf={'spark.sql.variant.validateUnicodeInJsonParsing': 'true'}, fallback=False)


@allow_non_gpu('RDDScanExec')
@pytest.mark.skipif(not is_spark_420_or_later(), reason='Unicode validation requires Spark 4.2+')
@pytest.mark.parametrize('function', ['parse_json', 'try_parse_json'])
def test_parse_json_unicode_validation_valid_unicode_replay(function):
    _assert_variant_bytes(lambda spark: _input(spark, ['{"é":"λ"}',
        '{"\\uD800\\uDC00":1}']).selectExpr('id', f'{function}(json) AS v'),
        conf={'spark.sql.variant.validateUnicodeInJsonParsing': 'true'}, fallback=True)


@allow_non_gpu('RDDScanExec')
@pytest.mark.skipif(not is_spark_420_or_later(), reason='Unicode validation requires Spark 4.2+')
def test_try_parse_json_unicode_validation_invalid_surrogate():
    _assert_variant_bytes(lambda spark: _input(spark, ['{"a":1}', '{"\\uD800":1}',
        '{"a":"\\uD800"}']).selectExpr('id', 'try_parse_json(json) AS v'),
        conf={'spark.sql.variant.validateUnicodeInJsonParsing': 'true'}, fallback=True)


@allow_non_gpu('RDDScanExec')
@pytest.mark.skipif(not is_spark_420_or_later(), reason='Unicode validation requires Spark 4.2+')
def test_parse_json_unicode_validation_invalid_surrogate_error():
    assert_gpu_and_cpu_error(
        lambda spark: _input(spark, ['{"\\uD800":1}']).selectExpr('parse_json(json)').collect(),
        conf={**_conf, 'spark.sql.variant.validateUnicodeInJsonParsing': 'true'},
        error_message='MALFORMED_RECORD_IN_PARSING')


@allow_non_gpu('RDDScanExec')
@pytest.mark.skipif(not is_spark_420_or_later(), reason='Unicode validation requires Spark 4.2+')
def test_parse_json_unicode_validation_masked_failure():
    def query(spark):
        rows = [(0, '{"a":1}', '{"\\uD800":1}'),
                (1, '{"a":"\\uD800"}', '{"é":"λ"}')]
        return spark.createDataFrame(rows, 'id LONG, json STRING, other STRING').coalesce(1)\
            .selectExpr('id', 'IF(id = 0, parse_json(json), parse_json(other)) AS v')
    _assert_variant_bytes(query, conf={'spark.sql.variant.validateUnicodeInJsonParsing': 'true'},
                          fallback=True)


@allow_non_gpu('RDDScanExec')
@pytest.mark.parametrize('expression', [
    "IF(id % 2 = 0, parse_json(json), parse_json(other))",
    "CASE WHEN id % 2 = 0 THEN parse_json(json) ELSE parse_json(other) END",
    "COALESCE(try_parse_json(json), parse_json(other))"])
def test_parse_json_lazy_branch_evaluation(expression):
    def query(spark):
        if expression.startswith('COALESCE'):
            rows = [(0, '{"a":1}', '{'), (1, '{', '{"a":2}')]
        else:
            rows = [(0, '{"a":1}', '{'), (1, '{', '{"a":2}')]
        return spark.createDataFrame(rows, 'id LONG, json STRING, other STRING').selectExpr(
            'id', expression + ' AS v')
    _assert_variant_bytes(query)


@allow_non_gpu('RDDScanExec')
@incompat
@pytest.mark.parametrize('key_count', [2, 34])
def test_parse_json_unicode_then_native_extraction(key_count):
    # CPU parser replay retains legacy UTF-16 key order. Current GPU paths are ASCII only:
    # their comparison signs are identical for UTF-8 and UTF-16, even with Unicode dictionary
    # keys present. Keep this safe boundary covered, including 32+ keys.
    keys = {'\ue000': 11, '\U00010000': 22}
    keys.update({f'k{i}': i for i in range(key_count - 2)})
    document = json.dumps(keys, ensure_ascii=False)
    path = '$.k0' if key_count > 2 else '$.missing'
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _input(spark, [document]).selectExpr('parse_json(json) AS v')
            .selectExpr(f"try_variant_get(v, '{path}', 'bigint') AS x"),
        exist_classes='GpuParseJson,GpuVariantGet', conf=_conf)


@allow_non_gpu('RDDScanExec')
@incompat
@pytest.mark.parametrize('document', ['{"\\uD800":1,"a":2}',
    '{"a":1,"\\uD800":2,"b":3,"z":4}', '{"a":1,"b\\uD800":2,"b_a":3,"z":4}'])
def test_parse_json_surrogate_dictionary_then_ascii_extraction(document):
    # Legacy Spark sorts logical keys using UTF-16 before encoding a lone surrogate as '?'.
    # The encoded object can therefore be unsorted even for ASCII lookup: ['a', '?'].
    # Parser CPU replay alone does not fix the downstream cuDF binary-search assumption.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _input(spark, [document]).selectExpr('parse_json(json) AS v').selectExpr(
            "try_variant_get(v, '$.a', 'bigint') AS a",
            "try_variant_get(v, '$.b_a', 'bigint') AS b_a",
            "try_variant_get(v, '$.z', 'bigint') AS z"),
        exist_classes='GpuParseJson,GpuVariantGet', conf=_conf)


@allow_non_gpu('RDDScanExec', 'VariantGet')
@incompat
def test_parse_json_surrogate_dictionary_question_mark_path():
    # This non-identifier path is intentionally evaluated through the existing CPU bridge.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _input(spark, ['{"\\uD800":1,"?":2}']).selectExpr(
            'parse_json(json) AS v').selectExpr("try_variant_get(v, '$.?', 'bigint') AS x"),
        exist_classes='GpuParseJson', non_exist_classes='GpuVariantGet', conf=_conf)


@incompat
@pytest.mark.parametrize('document, fallback', [
    ('{"a":2,"é":1,"𐀀":3,"\ue000":4}', False),
    ('{"\\uD800":1,"a":2}', True),
    ('{"?":1,"a":2}', True)])
def test_variant_metadata_guard_preserves_native_unicode_ascii_paths(
        spark_tmp_path, document, fallback):
    path = spark_tmp_path + '/METADATA_GUARD'
    with_cpu_session(lambda spark: _input(spark, [document]).selectExpr('parse_json(json) AS v')
        .write.parquet(path), conf={**_conf, 'spark.sql.variant.writeShredding.enabled': 'false'})

    def check_plan(cpu_plan, gpu_plan):
        bridge_time = _cpu_bridge_time(gpu_plan)
        if fallback:
            assert bridge_time > 0, 'Expected guarded metadata extraction to replay on CPU'
        else:
            assert bridge_time == 0, 'Valid Unicode dictionary must not penalize ASCII extraction'

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.parquet(path).selectExpr(
            "try_variant_get(v, '$.a', 'bigint') AS a"),
        exist_classes='GpuVariantGet', conf={**_conf,
            'spark.sql.variant.allowReadingShredded': 'false',
            'spark.sql.variant.pushVariantIntoScan': 'false'}, gpu_plan_assertion=check_plan)


@allow_non_gpu('RDDScanExec')
def test_parse_json_large_row_guard():
    # Production Variant limits are 128 MiB, but SPARK_TESTING lowers them to 16 MiB.
    # Force the production 16 MiB preflight boundary regardless of SPARK_TESTING. Tolerant
    # parsing yields the CPU result: either a valid Variant or SQL null under the test limit.
    document = '"' + 'x' * (17 * 1024 * 1024) + '"'
    _assert_variant_bytes(lambda spark: _input(spark, [document]).selectExpr(
        'id', 'try_parse_json(json) AS v'), fallback=True)


@allow_non_gpu('RDDScanExec')
@pytest.mark.parametrize('function', ['parse_json', 'try_parse_json'])
def test_parse_json_invalid_utf8_input(function):
    def query(spark):
        return spark.createDataFrame([(0, bytearray(b'{"a":"\xff"}'))],
            'id LONG, bytes BINARY').selectExpr('id', 'CAST(bytes AS STRING) AS json')\
            .selectExpr('id', f'{function}(json) AS v')
    _assert_variant_bytes(query, fallback=True)


@allow_non_gpu('RDDScanExec')
def test_parse_json_empty_input():
    _assert_variant_bytes(lambda spark: _input(spark, []).selectExpr(
        'id', 'parse_json(json) AS v'))


@incompat
def test_parse_json_parquet_extract_filter_aggregate(spark_tmp_path):
    path = spark_tmp_path + '/JSON_STRINGS'
    documents = ['{"region":"west","total":10,"items":[{"sku":"a"}]}',
                 '{"region":"west","total":20,"items":[{"sku":"b"}]}',
                 '{"region":"east","total":7,"items":[]}', None]
    with_cpu_session(lambda spark: _input(spark, documents).write.parquet(path))

    def query(spark):
        return spark.read.parquet(path).selectExpr('parse_json(json) AS v').selectExpr(
            "try_variant_get(v, '$.region', 'string') AS region",
            "try_variant_get(v, '$.total', 'bigint') AS total",
            "try_variant_get(v, '$.items[0].sku', 'string') AS sku").where('sku IS NOT NULL')\
            .groupBy('region').sum('total')

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, exist_classes='GpuParseJson,GpuVariantGet,GpuHashAggregateExec', conf=_conf,
        gpu_plan_assertion=lambda cpu_plan, gpu_plan: _assert_no_cpu_bridge(gpu_plan))


def _assert_no_cpu_bridge(plan):
    assert _cpu_bridge_time(plan) == 0, 'Expected the complete native pipeline without CPU replay'
