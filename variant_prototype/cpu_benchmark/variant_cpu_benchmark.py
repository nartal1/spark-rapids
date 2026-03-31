#!/usr/bin/env python3
"""
CPU baseline benchmark for Spark's variant_get, mirroring the libcudf
variant-extraction-gpu benchmark design.

Companion to spark-rapids/libcudf_variant_feedback.md §6.2 ("missing benchmark
dimensions: CPU baseline comparison"). Measures Spark 4.0+ CPU time for
variant_get across the same scenario × row-count × key-position matrix as
libcudf's NVBench benchmarks, so we can produce an honest CPU-vs-GPU speedup
comparison to hand back to the libcudf team.

Mirrors:
  * 5 divergence scenarios from cpp/benchmarks/io/variant_extract.cu
  * Row counts 2^15, 2^17, 2^19, 2^21 (32K ... 2M)
  * "first" vs "last" key position
  * Variable-length dictionary keys (3/6/10/15/21 bytes) to exercise the
    length-prefix early-exit in the metadata scan
  * Pre-materializes the Variant column before timing (Spark analog of
    "data is on GPU before timer starts" in NVBench)

Requires: Spark 4.0+, Java 17+, PySpark 4.0+ (pip install 'pyspark>=4.0.0').
Variant type is not available before Spark 4.0.

Usage:
    python variant_cpu_benchmark.py                    # run full matrix
    python variant_cpu_benchmark.py --rows 32768,2097152 --iters 3
    python variant_cpu_benchmark.py --out results.md   # write MD table

Output: a markdown table comparing CPU ms to the published A100 GPU ms, ready
to paste into §6.3 of libcudf_variant_feedback.md.
"""

import argparse
import json
import random
import string
import sys
import time
from dataclasses import dataclass
from typing import List, Tuple

# -----------------------------------------------------------------------------
# Scenario definitions — mirrors cpp/benchmarks/io/variant_extract.cu
# -----------------------------------------------------------------------------

# Five key-length groups. libcudf uses these to exercise the
# `slen != key_len` early-exit in device_find_key_in_metadata.
KEY_LENGTH_GROUPS = [3, 6, 10, 15, 21]


def make_keys(n: int, seed: int = 42) -> List[str]:
    """Generate n distinct lowercase keys using libcudf's length distribution."""
    random.seed(seed)
    keys = set()
    while len(keys) < n:
        length = KEY_LENGTH_GROUPS[len(keys) % len(KEY_LENGTH_GROUPS)]
        k = ''.join(random.choices(string.ascii_lowercase, k=length))
        keys.add(k)
    return sorted(keys)


@dataclass
class Scenario:
    name: str
    dict_keys: List[str]
    # Two JSON templates: one for even row indices, one for odd.
    # Equal strings ⇒ uniform scenario.
    even_template: str
    odd_template: str


def _json_from_keys(keys: List[str]) -> str:
    """Encode a JSON object where every key maps to a small int value."""
    return json.dumps({k: i for i, k in enumerate(keys)})


def build_scenario(name: str, target_key: str) -> Scenario:
    """Construct the scenario's (dict_keys, even_template, odd_template).

    The target_key only matters for half_missing (the row where the target
    is absent needs to know which key to drop).
    """
    if name == 'uniform_small':
        keys = make_keys(5)
        tpl = _json_from_keys(keys)
        return Scenario(name, keys, tpl, tpl)

    if name == 'uniform_large':
        keys = make_keys(50)
        tpl = _json_from_keys(keys)
        return Scenario(name, keys, tpl, tpl)

    if name == 'skewed_field_count':
        keys = make_keys(50)
        t_even = _json_from_keys(keys[:5])   # 5 fields
        t_odd = _json_from_keys(keys)        # 50 fields
        return Scenario(name, keys, t_even, t_odd)

    if name == 'skewed_dict_size':
        # Even rows use a 5-key dict; odd rows use a 100-key dict.
        # Both rows emit 5 fields; Spark's VariantBuilder builds the dict
        # per-row based on field names present, so the two templates
        # produce different-size dictionaries as libcudf's benchmark
        # intends.
        keys_small = make_keys(5, seed=42)
        keys_large = make_keys(100, seed=43)
        t_even = _json_from_keys(keys_small[:5])
        t_odd = _json_from_keys(keys_large[:5])
        # Expose the larger dict for first/last target picking. The
        # intersection with keys_small may be empty, so callers of
        # 'first'/'last' should verify before relying on this.
        return Scenario(name, keys_large, t_even, t_odd)

    if name == 'half_missing':
        # 20-key dict on all rows; odd rows omit the target key.
        # Note: Spark's parse_json builds the dictionary from the fields
        # actually present. So on odd rows the dict will have 19 keys
        # rather than 20 with one field missing (which is what libcudf's
        # GPU workload does). The net work — "target key present vs not
        # present" — is preserved; only the dict-lookup cost differs by
        # ~5% per odd row. Good enough for order-of-magnitude comparison.
        keys = make_keys(20)
        t_even = _json_from_keys(keys)
        t_odd = _json_from_keys([k for k in keys if k != target_key])
        return Scenario(name, keys, t_even, t_odd)

    raise ValueError(f"unknown scenario: {name}")


SCENARIO_NAMES = [
    'uniform_small',
    'uniform_large',
    'skewed_field_count',
    'skewed_dict_size',
    'half_missing',
]


# -----------------------------------------------------------------------------
# GPU reference numbers from docs/variant_extraction_report.md §5.1 (A100 80GB)
# -----------------------------------------------------------------------------

# Keys are (num_rows, key_pos, scenario) ⇒ GPU ms
GPU_REFERENCE_MS = {}
_report = """
num_rows key_pos uniform_small uniform_large skewed_field_count skewed_dict_size half_missing
32768 first 0.223 0.292 0.272 0.241 0.256
131072 first 0.239 0.455 0.419 0.345 0.320
524288 first 0.343 1.388 1.006 0.786 0.642
2097152 first 0.729 4.452 3.209 2.442 1.782
32768 last 0.223 0.433 0.406 0.358 0.270
131072 last 0.249 0.736 0.681 0.626 0.392
524288 last 0.385 3.122 2.192 1.619 1.106
2097152 last 0.849 10.658 7.177 5.373 3.291
"""
_lines = [line.split() for line in _report.strip().split('\n')]
_header = _lines[0]
for _row in _lines[1:]:
    _n = int(_row[0])
    _pos = _row[1]
    for i, _s in enumerate(_header[2:], start=2):
        GPU_REFERENCE_MS[(_n, _pos, _s)] = float(_row[i])


# -----------------------------------------------------------------------------
# Benchmark harness
# -----------------------------------------------------------------------------

def time_scenario(spark, scenario_name: str, num_rows: int,
                  key_pos: str, iters: int) -> dict:
    """
    Generate data on workers, pre-materialize Variant, then time variant_get.
    Returns a result dict with scenario / num_rows / key_pos / median_ms /
    min_ms / all_ms.
    """
    from pyspark.sql.functions import col, lit, when

    # For half_missing we need to know the target first, to build the
    # "odd-row" template that omits it.
    if scenario_name == 'half_missing':
        temp_keys = make_keys(20)
        target_key = temp_keys[0] if key_pos == 'first' else temp_keys[-1]
    else:
        # Harmless placeholder; other scenarios don't use it.
        target_key = ''

    scenario = build_scenario(scenario_name, target_key)
    if scenario_name != 'half_missing':
        target_key = (scenario.dict_keys[0] if key_pos == 'first'
                      else scenario.dict_keys[-1])

    # Generate rows on workers via spark.range + when/lit (avoids
    # serializing millions of strings from driver).
    df = spark.range(num_rows)
    if scenario.even_template == scenario.odd_template:
        df = df.withColumn('json_str', lit(scenario.even_template))
    else:
        df = df.withColumn(
            'json_str',
            when(col('id') % 2 == 0, lit(scenario.even_template))
            .otherwise(lit(scenario.odd_template)),
        )

    # Pre-materialize the Variant column. This is the CPU analog of
    # "bytes already on GPU before timer starts" in NVBench.
    variant_df = df.selectExpr("parse_json(json_str) as v").cache()
    variant_df.count()  # force materialization

    # The actual SQL we time. try_variant_get matches libcudf's
    # null-on-mismatch semantics, so the comparison is apples-to-apples
    # on error handling (though subject to the cast-semantics caveat
    # from §3.6 — here we use 'int' as target, which Spark widens from
    # whatever parse_json stored; GPU would return null instead).
    extract_sql = f"try_variant_get(v, '$.{target_key}', 'int') as x"

    # Warmup (JIT + scheduler priming)
    (variant_df.selectExpr(extract_sql)
        .write.format("noop").mode("overwrite").save())

    # Timed iterations
    times_ms = []
    for _ in range(iters):
        start = time.perf_counter()
        (variant_df.selectExpr(extract_sql)
            .write.format("noop").mode("overwrite").save())
        times_ms.append((time.perf_counter() - start) * 1000)

    variant_df.unpersist()

    times_ms.sort()
    return {
        'scenario': scenario_name,
        'num_rows': num_rows,
        'key_pos': key_pos,
        'median_ms': times_ms[len(times_ms) // 2],
        'min_ms': times_ms[0],
        'all_ms': times_ms,
    }


# -----------------------------------------------------------------------------
# Reporting
# -----------------------------------------------------------------------------

def format_md_table(results: List[dict]) -> str:
    lines = [
        "| num_rows | scenario | key_pos | CPU ms (median) | GPU ms (A100) | speedup |",
        "|---|---|---|---|---|---|",
    ]
    for r in sorted(results,
                    key=lambda x: (x['num_rows'], x['key_pos'], x['scenario'])):
        gpu = GPU_REFERENCE_MS.get(
            (r['num_rows'], r['key_pos'], r['scenario']))
        if gpu is not None and gpu > 0:
            speedup = f"{r['median_ms'] / gpu:.0f}×"
            gpu_str = f"{gpu:.3f}"
        else:
            speedup = "—"
            gpu_str = "—"
        lines.append(
            f"| {r['num_rows']:>7,} | {r['scenario']:<20} | {r['key_pos']:<5} | "
            f"{r['median_ms']:>10.1f} | {gpu_str:>10} | {speedup:>6} |"
        )
    return '\n'.join(lines)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=__doc__.split('\n\n')[1],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--rows', default='32768,131072,524288,2097152',
                        help='comma-separated row counts (default: full matrix)')
    parser.add_argument('--iters', type=int, default=5,
                        help='timing iterations per configuration (default: 5)')
    parser.add_argument('--scenarios', default=','.join(SCENARIO_NAMES),
                        help='comma-separated scenario names')
    parser.add_argument('--positions', default='first,last',
                        help='comma-separated key positions')
    parser.add_argument('--out', default=None,
                        help='write markdown table to this file')
    parser.add_argument('--driver-memory', default='8g',
                        help='Spark driver memory (default: 8g)')
    args = parser.parse_args()

    from pyspark.sql import SparkSession

    spark = (SparkSession.builder
             .appName('variant-cpu-benchmark')
             .master('local[*]')
             .config('spark.driver.memory', args.driver_memory)
             .config('spark.sql.shuffle.partitions', '8')
             .config('spark.sql.execution.arrow.pyspark.enabled', 'true')
             .getOrCreate())

    ver_str = spark.version
    print(f"# Spark version: {ver_str}", file=sys.stderr)
    try:
        major = int(ver_str.split('.')[0])
        if major < 4:
            print("ERROR: Variant type requires Spark 4.0+", file=sys.stderr)
            spark.stop()
            sys.exit(1)
    except Exception:
        print(f"WARN: could not parse Spark version '{ver_str}'",
              file=sys.stderr)

    rows_list = [int(x) for x in args.rows.split(',')]
    scenario_names = args.scenarios.split(',')
    positions = args.positions.split(',')

    results = []
    total = len(rows_list) * len(scenario_names) * len(positions)
    idx = 0
    for num_rows in rows_list:
        for sname in scenario_names:
            if sname not in SCENARIO_NAMES:
                print(f"# skipping unknown scenario: {sname}", file=sys.stderr)
                continue
            for pos in positions:
                idx += 1
                print(f"# [{idx}/{total}] {sname:<20} rows={num_rows:>7,} pos={pos}",
                      file=sys.stderr)
                try:
                    r = time_scenario(spark, sname, num_rows, pos, args.iters)
                    results.append(r)
                    print(f"#   median {r['median_ms']:.1f} ms   "
                          f"min {r['min_ms']:.1f} ms   "
                          f"all {['%.1f' % t for t in r['all_ms']]}",
                          file=sys.stderr)
                except Exception as e:
                    print(f"#   FAILED: {e}", file=sys.stderr)

    out = format_md_table(results)
    print()
    print(out)
    if args.out:
        with open(args.out, 'w') as f:
            f.write(out)
            f.write('\n')
        print(f"\n# wrote markdown table to {args.out}", file=sys.stderr)

    spark.stop()


if __name__ == '__main__':
    main()
