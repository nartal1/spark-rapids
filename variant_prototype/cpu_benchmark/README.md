# CPU Baseline Benchmark for Variant

A PySpark harness that mirrors the libcudf `variant-extraction-gpu` NVBench
design, so we can produce an apples-to-apples CPU-vs-GPU comparison to hand
back to the libcudf team as feedback on
[`libcudf_variant_feedback.md §6.2`](../../libcudf_variant_feedback.md#62-missing-benchmark-dimensions-asks).

## Prerequisites

- **Spark 4.0+** (Variant type was introduced in Spark 4.0 — 3.x will not work).
- **Java 17+** (required by Spark 4.0).
- **Python 3.8+** with `pyspark>=4.0.0`:

```bash
pip install 'pyspark>=4.0.0'
```

If you do not have Spark 4.0 available locally, two options:

- Use a Docker image with Spark 4.0 pre-installed (e.g., `apache/spark:4.0.0`).
- Run against a Spark 4.0 cluster — override `--master` via `$SPARK_MASTER_URL`
  and the script's `.master('local[*]')` line.

## Running

```bash
# Full matrix (5 scenarios × 4 row counts × 2 positions = 40 runs, ~10–30 min
# on a laptop depending on CPU)
python variant_cpu_benchmark.py

# Quick smoke test — 8 runs, ~1 minute
python variant_cpu_benchmark.py --rows 32768,2097152 --scenarios uniform_small,uniform_large --iters 3

# Write the result table to a file
python variant_cpu_benchmark.py --out results.md
```

Progress is logged to stderr; the markdown results table is written to stdout
(and optionally to `--out <file>`).

## What the harness measures

**Mirrors libcudf's benchmark design from
[`cpp/benchmarks/io/variant_extract.cu`](https://github.com/vuule/cudf/blob/variant-extraction-gpu/cpp/benchmarks/io/variant_extract.cu):**

- Five scenarios:
  - `uniform_small` — 5-key dictionary, 5 fields per row, uniform
  - `uniform_large` — 50-key dictionary, 50 fields per row, uniform
  - `skewed_field_count` — 50-key dictionary; even rows have 5 fields, odd rows have 50
  - `skewed_dict_size` — even rows use a 5-key dictionary, odd rows use a 100-key dictionary
  - `half_missing` — 20-key dictionary; target key is present on even rows, absent on odd
- Row counts: 2^15, 2^17, 2^19, 2^21 (32K, 128K, 512K, 2M)
- Key positions: `first` and `last` (probes best- and worst-case dictionary scan)
- Variable-length dictionary keys drawn from length groups {3, 6, 10, 15, 21} bytes,
  matching libcudf's design for exercising the length-prefix early-exit path.

**Methodology:**

1. Data is generated on workers via `spark.range(N).withColumn('json_str', ...)` — so
   driver doesn't serialize millions of strings.
2. The JSON is parsed to Variant (`parse_json`) and the resulting column is `.cache()`-d
   and forced with `.count()`. This matches libcudf's methodology of "bytes already on
   GPU before timer starts."
3. A warmup iteration primes the JIT and Spark's scheduler.
4. Then `--iters` iterations of `try_variant_get(v, '$.<target>', 'int')` are run
   against a `noop` write sink (forces action without disk IO). Per-iteration wall
   time is recorded; the script reports median and min.
5. Between scenarios, the cached Variant column is unpersisted.

**Why `try_variant_get` and not `variant_get`**: libcudf returns null on type
mismatch, which matches Spark's `try_` semantics exactly. Using strict
`variant_get` would conflate benchmark results with host-side throw handling.

## Output format

The script prints a markdown table with CPU median ms, the corresponding GPU ms
from the libcudf report (§5.1, A100 80GB), and a speedup multiplier. Example
(numbers illustrative):

```
| num_rows | scenario             | key_pos | CPU ms (median) | GPU ms (A100) | speedup |
|----------|----------------------|---------|-----------------|---------------|---------|
|   32,768 | uniform_small        | first   |           150.2 |         0.223 |    674× |
|   32,768 | uniform_large        | first   |           220.5 |         0.292 |    755× |
|2,097,152 | uniform_large        | last    |          3825.0 |        10.658 |    359× |
|2,097,152 | half_missing         | last    |          1520.0 |         3.291 |    462× |
```

This table is designed to be dropped into `libcudf_variant_feedback.md §6.3`
once real numbers are in.

## Caveats worth noting when you share results

1. **CPU vs GPU methodology are not identical.** Spark measures end-to-end
   query time including task scheduling, serialization, and writer overhead.
   NVBench measures only the kernel. CPU numbers will include overhead that
   GPU numbers don't, so the speedup is an overestimate vs. "pure compute"
   — but it's the ratio a Spark user actually sees end-to-end, which is what
   matters for the GPU value proposition.
2. **Cast semantics gap (§3.6).** CPU Spark returns the integer (e.g. 0)
   under `try_variant_get(..., 'int')` because Spark casts int8/int16/etc.
   to int32. cuDF's `cast_variant(INT32)` on the same bytes returns NULL.
   So our CPU benchmark will run to completion while GPU would fall back
   to CPU in production. Reflect this in the writeup: the CPU numbers
   we're measuring are *the thing the GPU should be beating, once cast
   semantics are in*.
3. **`skewed_dict_size` is an approximation.** libcudf swaps the dict
   subset per row; Spark builds the dict from fields present in each row.
   The dictionary sizes match but the keys may differ. Acceptable for
   order-of-magnitude comparison.
4. **`half_missing` is an approximation.** On odd rows libcudf keeps a
   20-key dict but omits one field; Spark builds a 19-key dict. The
   dict-lookup cost on odd rows differs by a small amount but the
   "target present / absent" workload shape is preserved.
5. **Cold-start JIT.** The first iteration after cache is typically 2–3×
   slower than steady state on JVM. The harness does one warmup iteration
   to counter this; if you see high variance, raise `--iters` to 10.

## Integrating results into the feedback doc

After running, the markdown table produced by the script can be pasted into
[`libcudf_variant_feedback.md`](../../libcudf_variant_feedback.md) as a new
subsection under §6.3 (e.g., §6.3.1 "Measured CPU baseline (PySpark 4.0,
<your hardware>)"). Include your hardware / Spark config alongside — the
numbers only mean something with the setup disclosed.

## Next steps if the CPU/GPU gap is meaningful

If the median speedup across scenarios comes out at 100× or higher, that
strengthens the case for `parse_json` on GPU (§2.4) and for the target-type
expansion (§2.1) — since the whole variant_get pipeline lifts up. Numbers
will give us a concrete pitch to management for prioritizing the GPU work.
