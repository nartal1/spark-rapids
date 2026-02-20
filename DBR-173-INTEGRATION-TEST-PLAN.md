# Plan: Run DBR 17.3 Integration Tests File-by-File and Track Failures

## Context

The DBR 17.3 shim (`spark400db173`) now builds successfully after the `ProxyRapidsShuffleManagerShim` refactoring. The next step is to validate correctness by running integration tests against the DBR 17.3 runtime. Since this is a new shim, we don't know how many tests will fail, so we need a systematic approach: run tests **file by file**, capture failures, and create GitHub issue templates for each failing test file.

## How Tests Work

### Test Infrastructure
- **Entry point**: `./jenkins/databricks/test.sh` -- sources `setup.sh` (installs deps) and `common_vars.sh` (sets `SPARK_HOME`, `SPARK_SHIM_VER`, etc.), then calls `run_pyspark_from_build.sh`
- **Test runner**: `integration_tests/run_pyspark_from_build.sh` -- sets up Spark config, determines parallelism, runs `runtests.py` (which is just a thin wrapper around `pytest`)
- **`TESTS` env var**: Space-separated list of test file names (e.g., `TESTS="aqe_test.py"`) -- passed through to pytest to limit which files run
- **`TEST_PARALLEL` env var**: Controls pytest-xdist parallelism (number of parallel workers WITHIN a single run). Auto-detected if not set. `0` or `1` = no parallelism, `2+` = xdist with `-n N` workers. Capped at `MAX_PARALLEL` (default 8).
- **`WITH_DEFAULT_UPSTREAM_SHIM=0`**: Required for DBR 17.3 to skip the two-shim smoke test

### Run Command Pattern
```bash
TESTS=<file>.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
```

### TEST_PARALLEL Guidance
- `TEST_PARALLEL` controls **within-file** parallelism (how many tests from one file run concurrently via pytest-xdist)
- Each parallel worker gets its own Spark session, consuming ~1.5 GiB GPU memory + 750 MiB overhead + 8 GiB host memory + 1 CPU core
- **Recommendation: Do NOT set it.** If unset, `run_pyspark_from_build.sh` auto-detects the optimal value from GPU memory, CPU cores, and host memory (capped at `MAX_PARALLEL=8`). This is the safest approach.
- Override only if auto-detection fails (e.g., `nvidia-smi` unavailable): `TEST_PARALLEL=4` is a safe manual fallback
- Setting `TEST_PARALLEL=0` runs tests sequentially (safest, slowest) -- useful for debugging or if parallel runs fail with OOM

## Test File Categories

### Category 1: Standard Tests (75 files)
Run with the base command. These are the bulk of tests:

```
aqe_test.py, arithmetic_ops_test.py, array_test.py, ast_test.py, assert_in_tests_test.py,
avro_test.py, cache_test.py, cast_test.py, cmp_test.py, col_size_exceeding_cudf_limit_test.py,
collection_ops_test.py, conditionals_test.py, csv_test.py, datasourcev2_read_test.py,
datasourcev2_write_test.py, date_time_test.py, decimal_precision_over_max_test.py, dpp_test.py,
expand_exec_test.py, explain_mode_test.py, explain_test.py, generate_expr_test.py,
get_json_test.py, grouping_sets_test.py, hash_aggregate_test.py, hashing_test.py,
higher_order_functions_test.py, hive_delimited_text_test.py, hive_parquet_write_test.py,
hive_write_test.py, hybrid_parquet_test.py, hyper_log_log_plus_plus_test.py, inset_test.py,
join_test.py, json_fuzz_test.py, json_matrix_test.py, json_test.py, json_tuple_test.py,
kudo_dump_test.py, limit_test.py, logic_test.py, map_test.py, misc_expr_test.py, misc_test.py,
orc_cast_test.py, orc_test.py, orc_write_test.py, parquet_pyarrow_test.py, parquet_test.py,
parquet_testing_test.py, parquet_write_test.py, project_lit_alias_test.py,
project_presplit_test.py, prune_partition_column_test.py, qa_nightly_select_test.py,
rand_test.py, range_test.py, regexp_no_unicode_test.py, regexp_test.py, repart_test.py,
row_conversion_test.py, row-based_udf_test.py, sample_test.py, scan_default_values_test.py,
schema_evolution_test.py, sort_test.py, string_test.py, string_type_test.py, struct_test.py,
subquery_test.py, time_window_test.py, udf_test.py, url_test.py, window_function_test.py
```

### Category 2: Delta Lake Tests (12 files) -- DEFERRED
These have `@delta_lake` markers and are **skipped** unless `--delta_lake` is passed to pytest. Will be run as a separate pass after standard tests.

### Category 3: Special Tests -- SKIPPED
- `udf_cudf_test.py` -- needs `--cudf_udf` flag and cudf UDF runtime
- `mortgage_test.py` -- needs external mortgage dataset
- `fastparquet_compatibility_test.py` -- needs fastparquet library
- `iceberg/*_test.py` (11 files) -- needs `--iceberg` flag and Iceberg runtime

## How to Run

### Quick: Run a single test file
```bash
TESTS=aqe_test.py WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
```

### Full: Run all 75 standard test files
```bash
./run_dbr173_tests.sh
```

The runner script iterates over all 75 files, captures logs to `integration_tests/dbr173_test_results/`, and prints a summary at the end.

### Generate GitHub issues for failures
```bash
./generate_dbr173_issues.sh
```

This reads the test logs and generates one markdown issue file per failing test file in `integration_tests/dbr173_test_results/issues/`.

## Resume Instructions

If you need to resume after a partial run:
1. The runner script tracks completed files in `integration_tests/dbr173_test_results/completed.txt`
2. Re-running `./run_dbr173_tests.sh` will skip already-completed files
3. To force re-run a specific file: delete its `.log` file from the results directory and re-run
