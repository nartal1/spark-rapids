# Databricks 18.3 Integration-Test Progress and Failures

This is a cumulative checkpoint of the Databricks 18.3 integration run. It is regenerated from the available logs on each checkpoint so failures and completed test files are not duplicated.

## Snapshot

- Snapshot time: 2026-09-24 04:45:22 UTC
- Runtime: Databricks 18.3, Spark 4.1.0, Java 21
- Shim: `spark410db183`
- Test type: `nightly`
- Current public test checkout and loaded plugin revision: `cf54d1b4bd735ffc459ad865ea56d2562f8a1f92` (`db_18_support`)
- Current private shim checkout revision: `15bb08c0fc5f4177496191bbb808c760d4caaac4` (`db_18_support`)
- Recovery build state: the private two-module Databricks build completed successfully at 18:31 UTC; the public 20-module Databricks build completed successfully at 18:44 UTC and produced the DBR 18.3 distribution and integration-test JARs.
- First-run state: complete; PID `569029` exited after the PyArrow phase.
- First clean main/cache continuation state: three files completed under PID `874466`; its incomplete `arithmetic_ops_test.py` evidence is retained as diagnostic history but is superseded by the complete rerun below.
- Recovered continuation state at this bounded capture: unavailable for verification. Two bounded SSH attempts failed with `Network is unreachable`; the last verified state at 04:12:39 UTC was active under PID `28189`, one Python file at a time against exact checkout `cf54d1b4b` with `TEST_PARALLEL=0`.
- First-run evidence directory: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z`
- First clean main/cache evidence directory: `/home/ubuntu/spark-rapids/db18-clean-main-cache-results/20260923T072607Z`
- Recovered clean main/cache evidence directory: `/home/ubuntu/cudf-spark/db18-clean-main-cache-results/20260923T184559Z`
- Clean-main marker expression: `not delta_lake and not shuffle_test and not pyarrow_test`; the 15 completed Delta files and `parquet_pyarrow_test.py` are omitted from the file plan, while non-shuffle coverage in shared files remains eligible.
- Delta Lake random seed: `1790135081` (the invalid main/cache attempts used different seeds)
- Multithreaded-shuffle random seed: `1790144460`
- PyArrow random seed: `1790144768`
- Recovered arithmetic random seed: `1790189207`
- Recovered AST random seed: `1790192277`
- Recovered collection-operations random seed: `1790194588`
- Recovered datasource-v2 read random seed: `1790197601`

## Phase status

| Phase | Status | Evidence at this checkpoint |
| --- | --- | --- |
| Setup | Complete, passed | Exit 0; 03:41:32–03:41:59 UTC |
| Environment initialization | Complete, passed | Exit 0; DBR 18.3 / Spark 4.1.0 / `spark410db183` validated |
| Main | Invalid collection attempt; full rerun required | Exit 2; 32,513 items found but collection stopped on the fixed `udf_test.py` `NameError` |
| Cache | Invalid collection attempt; full rerun required | Exit 2; 615 selected, but collection stopped on the same fixed `NameError` |
| Delta Lake | Complete, failed | Exit 1; 03:44:33–06:20:39 UTC; all 947 selected tests reached terminal outcomes, with 30 failures |
| Multithreaded shuffle | Complete, passed | Exit 0; 06:20:39–06:26:00 UTC; all 85 selected tests passed |
| PyArrow | Complete, passed | Exit 0; 06:26:00–06:54:48 UTC; all 144 selected tests reached terminal outcomes with no failures |
| Private recovery build | Complete, passed | Exact private revision `15bb08c0f`; DBR 18.3 private shim JAR installed locally |
| Public recovery build | Complete, passed | Exact public revision `cf54d1b4b`; all 20 reactor modules succeeded |
| Clean main continuation | State unknown; host unreachable | 92 Python files cumulatively planned; 39 complete. `join_test.py` was active at the last verified capture; it and 52 later files have no new verifiable terminal evidence at this checkpoint |
| Clean cache continuation | Pending | Runs `cache_test.py` with the cache serializer after clean main finishes |

## Cumulative terminal outcome counts

These counts include completed Delta, multithreaded-shuffle, PyArrow, and clean-main files. The invalid initial main/cache collection attempts and the superseded incomplete arithmetic attempt are excluded.

| Outcome | Count |
| --- | ---: |
| Passed | 12,059 |
| Failed | 42 |
| Skipped | 767 |
| Expected failure (`XFAIL`) | 516 |
| Unexpected pass (`XPASS`) | 80 |
| Total observed | 13,464 |

## Delta Lake terminal outcome counts

These counts are from the terminal pytest summary and JUnit XML.

| Outcome | Count |
| --- | ---: |
| Passed | 567 |
| Failed | 30 |
| Skipped | 188 |
| Expected failure (`XFAIL`) | 131 |
| Unexpected pass (`XPASS`) | 31 |
| Total observed | 947 |

## Delta Lake failures

The descriptions below are based on the terminal pytest summary and JUnit XML.

1. `delta_lake_liquid_clustering_test.py::test_delta_rtas_sql_liquid_clustering`
   - GPU override raises `IllegalArgumentException` because `OverwriteByExpressionExecV1` is not columnar.
2. `delta_lake_liquid_clustering_test.py::test_delta_append_sql_liquid_clustering`
   - GPU override raises `IllegalArgumentException` because `DataWritingCommandExec` is not columnar.
3. `delta_lake_liquid_clustering_test.py::test_delta_insert_overwrite_static_sql_liquid_clustering`
   - GPU override raises the same non-columnar `DataWritingCommandExec` exception.
4. `delta_lake_liquid_clustering_test.py::test_delta_append_df_liquid_clustering`
   - GPU override raises the same non-columnar `DataWritingCommandExec` exception.
5. `delta_lake_liquid_clustering_test.py::test_delta_insert_overwrite_df_liquid_clustering[overwrite_mode=STATIC]`
   - GPU override raises the same non-columnar `DataWritingCommandExec` exception.
6. `delta_lake_merge_test.py::test_delta_merge_not_matched_by_source_schema_evolution_db173`
   - The GPU merge job aborts in `GpuRapidsProcessDeltaMergeJoinIterator` with a cuDF concatenate type mismatch (`Type mismatch in columns to concatenate`).
7. `delta_lake_optimize_table_test.py::test_delta_optimize_clustered_table[False]`
   - CPU/GPU Delta-log parity fails at the `add` action because the generated `LIQUID_METADATA_ID` tag values differ.
8. `delta_lake_optimize_table_test.py::test_delta_optimize_clustered_table[True]`
   - The deletion-vector variant has the same CPU/GPU `LIQUID_METADATA_ID` mismatch.
9. `delta_lake_optimize_table_test.py::test_delta_optimize_clustered_table_gpu_write`
   - CPU/GPU Delta-log parity fails because the `add.tags.LIQUID_METADATA_ID` values differ.
10. `delta_lake_optimize_table_test.py::test_delta_optimize_clustered_table_gpu_write_with_deletion_vectors`
    - The deletion-vector GPU-write variant has the same `LIQUID_METADATA_ID` parity mismatch.
11. `delta_lake_optimize_table_test.py::test_delta_optimize_clustered_table_gpu_write_repeated`
    - Repeated OPTIMIZE fails CPU/GPU Delta-log parity on differing `LIQUID_METADATA_ID` values.
12. `delta_lake_optimize_table_test.py::test_delta_optimize_clustered_row_tracking_table_gpu_write`
    - Row-tracking GPU-write coverage fails CPU/GPU Delta-log parity on differing `LIQUID_METADATA_ID` values.
13. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[False-dv_default-no_mapping]`
    - `GpuOverrideUtil` aborts on a non-columnar `OverwriteByExpressionExecV1` while applying GPU overrides.
14. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[False-dv_false-no_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
15. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[False-dv_true-no_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
16. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[False-dv_default-name_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
17. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[False-dv_default-id_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
18. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[True-dv_default-no_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
19. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[True-dv_false-no_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
20. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[True-dv_true-no_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
21. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[True-dv_default-name_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
22. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[True-dv_default-id_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
23. `delta_lake_write_test.py::test_delta_db173_native_sql_ctas_rtas[optimize_off-aqe_off]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
24. `delta_lake_write_test.py::test_delta_db173_native_sql_ctas_rtas[optimize_off-aqe_on]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
25. `delta_lake_write_test.py::test_delta_db173_native_sql_ctas_rtas[optimize_on-aqe_on]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
26. `delta_lake_write_test.py::test_delta_db173_native_sql_ctas_rtas_legacy_optimized_write`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
27. `delta_lake_write_test.py::test_delta_db173_native_sql_ctas_rtas_empty_input`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
28. `delta_lake_write_test.py::test_delta_db173_native_sql_ctas_rtas_existing_table_branches`
    - The existing-table branch first reports Hive incompatible-column types, then the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
29. `delta_lake_write_test.py::test_delta_rtas_truncate_capability`
    - The RTAS path reports Hive incompatible-column types and then aborts GPU override on non-columnar `OverwriteByExpressionExecV1`.
30. `delta_lake_write_test.py::test_delta_overwrite_schema_evolution_arrays[False]`
    - CPU/GPU Delta-log `commitInfo.operationParameters` differ: CPU records `canMergeSchema: true`, while GPU omits it.

## Completed Delta Lake Python files

A file is marked complete only after the sequential pytest log advances to a later Python file. Failed, skipped, and expected-failure outcomes still count as executed coverage.

| Python file | Observed outcomes |
| --- | --- |
| `delta_lake_auto_compact_test.py` | 15 passed |
| `delta_lake_catalog_managed_test.py` | 25 skipped |
| `delta_lake_clustered_reads_test.py` | 2 passed |
| `delta_lake_delete_test.py` | 49 passed, 6 skipped, 8 xfailed |
| `delta_lake_liquid_clustering_test.py` | 6 passed, 5 failed, 2 skipped |
| `delta_lake_low_shuffle_merge_test.py` | 62 skipped |
| `delta_lake_merge_test.py` | 124 passed, 1 failed, 19 skipped, 78 xfailed, 4 xpassed |
| `delta_lake_optimize_table_test.py` | 8 passed, 6 failed |
| `delta_lake_reorg_liquid_clustering_test.py` | 2 skipped |
| `delta_lake_reorg_table_test.py` | 9 skipped |
| `delta_lake_test.py` | 213 passed, 51 skipped, 13 xpassed |
| `delta_lake_time_travel_test.py` | 9 passed |
| `delta_lake_update_test.py` | 25 passed, 5 skipped, 20 xfailed, 3 xpassed |
| `delta_lake_write_test.py` | 105 passed, 18 failed, 7 skipped, 25 xfailed, 1 xpassed |
| `delta_zorder_test.py` | 11 passed, 10 xpassed |

Completed Delta-file count: **15**.

## Completed multithreaded-shuffle Python files

| Python file | Observed outcomes |
| --- | --- |
| `hash_aggregate_test.py` | 85 passed |

## Completed PyArrow Python files

| Python file | Observed outcomes |
| --- | --- |
| `parquet_pyarrow_test.py` | 143 passed, 1 xpassed |

## Completed clean-main Python files

| Python file | Observed outcomes |
| --- | --- |
| `allow_non_gpu_conditional_marker_test.py` | 27 passed |
| `allow_non_gpu_conditional_scope_test.py` | 7 passed |
| `aqe_test.py` | 18 passed, 5 skipped |
| `arithmetic_ops_test.py` | 1,429 passed, 4 failed, 37 skipped, 24 xfailed, 1 xpassed |
| `array_test.py` | 660 passed, 73 skipped, 2 xfailed |
| `assert_in_tests_test.py` | 2 passed |
| `asserts_regression_test.py` | 2 passed |
| `ast_test.py` | 182 passed, 2 failed, 1 xpassed |
| `avro_test.py` | 44 skipped |
| `cache_test.py` | 595 passed, 20 skipped |
| `cast_test.py` | 505 passed, 14 skipped, 18 xpassed |
| `cmp_test.py` | 443 passed |
| `collection_ops_test.py` | 344 passed, 5 failed |
| `col_size_exceeding_cudf_limit_test.py` | 72 passed |
| `conditionals_test.py` | 250 passed |
| `cpu_bridge_test.py` | 46 passed, 2 skipped |
| `csv_test.py` | 918 passed, 160 skipped, 40 xfailed, 8 xpassed |
| `data_gen_test.py` | 3 passed |
| `datasourcev2_read_test.py` | 5 passed, 1 failed, 1 skipped |
| `datasourcev2_write_test.py` | 10 passed |
| `date_time_test.py` | 3,006 passed, 3 skipped, 4 xfailed, 4 xpassed |
| `decimal_precision_over_max_test.py` | 1 passed |
| `dpp_test.py` | 105 passed |
| `expand_exec_test.py` | 16 passed |
| `explain_mode_test.py` | 2 passed |
| `explain_test.py` | 6 passed |
| `fastparquet_compatibility_test.py` | 39 passed, 16 xfailed, 8 xpassed |
| `generate_expr_test.py` | 404 passed |
| `get_json_test.py` | 54 passed |
| `grouping_sets_test.py` | 8 passed |
| `group_partitions_test.py` | 4 passed, 2 skipped |
| `hash_aggregate_test.py` | 1,766 passed, 90 skipped, 293 xfailed, 8 xpassed; 85 shuffle-marked tests deselected |
| `hashing_test.py` | 74 passed |
| `higher_order_functions_test.py` | 68 passed |
| `hive_delimited_text_test.py` | 143 passed, 6 xfailed |
| `hive_parquet_write_test.py` | 14 passed, 2 skipped |
| `hive_write_test.py` | 28 passed, 3 skipped |
| `hyper_log_log_plus_plus_test.py` | 123 skipped |
| `inset_test.py` | 8 passed |

## Current clean-main Python file

- `join_test.py`
- It started at 03:35:33 UTC and collected 2,330 tests. It remained active and had reached about 62% at the last successful inspection at 04:12:39 UTC. Two bounded SSH attempts at this checkpoint failed with `Network is unreachable`, so no later progress or terminal state can be verified. All partial outcomes from this file are excluded from the cumulative terminal counts.
- If the runner stops before this file receives a terminal ledger entry, rerun it from the beginning.

### Terminal arithmetic failures

The complete rerun reproduced the same four test parameterizations seen in the interrupted session, under a new random seed. All four are CPU/GPU result mismatches for large-magnitude inverse hyperbolic inputs where the GPU result becomes positive or negative infinity.

1. `arithmetic_ops_test.py::test_acosh[Double][DATAGEN_SEED=1790189207, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
   - GPU returned `inf` where CPU returned `496.1282635955343`.
2. `arithmetic_ops_test.py::test_asinh[Double][DATAGEN_SEED=1790189207, TZ=UTC, APPROXIMATE_FLOAT]`
   - GPU returned `inf` where CPU returned `496.1282635955343`.
3. `arithmetic_ops_test.py::test_asinh[Float][DATAGEN_SEED=1790189207, TZ=UTC, APPROXIMATE_FLOAT]`
   - GPU returned `-inf` where CPU returned `-43.836294594133975`.
4. `arithmetic_ops_test.py::test_asinh[Decimal(12,2)][DATAGEN_SEED=1790189207, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
   - GPU returned `-inf` where CPU returned `-23.631791156016195`.

### Terminal AST failures

Both AST failures are the same inverse-hyperbolic overflow class as the arithmetic failures: GPU produced infinity for a large finite CPU result.

1. `ast_test.py::test_asinh[(Double, False)][DATAGEN_SEED=1790192277, TZ=UTC, APPROXIMATE_FLOAT]`
   - GPU returned `inf` where CPU returned `576.5135014024883`.
2. `ast_test.py::test_acosh[(Double, True)][DATAGEN_SEED=1790192277, TZ=UTC, APPROXIMATE_FLOAT]`
   - GPU returned `inf` where CPU returned `576.5135014024883`.

### Terminal collection-operations failures

All five cases received the expected illegal-sequence exception, but the assertion searched for the legacy text `Illegal sequence boundaries`. DBR 18.3 instead emits the structured error prefix `[ILLEGAL_SEQUENCE_BOUNDARIES]` followed by a detailed step/start/stop explanation.

1. `collection_ops_test.py::test_sequence_illegal_boundaries[Short-Short-Short][DATAGEN_SEED=1790194588, TZ=UTC]`
   - Expected legacy error text was absent from the structured illegal-boundaries exception.
2. `collection_ops_test.py::test_sequence_illegal_boundaries[Long-Long-Long][DATAGEN_SEED=1790194588, TZ=UTC, INJECT_OOM]`
   - Expected legacy error text was absent from the structured illegal-boundaries exception.
3. `collection_ops_test.py::test_sequence_illegal_boundaries[Byte-Byte-Byte][DATAGEN_SEED=1790194588, TZ=UTC, INJECT_OOM]`
   - Expected legacy error text was absent from the structured illegal-boundaries exception.
4. `collection_ops_test.py::test_sequence_illegal_boundaries[Integer-Integer-Integer0][DATAGEN_SEED=1790194588, TZ=UTC, INJECT_OOM]`
   - Expected legacy error text was absent from the structured illegal-boundaries exception.
5. `collection_ops_test.py::test_sequence_illegal_boundaries[Integer-Integer-Integer1][DATAGEN_SEED=1790194588, TZ=UTC, INJECT_OOM]`
   - Expected legacy error text was absent from the structured illegal-boundaries exception.

### Terminal datasource-v2 read failure

1. `datasourcev2_read_test.py::test_arrow_source_pandas_udf[DATAGEN_SEED=1790197601, TZ=UTC, ALLOW_NON_GPU(BatchScanExec)]`
   - Spark cancelled job 20 after the RAPIDS integration-test job reached its 3,600-second timeout. The two GPU Arrow Python tasks had stopped making metric progress and repeatedly logged 600-second worker-idle timeouts; the underlying cause remains undetermined.

Completed phase/file invocation count across valid and clean phases: **56** (15 Delta Lake, 1 multithreaded shuffle, 1 PyArrow, and 39 clean-main invocations). This represents **55 distinct Python filenames** because `hash_aggregate_test.py` completed two disjoint selections: 85 shuffle-marked tests and 2,157 clean-main tests.

Terminal pytest-failure count across all valid completed evidence: **42 of 13,464 outcomes** (30 Delta, 4 arithmetic, 2 AST, 5 collection-operations, and 1 datasource-v2 read failure; 0.31%).

## Harness and infrastructure observations

- At 04:44-04:45 UTC, two bounded SSH attempts to `db_aws_18.0` failed before login with `Network is unreachable`. This is recorded as an infrastructure-access failure, not a pytest failure. No counts or completion claims were advanced; the last verified runner state remains PID `28189` with `join_test.py` active at about 62% at 04:12:39 UTC.
- The first DB18 cluster became unreachable after its 07:58:30 UTC capture and its local directories were lost. The fresh cluster recovery rebuilt both exact DB18 branches before testing resumed.
- The recovered `arithmetic_ops_test.py` run reached a terminal summary and JUnit result for all 1,495 selected tests, so the earlier partial run is no longer in the resume scope and is not double-counted.
- `array_test.py` logged an expected Spark task failure for unequal `MapData` key/value lengths during negative-path coverage, but pytest completed all 735 selected tests without a failure; it is not counted as a test failure.
- All 44 selected `avro_test.py` cases were skipped by runtime markers; the file completed successfully and remains counted as completed coverage.
- `datasourcev2_read_test.py` reached terminal JUnit evidence for all seven selected tests after the 3,600-second Spark job watchdog cancelled the stalled `test_arrow_source_pandas_udf` job. The file has 5 passed, 1 failed, and 1 skipped outcome and no longer remains in the resume scope. The runner recovered automatically and continued to later files.
- `date_time_test.py` logged expected exception-path Spark task failures, including arithmetic overflow cases, but its terminal pytest summary and JUnit contain no failed tests: all 3,017 selected tests reached passed, skipped, xfailed, or xpassed outcomes.
- `fastparquet_compatibility_test.py` logged expected exception-path Spark task failures, but its terminal pytest summary and JUnit contain no failed tests: all 63 selected tests reached passed, xfailed, or xpassed outcomes.
- The clean-main `hash_aggregate_test.py` invocation logged expected negative-path Spark task failures, but its terminal pytest summary and JUnit contain no failed tests: all 2,157 selected tests reached passed, skipped, xfailed, or xpassed outcomes.
- The initial main and cache attempts failed collection because the two new DB18 UDF decorators referenced `is_databricks_version` without importing it. Commit `ad3341d06` adds the missing import. The Delta phase subsequently collected all 32,659 discoverable items and selected 947 without that error, confirming the collection fix.
- The PyArrow log contains three startup-time `ERROR` messages: a Spark Connect gRPC Unix-socket permission denial, a transient `BlockManagerMasterEndpoint` null-pointer error while re-registering a null block manager, and a SafeSpark UC securable-server permission failure. Spark continued and all 144 selected PyArrow tests reached terminal outcomes, so none is counted as a pytest failure.
- Some merge tests emit background auto-compaction `DBR_FILE_NOT_EXIST` errors after temporary files are removed. Tests surrounding the observed messages have continued to pass; these messages are not counted as failures unless the final pytest result says otherwise.
- After the Delta terminal summary and JUnit write, shutdown logging reported a closed stream and inability to create a logging file. This occurred after all 947 results were recorded and is tracked as harness noise, not a lost test result.
- PyArrow shutdown logged the same closed-stream error after its terminal summary and JUnit write. All 144 results had already been recorded, so this is also harness noise rather than a lost result.

## Resume plan

If the cluster stops after this checkpoint:

1. Do not rerun the fifteen completed Delta Python files for coverage continuation; the Delta phase is complete. Rerun its failed tests separately only for diagnosis or verification.
2. Do not rerun the completed multithreaded-shuffle invocation of `hash_aggregate_test.py`; all 85 shuffle-selected tests passed. This is distinct from the completed 2,157-test clean-main invocation included in step 4.
3. Do not rerun `parquet_pyarrow_test.py`; the PyArrow phase is complete and all 144 selected tests reached terminal outcomes without a failure.
4. Do not rerun the thirty-nine completed invocations under the clean-main configuration, including the complete HLL++ and inset runs. The separate alternate-serializer cache phase in step 6 is still required.
5. When host access returns, inspect the existing ledger before relaunching anything. If `join_test.py` did not reach terminal evidence, rerun that entire file from the beginning. Then run the 52 clean-main files not yet started, preserving the marker exclusions above so valid Delta, shuffle, and PyArrow tests are not repeated.
6. Run the separate alternate-serializer cache invocation for `cache_test.py` after main finishes. The completed clean-main `cache_test.py` result does not cover this configuration; the initial alternate-serializer cache attempt remains invalid and supplies no resumable coverage.

## Evidence locations

### Historical evidence from the replaced cluster

These paths are preserved from the prior reviewed checkpoint. The cluster-local files are no longer accessible after the cluster replacement; the outcome details above retain the evidence captured before that loss.

- Phase ledger: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z/phase-status.tsv`
- Terminal Delta log: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z/delta.log`
- Delta JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923034441-XRiS/TEST-pytest-1790135081138085498.xml`
- Terminal multithreaded-shuffle log: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z/multithreaded_shuffle.log`
- Multithreaded-shuffle JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923062100-32Su/TEST-pytest-1790144460955804202.xml`
- Terminal PyArrow log: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z/pyarrow.log` (SHA-256 `3f3f360997505b262eac79138a78c840d87d0f1b3d7fb5221570a3ccd1965104`)
- PyArrow JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923062608-CYhd/TEST-pytest-1790144768734234991.xml` (SHA-256 `4fd412620d74f494c7958a9e648e4d3b8efb44fa77b19e08d2f1527382b641f0`)
- Invalid main log: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z/main.log`
- Invalid cache log: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z/cache.log`
- Main collection JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923034220-M060/TEST-pytest-1790134940877003166.xml`
- Cache collection JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923034333-Sqz9/TEST-pytest-1790135013130855509.xml`
- First clean main/cache file ledger: `/home/ubuntu/spark-rapids/db18-clean-main-cache-results/20260923T072607Z/file-status.tsv`
- Completed clean-main JUnits:
  - `allow_non_gpu_conditional_marker_test.py`: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923072640-toIc/TEST-pytest-1790148400849689189.xml`
  - `allow_non_gpu_conditional_scope_test.py`: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923072733-5T0I/TEST-pytest-1790148453429816357.xml`
  - `aqe_test.py`: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923072823-NWiX/TEST-pytest-1790148503506090068.xml`

### Current recovered-cluster evidence

These paths were available through the 04:12:39 UTC inspection but could not be reached at this 04:45:22 UTC checkpoint because SSH failed at the network layer. They are not treated as lost; their contents simply could not be re-read for this bounded snapshot.

- Recovered clean main/cache file ledger: `/home/ubuntu/cudf-spark/db18-clean-main-cache-results/20260923T184559Z/file-status.tsv`
- Recovered clean main/cache completed-file ledger: `/home/ubuntu/cudf-spark/db18-clean-main-cache-results/20260923T184559Z/completed-files.tsv`
- Recovered clean main/cache current-file record: `/home/ubuntu/cudf-spark/db18-clean-main-cache-results/20260923T184559Z/current-file.tsv`
- Terminal recovered arithmetic log: `/home/ubuntu/cudf-spark/db18-clean-main-cache-results/20260923T184559Z/main__arithmetic_ops_test.log`
- Recovered arithmetic JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923184647-t6Ig/TEST-pytest-1790189207445451310.xml`
- Recovered array JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923191420-TAwl/TEST-pytest-1790190860659267943.xml`
- Recovered assert-in-tests JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923193558-WJ26/TEST-pytest-1790192158027611928.xml`
- Recovered asserts-regression JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923193653-ktGw/TEST-pytest-1790192213632422363.xml`
- Recovered AST JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923193757-UeLU/TEST-pytest-1790192277511836399.xml`
- Recovered Avro JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923194144-h4Js/TEST-pytest-1790192504818999516.xml`
- Recovered clean-main `cache_test.py` JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923194235-Zx6x/TEST-pytest-1790192555646291250.xml`
- Recovered cast JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923195712-1oU0/TEST-pytest-1790193432101879397.xml`
- Recovered comparison JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923200636-NyWl/TEST-pytest-1790193996059915811.xml`
- Recovered collection-operations JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923201628-d6yu/TEST-pytest-1790194588730920290.xml`
- Recovered column-size-limit JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923202735-gW1Y/TEST-pytest-1790195255212472073.xml`
- Recovered conditionals JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923203109-Pdsv/TEST-pytest-1790195469132739240.xml`
- Recovered CPU-bridge JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923204749-49mk/TEST-pytest-1790196469929328686.xml`
- Recovered CSV JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923205039-3n3d/TEST-pytest-1790196639053468621.xml`
- Recovered data-generator JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923210547-XTSe/TEST-pytest-1790197547244007844.xml`
- Recovered datasource-v2 read JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923210641-pldf/TEST-pytest-1790197601972642595.xml`
- Recovered datasource-v2 write JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923220758-cRgC/TEST-pytest-1790201278080333314.xml`
- Recovered date/time JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923221048-Knme/TEST-pytest-1790201448707253541.xml`
- Recovered decimal-precision JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923224556-7xu7/TEST-pytest-1790203556402048683.xml`
- Recovered DPP JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923224716-JPZJ/TEST-pytest-1790203636302282427.xml`
- Recovered expand-exec JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923230300-XOp1/TEST-pytest-1790204580882184478.xml`
- Recovered explain-mode JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923230457-dxw6/TEST-pytest-1790204697574062104.xml`
- Recovered explain JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923230623-pqQ8/TEST-pytest-1790204783695326619.xml`
- Recovered Fastparquet-compatibility JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923230811-PFfL/TEST-pytest-1790204891299208880.xml`
- Recovered generate-expression JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923231038-lp4l/TEST-pytest-1790205038925595671.xml`
- Recovered get-JSON JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924000616-qqTw/TEST-pytest-1790208376473789666.xml`
- Recovered grouping-sets JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924000823-sTCP/TEST-pytest-1790208503769344577.xml`
- Recovered group-partitions JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924001008-02hv/TEST-pytest-1790208608166938885.xml`
- Recovered clean-main hash-aggregate JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924001104-6M3s/TEST-pytest-1790208664084095880.xml`
- Recovered hashing JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924031325-O8mG/TEST-pytest-1790219605428709700.xml`
- Recovered higher-order-functions JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924031651-f4zb/TEST-pytest-1790219811079076279.xml`
- Recovered Hive-delimited-text JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924031919-tot0/TEST-pytest-1790219959461927943.xml`
- Recovered Hive-Parquet-write JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924032443-xCIR/TEST-pytest-1790220283769915196.xml`
- Recovered Hive-write JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924032932-9eUR/TEST-pytest-1790220572140964904.xml`
- Recovered HLL++ JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924033324-WOj0/TEST-pytest-1790220804217328334.xml`
- Recovered inset JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924033417-7ONa/TEST-pytest-1790220857429187838.xml`
