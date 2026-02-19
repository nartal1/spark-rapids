#!/bin/bash
#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
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
#
# Run DBR 17.3 integration tests file-by-file and capture results.
# Usage: ./run_dbr173_tests.sh [test_file.py ...]
#   If no arguments given, runs all 75 standard test files.
#   If arguments given, runs only those test files.
#
# Results are saved to: integration_tests/dbr173_test_results/
# Already-completed tests (with existing .log files) are skipped.
# To re-run a test, delete its .log file first.

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/integration_tests/dbr173_test_results"
SUMMARY_FILE="${RESULTS_DIR}/failures_summary.txt"
COMPLETED_FILE="${RESULTS_DIR}/completed.txt"
OVERALL_SUMMARY="${RESULTS_DIR}/overall_summary.txt"

# Standard test files (Category 1 - 75 files)
ALL_STANDARD_TESTS=(
    aqe_test.py
    arithmetic_ops_test.py
    array_test.py
    ast_test.py
    assert_in_tests_test.py
    avro_test.py
    cache_test.py
    cast_test.py
    cmp_test.py
    col_size_exceeding_cudf_limit_test.py
    collection_ops_test.py
    conditionals_test.py
    csv_test.py
    datasourcev2_read_test.py
    datasourcev2_write_test.py
    date_time_test.py
    decimal_precision_over_max_test.py
    dpp_test.py
    expand_exec_test.py
    explain_mode_test.py
    explain_test.py
    generate_expr_test.py
    get_json_test.py
    grouping_sets_test.py
    hash_aggregate_test.py
    hashing_test.py
    higher_order_functions_test.py
    hive_delimited_text_test.py
    hive_parquet_write_test.py
    hive_write_test.py
    hybrid_parquet_test.py
    hyper_log_log_plus_plus_test.py
    inset_test.py
    join_test.py
    json_fuzz_test.py
    json_matrix_test.py
    json_test.py
    json_tuple_test.py
    kudo_dump_test.py
    limit_test.py
    logic_test.py
    map_test.py
    misc_expr_test.py
    misc_test.py
    orc_cast_test.py
    orc_test.py
    orc_write_test.py
    parquet_pyarrow_test.py
    parquet_test.py
    parquet_testing_test.py
    parquet_write_test.py
    project_lit_alias_test.py
    project_presplit_test.py
    prune_partition_column_test.py
    qa_nightly_select_test.py
    rand_test.py
    range_test.py
    regexp_no_unicode_test.py
    regexp_test.py
    repart_test.py
    row_conversion_test.py
    row-based_udf_test.py
    sample_test.py
    scan_default_values_test.py
    schema_evolution_test.py
    sort_test.py
    string_test.py
    string_type_test.py
    struct_test.py
    subquery_test.py
    time_window_test.py
    udf_test.py
    url_test.py
    window_function_test.py
)

# Use command-line args if provided, otherwise use full list
if [[ $# -gt 0 ]]; then
    TEST_FILES=("$@")
else
    TEST_FILES=("${ALL_STANDARD_TESTS[@]}")
fi

# Create results directory
mkdir -p "${RESULTS_DIR}"

# Initialize tracking files if they don't exist
touch "${SUMMARY_FILE}"
touch "${COMPLETED_FILE}"

# Counters
total_files=${#TEST_FILES[@]}
files_run=0
files_passed=0
files_failed=0
files_skipped=0
total_test_failures=0

echo "=============================================="
echo "DBR 17.3 Integration Test Runner"
echo "=============================================="
echo "Total test files to run: ${total_files}"
echo "Results directory: ${RESULTS_DIR}"
echo "Started at: $(date)"
echo "=============================================="

for test_file in "${TEST_FILES[@]}"; do
    log_file="${RESULTS_DIR}/${test_file}.log"

    # Skip if already completed (log file exists)
    if [[ -f "${log_file}" ]]; then
        echo "[SKIP] ${test_file} -- already has log file. Delete ${log_file} to re-run."
        files_skipped=$((files_skipped + 1))
        continue
    fi

    files_run=$((files_run + 1))
    echo ""
    echo "----------------------------------------------"
    echo "[${files_run}/${total_files}] Running: ${test_file}"
    echo "Started: $(date)"
    echo "----------------------------------------------"

    # Run the test and capture output + exit code
    cd "${SCRIPT_DIR}"
    TESTS="${test_file}" WITH_DEFAULT_UPSTREAM_SHIM=0 \
        ./jenkins/databricks/test.sh 2>&1 | tee "${log_file}"
    exit_code=${PIPESTATUS[0]}

    echo "Finished: $(date), exit code: ${exit_code}"

    # Extract pytest summary line from the log
    # Looks for lines like: "= 5 passed, 2 failed, 1 error in 120.5s ="
    # or "= 10 passed in 60.2s ="
    summary_line=$(grep -E '=+ .*(passed|failed|error).* =+' "${log_file}" | tail -1)

    # Count failures from summary line
    failed_count=0
    error_count=0
    passed_count=0
    if [[ -n "${summary_line}" ]]; then
        failed_count=$(echo "${summary_line}" | grep -oP '\d+(?= failed)' || echo 0)
        error_count=$(echo "${summary_line}" | grep -oP '\d+(?= error)' || echo 0)
        passed_count=$(echo "${summary_line}" | grep -oP '\d+(?= passed)' || echo 0)
        [[ -z "${failed_count}" ]] && failed_count=0
        [[ -z "${error_count}" ]] && error_count=0
        [[ -z "${passed_count}" ]] && passed_count=0
    fi

    failure_total=$((failed_count + error_count))

    # Record completion
    echo "${test_file}" >> "${COMPLETED_FILE}"

    if [[ ${exit_code} -ne 0 ]] || [[ ${failure_total} -gt 0 ]]; then
        files_failed=$((files_failed + 1))
        total_test_failures=$((total_test_failures + failure_total))

        # Extract individual FAILED test names
        failed_tests=$(grep -E '^FAILED ' "${log_file}" | sed 's/^FAILED //' || true)

        # Extract short test summary section (from "short test summary info" to the
        # pytest results line). This contains each FAILED test with its error message.
        # Strip ANSI color codes so it's clean for GitHub issues.
        short_summary_file="${RESULTS_DIR}/${test_file}.short_summary.txt"
        sed -n '/=.*short test summary info/,/^=.*passed.*=/p' "${log_file}" \
            | sed 's/\x1b\[[0-9;]*m//g' > "${short_summary_file}" 2>/dev/null || true

        echo "[FAIL] ${test_file}: ${failed_count} failed, ${error_count} errors, ${passed_count} passed (exit code: ${exit_code})"

        # Append to failures summary
        {
            echo "=== ${test_file} ==="
            echo "Exit code: ${exit_code}"
            echo "Summary: ${summary_line}"
            echo "Failed tests:"
            echo "${failed_tests}"
            echo ""
        } >> "${SUMMARY_FILE}"
    else
        files_passed=$((files_passed + 1))
        echo "[PASS] ${test_file}: ${passed_count} passed"
    fi
done

# Print overall summary
{
    echo "=============================================="
    echo "OVERALL SUMMARY"
    echo "=============================================="
    echo "Finished at: $(date)"
    echo "Total files:    ${total_files}"
    echo "Files run:      ${files_run}"
    echo "Files passed:   ${files_passed}"
    echo "Files failed:   ${files_failed}"
    echo "Files skipped:  ${files_skipped}"
    echo "Total test failures: ${total_test_failures}"
    echo "=============================================="
    if [[ ${files_failed} -gt 0 ]]; then
        echo ""
        echo "FAILING TEST FILES:"
        grep '^=== ' "${SUMMARY_FILE}" | sed 's/^=== //; s/ ===$//'
    fi
    echo ""
    echo "Full results: ${RESULTS_DIR}"
    echo "Failure details: ${SUMMARY_FILE}"
    echo "Run ./generate_dbr173_issues.sh to generate GitHub issue templates."
} | tee "${OVERALL_SUMMARY}"
