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
# Generate GitHub issue templates from DBR 17.3 integration test failure logs.
# Usage: ./generate_dbr173_issues.sh
#
# Reads logs from: integration_tests/dbr173_test_results/
# Generates issues to: integration_tests/dbr173_test_results/issues/

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/integration_tests/dbr173_test_results"
ISSUES_DIR="${RESULTS_DIR}/issues"
SUMMARY_FILE="${RESULTS_DIR}/failures_summary.txt"
BRANCH_NAME="dbr_173_support_Feb2"

if [[ ! -f "${SUMMARY_FILE}" ]]; then
    echo "No failures_summary.txt found at ${SUMMARY_FILE}"
    echo "Run ./run_dbr173_tests.sh first."
    exit 1
fi

mkdir -p "${ISSUES_DIR}"

issue_count=0

# Parse the failures summary to find failing test files
# Format in summary file:
#   === <test_file>.py ===
#   Exit code: N
#   Summary: <pytest summary line>
#   Failed tests:
#   <test names, one per line>
#   <blank line>

current_file=""
current_exit_code=""
current_summary=""
current_failed_tests=""
in_failed_section=false

process_file() {
    local test_file="$1"
    local exit_code="$2"
    local summary="$3"
    local failed_tests="$4"

    [[ -z "${test_file}" ]] && return

    local log_file="${RESULTS_DIR}/${test_file}.log"
    local issue_file="${ISSUES_DIR}/${test_file%.py}.md"

    # Count failures
    local fail_count=0
    if [[ -n "${failed_tests}" ]]; then
        fail_count=$(echo "${failed_tests}" | grep -c '.' || echo 0)
    fi

    # Build the test failure table
    local table_rows=""
    if [[ -n "${failed_tests}" ]]; then
        while IFS= read -r test_name; do
            [[ -z "${test_name}" ]] && continue
            # Try to extract error type from the log
            local error_type="See details below"
            if [[ -f "${log_file}" ]]; then
                # Look for the FAILED line and nearby error info
                local error_line
                error_line=$(grep -A2 "FAILED ${test_name}" "${log_file}" | grep -oP '(Error|Exception|AssertionError|TypeError|ValueError|RuntimeError|IllegalArgumentException|AnalysisException|Py4JJavaError)\w*' | head -1 || true)
                [[ -n "${error_line}" ]] && error_type="${error_line}"
            fi
            table_rows="${table_rows}| \`${test_name}\` | ${error_type} |
"
        done <<< "${failed_tests}"
    fi

    # Extract failure details from log (the FAILURES section)
    local failure_details=""
    if [[ -f "${log_file}" ]]; then
        # Extract from "= FAILURES =" to the final summary line
        failure_details=$(sed -n '/= FAILURES =/,/=.*passed.*=/p' "${log_file}" | head -500)
        # If no FAILURES section, try to get ERROR section
        if [[ -z "${failure_details}" ]]; then
            failure_details=$(sed -n '/= ERRORS =/,/=.*passed.*=/p' "${log_file}" | head -500)
        fi
        # Fallback: get last 100 lines
        if [[ -z "${failure_details}" ]]; then
            failure_details=$(tail -100 "${log_file}")
        fi
    fi

    # Generate the issue markdown
    cat > "${issue_file}" << ISSUE_EOF
## [DBR 17.3] Integration test failures in ${test_file}

### Environment
- **Branch**: \`${BRANCH_NAME}\`
- **Shim**: \`spark400db173\` (Databricks Runtime 17.3)
- **Spark Version**: 4.0.0
- **Scala Version**: 2.13

### Test Failures
${fail_count} test(s) failed in \`${test_file}\`:

| Test Name | Error Type |
|-----------|------------|
${table_rows}
### Pytest Summary
\`\`\`
${summary}
\`\`\`

### Failure Details
<details>
<summary>Click to expand full failure output</summary>

\`\`\`
${failure_details}
\`\`\`

</details>

### Repro Steps
1. Checkout branch:
   \`\`\`
   git checkout ${BRANCH_NAME}
   \`\`\`
2. Build:
   \`\`\`
   WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
   \`\`\`
3. Run failing tests:
   \`\`\`
   TESTS=${test_file} WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
   \`\`\`

### Root Cause
_TBD - needs investigation_

### Labels
\`databricks\`, \`bug\`, \`integration-test\`
ISSUE_EOF

    issue_count=$((issue_count + 1))
    echo "Generated: ${issue_file}"
}

# Parse the failures summary file
while IFS= read -r line; do
    if [[ "${line}" =~ ^===\ (.+)\ ===$ ]]; then
        # Process previous file if any
        process_file "${current_file}" "${current_exit_code}" "${current_summary}" "${current_failed_tests}"
        # Start new file
        current_file="${BASH_REMATCH[1]}"
        current_exit_code=""
        current_summary=""
        current_failed_tests=""
        in_failed_section=false
    elif [[ "${line}" =~ ^Exit\ code:\ (.+)$ ]]; then
        current_exit_code="${BASH_REMATCH[1]}"
    elif [[ "${line}" =~ ^Summary:\ (.+)$ ]]; then
        current_summary="${BASH_REMATCH[1]}"
    elif [[ "${line}" == "Failed tests:" ]]; then
        in_failed_section=true
    elif [[ "${in_failed_section}" == true ]]; then
        if [[ -z "${line}" ]]; then
            in_failed_section=false
        else
            if [[ -n "${current_failed_tests}" ]]; then
                current_failed_tests="${current_failed_tests}
${line}"
            else
                current_failed_tests="${line}"
            fi
        fi
    fi
done < "${SUMMARY_FILE}"

# Process the last file
process_file "${current_file}" "${current_exit_code}" "${current_summary}" "${current_failed_tests}"

echo ""
echo "=============================================="
echo "Generated ${issue_count} GitHub issue template(s)"
echo "Location: ${ISSUES_DIR}/"
echo "=============================================="

if [[ ${issue_count} -gt 0 ]]; then
    echo ""
    echo "Issue files:"
    ls -1 "${ISSUES_DIR}"/*.md 2>/dev/null
fi
