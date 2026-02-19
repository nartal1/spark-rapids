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
# Reads logs from: failing_tests_short_summary/
# Generates issues to: failing_tests_short_summary/issues/

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/failing_tests_short_summary"
ISSUES_DIR="${RESULTS_DIR}/issues"
BRANCH_NAME="databricks_173_support"

if [[ ! -d "${RESULTS_DIR}" ]]; then
    echo "No failing_tests_short_summary directory found at ${RESULTS_DIR}"
    exit 1
fi

# Check if there are any .txt files
shopt -s nullglob
txt_files=("${RESULTS_DIR}"/*.txt)
shopt -u nullglob

if [[ ${#txt_files[@]} -eq 0 ]]; then
    echo "No .txt files found in ${RESULTS_DIR}"
    exit 1
fi

mkdir -p "${ISSUES_DIR}"

issue_count=0

for summary_file in "${RESULTS_DIR}"/*.txt; do
    # Get filename without path
    filename=$(basename "$summary_file")

    # Extract test file name from summary filename
    # e.g., "date_time_test.py.short_summary.txt" -> "date_time_test.py"
    # Handle various patterns:
    # - date_time_test.py.short_summary.txt
    # - parquet_test_short_summary.txt (note: underscore instead of dot)
    # - hash_aggregate_test.py,short_summary.txt (note: comma instead of dot)

    if [[ "$filename" =~ ^(.+\.py)[.,]short_summary\.txt$ ]]; then
        test_file="${BASH_REMATCH[1]}"
    elif [[ "$filename" =~ ^(.+)_short_summary\.txt$ ]]; then
        test_file="${BASH_REMATCH[1]}.py"
    else
        echo "Warning: Could not parse test filename from: $filename"
        continue
    fi

    # Extract first 50 lines (showing the FAILED tests)
    first_50_lines=$(head -50 "$summary_file")

    # Extract the summary line with failure count
    # Format: "= X failed, Y passed, ... =" or "======= X failed, Y passed, ... ======="
    summary_line=$(grep -E "^=+ [0-9]+ failed" "$summary_file" || echo "")

    if [[ -z "$summary_line" ]]; then
        echo "Warning: No summary line found in: $filename"
        summary_line="(summary not found)"
        fail_count="?"
    else
        # Extract failure count from summary line
        fail_count=$(echo "$summary_line" | grep -oP "^=+ \K[0-9]+" || echo "?")
    fi

    # Generate issue filename
    issue_file="${ISSUES_DIR}/${test_file%.py}.md"

    # Generate the issue markdown
    cat > "${issue_file}" << ISSUE_EOF
## [DBR 17.3] Integration test failures in ${test_file}

**Describe the bug**
${fail_count} integration test(s) failed in \`${test_file}\` when running on Databricks Runtime 17.3 (spark400db173 shim).

\`\`\`
${summary_line}
\`\`\`

<details>
<summary>Failing Tests (first 50 lines)</summary>

\`\`\`
${first_50_lines}
\`\`\`

</details>

**Steps/Code to reproduce bug**
1. Checkout branch from fork:
   \`\`\`
   git remote add nartal1 https://github.com/nartal1/spark-rapids.git  # if not already added
   git fetch nartal1
   git checkout nartal1/${BRANCH_NAME}
   \`\`\`
2. Build:
   \`\`\`
   WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
   \`\`\`
3. Run failing tests:
   \`\`\`
   TESTS=${test_file} WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/test.sh
   \`\`\`

**Expected behavior**
All tests should pass as they do on other Spark versions.

**Environment details (please complete the following information)**
- Environment location: Databricks (DBR 17.3)
- Branch: \`${BRANCH_NAME}\`
- Shim: \`spark400db173\`
- Scala Version: 2.13

**Additional context**
This is part of the DBR 17.3 integration effort. Root cause needs investigation.
ISSUE_EOF

    issue_count=$((issue_count + 1))
    echo "Generated: ${issue_file} (${fail_count} failures)"
done

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
