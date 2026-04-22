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

# Builds spark-rapids against Apache Spark master SNAPSHOT to detect API breaks early.
#
# Spark master SNAPSHOT artifacts are resolved from URM (built nightly), so there is
# no need to clone or build Apache Spark from source.
#
# Dependency chain: spark-rapids-private -> spark-rapids
# (both resolve Spark artifacts from URM)
#
# Usage:
#   URM_URL=https://urm.nvidia.com/artifactory/sw-spark-maven ./jenkins/spark-master-shim-ci.sh
#
# Environment variables (all optional except URM_URL):
#   SANDBOX_DIR            - Working directory (default: ~/sandbox_test)
#   URM_URL                - NVIDIA internal Maven repo (required, has Spark SNAPSHOT jars)
#   SPARKMASTER_VERSION    - Override Spark version (default: auto-detected from GitHub)
#   PRIVATE_UPSTREAM_URL   - spark-rapids-private git URL (GitLab)
#   RAPIDS_UPSTREAM_URL    - spark-rapids git URL (GitHub)
#   PRIVATE_BRANCH         - spark-rapids-private branch (default: main)
#   RAPIDS_BRANCH          - spark-rapids branch (default: main)
#   BASE_BUILDVER          - Existing shim to clone from (default: 411)
#   PRIVATE_LOCAL_DIR      - Skip clone for private repo, use this path
#   RAPIDS_LOCAL_DIR       - Skip clone for rapids repo, use this path

set -euo pipefail

SANDBOX_DIR="${SANDBOX_DIR:-$HOME/sandbox_test}"
PRIVATE_UPSTREAM_URL="${PRIVATE_UPSTREAM_URL:-git@gitlab-master.nvidia.com:nvspark/spark-rapids-private.git}"
RAPIDS_UPSTREAM_URL="${RAPIDS_UPSTREAM_URL:-https://github.com/NVIDIA/spark-rapids.git}"
PRIVATE_BRANCH="${PRIVATE_BRANCH:-main}"
RAPIDS_BRANCH="${RAPIDS_BRANCH:-main}"
BASE_BUILDVER="${BASE_BUILDVER:-auto}"
BUILDVER="master"
URM_URL="${URM_URL:-}"
SPARKMASTER_VERSION="${SPARKMASTER_VERSION:-}"

PRIVATE_LOCAL_DIR="${PRIVATE_LOCAL_DIR:-}"
RAPIDS_LOCAL_DIR="${RAPIDS_LOCAL_DIR:-}"

PRIVATE_DIR="${PRIVATE_LOCAL_DIR:-$SANDBOX_DIR/spark-rapids-private}"
RAPIDS_DIR="${RAPIDS_LOCAL_DIR:-$SANDBOX_DIR/spark-rapids}"

MANIFEST="$SANDBOX_DIR/run-manifest.txt"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() {
    echo "=== [$(date '+%Y-%m-%d %H:%M:%S')] $* ==="
}

# ─── Step 1: Sync repos ─────────────────────────────────────────────────────

sync_repo() {
    local url="$1" dir="$2" branch="$3" label="$4"
    log "Syncing $label ($branch)"
    if [[ -d "$dir/.git" ]]; then
        git -C "$dir" fetch origin "$branch"
        git -C "$dir" reset --hard "origin/$branch"
    else
        git clone --branch "$branch" "$url" "$dir"
    fi
    echo "$label: $(git -C "$dir" rev-parse HEAD) ($branch)" >> "$MANIFEST"
}

sync_sources() {
    mkdir -p "$SANDBOX_DIR"
    echo "--- Run started at $(date -u '+%Y-%m-%dT%H:%M:%SZ') ---" > "$MANIFEST"

    if [[ -z "$PRIVATE_LOCAL_DIR" ]]; then
        sync_repo "$PRIVATE_UPSTREAM_URL" "$PRIVATE_DIR" "$PRIVATE_BRANCH" "spark-rapids-private"
    else
        log "Using local spark-rapids-private at $PRIVATE_LOCAL_DIR"
        echo "spark-rapids-private: LOCAL ($PRIVATE_LOCAL_DIR)" >> "$MANIFEST"
    fi

    if [[ -z "$RAPIDS_LOCAL_DIR" ]]; then
        sync_repo "$RAPIDS_UPSTREAM_URL" "$RAPIDS_DIR" "$RAPIDS_BRANCH" "spark-rapids"
    else
        log "Using local spark-rapids at $RAPIDS_LOCAL_DIR"
        echo "spark-rapids: LOCAL ($RAPIDS_LOCAL_DIR)" >> "$MANIFEST"
    fi
}

# ─── Step 2: Detect Spark master version ─────────────────────────────────────

detect_spark_version() {
    if [[ -n "$SPARKMASTER_VERSION" ]]; then
        log "Using provided SPARKMASTER_VERSION=$SPARKMASTER_VERSION"
    else
        log "Detecting Apache Spark master version from GitHub"
        SPARKMASTER_VERSION=$(python3 -c "
import urllib.request
import xml.etree.ElementTree as ET
url = 'https://raw.githubusercontent.com/apache/spark/master/pom.xml'
with urllib.request.urlopen(url) as resp:
    root = ET.parse(resp).getroot()
ns = {'p': 'http://maven.apache.org/POM/4.0.0'}
print(root.find('p:version', ns).text)
")
    fi

    if [[ ! "$SPARKMASTER_VERSION" == *-SNAPSHOT ]]; then
        echo "ERROR: Spark master version '$SPARKMASTER_VERSION' does not end with -SNAPSHOT."
        echo "This is unexpected for a master branch build."
        exit 1
    fi

    MAJOR=$(echo "$SPARKMASTER_VERSION" | cut -d. -f1)
    MINOR=$(echo "$SPARKMASTER_VERSION" | cut -d. -f2)
    PATCH=$(echo "$SPARKMASTER_VERSION" | cut -d. -f3 | cut -d- -f1)

    # Auto-detect BASE_BUILDVER: highest non-DB, non-snapshot buildver from the rapids pom
    if [[ "$BASE_BUILDVER" == "auto" ]]; then
        BASE_BUILDVER=$(python3 -c "
import subprocess, sys
out = subprocess.check_output([sys.executable, 'build/get_buildvers.py',
    'no_snapshots', 'scala2.13/pom.xml']).decode().strip()
vers = [v.strip() for v in out.split(',') if 'db' not in v and 'cdh' not in v]
print(vers[-1])
" 2>/dev/null || echo "411")
        log "Auto-detected BASE_BUILDVER=$BASE_BUILDVER"
    fi

    echo "sparkmaster.version: $SPARKMASTER_VERSION" >> "$MANIFEST"
    echo "base.buildver: $BASE_BUILDVER" >> "$MANIFEST"
    log "Spark master version: $SPARKMASTER_VERSION (${MAJOR}.${MINOR}.${PATCH}), base shim: $BASE_BUILDVER"
}

# ─── Step 3: Bootstrap & install spark-rapids-private ────────────────────────

fix_jsonlines() {
    local repo_dir="$1" base_buildver="$2"
    log "Fixing incomplete JSON-lines tags (adding master where $base_buildver exists)"
    python3 -c "
import os

base_tag = '\"spark\": \"${base_buildver}\"'
new_tag = '\"spark\": \"master\"'
fixed = 0
for search_root in ['sql-plugin/src', 'tests/src', 'core/src']:
    full_root = os.path.join('${repo_dir}', search_root)
    if not os.path.isdir(full_root):
        continue
    for root, dirs, files in os.walk(full_root):
        for f in files:
            if not (f.endswith('.scala') or f.endswith('.java')):
                continue
            path = os.path.join(root, f)
            with open(path) as fh:
                lines = fh.readlines()
            if any(base_tag in l for l in lines) and not any(new_tag in l for l in lines):
                new_lines = []
                for line in lines:
                    new_lines.append(line)
                    if base_tag in line:
                        new_lines.append(line.replace('\"${base_buildver}\"', '\"master\"'))
                with open(path, 'w') as fh:
                    fh.writelines(new_lines)
                fixed += 1
print(f'Fixed {fixed} files with missing master JSON-lines tag')
"
}

widen_enforcer_regex() {
    local pom_path="$1"
    if ! grep -q '<regex>.*|master</regex>' "$pom_path" 2>/dev/null; then
        if grep -q 'Unexpected buildver value' "$pom_path" 2>/dev/null; then
            log "Widening Maven enforcer buildver regex to accept master"
            sed -i 's|<regex>\(.*\)db\\d\*)?</regex>|<regex>\1db\\d*)?|master</regex>|' "$pom_path"
        fi
    fi
}

widen_shimplify_regex() {
    local shimplify_path="$1"
    if [[ ! -f "$shimplify_path" ]]; then
        log "No shimplify.py found at $shimplify_path, skipping regex update"
        return
    fi
    if grep -q "spark(?:" "$shimplify_path"; then
        log "shimplify.py regex already widened"
        return
    fi
    log "Widening shimplify.py regex to accept sparkmaster"
    python3 -c "
path = '$shimplify_path'
with open(path) as f:
    content = f.read()
old = r\"re.compile(r'spark\d{3}')\"
new = r\"re.compile(r'spark(?:\d{3}\w*|master)')\"
content = content.replace(old, new)
with open(path, 'w') as f:
    f.write(content)
print('done')
"
}

ensure_sparkmaster_shim() {
    local repo_dir="$1" private_flag="$2"
    local label="spark-rapids"
    [[ "$private_flag" == "--private" ]] && label="spark-rapids-private"

    log "Bootstrapping sparkmaster shim in $label"
    pushd "$repo_dir"

    # Add/update releasemaster profile + sparkmaster.version property
    python3 "$RAPIDS_DIR/build/add_sparkmaster_profile.py" \
        $private_flag pom.xml "$SPARKMASTER_VERSION"

    # Regenerate scala2.13/pom.xml
    if [[ -f build/make-scala-version-build-files.sh ]]; then
        ./build/make-scala-version-build-files.sh 2.13
    fi

    # Widen shimplify regex if needed
    if [[ -f build/shimplify.py ]]; then
        widen_shimplify_regex "build/shimplify.py"
    fi

    # Widen Maven enforcer buildver regex if needed
    widen_enforcer_regex "pom.xml"

    popd
}

install_private() {
    ensure_sparkmaster_shim "$PRIVATE_DIR" "--private"

    log "Building spark-rapids-private (buildver=$BUILDVER)"
    pushd "$PRIVATE_DIR"

    # Check if shimplify needs to create the shim files
    if [[ ! -d core/src/main/sparkmaster ]]; then
        log "Installing base buildver=$BASE_BUILDVER (prerequisite for shimplify)"
        URM_URL="$URM_URL" mvn install -DskipTests \
            -f scala2.13/pom.xml -Dbuildver="$BASE_BUILDVER" --batch-mode
        log "Running shimplify to clone $BASE_BUILDVER -> master in spark-rapids-private"
        URM_URL="$URM_URL" mvn -f scala2.13/pom.xml generate-sources \
            -Dbuildver="$BASE_BUILDVER" \
            -Dshimplify=true -Dshimplify.move=true -Dshimplify.overwrite=true \
            -Dshimplify.add.shim=master -Dshimplify.add.base="$BASE_BUILDVER" \
            --batch-mode
        fix_jsonlines "$PRIVATE_DIR" "$BASE_BUILDVER"
    fi

    URM_URL="$URM_URL" mvn clean install -DskipTests \
        -f scala2.13/pom.xml -Dbuildver=master --batch-mode
    popd

    echo "spark-rapids-private: installed (buildver=$BUILDVER)" >> "$MANIFEST"
    log "spark-rapids-private install complete"
}

# ─── Step 4: Bootstrap sparkmaster shim in spark-rapids ──────────────────────

ensure_rapids_shim() {
    ensure_sparkmaster_shim "$RAPIDS_DIR" ""

    pushd "$RAPIDS_DIR"

    # Check if shimplify needs to create the shim files
    if [[ ! -d sql-plugin/src/main/sparkmaster ]]; then
        log "Installing base buildver=$BASE_BUILDVER (prerequisite for shimplify)"
        URM_URL="$URM_URL" mvn install -DskipTests \
            -f scala2.13/pom.xml -Dbuildver="$BASE_BUILDVER" --batch-mode
        log "Running shimplify to clone $BASE_BUILDVER -> master in spark-rapids"
        URM_URL="$URM_URL" mvn -f scala2.13/pom.xml generate-sources \
            -Dbuildver="$BASE_BUILDVER" \
            -Dshimplify=true -Dshimplify.move=true -Dshimplify.overwrite=true \
            -Dshimplify.add.shim=master -Dshimplify.add.base="$BASE_BUILDVER" \
            --batch-mode
        fix_jsonlines "$RAPIDS_DIR" "$BASE_BUILDVER"
    fi

    # Post-shimplify version fixup
    log "Patching SparkShimVersion to ($MAJOR, $MINOR, $PATCH)"
    local shim_provider="sql-plugin/src/main/sparkmaster/scala/com/nvidia/spark/rapids/shims/sparkmaster/SparkShimServiceProvider.scala"
    local shim_suite="sql-plugin/src/test/sparkmaster/scala/com/nvidia/spark/rapids/shims/sparkmaster/SparkShimsSuite.scala"
    for f in "$shim_provider" "$shim_suite"; do
        if [[ -f "$f" ]]; then
            sed -i "s/SparkShimVersion(4, 1, 1)/SparkShimVersion($MAJOR, $MINOR, $PATCH)/g" "$f"
        fi
    done

    popd
}

# ─── Step 5: Build spark-rapids ──────────────────────────────────────────────

build_rapids() {
    log "Building spark-rapids (buildver=$BUILDVER, verify)"
    pushd "$RAPIDS_DIR"
    URM_URL="$URM_URL" mvn verify -f scala2.13/pom.xml \
        -P individual \
        -Dbuildver=master \
        -Ddist.jar.compress=false \
        --batch-mode
    popd
    log "spark-rapids build complete"
}

# ─── Step 6: Manifest ────────────────────────────────────────────────────────

emit_manifest() {
    echo "--- Run finished at $(date -u '+%Y-%m-%dT%H:%M:%SZ') ---" >> "$MANIFEST"
    log "Manifest written to $MANIFEST"
    cat "$MANIFEST"
}

# ─── Main ────────────────────────────────────────────────────────────────────

main() {
    if [[ -z "$URM_URL" ]]; then
        echo "ERROR: URM_URL is required. Set it to the NVIDIA internal Maven repo URL."
        echo "Example: URM_URL=https://urm.nvidia.com/artifactory/sw-spark-maven"
        exit 1
    fi

    log "Starting Spark master shim CI (SANDBOX_DIR=$SANDBOX_DIR)"

    sync_sources
    detect_spark_version
    install_private
    ensure_rapids_shim
    build_rapids
    emit_manifest

    log "All steps completed successfully"
}

main "$@"
