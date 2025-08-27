#!/bin/bash
# Copyright (c) 2025, NVIDIA CORPORATION.
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

# Debug script for Spark Connect connectivity issues
set -e

echo "=== Spark Connect Debug Information ==="

# Check if SPARK_HOME is set
if [[ -z "$SPARK_HOME" ]]; then
    echo "ERROR: SPARK_HOME is not set"
    exit 1
fi

echo "SPARK_HOME: $SPARK_HOME"

# Check Spark version
echo
echo "=== Spark Version ==="
"$SPARK_HOME/bin/spark-submit" --version 2>&1 | head -5

# Check for Connect JAR
echo
echo "=== Spark Connect JAR ==="
SCALA_VERSION=$(ls "$SPARK_HOME"/jars/spark-core_*.jar | head -1 | sed 's/.*spark-core_\([0-9]\+\.[0-9]\+\).*/\1/')
echo "Detected Scala version: $SCALA_VERSION"

CONNECT_JAR_PATTERN="${SPARK_HOME}/jars/spark-connect_${SCALA_VERSION}*.jar"
CONNECT_JAR=$(ls $CONNECT_JAR_PATTERN 2>/dev/null | head -1)

if [[ -n "$CONNECT_JAR" && -f "$CONNECT_JAR" ]]; then
    echo "Found Connect JAR: $CONNECT_JAR"
else
    echo "ERROR: Connect JAR not found at $CONNECT_JAR_PATTERN"
    echo "Available JARs in $SPARK_HOME/jars/:"
    ls "$SPARK_HOME"/jars/spark-connect* 2>/dev/null || echo "No Connect JARs found"
    exit 1
fi

# Check network connectivity
echo
echo "=== Network Connectivity ==="
CONNECT_HOST="127.0.0.1"

# Check if we can bind to the ports
echo "Checking if ports 7077 and 15002 are available..."
if netstat -ln 2>/dev/null | grep -q ":7077 "; then
    echo "WARNING: Port 7077 is already in use"
    netstat -ln | grep ":7077 "
fi

if netstat -ln 2>/dev/null | grep -q ":15002 "; then
    echo "WARNING: Port 15002 is already in use" 
    netstat -ln | grep ":15002 "
fi

# Check if we can connect to localhost
echo "Testing TCP connectivity to localhost..."
if timeout 5 bash -c "echo >/dev/tcp/${CONNECT_HOST}/22" 2>/dev/null; then
    echo "Can connect to localhost (SSH port test)"
else
    echo "WARNING: Cannot establish TCP connections to localhost"
fi

# Check for running Spark processes
echo
echo "=== Running Spark Processes ==="
ps aux | grep -E "(spark|java)" | grep -v grep || echo "No Spark processes found"

# Check Java version
echo
echo "=== Java Version ==="
"$SPARK_HOME/bin/spark-submit" --version 2>&1 | grep -i java || echo "Java version not found in spark-submit output"

# Check conda environment
echo
echo "=== Conda Environment ==="
if command -v conda &> /dev/null; then
    echo "Conda is available"
    echo "Current environment: ${CONDA_DEFAULT_ENV:-none}"
    
    if conda info --envs | grep -q "py3_10"; then
        echo "py3_10 environment exists"
    else
        echo "WARNING: py3_10 environment not found"
    fi
else
    echo "WARNING: conda not found in PATH"
fi

echo
echo "=== Recommendations ==="
echo "1. Ensure no other Spark processes are running:"
echo "   pkill -f spark"
echo
echo "2. Try running the test with verbose output:"
echo "   SPARK_CONNECT_SMOKE_TEST=1 bash -x ./integration_tests/run_pyspark_from_build.sh"
echo
echo "3. Check the Spark logs in:"
echo "   $SPARK_HOME/logs/"
echo
echo "4. For connection issues, try starting services manually:"
echo "   $SPARK_HOME/sbin/start-master.sh"
echo "   $SPARK_HOME/sbin/start-worker.sh spark://127.0.0.1:7077"
echo "   $SPARK_HOME/sbin/start-connect-server.sh --master spark://127.0.0.1:7077"
echo
echo "=== Debug Information Complete ==="
