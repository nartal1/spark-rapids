# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RAPIDS Accelerator for Apache Spark (`com.nvidia/rapids-4-spark`) is an NVIDIA plugin that accelerates Spark SQL workloads on GPUs. It extends Spark's Catalyst optimizer to replace CPU plan nodes with GPU equivalents (prefixed `Gpu`, e.g. `GpuSort`, `GpuHashAggregate`). When GPU execution isn't possible, `GpuColumnarToRow`/`GpuRowToColumnar` transition nodes bridge CPU and GPU processing.

## Build System

Maven-based, Scala 2.12 (primary) / 2.13, Java 8 (primary; 11/17 also supported).

### Common Commands

```bash
# Build + verify for default Spark version (3.3.0)
mvn verify

# Build for a specific Spark version
mvn verify -Dbuildver=356

# Build without tests (fast iteration)
mvn -Dbuildver=330 install -DskipTests

# Multi-shim parallel build script
./build/buildall --profile=noSnapshots
./build/buildall --profile=330            # single version, fastest

# Fast iterative dist jar rebuild (skip compression + JNI unpack)
mvn package -pl dist -PnoSnapshots -Ddist.jar.compress=false -Drapids.jni.unpack.skip
```

### After Modifying Any pom.xml

Always sync Scala 2.13 build files:
```bash
./build/make-scala-version-build-files.sh 2.13
```

## Testing

### Unit Tests (ScalaTest)

Tests are in `tests/` module (not co-located with source). They run against the shaded aggregator jar. Minimum phase is `package`.

```bash
# All unit tests for default Spark version
mvn package -pl tests -am

# Specific Spark version
mvn package -pl tests -am -Dbuildver=356

# Specific test suite
mvn package -pl tests -am -DwildcardSuites="com.nvidia.spark.rapids.ParquetWriterSuite"

# Filter by suite name suffix
mvn package -pl tests -am -Dsuffixes='.*CastOpSuite'

# Filter by test name keyword
mvn package -pl tests -am -Dtests=decimal

# Combined filters, Scala 2.13
mvn package -f scala2.13 -pl tests -am -Dbuildver=330 -Dsuffixes='.*CastOpSuite' -Dtests=decimal
```

Pass Spark configs: `SPARK_CONF="spark.dynamicAllocation.enabled=false,spark.task.cpus=1" mvn ...`

### Integration Tests (PySpark/pytest)

In `integration_tests/`. Requires `SPARK_HOME`, Python 3, and a built plugin jar.

```bash
pip install -r integration_tests/requirements.txt
./integration_tests/run_pyspark_from_build.sh                    # all tests
./integration_tests/run_pyspark_from_build.sh -k map_test.py     # specific file
```

## Linting

Runs automatically during `mvn verify`:
- **Scalastyle**: configured in `scalastyle-config.xml` (100-char line limit, no tabs). Skip: `-Dmaven.scalastyle.skip=true`
- **Apache RAT**: license header check. Skip: `-Drat.skip=true`
- **Compiler**: `-Xfatal-warnings` and unused-import warnings are errors

## Supported Spark Versions (`-Dbuildver=`)

3.2.x: `321` | 3.3.x: `330`(default), `331`-`334` | 3.4.x: `340`-`344` | 3.5.x: `350`-`357` | 4.0.x: `400`-`402` (Scala 2.13 only) | 4.1.x: `411` (Scala 2.13 only) | Databricks: `330db`, `332db`, `341db`, `350db143`, `400db173`

## Architecture

### Module Dependency Graph

```
sql-plugin-api  →  sql-plugin  →  udf-compiler / shuffle-plugin / delta-lake/* / iceberg/*
                                          ↓
                                     aggregator  (shades into single-shim jar)
                                          ↓
                                        dist  (multi-shim distribution jar with ParallelWorldClassLoader)
                                          ↓
                                       tests / integration_tests
```

### Shim Layer ("Parallel World" class loading)

Supports many incompatible Spark versions via version-specific source directories:
- Files under `src/main/spark${buildver}/` with a shim descriptor comment header:
  ```scala
  /*** spark-rapids-shim-json-lines
  {"spark": "330"}
  {"spark": "331"}
  spark-rapids-shim-json-lines ***/
  ```
- `build/shimplify.py` manages symlinks and code generation during `generate-sources`
- The dist jar uses JDK `ParallelWorldClassLoader` with prefixed directories (`spark330/`, `spark356/`, etc.)
- `ShimLoader` in `sql-plugin-api` bootstraps the correct shim at runtime

### Key Packages

- `com.nvidia.spark.rapids` — core plugin (GPU overrides, expressions, operators, `RapidsConf`)
- `com.nvidia.spark.rapids.shims` — version-specific shim implementations
- `com.nvidia.spark.udf` — UDF-to-Catalyst compiler
- `com.nvidia.spark.rapids.shuffle` — UCX-based GPU shuffle

## Critical Development Rules

1. **Never change output column order** — Spark assumes fixed output schema; reordering causes data corruption
2. **Do NOT derive from CPU case classes** — create new case classes with the same parent instead
3. **All new config properties** go in `RapidsConf` with documentation
4. **Shim files** must include the `spark-rapids-shim-json-lines` header comment
5. **Code must compile with both Scala 2.12 and 2.13** for Spark 3.3.0+
6. **Sync pom.xml changes**: run `./build/make-scala-version-build-files.sh 2.13` after any pom.xml edit

## Code Style

Follows [Databricks Scala guide](https://github.com/databricks/scala-style-guide) and [Scala style guide](https://docs.scala-lang.org/style/). Max line length: 100 chars. Scalastyle inline suppression:
```scala
// scalastyle:off
...
// scalastyle:on
```

## Git Conventions

- All commits require `Signed-off-by:` line (`git commit -s`)
- PR titles with `[databricks]` trigger Databricks CI; `[skip ci]` skips tests (doc-only)
- Uses git submodules (`thirdparty/`): run `git submodule update --init` after cloning
- CI triggered by commenting `build` on PRs
