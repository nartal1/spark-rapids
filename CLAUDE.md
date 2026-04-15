# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RAPIDS Accelerator for Apache Spark — a Catalyst plugin that replaces Spark SQL executor/expression nodes with GPU-accelerated equivalents using NVIDIA RAPIDS. It processes data in columnar (`ColumnarBatch`) format on GPUs while maintaining bit-for-bit compatibility with CPU Spark results.

## Build System

Maven 3.6.0+ (tested with 3.6.x, 3.8.x, 3.9.x). JDK 17 is the primary JDK; JDK 8 and 11 are also tested.

### Common Build Commands

```bash
# Full build (default: Spark 3.3.0)
mvn verify

# Build for a specific Spark version
mvn -Dbuildver=340 verify

# Build only the dist jar (fast iteration)
mvn package -pl dist -am -Dbuildver=330

# Build with Scala 2.13
mvn -f scala2.13 verify

# Fast repackage (skip compression + JNI unpack)
mvn package -pl dist -am -Ddist.jar.compress=false -Drapids.jni.unpack.skip

# Parallel multi-version build
./build/buildall --profile=noSnapshots

# Single-version quick build
./build/buildall --profile=330
```

### Available Build Versions (`-Dbuildver=XXX`)

321, 330 (default), 331, 332, 333, 334, 340, 341, 342, 343, 344, 350, and Databricks variants (341db, 350db143). See profiles in the root `pom.xml`.

### Scala 2.12/2.13 Dual Support

Code must compile with both Scala 2.12 and 2.13. After changing any `pom.xml`, run:
```bash
./build/make-scala-version-build-files.sh 2.13
```

## Running Tests

### Unit Tests (ScalaTest)

Tests are in the `tests/` module and run against the shaded aggregator jar. **Minimum Maven phase is `package`** (not `test`).

```bash
# All unit tests for Spark 3.3.0
mvn package -pl tests -am -Dbuildver=330

# Run a specific test suite
mvn package -pl tests -am -DwildcardSuites="com.nvidia.spark.rapids.ParquetWriterSuite"

# Run by suffix regex
mvn package -pl tests -am -Dsuffixes='.*CastOpSuite'

# Run specific test names within suites
mvn package -pl tests -am -Dsuffixes='.*CastOpSuite' -Dtests=decimal

# Pass Spark configs
SPARK_CONF="spark.dynamicAllocation.enabled=false,spark.task.cpus=1" mvn package -pl tests -am
```

### Integration Tests (PySpark + pytest)

Located in `integration_tests/`. Requires Python 3 with pytest, pyspark, pandas, pyarrow, pytest-xdist.

## Code Style

Follows the [Databricks Scala guide](https://github.com/databricks/scala-style-guide) (preferred) and [official Scala style guide](https://docs.scala-lang.org/style/). Enforced via scalastyle:

- **Max line length: 100 characters** (imports excluded)
- **No tabs** — spaces only
- Import ordering: java, scala, 3rd-party, spark
- Class names: PascalCase; objects: PascalCase (or config-prefixed)

Disable scalastyle checks inline with:
```scala
// scalastyle:off <rule-id>
...
// scalastyle:on <rule-id>
```

## Architecture

### Shim Layer (Multi-Spark-Version Support)

The most important architectural pattern. The plugin supports 10+ Spark versions via a ServiceProvider/ClassLoader shim system:

- **`ShimLoader`** (in `sql-plugin-api`) detects the runtime Spark version and loads the correct shim implementation.
- **Parallel World ClassLoader**: Version-specific classes are stored in prefixed jar locations (e.g., `spark330/`, `spark340/`) and loaded via a custom classloader at runtime.
- **Shimplify**: Source files declare which Spark versions they apply to via a JSON-lines comment:
  ```scala
  /*** spark-rapids-shim-json-lines
  {"spark": "330"}
  {"spark": "331"}
  spark-rapids-shim-json-lines ***/
  ```
  The lowest version in the comment is the "owner shim" — the file lives under that version's directory (e.g., `src/main/spark330/`).
- Version-specific source code lives in `src/main/spark${buildver}/` directories within modules.
- Common code lives in `src/main/scala/`.

### Module Dependency Chain

```
jdk-profiles → shim-deps → sql-plugin-api → sql-plugin + shuffle-plugin + delta-lake + iceberg
  → aggregator → dist
  → tests (depends on aggregator)
```

### Key Modules

- **`sql-plugin`**: Core GPU-accelerated SQL operations. Contains most shim implementations and Catalyst overrides (`GpuOverrides`, `ColumnarOverrideRules`).
- **`sql-plugin-api`**: Public API and `ShimLoader` entry point.
- **`shuffle-plugin`**: GPU shuffle with UCX support.
- **`tests`**: Unit tests (unconventionally separated from source modules to run against the shaded jar).
- **`aggregator`**: Shades external dependencies into a single artifact per Spark version.
- **`dist`**: Creates the final uber jar (`rapids-4-spark_2.12-*.jar`).
- **`delta-lake/`**, **`iceberg/`**: Data lake integrations with version-specific submodules.
- **`integration_tests`**: PySpark-based end-to-end tests.
- **`udf-compiler`**: Compiles Scala UDFs to GPU code.

### Plugin Mechanism

The plugin extends Catalyst's optimizer: `ColumnarOverrideRules` walks the physical plan node-by-node, `GpuOverrides` matches CPU nodes to GPU replacements, and transition nodes (`GpuColumnarToRow`, `GpuRowToColumnar`) bridge between CPU row and GPU columnar processing.

### Important Design Rules

- **Never derive GPU node classes from CPU case classes.** Create a new case class deriving from the same parent.
- **Never change the order of output columns** — Spark's `collect` relies on the logical plan's output schema.
- **GPU Semaphore** (`GpuSemaphore`): Only "transition" nodes (CPU↔GPU boundary) acquire/release it. Interior GPU nodes don't touch it.
- Configuration properties go in `RapidsConf`.

## Git Workflow

- `branch-[version]`: active development branches (merge target is the highest version)
- `main`: latest released code with version tags
- Uses git submodules — run `git submodule update --init` after clone or branch switch
- Commits require sign-off (`git commit -s`)
- CI triggered by commenting `build` on PRs; `[skip ci]` in title skips tests; `[databricks]` adds Databricks test runs

## IDE Setup

For IntelliJ: run `mvn clean install -Dbuildver=XXX -Dmaven.scaladoc.skip -DskipTests` first, then open the root `pom.xml`. Unselect "Keep source and test folders on reimport". Set `Generated sources folders` to `Detect automatically` and `Phase to be used for folders update` to `process-test-resources`. Install the Resolve Symlinks plugin for shimplified sources.
