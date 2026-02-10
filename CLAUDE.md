# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RAPIDS Accelerator for Apache Spark is a plugin that leverages GPUs to accelerate Apache Spark processing via RAPIDS libraries. The plugin uses Catalyst optimizer extensions to replace CPU-based executors and expressions with GPU versions, processing data in columnar batches for better GPU performance.

## Build Commands

### Basic Build
```bash
# Standard build (runs up to verify phase)
mvn verify

# Build for specific Spark version (e.g., Spark 3.3.0)
mvn verify -Dbuildver=330
```

**Buildver values** identify the target Spark version. Common examples:
| buildver | Spark Version | Scala | Notes |
|----------|---------------|-------|-------|
| 330 | 3.3.0 | 2.12 | **Default build** |
| 340 | 3.4.0 | 2.12 | |
| 350 | 3.5.0 | 2.12/2.13 | |
| 400 | 4.0.0 | 2.13 | Spark 4.x requires Scala 2.13 |
| 411 | 4.1.1 | 2.13 | |
| 321cdh | 3.2.1 | 2.12 | Cloudera CDH |
| 330cdh | 3.3.0 | 2.12 | Cloudera CDH |
| 330db | 3.3.0 | 2.12 | Databricks Runtime 13.x |
| 350db143 | 3.5.0 | 2.13 | Databricks Runtime 14.3 |
| 400db173 | 4.0.0 | 2.13 | Databricks Runtime 17.3 |

Intermediate patch versions also exist (e.g., 331-334, 341-344, 351-357, 332db, 341db). See pom.xml `<profiles>` for the complete list.

**Scala version rule:** Spark 4.x, DBR 17.3+, and optionally Spark 3.5.x use Scala 2.13. Everything else uses Scala 2.12. Scala 2.13 builds require `-f scala2.13/pom.xml`.

### Multi-Version Builds
```bash
# Build all versions using buildall script (recommended)
./build/buildall --help  # See all options

# Build single version with buildall
./build/buildall --profile=330

# Build for development iteration (faster, skips tests)
./build/buildall --profile=minimumFeatureVersionMix

# Key distribution profiles for buildall --profile=<name>:
#   noSnapshots              - All released versions
#   snapshotsWithDatabricks  - All versions including Databricks
#   minimumFeatureVersionMix - Just 330, fastest iteration

# Build all released versions (no snapshots)
mvn clean
mvn -Dbuildver=330 install -Drat.skip=true -DskipTests
mvn -Dbuildver=340 install -Drat.skip=true -DskipTests
mvn -pl dist -PnoSnapshots package -DskipTests
```

### Quick Iteration During Development
```bash
# Fast repackaging (skips compression, reuses JNI dependencies)
mvn package -pl dist -PnoSnapshots -Ddist.jar.compress=false -Drapids.jni.unpack.skip

# Or with buildall
./build/buildall --rebuild-dist-only --option="-Ddist.jar.compress=false -Drapids.jni.unpack.skip"
```

### Databricks Builds
```bash
# Build for Databricks runtime (uses specialized build script)
./jenkins/databricks/build.sh

# Build for specific Databricks runtime
BASE_SPARK_VERSION=3.3.0 ./jenkins/databricks/build.sh

# Build without dependency installation (for iteration)
BASE_SPARK_VERSION=3.3.0 SKIP_DEP_INSTALL=1 ./jenkins/databricks/build.sh

# Build DBR 17.3 (requires WITH_DEFAULT_UPSTREAM_SHIM=0)
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
```

**Note:** Databricks builds use Scala 2.13 for DBR 17.3+ and may require special shims. The build script automatically detects the runtime version and uses the appropriate buildver.

### Scala 2.13 Support
```bash
# After modifying any pom.xml, sync Scala 2.13 versions
./build/make-scala-version-build-files.sh 2.13

# Build for Scala 2.13
mvn -f scala2.13/pom.xml verify -Dbuildver=350
```

## Code Quality Commands

### Code Style (Scalastyle)
```bash
# Check code style (runs automatically during verify phase)
mvn scalastyle:check

# Skip scalastyle during build
mvn verify -Dmaven.scalastyle.skip=true
```

### License Check (RAT)
```bash
# Skip license checks during build
mvn verify -Drat.skip=true
```

## Test Commands

### Unit Tests
```bash
# Run all unit tests for specific Spark version
mvn package -pl tests -am -Dbuildver=330

# Run specific test suite
mvn package -pl tests -am -DwildcardSuites="com.nvidia.spark.rapids.ParquetWriterSuite"

# Filter test suites by suffix (e.g., all CastOpSuite tests)
mvn package -pl tests -am -Dsuffixes='.*CastOpSuite'

# Filter tests within suites by keyword
mvn package -pl tests -am -Dsuffixes='.*CastOpSuite' -Dtests=decimal

# Run Scala 2.13 tests
mvn package -f scala2.13/pom.xml -pl tests -am -Dbuildver=330

# Pass Spark configs (comma-separated)
SPARK_CONF="spark.dynamicAllocation.enabled=false,spark.task.cpus=1" mvn package -pl tests -am
```

### Integration Tests

Integration tests are Python-based using pytest. They require `SPARK_HOME` to be set.

```bash
# Install dependencies (only needed once)
pip install -r integration_tests/requirements.txt

# Set SPARK_HOME to your Spark installation
export SPARK_HOME=/path/to/spark

# Run all integration tests
./integration_tests/run_pyspark_from_build.sh

# Run tests from specific module
./integration_tests/run_pyspark_from_build.sh -k map_test.py

# Run a single test
./integration_tests/run_pyspark_from_build.sh -k 'map_test.py and test_map_integration_1'

# Run tests matching keyword
./integration_tests/run_pyspark_from_build.sh -k exist

# Run specific tests with parameters
TESTS="arithmetic_ops_test.py::test_addition array_test.py::test_array_exists[3VL:off-data_gen0]" \
  ./integration_tests/run_pyspark_from_build.sh

# Run with verbose output
./integration_tests/run_pyspark_from_build.sh -v

# Show test summary
./integration_tests/run_pyspark_from_build.sh -rfExXs
```

**Note:** The `run_pyspark_from_build.sh` script automatically detects the Scala version and sets required flags for the plugin. Integration tests can also run as part of `mvn verify` if `SPARK_HOME` is set.

**Key test utilities** (in `integration_tests/src/main/python/`):
- `assert_gpu_and_cpu_are_equal_collect` — Core pattern: runs query on both CPU and GPU, compares results
- `data_gen.py` — Data generators for test inputs (e.g., `IntegerGen`, `StringGen`, `StructGen`)
- Test markers: `@ignore_order` (unordered comparison), `@incompat` (allow minor incompatibilities), `@approximate_float` (float tolerance), `@allow_non_gpu` (permit CPU fallback for specific ops)

## Code Architecture

### Module Structure

- **sql-plugin**: Core GPU acceleration logic, Catalyst plan replacement
- **sql-plugin-api**: Public API for shim layer and external integrations
- **shuffle-plugin**: GPU-accelerated shuffle manager
- **aggregator**: Aggregates and shades dependencies into single-shim artifact
- **dist**: Final distribution JAR supporting multiple Spark versions
- **tests**: ScalaTest-based unit tests
- **integration_tests**: Python/pytest integration tests
- **tools**: Utility tools for development
- **udf-compiler**: User-defined function compiler
- **shim-deps**: Shim dependencies for vendor-specific Spark versions (Cloudera, Databricks)
- **delta-lake**, **iceberg**: Integration with table formats

### Shim Layer Architecture

The project supports multiple incompatible Spark versions (3.2.x, 3.3.x, 3.4.x, 3.5.x, plus vendor releases like Databricks and CDH) through a shim layer:

**Key Concepts:**
- Each supported Spark version has a `buildver` identifier (e.g., `330` for Spark 3.3.0)
- Version-specific code lives in `src/main/spark${buildver}` directories (e.g., `src/main/spark330`)
- Common code shared across all versions lives in `src/main/scala` and `src/main/java`
- Shim files use special comment headers to declare which versions they support:
  ```scala
  /*** spark-rapids-shim-json-lines
  {"spark": "320"}
  {"spark": "330"}
  spark-rapids-shim-json-lines ***/
  ```

**Runtime Loading:**
- Uses a "Parallel World ClassLoader" approach with different JAR paths per version
- Example JAR layout for Spark 3.3.0:
  ```
  rapids-4-spark.jar!/
  rapids-4-spark.jar!/spark-shared/
  rapids-4-spark.jar!/spark330/
  ```
- `ShimLoader` detects runtime Spark version and loads appropriate shim implementation
- All shims implement the same `SparkShims` API

**When Adding Version-Specific Code:**
1. If code works across all versions: put in `src/main/scala` or `src/main/java`
2. If method signatures differ: add to `SparkShims` trait and implement in each shim
3. If base classes change incompatibly: create intermediate trait in shim directories
4. Use `shimplify.py` (runs during `generate-sources` phase) to manage shim metadata

### Plugin Mechanism

**How the Plugin Works:**
1. Plugin registers as a Catalyst optimizer extension
2. Analyzes the physical plan produced by Catalyst
3. Walks the plan tree, looking up replacement rules for each node
4. Replaces CPU executors/expressions with GPU versions when possible
5. Inserts `GpuColumnarToRow` and `GpuRowToColumnar` for CPU/GPU transitions
6. GPU nodes process data as `ColumnarBatch` (columnar format using Apache Arrow)

**Key Classes:**
- `ColumnarOverrideRules`: Entry point for plan transformation
- `GpuOverrides`: Main replacement rules engine
- `SparkShimImpl`: Version-specific shim implementation

**Example Transformation:**
```
CPU: Filter -> Project -> Scan
GPU: GpuFilter -> GpuProject -> GpuBatchScan
```

### Source Code Layout

**Conventional Structure:**
- `src/main/scala`: Scala code compatible with all Spark versions
- `src/main/java`: Java code compatible with all Spark versions
- `src/test/scala`: Test code compatible with all versions

**Shim-Specific Structure:**
- `src/main/spark${buildver}`: Version-specific implementation (e.g., `spark330`)
- Files contain `spark-rapids-shim-json-lines` metadata declaring supported versions
- Build process symlinks/materializes appropriate files based on target version

**Build-Time Processing:**
- `build-helper-maven-plugin` adds version-specific source directories
- `shimplify.py` validates shim metadata and creates symlinks
- Package name templates like `$_spark.version.classifier_` get interpolated

## Development Workflow

### Setting Up IDE

**IntelliJ IDEA:**
1. Run Maven install for target Spark version: `mvn clean install -Dbuildver=340 -DskipTests`
2. Open the root `pom.xml` in IDEA
3. In Maven settings: disable "Import using new IntelliJ Workspace Model API"
4. Unselect "Keep source and test folders on reimport"
5. Select a release profile (e.g., `release340`) in Maven tool window
6. Set "Phase to be used for folders update" to `process-test-resources`
7. Click "Reload all projects" then "Generate Sources and Update Folders"
8. Manually mark `tests/src/test/spark3*` as "Test Sources Root" if needed

**Bloop (for VS Code, Metals, etc.):**
```bash
./build/buildall --generate-bloop --profile=330
rm -f .bloop
ln -s .bloop-spark330 .bloop
# Open in VS Code with Metals extension
```

### Making Changes

**Before Modifying Code:**
1. Always read files before proposing changes
2. Check if change affects multiple Spark versions (might need shim-specific code)
3. If modifying pom.xml, run `./build/make-scala-version-build-files.sh 2.13`

**When Adding New Operators:**
1. Implement GPU version extending appropriate base class
2. Add replacement rule in `GpuOverrides`
3. Handle any shim-specific variations in `SparkShims` trait
4. Add unit tests in `tests/` module
5. Add integration tests in `integration_tests/` if needed

**Code Style:**
- Follows Apache Spark coding standards
- Scala: Follow [Databricks Scala Style Guide](https://github.com/databricks/scala-style-guide)
- Use provided [IDEA code style settings](docs/dev/idea-code-style-settings.xml)
- Run `scalastyle` check via Maven
- Pre-commit hooks (`.pre-commit-config.yaml`) handle copyright year updates automatically

**Developer Documentation:**
Deep-dive guides live in `docs/dev/`:
- `shims.md` — Shim development guide, handling Spark API incompatibilities
- `shimplify.md` — How the shimplify tool manages shim source code layout
- `testing.md` — Testing overview and strategies
- `adaptive-query.md` — Adaptive Query Execution support
- `data-sources.md` — Working with v1/v2 data sources

### Git Workflow

**Do not `git add` these local-only files:**
- `CLAUDE.md`
- `DBR-173-INTEGRATION-STATUS.md`
- `NEXT-STEPS.md`

**Signing Commits:**
All commits must be signed off with `-s` flag:
```bash
git commit -s -m "Your message"
```

**Pull Request Checks:**
- Sign-off check: At least one commit must be signed
- blossom-ci: Triggered by maintainer commenting `build`
  - Runs `mvn verify` and unit tests for multiple Spark versions
  - Add `[skip ci]` to PR title for doc-only changes
  - Add `[databricks]` to PR title to include Databricks runtime tests

## Common Pitfalls

### Build Issues
- **CodeCache full warning**: Increase `ReservedCodeCacheSize` in `MAVEN_OPTS`
- **Stale JARs**: Use `mvn clean` or rebuild with `-am` flag to rebuild dependencies
- **IDE not finding symbols**: Regenerate sources and reload Maven project

### Shim Issues
- **Wrong Spark version at runtime**: Check `ShimLoader` is selecting correct shim
- **ClassNotFoundException for shim classes**: Verify JAR layout with `javap -cp`
- **Source not compiling for some versions**: Check shim metadata in file header

### Test Issues
- **Tests pass locally but fail in CI**: Make sure to test against multiple Spark versions
- **Integration tests fail**: Verify `SPARK_HOME` is set and plugin JAR is in classpath

## Important Notes

### Artifact Locations
- **Scala 2.12 builds:** `dist/target/rapids-4-spark_2.12-*.jar`
- **Scala 2.13 builds:** `scala2.13/dist/target/rapids-4-spark_2.13-*.jar`
- **Databricks builds:** `scala2.13/dist/target/rapids-4-spark_2.13-*-cuda12.jar` (DBR 17.3+)
- Multi-version JARs contain multiple shim implementations in different classpath directories

### Key Facts
- Default build targets Spark 3.3.0; always specify `-Dbuildver` for other versions
- Unit tests are in `tests/` (not `src/test/`) to test final shaded artifact
- When git submodules are updated, run `git submodule update --init`
- The plugin requires CUDA-capable GPUs at runtime (development/testing can be done without)
