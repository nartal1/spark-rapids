# Databricks 17.3 Support Status for spark-rapids

**Status:** Work In Progress - Infrastructure Complete, Compilation Errors Remaining  
**Commit:** b5cfac355  
**Date:** December 16, 2025

## Overview

Adding support for Databricks Runtime 17.3 which is based on:
- Spark 4.0.0
- Scala 2.13
- Python 3.12

## ✅ Completed Work

### 1. POM Configuration
- **pom.xml**: Added `spark400db173.version` property and `release400db173` profile
- **scala2.13/pom.xml**: Added same version property and profile with modules
- **Fixed enforcer rule**: Updated regex to allow Databricks buildvers: `(?:[3-9][3-9]|[4-9][0-9])[0-9](?:db\d*)?`

### 2. Shim Creation
- **Used shimplify**: `mvn generate-sources -Dshimplify=true -Dshimplify.add.shim=400db173 -Dshimplify.add.base=400`
- **Created**: `sql-plugin/src/main/spark400db173/scala/com/nvidia/spark/rapids/shims/spark400db173/SparkShimServiceProvider.scala`
- **Updated**: 98 shared files with `{"spark": "400db173"}` annotation
- **Manually added** `400db173` to additional shared files:
  - TreeNode.scala
  - ShimLeafExecNode.scala (spark330db version for Databricks)
  - AggregationTagging.scala (both spark320 and spark330db versions)
  - GlobalLimitShims.scala
  - GetSequenceSize.scala
  - GpuCsvUtils.scala
  - Spark320PlusShims.scala
  - Spark340PlusNonDBShims.scala

### 3. Delta Lake Modules
- **Created**: `delta-lake/delta-spark400db173/pom.xml` (Scala 2.12)
- **Created**: `scala2.13/delta-lake/delta-spark400db173/pom.xml` (Scala 2.13)
- **Copied from**: delta-spark350db143 as base

### 4. Shim Dependencies (scala2.13/shim-deps/pom.xml)
Added `release400db173` profile with Databricks 17.3 specific dependencies:
- log4j-core
- parquet-format-internal
- spark-common-utils
- spark-sql-api
- spark-common-utils-other (new in Spark 4.0)
- spark-common-config (new in Spark 4.0)
- shaded-parquet-thrift
- avro-connector
- scala-collection-compat

### 5. Jenkins Build Scripts

#### build.sh
- Added Scala 2.13 detection for Spark 4.0+:
  ```bash
  if [[ "$BASE_SPARK_VERSION" == 4.* ]]; then
      export SCALA_BINARY_VER=2.13
  fi
  ```
- Added conditional pom selection based on Scala version
- Added 17.3 disambiguation logic
- Updated Maven evaluation to use correct pom file

#### deploy.sh
- Added 17.3 shim name mapping:
  ```bash
  elif [[ "$DB_RUNTIME" == "17.3"* ]]; then
      DB_SHIM_NAME="${SPARK_VERSION_STR}db173"
  ```

#### install_deps.py
- Added Spark 4.0 JAR prefix: `spark_prefix = '----ws_4_0'`
- Added Spark 4.0 dependencies (spark-common-utils, spark-sql-api, avro-connector, scala-collection-compat)
- Added Databricks 17.3 specific dependencies (spark-common-utils-other, spark-common-config)
- Fixed hive-metastore-client-patched to use Scala 2.12 for Spark 4.0
- Fixed spark-avro path from vendor to connector for Spark 4.0
- Updated parquet dependencies to use third_party path for Spark 4.0
- Made thrift version flexible (0.16.0 → *)

#### Jenkinsfile-blossom.premerge-databricks
- Added '17.3' to DB_RUNTIME matrix

### 6. API Compatibility Fixes

#### ProxyRapidsShuffleInternalManagerBase.scala
Added support for Databricks 17.3's new `prismMapStatusEnabled` parameter:
- 7-parameter `getReader()` now uses reflection to call 8-parameter version if available
- Added explicit 8-parameter `getReader()` overload for Databricks 17.3
- Maintains backward compatibility with older Spark versions using try-catch

### 7. Build Verification
- ✅ rapids-4-spark-db-bom: Builds successfully
- ✅ rapids-4-spark-jdk-profiles: Builds successfully  
- ✅ rapids-4-spark-shim-deps-parent: Builds successfully
- ✅ rapids-4-spark-sql-plugin-api: Builds successfully
- ❌ rapids-4-spark-sql-plugin: ~100 compilation errors

## 🔄 Remaining Work

### Compilation Errors in sql-plugin (~100 errors)

**Progress Update:**
- Added `{"spark": "400db173"}` to 84 additional shared files
- Total files with 400db173 annotation: 190+ files
- OrcProto dependency issue identified

**Error Categories:**
1. ✅ Missing shim object/trait references - MOSTLY FIXED
2. ❌ **ORC API breaking change** (~40 errors) - CRITICAL
   - OrcProto class removed in ORC 2.x (used by Databricks 17.3)
   - Classes moved from `org.apache.orc.OrcProto` to `org.apache.orc.protobuf` package
   - Requires creating Databricks-specific ORC shim implementations
3. ❌ Method signature changes (~20 errors)
   - newReuseInstance access visibility
   - DynamicPruningExpression pattern mismatch
   - MAX_BROADCAST_TABLE_BYTES moved/removed
   - WindowInPandasExec API changes
4. ❌ Type mismatches (~20 errors)

**Files with errors:**
- CostBasedOptimizer.scala
- GpuCast.scala
- GpuColumnarToRowExec.scala
- GpuDataWritingCommandExec.scala
- GpuMapUtils.scala
- GpuOrcScan.scala
- arithmetic.scala
- complexTypeExtractors.scala
- HashFunctions.scala
- Spark340PlusNonDBShims.scala
- GpuIntervalUtils.scala
- CudfUnsafeRow.scala
- GpuOrcDataReader.scala
- RapidsErrorUtils.scala

### Next Steps

1. **Systematic approach**: Fix errors file by file, starting with shim files
2. **Reference**: Use razajafri/spark-rapids:db-17-400 branch for guidance
3. **Create shims**: For Databricks-specific API changes, create dedicated shim implementations
4. **Test incrementally**: Build after each set of fixes to track progress

## Reference Branch

The razajafri/spark-rapids:db-17-400 branch has a working Databricks 17.3 implementation that can be referenced for API fixes:
- https://github.com/NVIDIA/spark-rapids/compare/main...razajafri:spark-rapids:db-17-400

## Build Command

```bash
cd /home/ubuntu/spark-rapids
BASE_SPARK_VERSION=4.0.0 WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
```

## Testing

Once compilation is successful:
1. Run integration tests on Databricks 17.3 cluster
2. Verify backward compatibility with other Databricks versions
3. Test with spark-rapids-private integration

