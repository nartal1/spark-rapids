# Databricks 17.3 Integration Status

## ✅ Completed Work

### Phase 1: Git Integration (COMPLETE)
- ✅ Created feature branch: `feature/dbr-17.3-integration-from-main`
- ✅ Cherry-picked DBR-17.3 commit: `f03307cfe9f38af830723740b604a10c6bc49b21`
- ✅ Resolved all merge conflicts:
  - `pom.xml`: Merged Scala 2.13 compiler arguments
  - `scala2.13/pom.xml`: Merged Scala 2.13 compiler arguments
  - `jenkins/databricks/install_deps.py`: Kept Netty dependencies from main
  - Source files: Strategic resolution (kept stable main versions, accepted DBR-17.3 shim files)
- ✅ Added complete `spark400db173/` directory (30 files)
- ✅ Checkpoint: `checkpoint-01-cherry-pick`

### Phase 2: Code Sharing (COMPLETE)
- ✅ Shared `TryModeShim.scala`:
  - Moved to `sql-plugin/src/main/spark400db173/.../TryModeShim.scala`
  - Updated metadata: `{"spark": "400db173"}` and `{"spark": "411"}`
  - Deleted `sql-plugin/src/main/spark411/.../TryModeShim.scala`
- ✅ Shared `TimeAddShims.scala`:
  - Moved to `sql-plugin/src/main/spark400db173/.../TimeAddShims.scala`
  - Updated metadata: `{"spark": "400db173"}` and `{"spark": "411"}`
  - Deleted `sql-plugin/src/main/spark411/.../TimeAddShims.scala`
- ✅ Checkpoint: `checkpoint-02-code-sharing`

## 📋 Remaining Work

### Phase 3: Compilation Testing (PENDING)

#### Step 3.1: Test Spark 4.1.1 Compilation
```bash
# Spark 4.1.1 requires Scala 2.13
cd /home/ubuntu/spark-rapids
mvn clean compile -f scala2.13/pom.xml -DskipTests -Dbuildver=411
```
**Expected:** SUCCESS (no regression from code sharing)

**What to verify:**
- No compilation errors
- TryModeShim is loaded from spark400db173 (shimplify symlinks)
- TimeAddShims is loaded from spark400db173

#### Step 3.2: Test DBR-17.3 Compilation
```bash
cd /home/ubuntu/spark-rapids
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
```

**Alternative (if build script doesn't work):**
```bash
mvn -Dmaven.wagon.http.retryHandler.count=3 -B -f scala2.13/pom.xml compile \
  -pl dist -am -DskipTests -Dmaven.scaladoc.skip -Ddatabricks -Dbuildver=400db173
```

**Expected:** SUCCESS

**What to verify:**
- spark400db173 shim compiles correctly
- All 30 new shim files compile
- Scala 2.13 compilation successful
- No conflicts with existing shims

#### Step 3.3: Test Base Spark 4.0.0 (Optional)
```bash
mvn clean compile -f scala2.13/pom.xml -Dbuildver=400 -DskipTests
```
**Purpose:** Ensure spark400 base version still works

### Phase 4: Unit Testing (PENDING)

#### Step 4.1: Spark 4.1.1 Unit Tests
```bash
cd /home/ubuntu/spark-rapids
mvn package -f scala2.13/pom.xml -pl tests -am -Dbuildver=411
```

**Expected:** All existing tests pass (no regression)

#### Step 4.2: DBR-17.3 Unit Tests
```bash
cd /home/ubuntu/spark-rapids
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
# This should build and run tests
```

**Alternative:**
```bash
mvn -Dmaven.wagon.http.retryHandler.count=3 -B -f scala2.13/pom.xml package \
  -pl tests -am -Dmaven.scaladoc.skip -Ddatabricks -Dbuildver=400db173
```

**New test file added:**
- `tests/src/test/spark400db173/scala/com/nvidia/spark/rapids/MetricsEventLogValidationSuite.scala`

**If tests fail:**
1. Check shuffle API issues (stageShuffleCount parameter)
2. Check Python UDF issues (ArrowWindowPythonExec, ArrowAggregatePythonExec)
3. Fix in spark400db173 shim files as needed

### Phase 5: Integration Testing on DBR 17.3 Cluster (PENDING)

#### Step 5.1: Build Distribution JAR
```bash
cd /home/ubuntu/spark-rapids
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
```

**Output:** `scala2.13/dist/target/rapids-4-spark_2.13-26.04.0-SNAPSHOT-cuda12.jar`

#### Step 5.2: Smoke Tests on DBR 17.3 Cluster

Upload JAR to Databricks and run these tests:

**Test 1: Basic Operations**
```python
df = spark.range(1000000).selectExpr("id", "id % 100 as value")
df.groupBy("value").count().show()
```

**Test 2: Shuffle Operations** (stageShuffleCount API)
```python
df1 = spark.range(1000000)
df2 = spark.range(1000000)
result = df1.join(df2, "id").count()
```

**Test 3: Pandas UDFs** (ArrowWindowPythonExec)
```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql import Window

@pandas_udf("double")
def rolling_avg(values: pd.Series) -> pd.Series:
    return values.rolling(3).mean()

df = spark.range(10).withColumn("value", col("id") * 2)
window = Window.orderBy("id").rowsBetween(-2, 0)
df.withColumn("rolling_avg", rolling_avg("value").over(window)).show()
```

**Test 4: Broadcast Joins**
```python
large_df = spark.range(10000000)
small_df = spark.range(100).withColumnRenamed("id", "key")
result = large_df.join(broadcast(small_df), large_df.id == small_df.key).count()
```

**Test 5: Parquet I/O**
```python
df = spark.range(1000000).selectExpr("id", "id % 100 as partition_key")
df.write.partitionBy("partition_key").parquet("/tmp/test_parquet")
read_df = spark.read.parquet("/tmp/test_parquet")
read_df.count()
```

### Phase 6: Multi-Shim Build Verification (PENDING)

```bash
cd /home/ubuntu/spark-rapids/scala2.13
mvn clean
mvn -Dbuildver=400db173 install -Drat.skip=true -DskipTests -Ddatabricks
mvn -Dbuildver=411 install -Drat.skip=true -DskipTests
mvn -pl dist -Dincluded_buildvers=400db173,411 package -DskipTests
```

**Verify JAR layout:**
```bash
jar tf scala2.13/dist/target/rapids-4-spark_2.13-*.jar | grep -E "spark400db173|spark411" | head -20
```

### Phase 7: Documentation Updates (PENDING)

#### Files to Update:

1. **README.md**
   - Add Databricks Runtime 17.3.x to supported versions list

2. **docs/compatibility.md**
   - Add DBR 17.3 compatibility matrix
   - Note Scala 2.13 requirement
   - Document any known limitations

3. **docs/get-started/getting-started-databricks.md**
   - Add DBR 17.3 setup instructions
   - Include cluster configuration examples

## 🔍 Key Files Added/Modified

### New Directories:
- `sql-plugin/src/main/spark400db173/` (30 files)
- `tests/src/test/spark400db173/` (1 file)

### Key Shim Files in spark400db173:
1. `SparkShims.scala` - Core shim implementation
2. `Spark400PlusDBShims.scala` - Databricks-specific base trait
3. `GpuShuffleExchangeExec.scala` - Shuffle with stageShuffleCount
4. `TryModeShim.scala` - Shared with spark411 ✅
5. `TimeAddShims.scala` - Shared with spark411 ✅
6. `SparkShimServiceProvider.scala` - Version matching

### Modified Files:
- `pom.xml` - Added spark400db173 profile and version property
- `scala2.13/pom.xml` - Added spark400db173 profile
- `jenkins/databricks/build.sh` - DBR 17.3 detection
- `jenkins/databricks/install_deps.py` - Kept Netty dependencies

## 🚀 How to Complete Integration

### Option 1: Local Development
1. Ensure Maven is installed and in PATH
2. Run Phase 3 compilation tests
3. Fix any compilation errors
4. Run Phase 4 unit tests
5. Fix any test failures
6. Proceed to cluster testing

### Option 2: CI/CD Pipeline
1. Push branch to GitHub: `git push origin feature/dbr-17.3-integration-from-main`
2. Create draft PR to trigger blossom-ci
3. Review CI results
4. Fix any issues found by CI
5. Convert to ready PR after CI passes

### Option 3: Continue Manually
1. Install Maven if not available
2. Follow Phase 3-7 steps in the plan file: `/home/ubuntu/.claude/plans/parallel-splashing-quokka.md`

## 📊 Branch Information

**Branch:** `feature/dbr-17.3-integration-from-main`

**Recent Commits:**
```
251aacafb Share common shim code between spark411 and spark400db173
71e853943 Basic infrastructure to include DB-17.3 and compile fixes
c125e895b [auto-merge] release/26.02 to main [skip ci] [bot] (#14245)
```

**Checkpoints:**
- `checkpoint-00-baseline` - Before cherry-pick
- `checkpoint-01-cherry-pick` - After resolving conflicts
- `checkpoint-02-code-sharing` - After sharing common shims

## ⚠️ Known Considerations

1. **Scala 2.13 Required:** DBR 17.3 mandates Scala 2.13 (unlike older DBR versions)
2. **Shuffle API Changes:** stageShuffleCount parameter added to getShuffleRDD()
3. **Python UDF Renames:** WindowInPandasExec → ArrowWindowPythonExec
4. **Build Dependencies:** Requires Maven 3.6.0+ for building
5. **Code Sharing:** TryModeShim and TimeAddShims now shared with spark411

## 📝 Pull Request Template

When ready to create PR, use this template:

```markdown
## Summary
Adds support for Databricks Runtime 17.3 (Spark 4.0.0-based) by integrating and refactoring the DBR-17.3 infrastructure commit.

## Changes
- ✅ Added spark400db173 shim directory (30 new files)
- ✅ Shared common code between spark411 and spark400db173:
  - TryModeShim.scala (evalContext.evalMode handling)
  - TimeAddShims.scala (TimeAdd→TimestampAddInterval rename)
- ✅ Updated pom.xml profiles for buildver=400db173
- ✅ Updated jenkins/databricks/build.sh for DBR 17.3 detection
- ✅ Resolved merge conflicts strategically

## Testing Status
- [ ] Spark 4.1.1 compilation (regression check)
- [ ] DBR-17.3 compilation
- [ ] Spark 4.1.1 unit tests
- [ ] DBR-17.3 unit tests
- [ ] Integration tests on DBR 17.3 cluster
- [ ] Multi-shim JAR build

## Related Issues
Closes #14015 (Development of support for Databricks 17.3.x)

## Checklist
- [x] Code follows project style guidelines
- [x] Merge conflicts resolved
- [ ] Compilation verified
- [ ] Unit tests pass
- [ ] Documentation updated
- [x] All commits are signed-off
```

## 🔧 Troubleshooting

### If Compilation Fails:
1. Check spark-rapids-shim-json-lines metadata is correct
2. Verify symlinks in `target/${buildver}/generated/src`
3. Run `mvn clean` and retry
4. Check Scala 2.13 compatibility

### If Tests Fail:
1. Review test logs for specific failures
2. Check shuffle-related tests (stageShuffleCount)
3. Check Python UDF tests (Arrow protocol changes)
4. Verify shim loading with correct version

### If Build Script Fails:
1. Check DBR_VER environment variable
2. Verify SCALA_BINARY_VER detection (should be 2.13)
3. Check Databricks JARs are available

## 📚 References

- Original DBR-17.3 commit: `f03307cfe9f38af830723740b604a10c6bc49b21`
- Spark 4.1.1 commit: `d13479cf17a3897092dd5f622123893d690ce209`
- GitHub Issue: https://github.com/NVIDIA/spark-rapids/issues/14015
- Plan file: `/home/ubuntu/.claude/plans/parallel-splashing-quokka.md`
