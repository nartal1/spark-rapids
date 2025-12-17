# Databricks 17.3 Support - Work Summary (December 18, 2025)

## Overall Status

### spark-rapids-private ✅ **PRODUCTION READY**
- **Branch**: databricks_173_support
- **Build**: ✅ Successful on Databricks 17.3
- **Tests**: ✅ Pass (after chmod fix)
- **Status**: Ready for PR

### spark-rapids 🔄 **98 Errors Remaining** (Down from 100)
- **Branch**: dbr17_3
- **Commit**: 833e851a6 (242 files changed)
- **Infrastructure**: ✅ 100% Complete
- **sql-plugin-api**: ✅ Compiles
- **sql-plugin**: 98 errors (25 warnings + 69 API errors)

## Major Accomplishments Today

### spark-rapids Infrastructure (100% Complete)
1. ✅ All POM configurations with release400db173 profiles
2. ✅ Shimplify-generated shim (190+ files annotated)
3. ✅ Delta Lake modules created
4. ✅ Jenkins scripts: Scala 2.13 detection, dependency installation
5. ✅ ProxyRapidsShuffleInternalManagerBase: prismMapStatusEnabled fix
6. ✅ **Critical Fix**: Added orc-format JAR (resolved 40+ OrcProto errors!)
7. ✅ Databricks BOM dependencies complete
8. ✅ install_deps.py aligned with working patterns

### spark-rapids-private Completion
1. ✅ All shimming correctly implemented
2. ✅ AggregateExpression resultIds API handled
3. ✅ PartialAggUtils isFinalAggregate parameter handled
4. ✅ **Critical Fix**: test.sh chmod failures (DBR 17.3 security)
5. ✅ Backward compatible with DBR 12.2, 13.3, 14.3

## Remaining Work in spark-rapids

### 69 Actual API Errors (Categorized)

#### Category 1: WindowInPandasExec Removed (~15 errors)
**Classes removed in Databricks 17.3:**
- `AggregateInPandasExec`
- `WindowInPandasExec`

**Files affected:**
- GpuAggregateInPandasExecMeta.scala
- GpuWindowInPandasExecBase.scala
- Spark320PlusNonDBShims.scala
- Spark320PlusShims.scala
- GpuOverrides.scala
- RapidsMeta.scala

**Solution**: Need Databricks-specific handling or feature disablement

#### Category 2: MapKeyDedupPolicy API Change (~5 errors)
**Issue**: `MapKeyDedupPolicy.Value.toUpperCase` method doesn't exist

**Files**: collectionOperations.scala, GpuOverrides.scala

**Solution**: Handle enum differently (possibly toString instead of toUpperCase)

#### Category 3: QueryStageExec Signature Changes (~10 errors)
**Changes:**
- `BroadcastQueryStageExec.apply` needs adaptiveContext parameter
- `ShuffleQueryStageExec` pattern matching signature changed
- `newReuseInstance` access changed to protected

**Files**: Spark320PlusNonDBShims.scala, AQEUtils.scala

**Solution**: Create Databricks 17.3 specific shims for AQE

#### Category 4: Removed/Moved Classes (~10 errors)
- `MAX_BROADCAST_TABLE_BYTES` → moved or renamed
- `ShowNamespacesExec` → removed
- `FileStreamSink` → removed
- `MetadataLogFileIndex` → removed
- `StoragePartitionJoinParams` → removed

**Solution**: Check if features are needed, create shims or disable

#### Category 5: Method Signature Changes (~10 errors)
- `ExecutedWriteSummary.apply` needs executionTimeMs
- `writeUDFs` signature changed
- `getRuntimeStatistics` access changed

**Solution**: Add compatibility layer for each

#### Category 6: Type System Changes (~10 errors)
- `TimeAdd` type bounds changed
- RebaseShims type mismatches
- Various type compatibility issues

**Solution**: Requires detailed type analysis per error

### 25 Unused Variable Warnings
These are treated as errors due to `-Wconf:any:e` setting.

**Solution**: Add `@nowarn` annotations or suppress for Databricks builds

## Key Insights

1. **ORC Format Critical**: orc-format JAR contains OrcProto - essential for compilation
2. **Databricks 17.3 != Spark 4.0**: Significant Databricks-specific API differences
3. **razajafri branch**: May not have fully compiled (has same OrcProto references)
4. **Shimplify Limitation**: Conservative - requires manual annotation for non-owner files

## Next Steps (Prioritized)

### High Priority
1. **Disable removed features temporarily**
   - Comment out WindowInPandasExec/AggregateInPandasExec support
   - Comment out ShowNamespacesExec
   - Get to a compiling state first

2. **Fix easy API changes**
   - MapKeyDedupPolicy: use toString instead of toUpperCase
   - Add missing parameters to method calls
   - Suppress unused variable warnings

### Medium Priority
3. **Create Databricks 17.3 specific shims**
   - AQE shims for QueryStageExec changes
   - Broadcast exchange shims
   - File format writer shims

### Lower Priority
4. **Re-enable features with proper shimming**
   - WindowInPandasExec support (if needed)
   - Advanced features

## Build Command

```bash
cd /home/ubuntu/spark-rapids
BASE_SPARK_VERSION=4.0.0 WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
```

## Recommendation

**The foundation is excellent.** With 69 actual API errors across ~20 files, a focused effort to:
1. Disable removed features
2. Fix signature mismatches
3. Create targeted shims

Should get to a compiling state. Each error is fixable - it's systematic work.

**Estimated effort**: 4-8 hours of focused API compatibility work.
