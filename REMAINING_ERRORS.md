# Remaining Compilation Errors (98 errors)

**Current Status**: Down from 100 → 98 errors
**Last Update**: Dec 18, 2025

## Major Fixes Completed
- ✅ OrcProto errors: RESOLVED by adding orc-format JAR to BOM
- ✅ 190+ files annotated with {"spark": "400db173"}
- ✅ DatabricksShimServiceProvider added to 400db173

## Remaining Error Categories

### 1. WindowInPandasExec Removed (~10 errors)
- `AggregateInPandasExec` and `WindowInPandasExec` classes don't exist in Databricks 17.3
- Need to create Databricks-specific shims or disable these features
- Files affected:
  - spark341db/GpuAggregateInPandasExecMeta.scala
  - spark320/Spark320PlusNonDBShims.scala
  - GpuOverrides.scala
  - RapidsMeta.scala
  - GpuWindowInPandasExecBase.scala

### 2. MapKeyDedupPolicy API Change (~5 errors)
- `MapKeyDedupPolicy.Value.toUpperCase` doesn't exist
- Need to handle the enum differently
- Files: collectionOperations.scala, GpuOverrides.scala

### 3. Method Signature Changes (~10 errors)
- `newReuseInstance` access changed (AQEUtils.scala)
- `BroadcastQueryStageExec.apply` signature changed
- `ExecutedWriteSummary.apply` needs additional parameters
- `writeUDFs` signature changed (WritePythonUDFUtils.scala)

### 4. Removed/Moved Classes (~10 errors)
- `MAX_BROADCAST_TABLE_BYTES` moved/removed
- `ShowNamespacesExec` not found
- `FileStreamSink` not found
- `MetadataLogFileIndex` not found  
- `StoragePartitionJoinParams` removed

### 5. Access Visibility Changes (~3 errors)
- `getRuntimeStatistics` cannot be accessed (CostBasedOptimizer.scala)

### 6. Type Mismatches (~10 errors)
- TimeAdd type bounds
- RebaseShims type mismatches
- Various parameter type changes

### 7. Unused Variable Warnings (~50 errors)
- Many "never used" warnings treated as errors due to -Wconf:any:e
- Could suppress these or fix individually

## Suggested Next Steps

1. **Check if features can be disabled**
   - WindowInPandasExec support for Databricks 17.3
   - ShowNamespacesExec support

2. **Create Databricks 17.3 specific shims**
   - For WindowInPandasExec handling
   - For MapKeyDedupPolicy changes
   - For method signature differences

3. **Suppress non-critical warnings**
   - Add @nowarn annotations for unused variables
   - Or relax -Wconf settings for Databricks builds

4. **Reference razajafri branch for specific fixes**
   - Their branch may have solutions for some of these

5. **Alternative: Minimal feature set**
   - Disable problematic features temporarily
   - Get a working build, add features incrementally

