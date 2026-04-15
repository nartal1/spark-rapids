# UC Table Scan Bug Fix — Progress Notes

## Bug Summary

Unity Catalog managed table scans fail on Databricks 17.3 with:
```
java.lang.IllegalArgumentException: Path must be absolute: part-00000-xxx.zstd.parquet
```

## Root Cause (Updated After Investigation)

The initial analysis pointed to `innerFiles` vs `filesWithAbsolutePaths` on `FilePartition`.
After deep investigation, the **actual root cause** is different:

### How Databricks 17.3 Handles UC Table Paths

- `FilePartition` has two key fields: `innerFiles` (may contain relative filenames) and `pathPrefix` (the table root, e.g., `s3://bucket/path/to/table`)
- `filesWithAbsolutePaths` resolves relative paths by prepending `pathPrefix`
- **When `pathPrefix` is `None`, `filesWithAbsolutePaths` returns `innerFiles` unchanged** — relative paths stay relative

### Why The GPU Plugin Fails

Databricks' CPU `FileSourceScanExec` preserves `pathPrefix` on all `FilePartition` objects it creates (proprietary code path). The GPU plugin's `GpuFileSourceScanExec.createNonBucketedReadRDD` recreates partitions via:

```scala
FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)
```

`FilePartition.getFilePartitions` uses the 2-arg constructor which sets `pathPrefix = None`. This destroys the path resolution capability. When `getFinalRDD` later calls `FilePartitionShims.getFiles(p)` → `p.filesWithAbsolutePaths`, it gets back the bare filenames unchanged.

### Why `innerFiles` vs `filesWithAbsolutePaths` Didn't Help

Both produce the same result when `pathPrefix = None`:
- `innerFiles` → bare filenames
- `filesWithAbsolutePaths` → `makePathsAbsolute(innerFiles)` → `pathPrefix.isEmpty` → returns `innerFiles` unchanged

## Verified Facts (From Notebook Testing on DB-17.3 Cluster)

1. **CPU `FileSourceScanExec.inputRDD` partitions have `pathPrefix = Some(s3://...)`** — confirmed via py4j introspection
2. **GPU path creates partitions with `pathPrefix = None`** — confirmed by log output showing bare filenames at `createColumnarReader`
3. **`PreparedDeltaFileIndex`** is the file index class (not `TahoeFileIndexWithStaticPartitions`)
4. **`shouldUseStaticScan` = false** for this table — `StaticPartitionShims` returns `None`
5. **`inputFiles()` returns absolute paths** from the file index — the root path is available
6. **The file index root path**: `s3://databricks-workspace-stack-02cb5-bucket/unity-catalog/1452527783459099/__unitystorage/catalogs/.../tables/...`

## Fix #1: Path Prefix Restoration — REVERTED (Not Shipped)

**Original commits**: `135d66ae1` (innerFiles→filesWithAbsolutePaths), `c3967dad9` (withPathPrefixIfNeeded)

### What It Did

- Changed `FilePartitionShims.getFiles()` and `Spark400PlusDBShims.getPartitionFiles()` from `p.innerFiles` to `p.filesWithAbsolutePaths`
- Added `FilePartitionShims.withPathPrefixIfNeeded()` to restore `pathPrefix` from `relation.location.rootPaths` on partitions that lost it
- Added no-op `withPathPrefixIfNeeded` to all other shims (321, 341db, 350, 355, 400)
- Called `withPathPrefixIfNeeded` from `GpuFileSourceScanExec.createNonBucketedReadRDD`

### Why It Was Reverted

After decompiling the actual Databricks 17.3 jars, we discovered that the GPU implementation **does not match how the CPU sets `pathPrefix`**. The implementation was a best-effort guess, not a faithful reproduction. See the "CPU `pathPrefix` Implementation (From Decompiled DB-17.3 Jars)" section below for details.

Since Fix #2 (Delta scan fallback) makes this code path unreachable for UC tables (the scan falls back to CPU which handles pathPrefix correctly), shipping an incorrect GPU-side path fix adds risk with no benefit.

### `filesWithAbsolutePaths` Is Idempotent (Reference)

Confirmed by decompiling `FilePartitionBase`:
- `makePathsAbsolute`: if `pathPrefix` is `None`, returns files unchanged
- `absolutePath`: checks `path.isAbsolute()` first — if already absolute, returns as-is
- `$anonfun$makePathsAbsolute$1`: checks `filePath.toUri.isAbsolute` — if URI has a scheme (s3://), returns as-is

So double-calling `filesWithAbsolutePaths` is safe. This is relevant for any future implementation.

---

## Fix #2: Delta Scan Fallback for DB-17.3 (Shipped)

### New Error After Fix #1

With the path fix deployed, UC table scans now fail with a **different** error:
```
[DELTA_SKIP_ROW_COLUMN_NOT_FILLED] The Skip Row Column was requested but not filled by the reader. SQLSTATE: XX000
```

### Root Cause Analysis

**DB-17.3 has no Delta provider** (`delta-stub` / `NoDeltaProvider`). The build profile at `pom.xml:648`:
```xml
<rapids.delta.artifactId1>rapids-4-spark-delta-stub</rapids.delta.artifactId1>
```

All UC managed tables are Delta. On DB-17.3, the Delta reader pipeline always injects a `_databricks_internal_edge_computed_column_skip_row` column. The CPU `DeltaParquetFileFormat` reader fills this column. The GPU Parquet reader has no Delta awareness and leaves it null.

The physical plan shows:
```
Project [id, name]
+- Filter if (isnotnull(skip_row)) (skip_row = false)
         else isnotnull(raise_error(DELTA_SKIP_ROW_COLUMN_NOT_FILLED, ...))
   +- ColumnarToRow
      +- FileScan parquet [..., skip_row]   ← scan works, but skip_row is null
```

The `raise_error` in the Filter fires because skip_row is null.

### Why Auto-Fallback Doesn't Trigger — Three Gaps

**Gap 1: `DeltaLakeUtils.isDatabricksDeltaLakeScan` allows skip_row**

`sql-plugin/src/main/spark350db143/.../DeltaLakeUtils.scala` (shared by 350db143 and 400db173):
```scala
def isDatabricksDeltaLakeScan(f: FileSourceScanExec): Boolean = {
  f.requiredSchema.fields.exists(f => f.name.startsWith("_databricks_internal") &&
    !f.name.startsWith("_databricks_internal_edge_computed_column_skip_row"))
}
```
Explicitly allows skip_row through. Correct for 350db143 (has Delta provider), wrong for 400db173 (no Delta provider). `isDeltaLakeMetadataQuery` at `GpuOverrides.scala:5006` returns `false` → no entire-plan CPU fallback.

**Gap 2: `FileSourceMetadataAttribute` check misses skip_row**

`ScanExecShims.tagGpuFileSourceScanExecSupport` checks:
```scala
if (meta.wrapped.expressions.exists {
  case FileSourceMetadataAttribute(_) => true
  case _ => false
})
```
But skip_row is a **plain `AttributeReference`**, not a `FileSourceMetadataAttribute`. Confirmed via py4j on the cluster:
```
id    → org.apache.spark.sql.catalyst.expressions.AttributeReference
name  → org.apache.spark.sql.catalyst.expressions.AttributeReference
_databricks_internal_edge_computed_column_skip_row → org.apache.spark.sql.catalyst.expressions.AttributeReference
```

**Gap 3: `DeltaParquetFileFormat` falls through to plain Parquet**

In `GpuFileSourceScanExec.tagSupport`:
```scala
if (ExternalSource.isSupportedFormat(cls)) {           // NoDeltaProvider → false
  ...
} else if (classOf[ParquetFileFormat].isAssignableFrom(cls)) {  // DeltaParquetFileFormat extends ParquetFileFormat → TRUE
  GpuReadParquetFileFormat.tagSupport(meta)            // tagged as plain Parquet, passes!
}
```
`DeltaParquetFileFormat extends ParquetFileFormat`, so it bypasses `ExternalSource` (correctly rejected) and gets treated as regular Parquet.

### The Fix: Scan-Only Fallback (Shim-Specific to 400db173)

**Approach**: Override `FileSourceScanExecMeta` for 400db173 to detect unsupported `ParquetFileFormat` subclasses and fall back just the scan to CPU. Other operations (Filter, Project, Agg, Join) still run on GPU.

**Files to change:**

| File | Change |
|------|--------|
| `sql-plugin/src/main/spark330db/.../FileSourceScanExecMeta.scala` | Remove `{"spark": "400db173"}` from shim-json-lines |
| `sql-plugin/src/main/spark400db173/.../FileSourceScanExecMeta.scala` | **New file** — copy of 330db version with added check in `tagPlanForGpu()` |

**The added check in `tagPlanForGpu()`:**
```scala
val fmtCls = wrapped.relation.fileFormat.getClass
if (classOf[ParquetFileFormat].isAssignableFrom(fmtCls) &&
    fmtCls != classOf[ParquetFileFormat] &&
    !ExternalSource.isSupportedFormat(fmtCls)) {
  willNotWorkOnGpu(s"unsupported file format: ${fmtCls.getCanonicalName}")
}
```

**Expected plan with fix:**
```
Project [id, name]           ← GPU
+- Filter (skip_row check)   ← GPU
   +- RowToColumnar           ← transition
      +- FileScan parquet     ← CPU (DeltaParquetFileFormat fills skip_row correctly)
```

### Why scan-only fallback (not entire plan)?

- `isDeltaLakeMetadataQuery` triggers `entirePlanWillNotWork` → everything on CPU, wasteful
- Scan-only fallback: CPU Delta reader handles all format-specific features, GPU handles everything above
- Comprehensive: catches ALL Delta features (skip_row, deletion vectors, etc.), not just one column
- Future-proof: when a `delta-spark400db173` Delta provider is created, `ExternalSource.isSupportedFormat` returns `true` and scan goes back to GPU automatically

### Status: SHIPPED

**Commit**: `abe3b2e4f` — forked `FileSourceScanExecMeta` for 400db173 with the format check.

### Why NOT other approaches?

| Approach | Problem |
|----------|---------|
| Change `DeltaLakeUtils` to block skip_row for 400db173 | Triggers `entirePlanWillNotWork` → entire query on CPU |
| Add check in common `GpuFileSourceScanExec.tagSupport` | Affects all Spark/DB versions, not shim-specific |
| Patch skip_row specifically | Fragile — next Delta feature on DB-17.3 breaks again |
| Handle skip_row in GPU reader | Major effort, and DB-17.3 has no Delta provider |

---

## CPU `pathPrefix` Implementation (From Decompiled DB-17.3 Jars)

Decompiled from `/databricks/jars/----ws_4_0--sql--core--core-hive-2.3__hadoop-3.2_2.13_deploy.jar`
on a DB-17.3 cluster (2026-04-15).

### How the CPU Creates `FilePartition` Objects

The CPU path for non-bucketed reads flows through:

1. `FileSourceScanExec.inputRDD` → `createPartitionsForNonBucketedRead()`
2. `SparkOrAetherFileSourceScanLike.createPartitionsForNonBucketedRead()` checks:
   - If `TahoeFileIndexSupportingStaticScan` with `shouldUseStaticScan == true` → calls `partitionsForStaticScan` (sets pathPrefix from `getPath()`)
   - If `TahoeFileIndexWithStaticPartitions` → returns `getStaticPartitions` directly
   - Otherwise → falls through to `createPartitionsFromSelectedPartitions`
3. `createPartitionsFromSelectedPartitions` delegates to `FileScanPartitioner.apply(...).buildAllPartitions()`

### Where `pathPrefix` Is Actually Set

In `FileScanPartitioner.closePartition()` (proprietary Databricks code), the pseudocode is:

```scala
private def closePartition(preferredHosts: Option[Seq[String]],
                           files: Array[PartitionedFile]): Unit = {
  if (files.nonEmpty) {
    val pathPrefix: Option[String] = relationLocation match {
      case tfi: TahoeFileIndex if usingRelativePaths =>
        driverMetrics(RELATIVE_PATHS_IN_DELTA_FILE_LISTING).set(1L)
        Some(tfi.path().toString)     // Path.toString(), NOT Path.toUri().toString()
      case _ =>
        driverMetrics(RELATIVE_PATHS_IN_DELTA_FILE_LISTING).set(0L)
        None
    }
    val partition = FilePartition(partitions.size, files, pathPrefix, preferredHosts)
    partitions += partition
  }
}
```

### Key Facts

1. **`pathPrefix` comes from `TahoeFileIndex.path()`** — NOT `rootPaths`
2. **It only sets `pathPrefix` when `usingRelativePaths == true`** — a flag tracked within `FileScanPartitioner`
3. **It only sets `pathPrefix` when the file index is `TahoeFileIndex`** — non-Delta tables always get `None`
4. **It uses `Path.toString()`**, not `Path.toUri().toString()` — subtle difference in string format
5. **`FilePartition.getFilePartitions()` always uses `apply$default$3` = `None` for pathPrefix** — confirmed in companion object bytecode

### Differences from the Reverted GPU `withPathPrefixIfNeeded`

| Aspect | CPU (`FileScanPartitioner`) | GPU (`withPathPrefixIfNeeded`) |
|--------|----------------------------|-------------------------------|
| Source of prefix | `TahoeFileIndex.path()` | `relation.location.rootPaths.head` |
| Format | `Path.toString()` | `Path.toString()` (same) |
| Guard condition | `instanceof TahoeFileIndex && usingRelativePaths` | `rootPaths.size == 1 && pathPrefix.isEmpty` |
| Non-Delta tables | Always `None` | Could incorrectly set prefix |
| Multi-root indexes | N/A (TahoeFileIndex has single path) | Skips (returns partitions unchanged) |

### Implications for Future Delta Provider (DB-17.3)

When implementing a Delta provider for DB-17.3, the path prefix restoration in the GPU plugin
**must** match the CPU behavior:

1. **Use `TahoeFileIndex.path()`** as the source of `pathPrefix`, not `rootPaths`
2. **Only set `pathPrefix` when paths are actually relative** — check whether the `PartitionedFile.filePath` URIs are relative, similar to the CPU's `usingRelativePaths` tracking
3. **Only apply to `TahoeFileIndex`** — plain `FileIndex` / `InMemoryFileIndex` should never get a `pathPrefix`
4. **Also fix `getFiles()` and `getPartitionFiles()`** to use `filesWithAbsolutePaths` instead of `innerFiles` (commit `135d66ae1` had this right)
5. **Also fix `createBucketedReadRDD`** — it creates `FilePartition` directly (lines 536, 540 of `GpuFileSourceScanExec.scala`) without `withPathPrefixIfNeeded`. If UC tables can be bucketed in the future, this is a gap.
6. **Access to `TahoeFileIndex`**: The class is at `com.databricks.sql.transaction.tahoe.files.TahoeFileIndex`. The `.path()` method returns the table root as a `org.apache.hadoop.fs.Path`. This requires a DB-specific shim since the class is proprietary.

---

## Attempts to Test Path Fix Without Delta

We tried several approaches to create a non-Delta UC table to isolate the path fix from the Delta skip_row issue. None worked:

| Approach | Result |
|----------|--------|
| UC managed table with USING PARQUET | UC managed tables are always Delta |
| External table at `/tmp/...` | `Missing cloud file system scheme` — UC requires cloud storage |
| External table at `dbfs:/...` | `UC_FILE_SCHEME_FOR_TABLE_CREATION_NOT_SUPPORTED` — UC doesn't support dbfs |
| External table at `/Volumes/...` | `Missing cloud file system scheme` — Volumes path not recognized for LOCATION |
| Direct write to `s3://workspace-bucket/...` | `AccessDeniedException: Forbidden` — no direct S3 write access |
| `spark.read.parquet("/Volumes/...")` | Works but doesn't involve UC at all (no PreparedDeltaFileIndex, no pathPrefix) |

**Conclusion**: The path fix cannot be tested in isolation on this cluster. It can only be validated through a UC managed Delta table, which requires both Fix #1 and Fix #2.

---

## Reproduction Steps

### Cluster Setup
- Databricks Runtime 17.3.x-gpu-ml-scala2.13
- Unity Catalog enabled with write access to a catalog
- RAPIDS plugin jar installed via init script + Spark config:
  ```
  spark.plugins com.nvidia.spark.SQLPlugin
  spark.rapids.sql.enabled true
  ```

### Init Script (copies jar from DBFS to classpath)
```bash
#!/bin/bash
cp /dbfs/FileStore/jars/rapids-4-spark_2.13-26.04.0-SNAPSHOT-cuda12.jar /databricks/jars/
```

### Install via Databricks UI
1. Compute -> cluster -> Edit -> Advanced options -> Init Scripts
2. Add: `dbfs:/databricks/scripts/init_rapids.sh`
3. Compute -> cluster -> Edit -> Advanced options -> Spark tab
4. Add Spark config: `spark.plugins com.nvidia.spark.SQLPlugin` and `spark.rapids.sql.enabled true`

### Repro Script (Notebook)
```python
# Create table (need UC write access)
spark.sql("USE CATALOG unity_catalog_test_workspace")
spark.sql("USE SCHEMA default")
spark.sql("DROP TABLE IF EXISTS rapids_uc_repro_test")
spark.sql("CREATE TABLE rapids_uc_repro_test (id INT, name STRING)")
spark.sql("INSERT INTO rapids_uc_repro_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")

# Trigger the bug
spark.conf.set("spark.rapids.sql.enabled", "true")
spark.conf.set("spark.rapids.sql.format.parquet.read.enabled", "true")

try:
  spark.table("unity_catalog_test_workspace.default.rapids_uc_repro_test").show()
  print("SUCCESS: GPU scan worked correctly")
except Exception as e:
  print(f"FAILED: {e}")
  if "Path must be absolute" in str(e):
      print("\n>>> PATH BUG CONFIRMED")
  elif "DELTA_SKIP_ROW_COLUMN_NOT_FILLED" in str(e):
      print("\n>>> PATH FIX WORKS, BUT DELTA SKIP_ROW BUG HIT")
```

### Verification Script (Confirm GPU Is Active)
```python
spark.range(100).selectExpr("id * 2 as doubled").explain()
# Should show GpuRange, GpuProject in the plan
```

### Verification Script (Confirm Jar Contains Fix)
```python
import subprocess, tempfile, os
jar = "/databricks/jars/rapids-4-spark_2.13-26.04.0-SNAPSHOT-cuda12.jar"
tmpdir = tempfile.mkdtemp()
subprocess.run(["jar", "xf", jar,
    "spark-shared/org/apache/spark/sql/execution/rapids/shims/FilePartitionShims$.class"], cwd=tmpdir)
result = subprocess.run(["javap", "-c", "-p",
    os.path.join(tmpdir, "spark-shared/org/apache/spark/sql/execution/rapids/shims/FilePartitionShims$.class")],
    capture_output=True, text=True)
for line in result.stdout.split("\n"):
    if "innerFiles" in line or "filesWithAbsolutePaths" in line or "withPathPrefix" in line:
        print(line)
```

### Verification Script (Check skip_row column type)
```python
plan = spark.table("unity_catalog_test_workspace.default.rapids_uc_repro_test") \
    ._jdf.queryExecution().sparkPlan()
node = plan
while "FileSourceScan" not in node.getClass().getSimpleName():
    node = node.children().apply(0)
print(node.getClass().getSimpleName())
output = node.output()
for i in range(output.size()):
    a = output.apply(i)
    print(a.name(), a.getClass().getName())
```

## Build Commands

```bash
# Build DB-17.3 dist jar (Scala 2.13 required for Spark 4.x)
mvn package -f scala2.13 -Dbuildver=400db173 -pl dist -am -DskipTests -Dmaven.scaladoc.skip --batch-mode

# Verify other versions compile (no regression)
mvn compile -Dbuildver=330 -pl sql-plugin -am -DskipTests -Dmaven.scaladoc.skip --batch-mode

# Sync Scala 2.13 pom files if pom.xml was changed
./build/make-scala-version-build-files.sh 2.13
```

## Deploy Fixed Jar to Cluster

```bash
# Copy to DBFS from the build node
sudo cp /home/ubuntu/spark-rapids/scala2.13/dist/target/rapids-4-spark_2.13-26.04.0-SNAPSHOT-cuda12.jar /dbfs/FileStore/jars/

# Restart the cluster to pick up the new jar
```

## What Remains

### Shipped
- [x] Fix #2: Delta scan fallback — committed (`abe3b2e4f`), `FileSourceScanExecMeta` forked for 400db173

### Reverted (Not Needed Now)
- [x] Fix #1: Path prefix restoration — reverted. The `withPathPrefixIfNeeded` implementation did not match CPU behavior (see "CPU `pathPrefix` Implementation" section). Not needed because Fix #2 makes the scan fall back to CPU for all UC managed (Delta) tables.

### To Do
- [ ] Build dist jar with Fix #2 only
- [ ] Deploy to DB-17.3 cluster with UC access
- [ ] Re-run repro script — expect successful read with scan on CPU, rest on GPU
- [ ] Verify explain plan shows scan on CPU, Project/Filter on GPU
- [ ] Verify raw Parquet reads still run on GPU (format check is `fmtCls != classOf[ParquetFileFormat]`, so plain Parquet is unaffected)
- [ ] Verify non-DB Spark versions compile (330, 350, 400) — ensure shim separation didn't break anything
- [ ] Run unit tests: `mvn package -f scala2.13 -pl tests -am -Dbuildver=400db173`

### Future: Delta Provider for DB-17.3
When a Delta provider (`rapids-4-spark-delta-400db173`) is created:
- `ExternalSource.isSupportedFormat(DeltaParquetFileFormat)` will return `true`
- The scan fallback check will no longer trigger → scans go back to GPU
- At that point, the `pathPrefix` fix will be needed again — but it **must** be reimplemented correctly using `TahoeFileIndex.path()` (see "Implications for Future Delta Provider" section above)
- The `innerFiles` → `filesWithAbsolutePaths` change will also be needed at that point
