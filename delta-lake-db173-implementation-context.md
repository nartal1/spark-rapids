# Delta Lake DB-17.3 Implementation Context
**For resuming work across sessions — push this file to git**

**Date compiled:** 2026-03-19
**Session:** Research and planning complete; implementation not yet started.

---

## 1. Goal

Add full Delta Lake GPU acceleration support for the Databricks 17.3 shim (`400db173`,
Spark `4.0.0-databricks-173`). Currently `delta-lake/delta-stub` is used, which means all
Delta DML (INSERT, DELETE, UPDATE, MERGE, OPTIMIZE) runs on CPU even when the RAPIDS plugin
is active.

**Deliverable:** Create `delta-lake/delta-spark400db173/` Maven module with 21 Scala files +
pom.xml, following the pattern of `delta-lake/delta-spark350db143/` (DB-14.3).

**Tracking issue:** https://github.com/NVIDIA/spark-rapids/issues/14015

---

## 2. Critical Build Fact: Scala 2.13 Only

DB-17.3 / Spark 4.0 is **Scala 2.13 only**. There is no Scala 2.12 artifact.

The `release400db173` Maven profile lives inside the `<!-- #if scala-2.13 --><!--` block
in the root `pom.xml` (~lines 633–654), meaning it is XML-commented out in the main pom
and only active in `scala2.13/pom.xml`.

**Consequence: ALL builds must use `-f scala2.13`:**
```bash
mvn -f scala2.13 -Dbuildver=400db173 ...
```

The primary `pom.xml` for the new module still uses `_2.12` artifact suffix (it's a template);
`./build/make-scala-version-build-files.sh 2.13` generates the actual `scala2.13/` mirror with
`_2.13` suffix. **Always run this script after any pom.xml change.**

---

## 3. Architecture Overview

### 3.1 Delta Lake Module Structure

```
delta-lake/
├── common/src/main/
│   ├── scala/                   # Universal: DeltaProviderImplBase, RapidsDeltaWrite, UDFs
│   ├── databricks/scala/        # Databricks-shared (17 files): DatabricksDeltaProviderBase,
│   │                            #   GpuDeltaLog, DeleteCommandMeta, etc.
│   └── delta-33x-40x/scala/    # Shared for OSS Delta 3.3+/4.0+ (DV common code)
├── delta-spark350db143/         # DB-14.3 ← PRIMARY COPY SOURCE
├── delta-spark400db173/         # NEW (to be created)
└── delta-stub/                  # Currently used by DB-17.3 (to be replaced)
```

### 3.2 Entry Point Chain

```
ShimLoader (SPI) → DeltaProbeImpl → DeltaSpark400DB173Provider
    → DatabricksDeltaProviderBase (common/databricks/scala)
        → GpuWriteIntoDelta / GpuDeltaCatalog / GPU DML commands
```

### 3.3 Files in a Databricks Delta Module (21 total)

Each `delta-spark{N}db` module contains:

**`com/nvidia/spark/rapids/delta/` (3 files):**
- `DeltaProbe.scala` — SPI entry point, returns the provider
- `DeltaSpark{N}Provider.scala` — extends `DatabricksDeltaProviderBase`
- `GpuDeltaParquetFileFormat.scala` — GPU Parquet file format (DV read support in DB-17.3; CPU fallback in DB-14.3)

**`com/databricks/sql/transaction/tahoe/rapids/` (11 files):**
- `GpuOptimisticTransactionBase.scala`
- `GpuOptimisticTransaction.scala`
- `GpuWriteIntoDelta.scala`
- `GpuDeleteCommand.scala`
- `GpuUpdateCommand.scala`
- `GpuMergeIntoCommand.scala`
- `GpuCreateDeltaTableCommand.scala`
- `GpuDeltaDataSource.scala`
- `GpuDeltaCatalog.scala`
- `GpuDoAutoCompaction.scala`
- `GpuOptimizeExecutor.scala`

**`com/nvidia/spark/rapids/delta/shims/` (7 files):**
- `DeleteCommandMetaShim.scala`
- `UpdateCommandMetaShim.scala`
- `MergeIntoCommandMetaShim.scala`
- `DeltaLogShim.scala`
- `MetadataShims.scala`
- `ShimDeltaUDF.scala`
- `InvariantViolationExceptionShim.scala`

---

## 4. CRITICAL: DB-17.3 Deletion Vector API — Confirmed via Jar Decompilation

### 4.1 What Was Decompiled

JAR locations on the cluster:
```
/databricks/jars/spark-sql_2.13-4.0.0-databricks-173.jar
/databricks/jars/spark-catalyst_2.13-4.0.0-databricks-173.jar
```
(Decompile command: `javap -p com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat`)

### 4.2 DB-14.3 vs DB-17.3 DV API Comparison

| Area | DB-14.3 | DB-17.3 (confirmed) |
|------|---------|---------------------|
| `DeltaParquetFileFormat` constructor | `(relation, columnMappingMode, referenceSchema, isSplittable, disablePushDowns, broadcastDvMap, tablePath, broadcastHadoopConf)` — 8 params | `(protocol, metadata, generateRowIndexFilterId, generateRowIndexFilterColumn, generateDeltaFileInScanId, nullableRowTrackingConstantFields, nullableRowTrackingGeneratedFields, optimizationsEnabled, tablePath, isCDCRead)` — 10 params |
| `broadcastDvMap` | Present | **Gone** — does not exist |
| `broadcastHadoopConf` | Present | **Gone** — does not exist |
| DV loading mechanism | Broadcast `Map[URI, DeletionVectorDescriptorWithFilterType]` | Per-file via `PartitionedFile.otherConstantMetadataColumnValues` |
| `TahoeFileIndex.rowIndexFilters` type | `Map[String, RowIndexFilterType]` | `Map[String, RowIndexFilterProvider]` |
| `FILE_ROW_INDEX_FILTER_ID_ENCODED` | Not present | Present as **public** constant on companion |
| `FILE_ROW_INDEX_FILTER_TYPE` | Not present | Present as **public** constant on companion |
| DV filter type accessor | `RowIndexFilterType` enum only | `RowIndexFilterProvider` interface |
| Raw bitmap bytes | Not exposed | `SerializedBitmap.buffer(): byte[]` |
| `RowIndexFilter.materializeIntoVectorWithRowIndex` | Not present | **Present** — new method |
| `DeletionVectorUtils.deletionVectorsWritable` | `(SnapshotDescriptor)` | Overloads: `(SnapshotDescriptor, Option[Protocol], Option[Metadata])` and `(Protocol, Metadata)` |
| `DELETE_USE_PERSISTENT_DELETION_VECTORS` in `DeltaSQLConf` | Present | **Confirmed present** |
| `UPDATE_USE_PERSISTENT_DELETION_VECTORS` in `DeltaSQLConf` | Present | **Confirmed present** |
| `DropMarkedRowsFilter.createInstance` | `(DeletionVectorDescriptor, Configuration, Option[Path])` | **Same API confirmed** |
| `KeepMarkedRowsFilter.createInstance` | `(DeletionVectorDescriptor, Configuration, Option[Path])` | **Same API confirmed** |

### 4.3 Confirmed DB-17.3 Companion Object Constants

```
com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat (companion):
  public static java.lang.String FILE_ROW_INDEX_FILTER_ID_ENCODED();  // PUBLIC
  public static java.lang.String FILE_ROW_INDEX_FILTER_TYPE();         // PUBLIC
  public static java.lang.String IS_ROW_DELETED_COLUMN_NAME();
  public static java.lang.String ROW_INDEX_COLUMN_NAME();
```

### 4.4 Confirmed DB-17.3 New Interfaces

```scala
// com.databricks.sql.io.RowIndexFilterProvider
interface RowIndexFilterProvider {
  def retrieve(conf: Configuration): RowIndexFilter
  def retrieveSerialized(conf: Configuration): SerializedBitmap  // NEW
  def getRowIndexFilterType(): RowIndexFilterType
}

// com.databricks.sql.transaction.tahoe.storage.dv.SerializedBitmap
class SerializedBitmap {
  def buffer(): Array[Byte]  // raw serialized bitmap bytes
}

// com.databricks.sql.io.RowIndexFilterType (enum values)
IF_CONTAINED, IF_NOT_CONTAINED, CONSTANT_TRUE_VECTOR_FILTER,
CONSTANT_FALSE_VECTOR_FILTER, MOD_SHARD, UNKNOWN

// com.databricks.sql.io.RowIndexFilter (added method in DB-17.3)
def materializeIntoVectorWithRowIndex(
  numRows: Int,
  rowIndexCol: ColumnVector,
  output: WritableColumnVector
): Unit
```

### 4.5 `DeltaParquetFileFormat` Instance Methods Confirmed

```scala
def fileConstantMetadataExtractors(): Map[String, PartitionedFile => Object]
def copyWithDVInfo(tablePath: String, hasFullDV: Boolean): DeltaParquetFileFormat
def tablePath: Option[String]  // field
```

### 4.6 DB-14.3 Has NO DV GPU Support (Corrected Understanding)

**Important correction:** The DB-14.3 RAPIDS plugin (`delta-spark350db143`) does **not**
support DV reads or writes on GPU. When deletion vectors are present:

- `GpuDeltaParquetFileFormat.tagSupportForGpuFileSourceScan()` calls
  `willNotWorkOnGpu("deletion vectors are not supported")` when `format.hasDeletionVectorMap`
  is true — the entire FileSourceScan falls back to CPU.
- `GpuDeleteCommand` and `GpuUpdateCommand` hardcode all DV metrics to 0 with comments
  `// We don't support deletion vectors`.
- `DeleteCommandMetaShim` and `UpdateCommandMetaShim` block GPU when
  `DELETE/UPDATE_USE_PERSISTENT_DELETION_VECTORS` is enabled.

**All DV GPU acceleration in the RAPIDS plugin exists exclusively in the OSS `delta-33x` and
`delta-40x` modules**, via the shared code in `delta-lake/common/src/main/delta-33x-40x/scala/`.
This code cannot be directly reused by Databricks modules because:

1. It imports from the OSS namespace (`org.apache.spark.sql.delta`), not the Databricks
   namespace (`com.databricks.sql.transaction.tahoe`)
2. It depends on OSS-only classes: `HadoopFileSystemDVStore`, `StoredBitmap`,
   `RoaringBitmapArray` — none of which exist in Databricks
3. RAPIDS utility classes (`RapidsDeletionVectorStore`, `RapidsStoredBitmap`) are placed
   in the `org.apache.spark.sql.delta.deletionvectors` package (OSS namespace)
4. Databricks module pom.xml files include `common/src/main/databricks/scala/` but **NOT**
   `common/src/main/delta-33x-40x/scala/`

**Consequence:** DB-17.3 DV GPU read support is entirely new functionality — not a port
from DB-14.3 or reuse of OSS code. It should use DB-17.3's native `RowIndexFilterProvider`
API rather than reimplementing the OSS disk-loading chain.

---

## 5. How to Implement `GpuDeltaParquetFileFormat` for DB-17.3

### 5.1 What NOT to Do

1. Do **NOT** copy DB-14.3's `GpuDeltaParquetFileFormat` for DB-17.3. The DB-14.3 file:
   - Takes `broadcastDvMap: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]]` — **this class/param does not exist in DB-17.3**
   - Tags GPU as unsupported when `hasDeletionVectorMap` — **wrong for DB-17.3** (DB-17.3 should support DV reads on GPU)
   - Has no `createMultiFileReaderFactory` — **DB-17.3 should support multi-threaded reader**

2. Do **NOT** try to directly reuse the OSS `GpuDeltaParquetFileFormatBase` from
   `delta-33x-40x`. The OSS code:
   - Imports from `org.apache.spark.sql.delta` (won't compile against Databricks namespace)
   - Depends on OSS-only classes: `HadoopFileSystemDVStore`, `StoredBitmap`, `RoaringBitmapArray`
   - Loads DVs from disk independently (bypasses `RowIndexFilterProvider`)
   - DB-17.3 module pom.xml includes `databricks/scala/` but NOT `delta-33x-40x/scala/`

### 5.2 Architectural Reference: OSS `GpuDeltaParquetFileFormatBase`

The OSS `GpuDeltaParquetFileFormatBase` in
`delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/`
is the **architectural reference** — it implements the same per-file DV pattern that DB-17.3
uses. Study its structure but write new Databricks-specific code:

**OSS approach (for understanding):**
- Reads `FILE_ROW_INDEX_FILTER_ID_ENCODED` and `FILE_ROW_INDEX_FILTER_TYPE` from
  `partitionedFile.otherConstantMetadataColumnValues`
- Decodes `DeletionVectorDescriptor` from Base64
- Loads bitmap from disk via `HadoopFileSystemDVStore` + `StoredBitmap.load()`
- Wraps in `RapidsDropMarkedRowsFilter` / `RapidsKeepMarkedRowsFilter`
- Materializes via cuDF `contains()` into a GPU column vector
- Implements `createMultiFileReaderFactory` → `DeltaMultiFileReaderFactory`

**DB-17.3 approach (use instead):**
- Use DB-17.3's native `RowIndexFilterProvider` API to load DVs (avoids reimplementing
  the OSS disk-loading chain with Databricks namespace equivalents)
- Extract `RowIndexFilterProvider` from `PartitionedFile.otherConstantMetadataColumnValues`
  using `FILE_ROW_INDEX_FILTER_ID_ENCODED` and `FILE_ROW_INDEX_FILTER_TYPE` constants
- Call `RowIndexFilterProvider.retrieve(conf)` → `RowIndexFilter` for CPU-side DV materialization
- Use `RowIndexFilter.materializeIntoVectorWithRowIndex()` to populate deletion column
- Alternatively, use `DropMarkedRowsFilter.createInstance()` / `KeepMarkedRowsFilter.createInstance()`
  from `com.databricks.sql.transaction.tahoe.deletionvectors` (confirmed same API as DB-14.3)

| DB-17.3 API | Purpose |
|-------------|---------|
| `RowIndexFilterProvider.retrieve(conf)` → `RowIndexFilter` | Load DV, get CPU filter |
| `RowIndexFilter.materializeIntoVectorWithRowIndex(numRows, rowIndexCol, output)` | Materialize DV as boolean column |
| `RowIndexFilterProvider.retrieveSerialized(conf)` → `SerializedBitmap.buffer()` | Raw bitmap bytes (for future cuDF-native path) |
| `RowIndexFilterProvider.getRowIndexFilterType()` | `IF_CONTAINED` / `IF_NOT_CONTAINED` |
| `DropMarkedRowsFilter.createInstance(dvDesc, conf, tablePath)` | Create "drop marked rows" filter |
| `KeepMarkedRowsFilter.createInstance(dvDesc, conf, tablePath)` | Create "keep marked rows" filter |

### 5.3 How `DeltaSpark400DB173Provider.convertToGpu` Should Construct the Format

```scala
// DB-14.3 (WRONG for DB-17.3):
case fmt: DeltaParquetFileFormat =>
  GpuDeltaParquetFileFormat(fmt.columnMappingMode, ..., fmt.broadcastDvMap,
                            fmt.tablePath, fmt.broadcastHadoopConf)

// DB-17.3 (CORRECT):
case fmt: DeltaParquetFileFormat =>
  GpuDeltaParquetFileFormat(fmt.protocol, fmt.metadata,
                            fmt.generateRowIndexFilterId,
                            fmt.generateRowIndexFilterColumn,
                            fmt.generateDeltaFileInScanId,
                            fmt.nullableRowTrackingConstantFields,
                            fmt.nullableRowTrackingGeneratedFields,
                            fmt.optimizationsEnabled,
                            fmt.tablePath,
                            fmt.isCDCRead)
```

### 5.4 Multi-Threaded Reader

The OSS `GpuDeltaParquetFileFormatBase` implements `createMultiFileReaderFactory` for
multi-threaded GPU Parquet reading with DV support. DB-17.3's `GpuDeltaParquetFileFormat`
should implement its own version of this, using the `RowIndexFilterProvider` API for DV
loading rather than the OSS disk-loading chain. This enables the multi-threaded Parquet
reader for DV tables.

Architectural reference (OSS, not directly reusable):
`delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala`

### 5.5 GPU-Native cuDF DV API (Follow-Up PR)

`GpuDeltaParquetFileFormatBase2` (also in `delta-33x-40x`) uses cuDF-native bitmap APIs:
- `DeletionVector.newParquetChunkedReader(HostMemoryBuffer)`
- `MakeParquetTableWithDVProducer`

For DB-17.3, `RowIndexFilterProvider.retrieveSerialized(conf).buffer(): byte[]` could
provide the raw bitmap bytes to feed into this GPU-native path. However, the serialization
format compatibility needs verification before enabling. Defer to a separate follow-up PR.

---

## 6. Key DB-14.3 → DB-17.3 Command Differences (Non-DV)

These are already in the DB-14.3 baseline (no change needed):

| Area | DB-14.3 (baseline) | DB-17.3 (same) |
|------|--------------------|--------------------|
| Write command | `WriteIntoDeltaEdge` | Same |
| `OptimisticTransaction` constructor | `(deltaLog, Option.empty[CatalogTable], snapshot)` | Same |
| DV metrics in Delete/Update | 3: `numDVAdded/Removed/Updated` | Same |
| `filterFiles` in Merge | `filterFiles(predicates, keepNumRecords=true)` | Same |
| Encoder construction | `ExpressionEncoder(RowEncoder.encoderFor(schema))` | Same |
| Schema attributes | `toAttributes(schema)` (via `DataTypeUtils`) | Same |
| `GpuDeltaCatalog.getWriter()` | Uses `WriteIntoDeltaEdge` | Same |
| `GpuLowShuffleMergeCommand` | Removed | N/A |
| DV GPU read support | **None** (CPU fallback via `willNotWorkOnGpu`) | **New** (via `RowIndexFilterProvider` — first Databricks DV GPU support) |
| DV GPU write support | None (CPU fallback) | None (CPU fallback, same as DB-14.3) |

**Unknowns requiring compile-time verification:**

1. **`SparkSession` aliasing** — OSS `delta-40x` imports
   `org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}`. DB-17.3 may need this.
2. **`DFUDFShims`** — OSS `delta-40x` uses `org.apache.spark.sql.nvidia.DFUDFShims` for
   UDF calls in `GpuMergeIntoCommand`. Check DB-17.3.
3. **`GpuFileFormatWriter.write()` signature** — cross-check with OSS `delta-40x`.
4. **`GpuOptimizeExecutor` clustering APIs** — `MultiDimClustering`, `ClusteringColumnInfo`
   may have changed packages.
5. **`RowTracking` import** — verify unchanged.

---

## 7. Build System Changes Required

### 7.1 Root `pom.xml` (inside `#if scala-2.13` block, ~lines 633–654)

```diff
 <properties>
     <spark.version.classifier>spark400db173</spark.version.classifier>
-    <rapids.delta.artifactId1>rapids-4-spark-delta-stub</rapids.delta.artifactId1>
+    <rapids.delta.artifactId1>rapids-4-spark-delta-spark400db173</rapids.delta.artifactId1>
 </properties>
 <modules>
     <module>shim-deps/databricks</module>
-    <module>delta-lake/delta-stub</module>
+    <module>delta-lake/delta-spark400db173</module>
 </modules>
```

### 7.2 New Module `delta-lake/delta-spark400db173/pom.xml`

Copy from `delta-lake/delta-spark350db143/pom.xml`, changing:
- `artifactId`: `rapids-4-spark-delta-spark400db173_2.12`
- `name`: "RAPIDS Accelerator for Apache Spark Databricks 17.3 Delta Lake Support"
- Parent, description, build-helper source dirs: identical to DB-14.3

### 7.3 Generate `scala2.13/` Mirror

```bash
./build/make-scala-version-build-files.sh 2.13
```

Run after any pom.xml change. Both the primary and `scala2.13/` poms must be committed.

---

## 8. Step-by-Step Implementation Plan

```bash
# Step 1: Scaffold
cp -r delta-lake/delta-spark350db143 delta-lake/delta-spark400db173

# Step 2: Rename all 350db143 → 400db173 occurrences
cd delta-lake/delta-spark400db173
find . -name "*350db143*" | while read f; do mv "$f" "${f//350db143/400db173}"; done
grep -rl "350db143" . | xargs sed -i 's/350db143/400db173/g'
grep -rl "DB143\|DB14\.3\|db143" . | xargs sed -i 's/DB143/DB173/g; s/DB14\.3/DB17\.3/g; s/db143/db173/g'
# Update copyright year to 2026 if needed
```

```bash
# Step 3: REDESIGN GpuDeltaParquetFileFormat.scala for DB-17.3
# See Section 5 above — model on GpuDeltaParquetFileFormatBase (delta-33x-40x)
# Do NOT use DB-14.3's broadcastDvMap approach
```

```bash
# Step 4: Update root pom.xml
# Apply diff from Section 7.1 to pom.xml (inside #if scala-2.13 block)
```

```bash
# Step 5: Sync scala2.13/
./build/make-scala-version-build-files.sh 2.13
```

```bash
# Step 6: Compile (must use -f scala2.13)
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests \
    -pl delta-lake/delta-spark400db173 -am \
    -Dmaven.scalastyle.skip=true  # skip for initial compile iteration
```

```bash
# Step 7: Fix compile errors
# Compare against delta-lake/delta-40x/ for Spark 4.0 API patterns
# Key unknowns: SparkSession aliasing, DFUDFShims, GpuFileFormatWriter.write() signature
```

```bash
# Step 8: Full build
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests
```

```bash
# Step 9: Run tests
mvn -f scala2.13 -Dbuildver=400db173 package -pl tests -am \
    -DwildcardSuites="com.nvidia.spark.rapids.delta.*"
```

---

## 9. Key Reference File Locations

| Purpose | Path |
|---------|------|
| **Primary copy source (DB-14.3)** | `delta-lake/delta-spark350db143/` |
| `scala2.13/` mirror of DB-14.3 | `scala2.13/delta-lake/delta-spark350db143/` |
| Databricks-shared common code (NO DV support) | `delta-lake/common/src/main/databricks/scala/` |
| Universal Delta common code | `delta-lake/common/src/main/scala/` |
| **OSS DV arch. reference (NOT directly reusable)** | `delta-lake/common/src/main/delta-33x-40x/scala/.../GpuDeltaParquetFileFormatBase.scala` |
| OSS cuDF-native DV (follow-up reference) | `delta-lake/common/src/main/delta-33x-40x/scala/.../GpuDeltaParquetFileFormatBase2.scala` |
| OSS DV bitmap loading (NOT reusable) | `delta-lake/common/src/main/delta-33x-40x/scala/.../RapidsDeletionVectorStore.scala` |
| OSS DV row index filters (NOT reusable) | `delta-lake/common/src/main/delta-33x-40x/scala/.../RapidsRowIndexFilters.scala` |
| OSS Spark 4.0 Delta (Spark 4.0 API patterns) | `delta-lake/delta-40x/` |
| `scala2.13/` mirror of OSS 4.0 Delta | `scala2.13/delta-lake/delta-40x/` |
| DB-17.3 SQL plugin shims | `sql-plugin/src/main/spark400db173/` |
| Root pom.xml (edit inside #if scala-2.13) | `pom.xml` lines ~633–654 |
| Scala 2.13 root pom | `scala2.13/pom.xml` |
| Scala version sync script | `build/make-scala-version-build-files.sh` |
| **Full design document** | `delta-lake-db173-design.md` |

---

## 10. Completed Research (Do Not Repeat)

- [x] Explored all `delta-lake/delta-spark*db*/` module structures
- [x] Read all 21 files in `delta-lake/delta-spark350db143/` (primary reference)
- [x] Read `GpuDeltaParquetFileFormatBase.scala` and `GpuDeltaParquetFileFormatBase2.scala`
- [x] Read `RapidsDeletionVectorStore.scala` and `RapidsStoredBitmap.scala`
- [x] Read `delta-lake/delta-40x/` OSS Spark 4.0 wrappers
- [x] Read `delta-lake/common/src/main/databricks/scala/` shared files
- [x] Decompiled `spark-sql_2.13-4.0.0-databricks-173.jar` — confirmed DV API
- [x] Decompiled `spark-catalyst_2.13-4.0.0-databricks-173.jar` — confirmed DeltaSQLConf
- [x] Verified `DELETE/UPDATE_USE_PERSISTENT_DELETION_VECTORS` present in DB-17.3
- [x] Verified `DropMarkedRowsFilter`/`KeepMarkedRowsFilter` API unchanged in DB-17.3
- [x] Updated design doc (`delta-lake-db173-design.md`) with confirmed findings
- [x] Updated plan file with critical DV API differences
- [x] **Confirmed DB-14.3 has NO DV GPU support** — `tagSupportForGpuFileSourceScan()` blocks
  GPU when `hasDeletionVectorMap` is true; all DV GPU code is in OSS `delta-33x-40x` only
- [x] **Confirmed OSS DV code cannot be reused by Databricks modules** — namespace mismatch
  (`org.apache.spark.sql.delta` vs `com.databricks.sql.transaction.tahoe`), OSS-only classes
  (`HadoopFileSystemDVStore`, `StoredBitmap`, `RoaringBitmapArray`), and build system separation
- [x] **Confirmed DB-17.3 should use `RowIndexFilterProvider` API** for DV loading instead of
  reimplementing the OSS disk-loading chain

---

## 11. Scope Clarification (This PR)

**In scope (new for DB-17.3):**
- DV **reads** on GPU — DB-17.3 will be the **first Databricks runtime** with GPU-accelerated
  DV reads in the RAPIDS plugin, using the `RowIndexFilterProvider` API. This is new
  functionality, not a port from DB-14.3 (which has no DV GPU support).

**Out of scope:**
- DV **writes** on GPU — CPU fallback retained (shims already block GPU for DV write configs)
- cuDF-native DV read path (equivalent to OSS `GpuDeltaParquetFileFormatBase2` using
  `DeletionVector.newParquetChunkedReader`) — follow-up PR. The `SerializedBitmap.buffer()`
  API in DB-17.3 could feed into cuDF's native bitmap APIs, but serialization format
  compatibility needs verification.
- `notMatchedBySourceClauses` in MERGE — CPU fallback retained (issue #8415)
- Scala 2.12 support — Spark 4.0 / DB-17.3 does not support it
