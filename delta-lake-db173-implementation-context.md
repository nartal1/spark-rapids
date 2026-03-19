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
- `GpuDeltaParquetFileFormat.scala` — DV read support for GPU Parquet scans

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

### 4.1 What Was Decompiled / Verified

**Original decompilation (2026-03-19):**
```
/databricks/jars/spark-sql_2.13-4.0.0-databricks-173.jar
/databricks/jars/spark-catalyst_2.13-4.0.0-databricks-173.jar
```

**Live cluster verification (2026-04-07):**

On the DB-17.3 cluster, the JARs use a different naming convention:
```
/databricks/jars/----ws_4_0--sql--core--core-hive-2.3__hadoop-3.2_2.13_deploy.jar      (core + Delta)
/databricks/jars/----ws_4_0--sql--catalyst--catalyst-hive-2.3__hadoop-3.2_2.13_deploy.jar (catalyst)
/databricks/jars/----ws_4_0--sql--api--sql-api-hive-2.3__hadoop-3.2_2.13_deploy.jar      (SQL API)
```

Decompile commands used:
```bash
javap -p -classpath "/databricks/jars/----ws_4_0--sql--core--core-hive-2.3__hadoop-3.2_2.13_deploy.jar" \
  com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat
# For classes in other JARs, use wildcard classpath:
javap -classpath "/databricks/jars/*" com.databricks.sql.transaction.tahoe.deletionvectors.RoaringBitmapArray
```

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
| `RoaringBitmapArray` | `o.a.s.sql.delta.deletionvectors.RoaringBitmapArray` | `c.d.s.t.tahoe.deletionvectors.RoaringBitmapArray` — **confirmed via javap** (same API) |
| `StoredBitmap.load()` return type | `RoaringBitmapArray` | `RoaringBitmapArray` — **confirmed** |
| `HadoopFileSystemDVStore.read()` | `(DeletionVectorDescriptor, Path): RoaringBitmapArray` | **Same API confirmed** |
| `PortableRoaringBitmapArraySerializationFormat` | `MAGIC_NUMBER`, `serialize`, `deserialize` | **Same API confirmed** |
| `NativeRoaringBitmapArraySerializationFormat` | `MAGIC_NUMBER`, `serialize`, `deserialize` | **Same API confirmed** |
| `DeletionVectorStoreEdge.createInstance` | N/A | `(Configuration, Option[DeletionVectorCacheConfig])` — **confirmed** |

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

---

## 5. How to Implement `GpuDeltaParquetFileFormat` for DB-17.3

### 5.1 What NOT to Do

Do **NOT** copy DB-14.3's `GpuDeltaParquetFileFormat` for DB-17.3. The DB-14.3 file:
- Takes `broadcastDvMap: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]]` — **this class/param does not exist in DB-17.3**
- Tags GPU as unsupported when `hasDeletionVectorMap` — **wrong for DB-17.3**
- Has no `createMultiFileReaderFactory` — **DB-17.3 should support multi-threaded reader**

### 5.2 Blueprint: `GpuDeltaParquetFileFormatBase` (OSS)

Use `delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala`
as the **pattern/blueprint** (NOT as compiled source). It implements the per-file DV approach:

- Reads `FILE_ROW_INDEX_FILTER_ID_ENCODED` and `FILE_ROW_INDEX_FILTER_TYPE` from
  `partitionedFile.otherConstantMetadataColumnValues`
- Uses `DropMarkedRowsFilter`/`KeepMarkedRowsFilter` from OSS namespace
- Implements `createMultiFileReaderFactory` → `DeltaMultiFileReaderFactory`
- GPU DV reads are supported (no GPU-unsupported tagging)

**⚠ CRITICAL: Cannot include `delta-33x-40x` sources via pom.xml.**

The `delta-33x-40x` common code imports from `org.apache.spark.sql.delta.*` (OSS namespace),
which **does not exist** in DB-17.3. DB-17.3 only provides the
`com.databricks.sql.transaction.tahoe.*` namespace. Verified on cluster: `jar tf` of the
DB-17.3 core JAR contains zero `org/apache/spark/sql/delta/` classes.

The DB-17.3 pom.xml correctly includes only:
- `delta-lake/common/src/main/scala` (universal)
- `delta-lake/common/src/main/databricks/scala` (Databricks-shared)

For DB-17.3, substitute the Databricks namespace equivalents (all confirmed on cluster):

| OSS (`org.apache.spark.sql.delta`) | Databricks (`com.databricks.sql.transaction.tahoe`) |
|------------------------------------|-----------------------------------------------------|
| `DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED` | Same constant, different package |
| `DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE` | Same constant, different package |
| `deletionvectors.RoaringBitmapArray` | `deletionvectors.RoaringBitmapArray` — **confirmed** |
| `deletionvectors.StoredBitmap` | `deletionvectors.StoredBitmap` — **confirmed** |
| `deletionvectors.DropMarkedRowsFilter` | `deletionvectors.DropMarkedRowsFilter` — **confirmed** |
| `deletionvectors.KeepMarkedRowsFilter` | `deletionvectors.KeepMarkedRowsFilter` — **confirmed** |
| `storage.dv.HadoopFileSystemDVStore` | `storage.dv.HadoopFileSystemDVStore` — **confirmed** |
| `actions.DeletionVectorDescriptor` | `actions.DeletionVectorDescriptor` — **confirmed** |
| `RowIndexFilter.materializeIntoVectorWithRowIndex` | `c.d.sql.io.RowIndexFilter.materializeIntoVectorWithRowIndex` — **confirmed** |
| `RowIndexFilterProvider` | `c.d.sql.io.RowIndexFilterProvider` — **confirmed** |
| `PortableRoaringBitmapArraySerializationFormat` | `deletionvectors.PortableRoaringBitmapArraySerializationFormat` — **confirmed** |
| `NativeRoaringBitmapArraySerializationFormat` | `deletionvectors.NativeRoaringBitmapArraySerializationFormat` — **confirmed** |
| `sources.DeltaSQLConf` | `sources.DeltaSQLConf` — **confirmed** |

**Note: DB-14.3 has zero GPU DV read support.** DB-14.3's `GpuDeltaParquetFileFormat`
explicitly tags `"deletion vectors are not supported"` when `hasDeletionVectorMap` is true.
DB-17.3 will be the **first** Databricks module with GPU-accelerated DV reads.

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

`GpuDeltaParquetFileFormatBase` already implements `createMultiFileReaderFactory`.
DB-17.3's `GpuDeltaParquetFileFormat` should also implement it (directly or by extending
the base class). This enables the multi-threaded Parquet reader for DV tables.

See: `delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala`

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

| Area | DB-14.3 (baseline) | DB-17.3 |
|------|--------------------|---------|
| Write command | `WriteIntoDeltaEdge` | Same (but constructor grew to 13 params; new params have defaults) |
| `OptimisticTransaction` constructor | `(deltaLog, Option.empty[CatalogTable], snapshot)` | Same — confirmed |
| `writeFiles()` override | `(Dataset, Option[DeltaOptions], Seq[Constraint])` | **CHANGED**: `(Dataset, TransactionalWriteOptions, Boolean, Boolean, Seq[Constraint], Boolean, Option[String])` |
| `TransactionalWriteOptions` | Does not exist | **NEW** case class wrapping `DeltaOptions` + optimize/validate flags |
| `MergeIntoCommandEdge` constructor | `(source, target, fileIndex, condition, clauses..., migratedSchema)` | **CHANGED**: added `catalogTable`, `targetFileIndex`, `trackHighWaterMarks`, `schemaEvolutionEnabled` |
| `DeleteCommandEdge` constructor | `(fileIndex, target, condition)` | **CHANGED**: added `catalogTable` |
| `UpdateCommandEdge` constructor | `(fileIndex, target, updateExprs, condition)` | **CHANGED**: added `catalogTable` |
| `Column(Expression)` constructor | Present | **REMOVED** — use `DFUDFShims.exprToColumn(expr)` |
| `Dataset.ofRows(spark, plan)` | Present | Needs `TrampolineConnectShims.createDataFrame()` |
| `DeletionVectorUtils` | Object | **Changed to trait+companion** (calls still work via defaults) |
| DV metrics in Delete/Update | 3: `numDVAdded/Removed/Updated` | Same |
| `filterFiles` in Merge | `filterFiles(predicates, keepNumRecords=true)` | Same |
| Encoder construction | `ExpressionEncoder(RowEncoder.encoderFor(schema))` | Same |
| Schema attributes | `toAttributes(schema)` (via `DataTypeUtils`) | Same |
| `GpuDeltaCatalog.getWriter()` | Uses `WriteIntoDeltaEdge` | Same |
| `GpuLowShuffleMergeCommand` | Removed | N/A |

**Previously listed as unknowns — all resolved via DB-17.3 cluster verification (2026-04-07):**

1. **`SparkSession` aliasing** — **CONFIRMED.** `org.apache.spark.sql.classic.SparkSession`
   present in DB-17.3. `TrampolineConnectShims` in `sql-plugin/src/main/spark400/` already
   includes `{"spark": "400db173"}`. DB-17.3 command files WILL use this.
2. **`DFUDFShims`** — **CONFIRMED.** `DFUDFShims` in `sql-plugin/src/main/spark400/` already
   includes `{"spark": "400db173"}`. Needed for `GpuMergeIntoCommand` UDF invocations.
3. **`GpuFileFormatWriter.write()` signature** — **CONFIRMED.** `GpuFileFormatWriter` in
   `sql-plugin/src/main/spark332db/` already includes `{"spark": "400db173"}`. No change needed.
4. **`GpuOptimizeExecutor` clustering APIs** — **CONFIRMED.** Same packages on cluster:
   `com.databricks.sql.io.skipping.MultiDimClustering` and
   `com.databricks.sql.io.skipping.liquid.ClusteringColumnInfo`. No import changes.
5. **`RowTracking` import** — **CONFIRMED.** `com.databricks.sql.transaction.tahoe.RowTracking`
   present on DB-17.3 cluster. Import path unchanged from DB-14.3.

Additional classes confirmed on the DB-17.3 cluster:
- `WriteIntoDelta` AND `WriteIntoDeltaEdge` — both present
- `AtomicCreateTableAsSelectExec` / `AtomicReplaceTableAsSelectExec` — confirmed
- `OptimizeExecutor` at `com.databricks.sql.transaction.tahoe.commands.optimize.OptimizeExecutor`
- `DeltaInvariantViolationException` at `com.databricks.sql.transaction.tahoe.schema`
- `DeltaUDF` at `com.databricks.sql.transaction.tahoe.DeltaUDF`
- `DeltaStatistics` at `com.databricks.sql.transaction.tahoe.stats.DeltaStatistics`
- `DataTypeUtils` at `org.apache.spark.sql.catalyst.types.DataTypeUtils`
- `ExpressionEncoder` / `RowEncoder` — confirmed in catalyst JAR
- `DeltaSQLConf` at `com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf`

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
# Step 7: Fix Spark 4.0 API incompatibilities (CRITICAL — see design doc §7.1)
# These are NOT unknowns — they are confirmed required changes:
#
# 7a. Replace `new Column(expr)` → `DFUDFShims.exprToColumn(expr)`
#     - GpuMergeIntoCommand.scala (~9 occurrences)
#     - GpuDeleteCommand.scala (~2 occurrences)
#     - GpuUpdateCommand.scala (~3 occurrences)
#     Add: import org.apache.spark.sql.nvidia.DFUDFShims
#
# 7b. Replace `Dataset.ofRows(spark, plan)` → `TrampolineConnectShims.createDataFrame()`
#     - GpuOptimisticTransaction.scala (2 occurrences)
#     - GpuUpdateCommand.scala (2 occurrences)
#     - GpuMergeIntoCommand.scala (6 occurrences)
#     Add: import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
#
# 7c. Replace `SparkSession.getActiveSession` → `TrampolineConnectShims.getActiveSession`
#     - GpuDeltaParquetFileFormat.scala
#
# 7d. Add RuntimeReplaceable handling for stats expressions
#     - GpuOptimisticTransaction.scala
#     Pattern: expr.transform { case rr: RuntimeReplaceable => rr.replacement }
#
# 7e. MAJOR: writeFiles() signature completely changed in DB-17.3
#     DB-14.3: writeFiles(Dataset, Option[DeltaOptions], Seq[Constraint])
#     DB-17.3: writeFiles(Dataset, TransactionalWriteOptions, Boolean, Boolean, Seq[Constraint], Boolean, Option[String])
#     TransactionalWriteOptions wraps DeltaOptions + optimize/validate flags
#     Must rewrite GpuOptimisticTransaction.writeFiles() override
#
# 7f. MAJOR: Command Edge constructors changed in DB-17.3
#     MergeIntoCommandEdge: added catalogTable, targetFileIndex, trackHighWaterMarks, schemaEvolutionEnabled
#     DeleteCommandEdge: added catalogTable (tahoeFileIndex was already there)
#     UpdateCommandEdge: added catalogTable
#     Must update GPU command constructors + shim convertToGpu methods
#
# 7g. WriteIntoDeltaEdge constructor grew to 13 params (params 7-13 have defaults)
#     New: clusteringColumns, tableAliasOpt, snapshotAtAnalysis, jobGroupIdAtAnalysis, autoUpdateStatsSchemaOpt
#     Existing call sites likely compile due to defaults — verify
#
# 7h. DeletionVectorUtils changed from object to trait+companion
#     Existing calls still work via companion + default params — no code change needed
#
# 7i. Remove `(implicit clock: Clock)` from GpuOptimisticTransactionBase and GpuOptimisticTransaction
#     DB-17.3 OptimisticTransaction no longer takes implicit Clock
#     Clock is obtained internally from DeltaLog.clock()
#     The shared GpuDeltaLog._clock implicit is unused but harmless (lazy val)
#
# 7j. MAJOR: PostCommitHook.run() signature completely changed
#     DB-14.3: run(spark, txn, committedVersion, postCommitSnapshot, committedActions)
#     DB-17.3: run(spark, txn: CommittedTransaction)  — new bundled case class
#     Must rewrite GpuDoAutoCompaction.run() to extract fields from CommittedTransaction
#
# 7k. MAJOR: LogicalRelation pattern match arity changed from 4 to 7 fields
#     DB-17.3 added: sparkDataStream, injectedConstraints, statistics
#     Fix: add wildcard `_` for new fields in pattern matches
#     Files: GpuMergeIntoCommand.scala (~line 948), GpuOptimisticTransactionBase.scala (~line 178)
#
# 7l. MAJOR (SHARED CODE): ShuffledRowRDD constructor changed
#     File: common/databricks/scala/.../OptimizeWriteExchangeExec.scala
#     DB-17.3 requires PrismMetrics parameter — shared code passes 2-3 args only
#     Need version-specific override or shim in delta-spark400db173 module
#
# 7m. MAJOR (SHARED CODE): CheckDeltaInvariant.columnExtractors type changed
#     File: common/databricks/scala/.../GpuCheckDeltaInvariant.scala
#     DB-14.3: Map[String, Expression]   DB-17.3: Seq[(String, Expression)]
#     Need .toMap in shared code or version-specific override
#
# Reference: delta-lake/delta-40x/ shows the exact Spark 4.0 patterns
# Reference: design doc §7.1.6-7.1.9 for full details
```

```bash
# Step 8: Fix remaining compile errors
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests \
    -pl delta-lake/delta-spark400db173 -am \
    -Dmaven.scalastyle.skip=true
# Compare against delta-lake/delta-40x/ for any other Spark 4.0 API patterns
```

```bash
# Step 9: Full build
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests
```

```bash
# Step 10: Run tests
mvn -f scala2.13 -Dbuildver=400db173 package -pl tests -am \
    -DwildcardSuites="com.nvidia.spark.rapids.delta.*"
```

```bash
# Step 11: Enable Delta tests in CI
# Remove the skip in jenkins/databricks/test.sh (lines 152-160)
# that says "Skipping Delta Lake tests: not yet supported for DB-17.3"
```

---

## 9. Key Reference File Locations

| Purpose | Path |
|---------|------|
| **Primary copy source (DB-14.3)** | `delta-lake/delta-spark350db143/` |
| `scala2.13/` mirror of DB-14.3 | `scala2.13/delta-lake/delta-spark350db143/` |
| Databricks-shared common code | `delta-lake/common/src/main/databricks/scala/` |
| Universal Delta common code | `delta-lake/common/src/main/scala/` |
| **Blueprint for DV file format (OSS)** | `delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase.scala` |
| GPU-native cuDF DV API (follow-up) | `delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/GpuDeltaParquetFileFormatBase2.scala` |
| DV bitmap loading utility | `delta-lake/common/src/main/delta-33x-40x/scala/org/apache/spark/sql/delta/deletionvectors/RapidsDeletionVectorStore.scala` |
| DV RapidsRowIndexFilters (OSS) | `delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/RapidsRowIndexFilters.scala` |
| OSS Spark 4.0 Delta (Spark 4.0 API patterns) | `delta-lake/delta-40x/` |
| `scala2.13/` mirror of OSS 4.0 Delta | `scala2.13/delta-lake/delta-40x/` |
| DB-17.3 SQL plugin shims | `sql-plugin/src/main/spark400db173/` |
| Spark 4.0 shared shims (covers 400db173) | `sql-plugin/src/main/spark400/` |
| `TrampolineConnectShims` (covers 400db173) | `sql-plugin/src/main/spark400/scala/org/apache/spark/sql/rapids/shims/TrampolineConnectShims.scala` |
| `DFUDFShims` (covers 400db173) | `sql-plugin/src/main/spark400/scala/org/apache/spark/sql/nvidia/DFUDFShims.scala` |
| `GpuFileFormatWriter` (covers 400db173) | `sql-plugin/src/main/spark332db/scala/org/apache/spark/sql/rapids/GpuFileFormatWriter.scala` |
| **Spark 4.0 command shims (reference)** | `delta-lake/delta-40x/src/main/scala/.../Delta40xCommandShims.scala` |
| **Spark 4.0 runtime shim (reference)** | `delta-lake/delta-40x/src/main/scala/.../Delta40xRuntimeShim.scala` |
| **Jenkins test skip (to remove)** | `jenkins/databricks/test.sh` lines 152–160 |
| Root pom.xml (edit inside #if scala-2.13) | `pom.xml` lines ~633–654 |
| Scala 2.13 root pom | `scala2.13/pom.xml` |
| Scala version sync script | `build/make-scala-version-build-files.sh` |
| **Full design document** | `delta-lake-db173-design.md` |

---

## 10. Completed Research (Do Not Repeat)

### Original research (2026-03-19)
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

### Live DB-17.3 cluster verification (2026-04-07)
- [x] Confirmed actual JAR naming on cluster (`----ws_4_0--sql--core--...` format)
- [x] Verified all 5 "Spark 4.0 Unknowns" are resolved:
  - [x] `SparkSession` aliasing (`classic.SparkSession` exists)
  - [x] `DFUDFShims` (covers `400db173` in `spark400` shim)
  - [x] `GpuFileFormatWriter.write()` (covers `400db173` in `spark332db` shim)
  - [x] `MultiDimClustering`/`ClusteringColumnInfo` (same packages)
  - [x] `RowTracking` (same import path)
- [x] Confirmed `RoaringBitmapArray` exists in DB-17.3 at `c.d.s.t.tahoe.deletionvectors` (via `javap`;
  located in the **catalyst** JAR: `----ws_4_0--sql--catalyst--catalyst-hive-2.3__hadoop-3.2_2.13_deploy.jar`)
- [x] Confirmed `PortableRoaringBitmapArraySerializationFormat` / `NativeRoaringBitmapArraySerializationFormat`
- [x] Confirmed `StoredBitmap`, `DeletionVectorStoredBitmap`, `HadoopFileSystemDVStore`
- [x] Confirmed `DeletionVectorStore` / `DeletionVectorStoreEdge` interfaces
- [x] Confirmed `DeltaSQLConf` at `c.d.s.t.tahoe.sources.DeltaSQLConf`
- [x] Confirmed `delta-33x-40x` common code uses OSS namespace (`org.apache.spark.sql.delta.*`)
  which does NOT exist in DB-17.3 — cannot be included via pom.xml
- [x] Confirmed DB-14.3 has zero GPU DV read support (explicitly tags `"deletion vectors are not supported"`)
- [x] Verified all additional command/utility classes present on cluster:
  `WriteIntoDelta`, `WriteIntoDeltaEdge`, `AtomicCreateTableAsSelectExec`,
  `AtomicReplaceTableAsSelectExec`, `OptimizeExecutor`, `DeltaInvariantViolationException`,
  `DeltaUDF`, `DeltaStatistics`, `DataTypeUtils`, `ExpressionEncoder`, `RowEncoder`

### Critical review — additional findings (2026-04-07)
- [x] `PostCommitHook.run()` signature changed: `(spark, txn, version, snapshot, actions)` →
  `(spark, CommittedTransaction)` — must rewrite `GpuDoAutoCompaction.run()`
- [x] `LogicalRelation` unapply arity changed from 4 to 7 fields — pattern matches in
  `GpuMergeIntoCommand` (~line 948) and `GpuOptimisticTransactionBase` (~line 178) must update
- [x] **SHARED CODE ISSUE**: `ShuffledRowRDD` constructor in DB-17.3 requires `PrismMetrics` —
  `OptimizeWriteExchangeExec.scala` (shared) creates it with 2-3 args, must add override
- [x] **SHARED CODE ISSUE**: `CheckDeltaInvariant.columnExtractors` type changed from
  `Map[String, Expression]` to `Seq[(String, Expression)]` — `GpuCheckDeltaInvariant.scala`
  (shared) must handle this
- [x] `DeleteCommand.deltaLog` / `DeleteCommandEdge.deltaLog` — confirmed accessible via
  `DeltaDMLCommandEdge` parent in DB-17.3 (shared code is compatible)
- [x] `UpdateCommand.tahoeFileIndex` / `UpdateCommandEdge.tahoeFileIndex` — confirmed present
- [x] `MergeIntoCommand(Edge)` fields (source, target, targetFileIndex, clauses, etc.) — all confirmed
- [x] `DeltaStatistics` constants (NUM_RECORDS, MIN, MAX, NULL_COUNT) — confirmed same path
- [x] `DeltaUDF.stringFromString` — confirmed same signature
- [x] `InvariantViolationException` / `DeltaInvariantViolationException` — confirmed compatible
- [x] `DeleteMetric` constructor grew to 59 params (24 original + 35 with defaults) — compatible
- [x] `UpdateMetric` constructor grew to 58 params (11 original + 47 with defaults) — compatible
- [x] `TahoeBatchFileIndex` 6th param type changed Snapshot→SnapshotDescriptor (compatible via inheritance)
- [x] RoaringBitmap version mismatch: pom declares 1.0.6, cluster has 1.2.1 — safe due to shading
- [x] 15 of 17 shared databricks files are compatible; 2 need version-specific overrides

---

## 11. Out of Scope (This PR)

- DV **writes** on GPU — CPU fallback retained (shims already block GPU for DV write configs)
- `notMatchedBySourceClauses` in MERGE — CPU fallback retained (issue #8415)
- GPU-native cuDF DV API (`GpuDeltaParquetFileFormatBase2` pattern) — follow-up PR
- Scala 2.12 support — Spark 4.0 / DB-17.3 does not support it
