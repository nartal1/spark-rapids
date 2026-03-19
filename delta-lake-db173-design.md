# Design: Delta Lake GPU Support for Databricks 17.3 (DB-17.3)

**Author:** NVIDIA RAPIDS Accelerator Team
**Date:** 2026-03-19
**Status:** Draft
**Tracking Issue:** https://github.com/NVIDIA/spark-rapids/issues/14015

---

## 1. Overview

This document describes the design for adding Delta Lake GPU acceleration support for
Databricks Runtime 17.3 (DBR 17.3, Spark version `4.0.0-databricks-173`) in the RAPIDS
Accelerator for Apache Spark.

Currently, the `release400db173` Maven profile uses `delta-lake/delta-stub`, which provides
a no-op `NoDeltaProvider`. This means all Delta Lake DML operations (INSERT, DELETE, UPDATE,
MERGE INTO, OPTIMIZE) run exclusively on CPU even when the RAPIDS plugin is enabled.

---

## 2. Motivation

- DBR 17.3 is a current Databricks LTS-track runtime. Customers running workloads on DBR 17.3
  expect GPU acceleration for Delta Lake operations, consistent with prior supported runtimes
  (DBR 14.3, DBR 13.3, etc.).
- The GPU plugin already fully supports the DBR 17.3 SQL layer (`sql-plugin` shims are
  complete). Delta Lake support is the remaining gap.
- DBR 17.3 is based on Spark 4.0, which is also supported in the OSS `delta-40x` module.
  Many Spark 4.0 API adaptations are already present in `delta-spark350db143` (DBR 14.3),
  simplifying the port.

---

## 3. Background

### 3.1 Delta Lake Module Architecture

The RAPIDS plugin organizes Delta Lake support into version-specific Maven modules under
`delta-lake/`. Each module provides GPU implementations of Delta write/DML operations for
a specific Delta version or runtime pairing.

```
delta-lake/
├── common/                          # Shared source directories (injected via build-helper)
│   └── src/main/
│       ├── scala/                   # Universal: DeltaProviderImplBase, RapidsDeltaWrite, UDFs
│       └── databricks/scala/        # Databricks-shared: DatabricksDeltaProviderBase,
│                                    #   DeleteCommandMeta, GpuDeltaLog, GpuDeltaCatalogBase, ...
├── delta-33x/                       # OSS Delta 3.3.x (Spark 3.5.x)
├── delta-40x/                       # OSS Delta 4.0.x (Spark 4.0.x)
├── delta-spark330db/                # Databricks 11.3
├── delta-spark332db/                # Databricks 12.2
├── delta-spark341db/                # Databricks 13.3
├── delta-spark350db143/             # Databricks 14.3  ← primary reference
└── delta-stub/                      # No-op fallback (currently used by DB-17.3)
```

**Databricks modules do not declare an explicit Delta Lake dependency.** Delta Lake is
bundled with the Databricks Runtime and provided at runtime via the
`com.databricks.sql.transaction.tahoe` package namespace.

### 3.2 Module Entry Point

The plugin discovers Delta providers via the Java SPI in `ShimLoader`. For each supported
Databricks runtime, the corresponding `delta-spark{version}db` module provides:

```
DeltaProbeImpl.getDeltaProvider()  →  DeltaSpark{X}Provider
    extends DatabricksDeltaProviderBase   (common/databricks/scala)
        extends DeltaProviderImplBase     (common/scala)
```

### 3.3 Shared Common Code

All Databricks Delta modules share 17 files from `common/src/main/databricks/scala/`:

| File | Purpose |
|------|---------|
| `DatabricksDeltaProviderBase.scala` | GPU rule registration (Delete/Update/Merge/CreateTable commands) |
| `GpuDeltaLog.scala` | Wrapper around Databricks `DeltaLog` for GPU transaction management |
| `GpuDeltaCatalogBase.scala` | Base class for `GpuDeltaCatalog` |
| `GpuDeltaInvariantCheckerExec.scala` | GPU constraint checker plan node |
| `GpuCheckDeltaInvariant.scala` | Invariant check GPU/CPU dispatch |
| `DeleteCommandMeta.scala` | Spark plan meta for `DeleteCommand` / `DeleteCommandEdge` |
| `UpdateCommandMeta.scala` | Spark plan meta for `UpdateCommand` / `UpdateCommandEdge` |
| `MergeIntoCommandMeta.scala` | Spark plan meta for `MergeIntoCommand` / `MergeIntoCommandEdge` |
| `DeltaWriteUtils.scala` | GPU/CPU conversion helpers |
| `GpuDeltaParquetFileFormatBase.scala` | Base class for Parquet file format with DV support |
| `RapidsDeltaSQLConf.scala` | RAPIDS-specific Delta SQL configs |
| `RapidsDeltaUtils.scala` | Utilities for Delta GPU acceleration |
| `GpuIdentityColumn.scala` | Identity column high-water mark tracking |
| `GpuOptimizeWriteExchangeExec.scala` | Optimize-write exchange node |
| `OptimizeWriteExchangeExec.scala` | CPU fallback for optimize-write exchange |
| `StatsExprShim.scala` | Statistics expression shim |
| `shims/package-shims.scala` | Shim package definition |

### 3.4 Scala Version: DB-17.3 is Scala 2.13 Only

**This is a critical architectural difference from all prior Databricks Delta modules.**

All existing Databricks Delta modules (330db, 332db, 341db, 350db143) are built for
**Scala 2.12** (and also optionally Scala 2.13 via the `scala2.13/` mirror). Their Maven
profiles live in the main `pom.xml` and are active by default.

The `release400db173` profile, however, lives **inside the `<!-- #if scala-2.13 --><!--`
... `--><!-- #endif scala-2.13 -->` block** in the root `pom.xml`. This means:

- In the main `pom.xml`, the `release400db173` profile is XML-commented out and **never
  active**.
- In `scala2.13/pom.xml` (generated by `make-scala-version-build-files.sh`), it is
  uncommented and active.
- **All `400db173` builds must use `-f scala2.13`** (e.g.,
  `mvn -f scala2.13 -Dbuildver=400db173 ...`).
- The module artifact suffix is `_2.13` (not `_2.12`).
- No Scala 2.12/2.13 cross-compilation concerns: the code is **Scala 2.13 only**.

This follows the same pattern as `delta-40x` (OSS Spark 4.0), which is also Scala 2.13 only
and lives inside the same `#if scala-2.13` block.

---

## 4. Technical Design

### 4.1 New Module: `delta-lake/delta-spark400db173`

Create a new Maven module following the same structure as `delta-spark350db143` (DBR 14.3).
This module contains 21 version-specific Scala files plus a `pom.xml`.

#### 4.1.1 `pom.xml` (primary template)

Path: `delta-lake/delta-spark400db173/pom.xml`

Following project convention, the primary `pom.xml` uses `_2.12` suffixes (as a template).
The `scala2.13/` mirror (generated by `make-scala-version-build-files.sh`) automatically
produces the `_2.13` artifact that is actually built and deployed.

```xml
<artifactId>rapids-4-spark-delta-spark400db173_2.12</artifactId>
<name>RAPIDS Accelerator for Apache Spark Databricks 17.3 Delta Lake Support</name>
<parent>
  <artifactId>rapids-4-spark-shim-deps-parent_2.12</artifactId>
  <relativePath>../../shim-deps/pom.xml</relativePath>
</parent>
<dependencies>
  <!-- RoaringBitmap for deletion vector bitsets -->
  <dependency>org.roaringbitmap:RoaringBitmap</dependency>
  <!-- SQL plugin provided at compile-time -->
  <dependency>com.nvidia:rapids-4-spark-sql_${scala.binary.version}
              :${spark.version.classifier}:provided</dependency>
</dependencies>
<build>
  <!-- Inject common Databricks sources -->
  <sources>
    delta-lake/common/src/main/scala
    delta-lake/common/src/main/databricks/scala
  </sources>
</build>
```

**After creating this file**, run:
```bash
./build/make-scala-version-build-files.sh 2.13
```
This generates `scala2.13/delta-lake/delta-spark400db173/pom.xml` with:
- `<artifactId>rapids-4-spark-delta-spark400db173_2.13</artifactId>`
- `<parent>rapids-4-spark-shim-deps-parent_2.13</parent>`

The generated `_2.13` pom is what is actually compiled and installed.

#### 4.1.2 Provider and Probe

**`com/nvidia/spark/rapids/delta/DeltaProbe.scala`**

```scala
class DeltaProbeImpl extends DeltaProbe {
  override def getDeltaProvider: DeltaProvider = DeltaSpark400DB173Provider
}
```

**`com/nvidia/spark/rapids/delta/DeltaSpark400DB173Provider.scala`**

Extends `DatabricksDeltaProviderBase` (from common code). Overrides:
- `toGpuWrite()` — constructs `GpuWriteIntoDelta(gpuDeltaLog, WriteIntoDeltaEdge(...))`
- `convertToGpu(AtomicCreateTableAsSelectExec)` — wraps with `GpuDeltaCatalog`
- `convertToGpu(AtomicReplaceTableAsSelectExec)` — wraps with `GpuDeltaCatalog`

#### 4.1.3 GPU Command Implementations

All command files reside in `com/databricks/sql/transaction/tahoe/rapids/`.

**`GpuOptimisticTransactionBase.scala`**

Base class for GPU-accelerated Delta transactions. Key design:
- Extends `OptimisticTransaction(deltaLog, Option.empty[CatalogTable], snapshot)` — the
  `CatalogTable` parameter was added to the Databricks `OptimisticTransaction` constructor
  in DBR 14.3 and must be carried forward.
- Provides GPU/CPU plan conversion helpers (`convertToGpu`, `convertToCpu`)
- Handles `GpuCheckDeltaInvariant` constraint checking
- Manages `GpuOptimizeWriteExchangeExec` for optimize-write

**`GpuOptimisticTransaction.scala`**

Extends `GpuOptimisticTransactionBase`. Overrides `writeFiles()`:
1. Partitions CDC data
2. Normalizes data schema
3. Wraps in `RapidsDeltaWrite` plan node (to prevent AQE transitions)
4. Collects per-file Delta statistics via `GpuStatisticsCollection`
5. Tracks identity column high-water marks via `GpuIdentityColumn`
6. Writes via `GpuFileFormatWriter.write()` with `GpuParquetFileFormat`
7. Registers `GpuDoAutoCompaction` post-commit hook if auto-compaction is enabled

**`GpuDeleteCommand.scala`**

Three execution paths:
1. **Unconditional delete** — truncate table via metadata-only commit
2. **Metadata-only delete** — partition predicate can exclude files without rewriting
3. **Data-driven delete** — scan files, rewrite touched files excluding deleted rows

Includes 3 deletion vector metrics (`numDeletionVectorsAdded/Removed/Updated`), initialized
to 0 (GPU path does not produce deletion vectors; `DeleteCommandMetaShim` falls back to CPU
when persistent deletion vectors are enabled).

**`GpuUpdateCommand.scala`**

Three execution paths (same structure as GpuDeleteCommand). Includes same 3 DV metrics.
Falls back to CPU via `UpdateCommandMetaShim` when `UPDATE_USE_PERSISTENT_DELETION_VECTORS`
is enabled on a table with `deletionVectorsWritable()`.

**`GpuMergeIntoCommand.scala`**

Two-phase merge algorithm:
1. **Phase 1** — Find all files touched by the merge condition
2. **Phase 2** — For each touched file batch: rewrite data applying MATCHED and NOT MATCHED clauses

Key implementation notes:
- `txn.filterFiles(predicates, keepNumRecords = true)` — preserves `numRecords` statistics
  regardless of deletion vector state (required for DBR 14.3+)
- Spark 4.0 encoder construction: `ExpressionEncoder(RowEncoder.encoderFor(schema))` instead
  of `RowEncoder(schema)`
- Schema attributes: `toAttributes(schema)` (via `DataTypeUtils`) instead of
  `schema.toAttributes`
- `notMatchedBySourceClauses` is **not supported** on GPU (see issue #8415); falls back to
  CPU via `MergeIntoCommandMetaShim`

**`GpuWriteIntoDelta.scala`**

Thin wrapper: uses `WriteIntoDeltaEdge` (the Databricks DBR 14.3+ API, replacing
`WriteIntoDelta`). Delegates `write()` to the CPU command, then commits via GPU transaction.
Handles idempotent streaming writes by checking `txnVersion`/`txnAppId`.

**`GpuDeltaDataSource.scala`**

`GpuCreatableRelationProvider` implementation. Creates `WriteIntoDeltaEdge` for the write
path and wraps it in `GpuWriteIntoDelta`.

**`GpuDeltaCatalog.scala`**

Extends `GpuDeltaCatalogBase`. Key differences from DBR 13.3 version:
- `getWriter()` method returns `WriteIntoDeltaEdge` (not `WriteIntoDelta`)
- `loadTable(ident, timestamp)` and `loadTable(ident, version)` time-travel overloads

**`GpuDoAutoCompaction.scala`**

`PostCommitHook` that triggers GPU-optimized OPTIMIZE after writes when auto-compaction is
enabled. Creates a new `GpuOptimisticTransaction` and calls `GpuOptimizeExecutor.optimize()`.

**`GpuOptimizeExecutor.scala`**

Implements the OPTIMIZE command with:
- Multi-file, multi-threaded bin packing
- Z-order clustering via `MultiDimClustering`
- Liquid clustering via `ClusteringColumnInfo`

**`GpuCreateDeltaTableCommand.scala`**

Handles CREATE TABLE / CREATE TABLE AS SELECT. Uses `WriteIntoDelta` (not `WriteIntoDeltaEdge`)
for the legacy CTAS path where the caller produces a `WriteIntoDelta` as the query plan.

#### 4.1.4 Version Shims

All shim files reside in `com/nvidia/spark/rapids/delta/shims/`.

| Shim | Purpose |
|------|---------|
| `DeleteCommandMetaShim` | Disables GPU DELETE when `deletionVectorsWritable() && DELETE_USE_PERSISTENT_DELETION_VECTORS`; handles both `DeleteCommand` and `DeleteCommandEdge` variants |
| `UpdateCommandMetaShim` | Same guard for UPDATE |
| `MergeIntoCommandMetaShim` | Guards `notMatchedBySourceClauses`; provides `convertToGpu()` for both `MergeIntoCommand` and `MergeIntoCommandEdge` |
| `DeltaLogShim` | `fileFormat(deltaLog)` and `getMetadata(deltaLog)` via `unsafeVolatileSnapshot` |
| `MetadataShims` | Exposes `DeltaStatistics.{NUM_RECORDS, MIN, MAX, NULL_COUNT}` |
| `ShimDeltaUDF` | `DeltaUDF.stringFromString()` wrapper |
| `InvariantViolationExceptionShim` | Factory for `DeltaInvariantViolationException` |

#### 4.1.5 `GpuDeltaParquetFileFormat.scala`

**⚠ DB-17.3 uses a fundamentally different DV mechanism than DB-14.3.** (Confirmed by
decompiling `spark-sql_2.13-4.0.0-databricks-173.jar`.) The DB-14.3 approach of passing
a `broadcastDvMap: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]]`
**does not exist in DB-17.3** — that constructor parameter was removed.

DB-17.3 uses the **same per-file DV mechanism as OSS Delta 3.3+**:

- DV info is embedded per-file in `PartitionedFile.otherConstantMetadataColumnValues` via
  `DeltaParquetFileFormat.fileConstantMetadataExtractors()`.
- Two public constants on the `DeltaParquetFileFormat` companion object identify the DV
  entries: `FILE_ROW_INDEX_FILTER_ID_ENCODED` and `FILE_ROW_INDEX_FILTER_TYPE`.
- Each file's DV is accessed via a `RowIndexFilterProvider` (new in DB-17.3), which has:
  - `retrieve(conf): RowIndexFilter` — CPU materialization
  - `retrieveSerialized(conf): SerializedBitmap` — raw serialized bitmap bytes
  - `getRowIndexFilterType(): RowIndexFilterType`
- `RowIndexFilterType` values: `IF_CONTAINED`, `IF_NOT_CONTAINED`, `CONSTANT_TRUE_VECTOR_FILTER`,
  `CONSTANT_FALSE_VECTOR_FILTER`, `MOD_SHARD`, `UNKNOWN`

**Blueprint: `GpuDeltaParquetFileFormatBase`** in
`delta-lake/common/src/main/delta-33x-40x/scala/com/nvidia/spark/rapids/delta/common/`
implements exactly this per-file DV approach for OSS Delta. The DB-17.3 implementation
follows the same structure, substituting Databricks-namespace classes:

| OSS class | Databricks equivalent (confirmed on cluster) |
|-----------|----------------------------------------------|
| `o.a.s.sql.delta.DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED` | `c.d.s.t.tahoe.DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED` |
| `o.a.s.sql.delta.DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE` | `c.d.s.t.tahoe.DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE` |
| `o.a.s.sql.delta.deletionvectors.RoaringBitmapArray` | `c.d.s.t.tahoe.deletionvectors.RoaringBitmapArray` |
| `o.a.s.sql.delta.deletionvectors.StoredBitmap` | `c.d.s.t.tahoe.deletionvectors.StoredBitmap` |
| `o.a.s.sql.delta.deletionvectors.DropMarkedRowsFilter` | `c.d.s.t.tahoe.deletionvectors.DropMarkedRowsFilter` (same `createInstance` API) |
| `o.a.s.sql.delta.deletionvectors.KeepMarkedRowsFilter` | `c.d.s.t.tahoe.deletionvectors.KeepMarkedRowsFilter` (same `createInstance` API) |
| `o.a.s.sql.delta.storage.dv.HadoopFileSystemDVStore` | `c.d.s.t.tahoe.storage.dv.HadoopFileSystemDVStore` |
| `o.a.s.sql.delta.actions.DeletionVectorDescriptor` | `c.d.s.t.tahoe.actions.DeletionVectorDescriptor` |
| `o.a.s.sql.delta.actions.Protocol` / `Metadata` | `c.d.s.t.tahoe.actions.Protocol` / `Metadata` |
| `o.a.s.sql.delta.sources.DeltaSQLConf` | `c.d.s.t.tahoe.sources.DeltaSQLConf` |
| `o.a.s.sql.delta.schema.SchemaMergingUtils` | `c.d.s.t.tahoe.schema.SchemaMergingUtils` |
| `RowIndexFilter.materializeIntoVectorWithRowIndex` | `c.d.sql.io.RowIndexFilter.materializeIntoVectorWithRowIndex` |
| `RowIndexFilterProvider` | `c.d.sql.io.RowIndexFilterProvider` |
| `PortableRoaringBitmapArraySerializationFormat` | `c.d.s.t.tahoe.deletionvectors.PortableRoaringBitmapArraySerializationFormat` |
| `NativeRoaringBitmapArraySerializationFormat` | `c.d.s.t.tahoe.deletionvectors.NativeRoaringBitmapArraySerializationFormat` |

(`o.a.s` = `org.apache.spark`, `c.d.s.t` = `com.databricks.sql.transaction`)

**⚠ CRITICAL: `delta-33x-40x` common code cannot be compiled into DB-17.3.**

The files in `delta-lake/common/src/main/delta-33x-40x/scala/` import from the OSS
`org.apache.spark.sql.delta.*` namespace, which **does not exist** in DB-17.3. DB-17.3
only has the `com.databricks.sql.transaction.tahoe.*` namespace. Therefore:

- The DB-17.3 `pom.xml` must NOT include `delta-33x-40x` as a source directory.
- The `GpuDeltaParquetFileFormat.scala` for DB-17.3 must be written as a new file in the
  `delta-spark400db173` module, using the `delta-33x-40x` code as a **pattern/blueprint**
  but with all Databricks-namespace imports.
- Similarly, DV utility code (`RapidsRowIndexFilters`, `RapidsDeletionVectors`,
  `RapidsDeletionVectorStore`) must be ported with Databricks namespace imports if needed.
  Alternatively, DB-17.3 can use the Databricks-native `DropMarkedRowsFilter.createInstance()`
  and `RowIndexFilterProvider.retrieve()` APIs directly, which handle DV loading internally.

**Note: DB-14.3 has ZERO GPU DV read support.** The DB-14.3 `GpuDeltaParquetFileFormat`
explicitly tags `"deletion vectors are not supported"` when `hasDeletionVectorMap` is true.
DB-17.3 will be the **first Databricks Delta module** with GPU-accelerated DV reads.

**Key design decisions for DB-17.3 `GpuDeltaParquetFileFormat`:**

1. Constructor mirrors `DeltaParquetFileFormat`: takes `protocol`, `metadata`, DV-related
   boolean flags, `tablePath`, and `isCDCRead` — no broadcast fields.
2. `createMultiFileReaderFactory` implemented (enables GPU multi-threaded reader for DV tables).
3. `tagSupportForGpuFileSourceScan` must **not** block GPU for DVs (unlike DB-14.3 which
   fell back to CPU when `hasDeletionVectorMap`). DB-17.3 DV reads ARE GPU-supported.
4. `DeltaSpark400DB173Provider.convertToGpu` constructs this format directly from
   `fmt.protocol`, `fmt.metadata`, and `fmt.tablePath` — no broadcast fields to pass.

**Follow-up (separate PR):** `GpuDeltaParquetFileFormatBase2` uses cuDF-native bitmap APIs
(`DeletionVector.newParquetChunkedReader`, `MakeParquetTableWithDVProducer`). The DB-17.3
`SerializedBitmap.buffer(): byte[]` could feed into this, but serialization format
compatibility with cuDF needs verification before enabling.

Note: DV **writes** are not GPU-accelerated (the shims fall back to CPU for DV write paths).
DV **reads** are GPU-accelerated in this file.

---

## 5. Key API Differences

### 5.1 DBR 13.3 → DBR 14.3 (already incorporated in the 350db143 baseline)

| Area | DBR 13.3 | DBR 14.3 |
|------|----------|----------|
| Write command class | `WriteIntoDelta` | `WriteIntoDeltaEdge` |
| `OptimisticTransaction` constructor | `(deltaLog, snapshot)(clock)` | `(deltaLog, Option.empty[CatalogTable], snapshot)` |
| DV metrics in Delete/Update | 2 | 3 (`numDVAdded/Removed/Updated`) |
| `filterFiles` in Merge | `filterFiles(predicates)` | `filterFiles(predicates, keepNumRecords=true)` |
| Encoder construction | `RowEncoder(schema)` | `ExpressionEncoder(RowEncoder.encoderFor(schema))` |
| Schema attributes | `schema.toAttributes` | `toAttributes(schema)` |
| `GpuDeltaCatalog.getWriter()` | Not present | Uses `WriteIntoDeltaEdge` |
| `GpuDeltaParquetFileFormat` | No DV read support (GPU blocks DVs) | DV read support via broadcast map (but GPU explicitly blocks it; falls back to CPU) |
| `GpuLowShuffleMergeCommand` | Present | Removed |

### 5.2 Confirmed API Differences: DBR 14.3 → DBR 17.3

Verified by decompiling DB-17.3 JARs and confirmed on a live DB-17.3 cluster (2026-04-07).

Note: On the cluster, the JAR is named
`----ws_4_0--sql--core--core-hive-2.3__hadoop-3.2_2.13_deploy.jar` (not
`spark-sql_2.13-4.0.0-databricks-173.jar` as originally expected). The catalyst JAR is
`----ws_4_0--sql--catalyst--catalyst-hive-2.3__hadoop-3.2_2.13_deploy.jar`.

#### Deletion Vector API (Major Change)

| Area | DB-14.3 | DB-17.3 |
|------|---------|---------|
| `DeltaParquetFileFormat` constructor | 8 params: `(relation, columnMappingMode, referenceSchema, isSplittable, disablePushDowns, broadcastDvMap, tablePath, broadcastHadoopConf)` | 10 params: `(protocol, metadata, generateRowIndexFilterId, generateRowIndexFilterColumn, generateDeltaFileInScanId, nullableRowTrackingConstantFields, nullableRowTrackingGeneratedFields, optimizationsEnabled, tablePath, isCDCRead)` |
| DV loading mechanism | Broadcast `Map[URI, DeletionVectorDescriptorWithFilterType]` | Per-file via `PartitionedFile.otherConstantMetadataColumnValues` |
| `TahoeFileIndex.rowIndexFilters` type | `Map[String, RowIndexFilterType]` | `Map[String, RowIndexFilterProvider]` |
| `FILE_ROW_INDEX_FILTER_ID_ENCODED` | Not present | **Public** constant on companion |
| `FILE_ROW_INDEX_FILTER_TYPE` | Not present | **Public** constant on companion |
| DV filter provider | `RowIndexFilterType` enum only | `RowIndexFilterProvider` interface (`retrieve`, `retrieveSerialized`, `getRowIndexFilterType`) |
| Raw bitmap bytes | Not exposed | `SerializedBitmap.buffer(): byte[]` via `RowIndexFilterProvider.retrieveSerialized()` |
| GPU materialization method | `materializeIntoVector(start, end, output)` | + `materializeIntoVectorWithRowIndex(numRows, rowIndexCol, output)` |
| `DeletionVectorUtils.deletionVectorsWritable` | `(SnapshotDescriptor): Boolean` | Overloads: `(SnapshotDescriptor, Option[Protocol], Option[Metadata])` and `(Protocol, Metadata)` |
| DV write configs | `DELETE_USE_PERSISTENT_DELETION_VECTORS` ✓, `UPDATE_USE_PERSISTENT_DELETION_VECTORS` ✓ | **Same** — both confirmed present in `DeltaSQLConf` |
| `DropMarkedRowsFilter.createInstance` | `(DeletionVectorDescriptor, Configuration, Option[Path])` | **Same API** — confirmed present |
| `KeepMarkedRowsFilter.createInstance` | `(DeletionVectorDescriptor, Configuration, Option[Path])` | **Same API** — confirmed present |
| `RoaringBitmapArray` | `o.a.s.sql.delta.deletionvectors.RoaringBitmapArray` | `c.d.s.t.tahoe.deletionvectors.RoaringBitmapArray` — confirmed via `javap` (same API: `readFrom(byte[])`, `toArray(): long[]`, `cardinality(): long`) |
| `StoredBitmap.load()` | Returns `RoaringBitmapArray` | Returns `RoaringBitmapArray` — confirmed via `javap` |
| `HadoopFileSystemDVStore.read()` | `(DeletionVectorDescriptor, Path): RoaringBitmapArray` | **Same API** — confirmed present |
| `PortableRoaringBitmapArraySerializationFormat` | `MAGIC_NUMBER`, `serialize`, `deserialize` | **Same API** — confirmed present |
| `NativeRoaringBitmapArraySerializationFormat` | `MAGIC_NUMBER`, `serialize`, `deserialize` | **Same API** — confirmed present |
| `DeletionVectorStore` / `DeletionVectorStoreEdge` | Present | **Confirmed present** — `DeletionVectorStoreEdge.createInstance(conf, cacheConfig)` |

#### Spark 4.0 APIs (Resolved via DB-17.3 Cluster Verification, 2026-04-07)

All 5 items previously listed as unknowns have been verified on a live DB-17.3 cluster:

1. **`SparkSession` aliasing** — **CONFIRMED.** `org.apache.spark.sql.classic.SparkSession`
   is present in the DB-17.3 runtime. The existing `TrampolineConnectShims` in
   `sql-plugin/src/main/spark400/` already includes `{"spark": "400db173"}` in its shim
   header. DB-17.3 command files WILL need to use `TrampolineConnectShims.SparkSession`
   and `TrampolineConnectShims.createDataFrame()` (same as OSS `delta-40x`).

2. **`DFUDFShims`** — **CONFIRMED.** `DFUDFShims` in
   `sql-plugin/src/main/spark400/scala/org/apache/spark/sql/nvidia/DFUDFShims.scala` already
   includes `{"spark": "400db173"}` in its shim header. DB-17.3 WILL need
   `DFUDFShims.exprToColumn()` for UDF invocations in `GpuMergeIntoCommand`.

3. **`GpuFileFormatWriter.write()` signature** — **CONFIRMED.** `GpuFileFormatWriter` in
   `sql-plugin/src/main/spark332db/scala/org/apache/spark/sql/rapids/GpuFileFormatWriter.scala`
   already includes `{"spark": "400db173"}` in its shim header. No signature change needed.

4. **`GpuOptimizeExecutor` clustering APIs** — **CONFIRMED.** On the DB-17.3 cluster:
   `com.databricks.sql.io.skipping.MultiDimClustering` and
   `com.databricks.sql.io.skipping.liquid.ClusteringColumnInfo` are at the same packages
   as DB-14.3. No import changes needed.

5. **`RowTracking` import** — **CONFIRMED.** `com.databricks.sql.transaction.tahoe.RowTracking`
   is present in the DB-17.3 runtime. Import path is unchanged from DB-14.3.

Additional classes confirmed on the DB-17.3 cluster:
- `WriteIntoDelta` and `WriteIntoDeltaEdge` — both present
- `AtomicCreateTableAsSelectExec` / `AtomicReplaceTableAsSelectExec` — confirmed
- `OptimizeExecutor` at `com.databricks.sql.transaction.tahoe.commands.optimize.OptimizeExecutor`
- `DeltaInvariantViolationException` — confirmed
- `DeltaUDF` — confirmed
- `DeltaStatistics` at `com.databricks.sql.transaction.tahoe.stats.DeltaStatistics`
- `DataTypeUtils` at `org.apache.spark.sql.catalyst.types.DataTypeUtils`
- `ExpressionEncoder` — confirmed
- `DeltaSQLConf` at `com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf`

### 5.3 Scala 2.13 Considerations

Since DB-17.3 is **Scala 2.13 only**, some differences from the Scala 2.12 codebase apply:

- **No cross-compilation concerns**: the `delta-spark400db173` module is never compiled with
  Scala 2.12. No need for `scala-2.12`/`scala-2.13` source directory splits or compatibility
  shims within this module.
- **Scala 2.13 collection APIs**: `LazyList` (not `Stream`), `ArraySeq` (not `WrappedArray`),
  etc., may be used freely. However, since the code is copied from the 350db143 baseline
  (which must support both 2.12 and 2.13), Scala 2.13-only APIs should not need to be
  introduced.
- **`-Xsource:2.13` and `-Wconf` flags**: automatically applied via the `scala2.13/`
  parent pom. No manual flag changes needed.
- The OSS `delta-40x` module (also Scala 2.13 only) is a useful reference for any
  compiler-error patterns specific to Scala 2.13.

---

## 6. Build System Changes

### 6.1 Root `pom.xml`

The `release400db173` profile already lives inside the `<!-- #if scala-2.13 --><!--` block.
Update the two property/module values inside that block:

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

Note: the `_2.12`/`_2.13` suffix is **not** appended to `rapids.delta.artifactId1` in the
profile property — the suffix is appended by the aggregator/dist POM when resolving the
dependency. Verify this matches the pattern used by other profiles (e.g.,
`release350db143` uses `rapids-4-spark-delta-spark350db143` without a suffix).

### 6.2 Generate `scala2.13/` Mirror Files

After all `pom.xml` changes, run:

```bash
./build/make-scala-version-build-files.sh 2.13
```

This script:
1. Generates `scala2.13/delta-lake/delta-spark400db173/pom.xml` from the primary
   `delta-lake/delta-spark400db173/pom.xml` (replacing `_2.12` → `_2.13` throughout)
2. Updates `scala2.13/pom.xml` to reflect the `release400db173` module change

Both the primary pom and the `scala2.13/` mirror must be committed to source control.

### 6.3 No Change to `delta-lake/delta-stub`

The stub module continues to be used for all Databricks runtimes that don't have a dedicated
Delta module (e.g., any future new Databricks versions until their Delta module is created).
The `release400db173` profile simply no longer references it.

---

## 7. Implementation Plan

### Step 1: Scaffold the module

Copy `delta-lake/delta-spark350db143/` to `delta-lake/delta-spark400db173/`.

### Step 2: Mechanical renames

In the new `delta-spark400db173/` directory:
- Rename all filenames containing `350db143` → `400db173`
- Replace all occurrences of `350db143` → `400db173` in class/object names and `pom.xml`
- Replace `DeltaSpark350DB143Provider` → `DeltaSpark400DB173Provider`
- Update copyright years → 2026 where applicable

### Step 3: Update root `pom.xml`

Apply the diff in §6.1 to swap the stub module for the new delta module inside the
`#if scala-2.13` block.

### Step 4: Sync `scala2.13/` mirror

```bash
./build/make-scala-version-build-files.sh 2.13
```

This is **mandatory** after any `pom.xml` change. Commit both the primary pom and the
generated `scala2.13/` pom.

### Step 5: Fix Spark 4.0 API incompatibilities

The DB-14.3 code uses Spark 3.x APIs that are **removed or changed** in Spark 4.0. These
must be fixed before compiling. See §7.1 below for the complete list.

### Step 6: Compile against DBR 17.3

```bash
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests \
    -pl delta-lake/delta-spark400db173 -am
```

Note: `-f scala2.13` is required because the `release400db173` profile is only active in
the Scala 2.13 build. Building without `-f scala2.13` will not find the profile.

Address remaining compile errors by cross-referencing:
- The OSS `delta-lake/delta-40x/` module for Spark 4.0 API patterns
- The Databricks 17.3 JARs for exact API signatures in `com.databricks.sql.transaction.tahoe`

### Step 7: Full build

```bash
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests
```

### Step 8: Run tests

```bash
mvn -f scala2.13 -Dbuildver=400db173 package -pl tests -am \
    -DwildcardSuites="com.nvidia.spark.rapids.delta.*"
```

Integration tests (requires a DBR 17.3 cluster):
```bash
./integration_tests/run_pyspark_from_build.sh -k delta_test.py
```

### Step 9: Enable Delta tests in CI

Remove the DB-17.3 skip in `jenkins/databricks/test.sh` (lines 152-160):
```diff
-    if [[ "$SPARK_SHIM_VER" == "spark400db173" ]]; then
-        echo "Skipping Delta Lake tests: not yet supported for DB-17.3 (spark400db173)"
-    else
-        ## Run Delta Lake tests
-        DRIVER_MEMORY="4g" \
-            bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks"  -m "delta_lake" --delta_lake --test_type=$TEST_TYPE
-    fi
+    ## Run Delta Lake tests
+    DRIVER_MEMORY="4g" \
+        bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks"  -m "delta_lake" --delta_lake --test_type=$TEST_TYPE
```

---

## 7.1 Spark 4.0 API Incompatibilities in DB-14.3 Code (Must Fix)

**Verified on DB-17.3 cluster (2026-04-07):** The following Spark 3.x APIs used in the
DB-14.3 command files are removed/changed in Spark 4.0 (DB-17.3). These MUST be fixed
when porting.

### 7.1.1 `new Column(Expression)` → `DFUDFShims.exprToColumn(expr)`

**Impact:** 14 occurrences across 3 files.

In Spark 4.0, `Column` only accepts `ColumnNode`, not `Expression`:
```java
// Spark 4.0 Column constructors (confirmed via javap on DB-17.3):
public Column(ColumnNode)
public Column(String, Option[Object])
public Column(String)
// Column(Expression) is GONE
```

Files affected and approximate occurrence count:
- `GpuMergeIntoCommand.scala` — ~9 occurrences of `new Column(expr)`
- `GpuDeleteCommand.scala` — ~2 occurrences
- `GpuUpdateCommand.scala` — ~3 occurrences

**Fix:** Replace all `new Column(expr)` with `DFUDFShims.exprToColumn(expr)`.
Add `import org.apache.spark.sql.nvidia.DFUDFShims` to each file.

Reference: OSS `delta-40x/GpuMergeIntoCommand.scala` uses
`DFUDFShims.exprToColumn(condition)` for the same pattern.

### 7.1.2 `Dataset.ofRows(spark, plan)` → `TrampolineConnectShims.createDataFrame()`

**Impact:** ~10 occurrences across 3 files.

In Spark 4.0, `SparkSession` is abstract and `Dataset.ofRows()` requires a
`ClassicSparkSession`. The safe wrapper is `TrampolineConnectShims.createDataFrame()`:

```scala
// DB-14.3 (Spark 3.x):
Dataset.ofRows(spark, plan)

// DB-17.3 (Spark 4.0):
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
TrampolineConnectShims.createDataFrame(
  spark.asInstanceOf[TrampolineConnectShims.SparkSession], plan)
```

Files affected:
- `GpuOptimisticTransaction.scala` — 2 occurrences
- `GpuUpdateCommand.scala` — 2 occurrences
- `GpuMergeIntoCommand.scala` — 6 occurrences

Reference: OSS `delta-40x/GpuOptimisticTransaction.scala` imports
`TrampolineConnectShims.SparkSession` and uses `TrampolineConnectShims.createDataFrame()`.

### 7.1.3 `SparkSession.getActiveSession` → `TrampolineConnectShims.getActiveSession`

**Impact:** Used in `GpuDeltaParquetFileFormat.scala` and potentially in command files.

```scala
// DB-14.3:
SparkSession.getActiveSession.get

// DB-17.3:
TrampolineConnectShims.getActiveSession
```

### 7.1.4 `RuntimeReplaceable` expressions in statistics

In Spark 4.0, JSON functions (e.g. `StructsToJson`) are `RuntimeReplaceable` and
unevaluable. Stats expression processing needs:

```scala
// Add to GpuOptimisticTransaction or stats processing:
expr.transform { case rr: RuntimeReplaceable => rr.replacement }
```

Reference: `delta-40x/Delta40xCommandShims.postProcessStatsExpr()`

### 7.1.5 Summary of Required Imports per File

| File | New imports needed |
|------|--------------------|
| `GpuMergeIntoCommand.scala` | `DFUDFShims`, `TrampolineConnectShims` |
| `GpuDeleteCommand.scala` | `DFUDFShims` |
| `GpuUpdateCommand.scala` | `DFUDFShims`, `TrampolineConnectShims` |
| `GpuOptimisticTransaction.scala` | `TrampolineConnectShims`, `RuntimeReplaceable` |
| `GpuDeltaParquetFileFormat.scala` | `TrampolineConnectShims` (for `getActiveSession`) |

### 7.1.6 `writeFiles()` Signature Completely Changed (MAJOR)

**Impact:** `GpuOptimisticTransaction.writeFiles()` override must change.

DB-14.3:
```scala
def writeFiles(inputData: Dataset[_],
    writeOptions: Option[DeltaOptions],
    additionalConstraints: Seq[Constraint]): Seq[FileAction]
```

DB-17.3 (confirmed via `javap`):
```scala
def writeFiles(inputData: Dataset[_],
    writeOptions: TransactionalWriteOptions,   // NEW CLASS
    isOptimize: Boolean,                        // NEW
    isLiquidClustering: Boolean,                // NEW (has default)
    additionalConstraints: Seq[Constraint],     // (has default)
    isCDCWritePhase: Boolean,                   // NEW (has default)
    context: Option[String]): Seq[FileAction]   // NEW (has default)
```

`TransactionalWriteOptions` is a new case class wrapping:
```scala
case class TransactionalWriteOptions(
    optimizeWrite: Option[Boolean],                // all have defaults
    overridingSQLConfs: Map[String, String],
    validateWrite: Option[Boolean],
    deltaOptions: Option[DeltaOptions],
    forcePreserveInputOrder: Boolean)
```

There is also `writeFilesAndGetQueryExecution()` that returns
`(Seq[FileAction], QueryExecution)` instead of just `Seq[FileAction]`.

**Fix:** Update `GpuOptimisticTransaction.writeFiles()` override signature to match.
Construct `TransactionalWriteOptions` wrapping the `DeltaOptions`.

### 7.1.7 Command Edge Constructors Changed (MAJOR)

All Databricks Delta command classes gained new constructor parameters in DB-17.3.
The GPU command classes and the `MergeIntoCommandMetaShim.convertToGpu` methods must
extract and pass these new fields.

**`MergeIntoCommandEdge` / `MergeIntoCommand`** (DB-17.3, confirmed via `javap`):
```scala
(source: LogicalPlan,
 target: LogicalPlan,
 catalogTable: Option[CatalogTable],       // NEW
 targetFileIndex: TahoeFileIndex,           // NEW
 condition: Expression,
 matchedClauses: Seq[DeltaMergeIntoMatchedClause],
 notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
 notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause],
 migratedSchema: Option[StructType],
 trackHighWaterMarks: Set[String],          // NEW
 schemaEvolutionEnabled: Boolean)           // NEW
```

**`DeleteCommandEdge`** (DB-17.3):
```scala
(tahoeFileIndex: TahoeFileIndex,            // NEW (replaces deltaLog in GPU cmd)
 catalogTable: Option[CatalogTable],        // NEW
 target: LogicalPlan,
 condition: Option[Expression])
```

**`UpdateCommandEdge`** (DB-17.3):
```scala
(tahoeFileIndex: TahoeFileIndex,
 catalogTable: Option[CatalogTable],        // NEW
 target: LogicalPlan,
 updateExpressions: Seq[Expression],
 condition: Option[Expression])
```

**Fix:** Update `GpuMergeIntoCommand`, `GpuDeleteCommand`, `GpuUpdateCommand` constructors
to accept and propagate the new fields. Update `MergeIntoCommandMetaShim.convertToGpu`,
`DeleteCommandMeta`, and `UpdateCommandMeta` to extract `catalogTable`, `targetFileIndex`,
`trackHighWaterMarks`, and `schemaEvolutionEnabled` from the CPU command.

### 7.1.8 `WriteIntoDeltaEdge` Constructor Grew (Minor)

DB-17.3 constructor has 13 params (up from ~8). Params 7-13 all have defaults:
```
$lessinit$greater$default$7()  // catalogTable: Option[CatalogTable]
$lessinit$greater$default$8()  // schemaInCatalog: Option[StructType]
$lessinit$greater$default$9()  // clusteringColumns: Option[Seq[String]]  NEW
$lessinit$greater$default$10() // tableAliasOpt: Option[String]           NEW
$lessinit$greater$default$11() // snapshotAtAnalysis: Option[Snapshot]     NEW
$lessinit$greater$default$12() // jobGroupIdAtAnalysis: Option[String]     NEW
$lessinit$greater$default$13() // autoUpdateStatsSchemaOpt: Option[_]      NEW
```

Existing call sites passing 6-8 positional args should compile because new params have
defaults. **Verify during compilation.** If issues arise, explicitly pass `None` for new params.

### 7.1.9 `implicit Clock` Parameter Removed from `OptimisticTransaction`

DB-14.3's `GpuOptimisticTransactionBase` and `GpuOptimisticTransaction` use
`(implicit clock: Clock)` because the DB-14.3 `OptimisticTransaction` constructor
requires it. In DB-17.3 (confirmed via `javap`), the constructor is:
```
OptimisticTransaction(DeltaLog, Option[CatalogTable], Snapshot)
```
**No `implicit Clock` parameter.** Clock is obtained internally from `DeltaLog.clock()`.

**Fix:** Remove `(implicit clock: Clock)` from `GpuOptimisticTransactionBase` and
`GpuOptimisticTransaction` constructors. These are version-specific files (not shared),
so this only affects the DB-17.3 module.

Note: The shared `GpuDeltaLog` has `private lazy implicit val _clock: Clock =
deltaLog.clock`. This implicit won't be consumed by the DB-17.3 constructors, but since
it's `lazy` and type-annotated, it won't trigger unused warnings. **No change needed to
the shared file.**

### 7.1.10 `PostCommitHook.run()` Signature Completely Changed (MAJOR)

DB-14.3:
```scala
def run(spark: SparkSession, txn: OptimisticTransactionImpl,
    committedVersion: Long, postCommitSnapshot: Snapshot,
    committedActions: Seq[Action]): Unit
```

DB-17.3:
```scala
def run(spark: SparkSession, txn: CommittedTransaction): Unit
```

`CommittedTransaction` is a new case class bundling the transaction, committed version,
snapshot, and committed actions.

**Fix:** Rewrite `GpuDoAutoCompaction.run()` to accept `CommittedTransaction` and extract
the individual fields from it. The `txn.asInstanceOf[GpuOptimisticTransaction]` cast must
be updated to extract the transaction from `CommittedTransaction`.

### 7.1.11 `LogicalRelation` Pattern Match Arity Changed (MAJOR)

DB-14.3 `LogicalRelation` unapply returns 4 fields:
```scala
case LogicalRelation(base, output, catalogTbl, isStreaming) =>
```

DB-17.3 `LogicalRelation` unapply returns 7 fields (added `sparkDataStream`,
`injectedConstraints`, `statistics`):
```scala
case LogicalRelation(base, output, catalogTbl, isStreaming, _, _, _) =>
```

Files affected:
- `GpuMergeIntoCommand.scala` (line ~948) — match with 4 fields
- `GpuOptimisticTransactionBase.scala` (line ~178) — match with 4 fields

**Fix:** Add wildcards for the 3 new fields in all pattern matches.

### 7.1.12 `ShuffledRowRDD` Constructor Changed (MAJOR — Shared Code)

**This affects a SHARED file:** `delta-lake/common/src/main/databricks/scala/
org/apache/spark/sql/rapids/delta/OptimizeWriteExchangeExec.scala`

DB-17.3's `ShuffledRowRDD` constructor now requires a `PrismMetrics` parameter that
did not exist in earlier versions. The shared code creates `ShuffledRowRDD` with 2-3 args,
but all DB-17.3 constructors require the `PrismMetrics` parameter.

**Fix:** This shared file CANNOT be used as-is for DB-17.3. Options:
1. Create a version-specific override in `delta-spark400db173` that shadows the shared file
2. Add a shim for `ShuffledRowRDD` construction
3. Pass `null` or a no-op `PrismMetrics` if the parameter is optional

### 7.1.13 `CheckDeltaInvariant.columnExtractors` Type Changed (MAJOR — Shared Code)

**This affects a SHARED file:** `delta-lake/common/src/main/databricks/scala/
com/databricks/sql/transaction/tahoe/rapids/GpuCheckDeltaInvariant.scala`

DB-14.3: `columnExtractors: Map[String, Expression]`
DB-17.3: `columnExtractors: Seq[(String, Expression)]`

The `GpuCheckDeltaInvariantMeta.convertToGpu()` passes `wrapped.columnExtractors` directly
to the `GpuCheckDeltaInvariant` constructor which expects `Map[String, Expression]`.

**Fix:** Either add `.toMap` in the shared code (if backward compatible) or create a
version-specific override in `delta-spark400db173`.

### 7.1.14 `DeletionVectorUtils` Changed from Object to Trait (Minor)

DB-17.3 `DeletionVectorUtils` is now a trait with a companion object. The companion
(`DeletionVectorUtils$`) implements the trait and exposes static-like methods.

The existing code calls `DeletionVectorUtils.deletionVectorsWritable(snapshot)` —
this still works because the companion has the method, and the single-arg overload
is supported via default params (`$default$2()` and `$default$3()` return `None`).

**No code change needed**, but document for awareness.

---

## 8. Out of Scope

- **Deletion vector writes on GPU** — DV write paths (`DELETE_USE_PERSISTENT_DELETION_VECTORS`,
  `UPDATE_USE_PERSISTENT_DELETION_VECTORS`) continue to fall back to CPU. Tracked in
  https://github.com/NVIDIA/spark-rapids/issues/8654.
- **`notMatchedBySourceClauses` in MERGE** — Continues to fall back to CPU. Tracked in
  https://github.com/NVIDIA/spark-rapids/issues/8415.
- **Iceberg / other table formats** — Not affected by this change.
- **Scala 2.12 support** — DBR 17.3 / Spark 4.0 does not support Scala 2.12. No Scala 2.12
  artifact for this module will be produced or is needed.

---

## 9. Reference Files

| Purpose | Location |
|---------|----------|
| Primary reference module (DBR 14.3) | `delta-lake/delta-spark350db143/` |
| `scala2.13/` mirror of DBR 14.3 | `scala2.13/delta-lake/delta-spark350db143/` |
| Databricks shared common code | `delta-lake/common/src/main/databricks/scala/` |
| Universal Delta common code | `delta-lake/common/src/main/scala/` |
| OSS Spark 4.0 Delta (API reference) | `delta-lake/delta-40x/` |
| Spark 4.0 command shims (Spark 4.0 API patterns) | `delta-lake/delta-40x/src/main/scala/.../Delta40xCommandShims.scala` |
| Spark 4.0 runtime shim (reference) | `delta-lake/delta-40x/src/main/scala/.../Delta40xRuntimeShim.scala` |
| `scala2.13/` mirror of OSS 4.0 Delta | `scala2.13/delta-lake/delta-40x/` |
| DB-17.3 SQL plugin shims | `sql-plugin/src/main/spark400db173/` |
| Spark 4.0 shared shims (covers 400db173) | `sql-plugin/src/main/spark400/` |
| `TrampolineConnectShims` (Spark 4.0 session/DF) | `sql-plugin/src/main/spark400/.../TrampolineConnectShims.scala` |
| `DFUDFShims` (Spark 4.0 Column API) | `sql-plugin/src/main/spark400/.../DFUDFShims.scala` |
| `GpuFileFormatWriter` (covers 400db173) | `sql-plugin/src/main/spark332db/.../GpuFileFormatWriter.scala` |
| Root Maven profile | `pom.xml` (inside `#if scala-2.13` block, ~lines 633–654) |
| Scala 2.13 root pom | `scala2.13/pom.xml` |
| Stub module being replaced | `delta-lake/delta-stub/` |
| Scala version sync script | `build/make-scala-version-build-files.sh` |
| **Jenkins test skip (to remove)** | `jenkins/databricks/test.sh` lines 152–160 |
