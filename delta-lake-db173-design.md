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

Extends `GpuDeltaParquetFileFormatBase` (common Databricks code). Adds deletion vector
**read** support (filtering rows during Parquet scan):

```
broadcastDvMap: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]]
    → DropMarkedRowsFilter    (for tables using row-level deletes)
    → KeepMarkedRowsFilter    (for tables using row-level updates/merges)
    → KeepAllRowsFilter       (no deletion vectors present)
```

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
| `GpuDeltaParquetFileFormat` | No DV read support | Full DV read support |
| `GpuLowShuffleMergeCommand` | Present | Removed |

### 5.2 Potential Additional Changes for DBR 17.3 (Spark 4.0)

These require verification by attempting compilation against DBR 17.3 dependencies.
The OSS `delta-40x` module (Spark 4.0 Delta) provides guidance on expected changes:

1. **`SparkSession` aliasing** — OSS delta-40x imports `org.apache.spark.sql.classic.SparkSession`
   and uses `TrampolineConnectShims.SparkSession`. The Databricks 17.3 runtime may require
   similar aliasing in command files that use `SparkSession` directly.

2. **`DFUDFShims`** — OSS delta-40x uses `org.apache.spark.sql.nvidia.DFUDFShims` for UDF
   invocations in `GpuMergeIntoCommand`. Check if Databricks 17.3 requires this.

3. **`GpuFileFormatWriter.write()` parameters** — Verify signature compatibility with
   `GpuFileFormatWriter` from the `sql-plugin` layer (may have changed for Spark 4.0).

4. **`GpuOptimizeExecutor` clustering APIs** — `MultiDimClustering`, `ClusteringColumnInfo`,
   and related classes (`com.databricks.sql.io.skipping.*`) may have changed packages
   or signatures between DBR 14.3 and DBR 17.3.

5. **Deletion Vector API evolution** — `DeletionVectorUtils`, `DeltaSQLConf` DV-related
   configs, and `DeltaParquetFileFormat` filter types may have evolved in DBR 17.3.

6. **`RowTracking` import** — Verify the import path in `GpuDeleteCommand` and
   `GpuUpdateCommand` is unchanged.

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

### Step 5: Compile against DBR 17.3

```bash
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests \
    -pl delta-lake/delta-spark400db173 -am
```

Note: `-f scala2.13` is required because the `release400db173` profile is only active in
the Scala 2.13 build. Building without `-f scala2.13` will not find the profile.

Address compile errors by cross-referencing:
- The OSS `delta-lake/delta-40x/` module for Spark 4.0 API patterns
- The Databricks 17.3 JARs for exact API signatures in `com.databricks.sql.transaction.tahoe`

### Step 6: Full build

```bash
mvn -f scala2.13 -Dbuildver=400db173 install -DskipTests
```

### Step 7: Run tests

```bash
mvn -f scala2.13 -Dbuildver=400db173 package -pl tests -am \
    -DwildcardSuites="com.nvidia.spark.rapids.delta.*"
```

Integration tests (requires a DBR 17.3 cluster):
```bash
./integration_tests/run_pyspark_from_build.sh -k delta_test.py
```

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
| `scala2.13/` mirror of OSS 4.0 Delta | `scala2.13/delta-lake/delta-40x/` |
| DB-17.3 SQL plugin shims | `sql-plugin/src/main/spark400db173/` |
| Root Maven profile | `pom.xml` (inside `#if scala-2.13` block, ~lines 633–654) |
| Scala 2.13 root pom | `scala2.13/pom.xml` |
| Stub module being replaced | `delta-lake/delta-stub/` |
| Scala version sync script | `build/make-scala-version-build-files.sh` |
