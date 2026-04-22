---
layout: page
title: Spark Master Shim CI
nav_order: 9
parent: Developer Overview
---

# Spark Master Shim CI

This document describes the automated CI workflow for building spark-rapids against
Apache Spark's `master` branch SNAPSHOT. The goal is to detect upstream API breaks
early, before a Spark release is cut, so compatibility work happens incrementally.

See [spark-rapids#14547](https://github.com/NVIDIA/spark-rapids/issues/14547) for background.

## Overview

The driver script `jenkins/spark-master-shim-ci.sh` manages three repos in dependency order:

```
Apache Spark master  →  spark-rapids-private  →  spark-rapids
     (build)              (build + install)        (verify)
```

On first run it bootstraps a `sparkmaster` shim (cloned from `spark411`) in both
spark-rapids-private and spark-rapids. On subsequent runs it detects version changes
and updates `sparkmaster.version` accordingly.

## Quick start

```bash
# Full end-to-end (clones all three repos, builds everything)
SANDBOX_DIR=~/sandbox_test \
URM_URL=https://urm.nvidia.com/artifactory/sw-spark-maven \
    ./jenkins/spark-master-shim-ci.sh
```

To use local checkouts instead of cloning:

```bash
SANDBOX_DIR=~/sandbox_test \
URM_URL=https://urm.nvidia.com/artifactory/sw-spark-maven \
PRIVATE_LOCAL_DIR=/path/to/spark-rapids-private \
RAPIDS_LOCAL_DIR=/path/to/spark-rapids \
    ./jenkins/spark-master-shim-ci.sh
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `SANDBOX_DIR` | `~/sandbox_test` | Working directory for cloned repos and manifest |
| `SPARK_UPSTREAM_URL` | `https://github.com/apache/spark.git` | Apache Spark git URL |
| `PRIVATE_UPSTREAM_URL` | `git@gitlab-master.nvidia.com:nvspark/spark-rapids-private.git` | spark-rapids-private git URL |
| `RAPIDS_UPSTREAM_URL` | `https://github.com/NVIDIA/spark-rapids.git` | spark-rapids git URL |
| `SPARK_BRANCH` | `master` | Apache Spark branch to track |
| `PRIVATE_BRANCH` | `main` | spark-rapids-private branch |
| `RAPIDS_BRANCH` | `main` | spark-rapids branch |
| `BASE_BUILDVER` | `411` | Existing shim to clone from on first bootstrap |
| `URM_URL` | *(empty)* | NVIDIA internal Maven repo, required for builds |
| `PRIVATE_LOCAL_DIR` | *(empty)* | Skip clone, use this local path for spark-rapids-private |
| `RAPIDS_LOCAL_DIR` | *(empty)* | Skip clone, use this local path for spark-rapids |

## What the script does

### Step 1 — Sync repos
Clones or fetches all three repos into `$SANDBOX_DIR`. When `*_LOCAL_DIR` is set,
that repo is used as-is (no fetch/reset).

### Step 2 — Detect Spark version
Reads `<version>` from Apache Spark's `pom.xml` (e.g., `4.2.0-SNAPSHOT`).
Aborts if it doesn't end with `-SNAPSHOT`.

### Step 3 — Build Apache Spark
Runs `mvn install -DskipTests` to populate `~/.m2` with Spark SNAPSHOT artifacts.

### Step 4 — Bootstrap & install spark-rapids-private
- Adds `releasemaster` profile + `sparkmaster.version` property to `pom.xml`
- Regenerates `scala2.13/pom.xml`
- Widens `shimplify.py` regex to accept `sparkmaster`
- Runs shimplify to clone shim files (first run only)
- Builds and installs with `mvn install -DskipTests -f scala2.13/pom.xml -Dbuildver=master`

### Step 5 — Bootstrap sparkmaster shim in spark-rapids
Same bootstrap as step 4, plus post-shimplify fixup of `SparkShimVersion` constants.

### Step 6 — Build spark-rapids
Runs `mvn verify -f scala2.13/pom.xml -Dbuildver=master -P individual`.

### Step 7 — Manifest
Writes `$SANDBOX_DIR/run-manifest.txt` with timestamps, SHAs, and resolved version.

## Shim naming conventions

| Concept | Value |
|---|---|
| Maven profile | `releasemaster` |
| `buildver` | `master` |
| Version property | `sparkmaster.version` |
| Shim source dirs | `sql-plugin/src/main/sparkmaster/`, `core/src/main/sparkmaster/` (private) |
| JSON-lines tag | `{"spark": "master"}` |
| Scala version | 2.13 only (Spark 4.x) |

## How it works with Spark releases

When a tracked Spark version (e.g., 4.2.0) is released:

1. **Freeze**: Clone `sparkmaster` into a permanent shim via shimplify:
   ```bash
   mvn generate-sources -Dshimplify=true -Dshimplify.move=true \
       -Dshimplify.overwrite=true -Dshimplify.add.shim=420 -Dshimplify.add.base=master
   ```
2. **Update**: Add `release420` profile + `spark420.version=4.2.0` to pom.xml.
3. **Continue**: The next CI run detects Spark master has bumped to `4.3.0-SNAPSHOT`
   and updates `sparkmaster.version` automatically.

## Files involved

| File | Role |
|---|---|
| `jenkins/spark-master-shim-ci.sh` | Main driver script |
| `build/add_sparkmaster_profile.py` | Adds/updates releasemaster profile in pom.xml (`--private` flag for spark-rapids-private) |
| `build/shimplify.py` | Shim layout tool (regex widened to accept `sparkmaster`) |
| `build/get_buildvers.py` | Parses pom profiles, classifies `master` as snapshot |
| `build/make-scala-version-build-files.sh` | Regenerates `scala2.13/pom.xml` |

## Cleaning up

```bash
# Remove sandbox
rm -rf ~/sandbox_test

# Remove Spark SNAPSHOT from local Maven cache
rm -rf ~/.m2/repository/org/apache/spark/spark-*_2.13/4.2.0-SNAPSHOT
```
