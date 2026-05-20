# DBR 17.3 OPTIMIZE and Auto Compaction Plan

## Context

This plan covers implementation of GPU OPTIMIZE and auto-compaction for DBR 17.3:

- Issue: https://github.com/NVIDIA/spark-rapids/issues/14599
- Reference PR 1: https://github.com/NVIDIA/spark-rapids/pull/14787
- Reference PR 2: https://github.com/NVIDIA/spark-rapids/pull/14810

The working branch is expected to be `delta_db173_OPTIMIZE_AUTO_COMPACT`.

Assumption for implementation: PR #14787 will be merged first, followed by PR #14810,
before starting the OPTIMIZE and auto-compaction work.

## Current Progress Checkpoint: May 20, 2026

Current branch: `delta_db173_OPTIMIZE_AUTO_COMPACT`.

The branch now has a clean four-commit feature stack on top of the prerequisite
DBR 17.3 DV/delete/update work. On 2026-05-20 the unstaged follow-up fixes were
split into fixup commits and autosquashed into the commits they belonged to.
A safety branch was created before the rewrite:

- `backup/delta_db173_OPTIMIZE_AUTO_COMPACT-before-fixup-split-20260519`

Current rewritten commits:

- `62765f8b9` - Merge PR #14787 DBR 17.3 DV reads
- `0c1b73f69` - Merge PR #14810 DBR 17.3 DELETE and UPDATE
- `eda0aa172` - Add DBR 17.3 Delta OPTIMIZE support
- `9ae539a3b` - Add DBR 17.3 Delta auto compaction support
- `0141241f0` - Add DBR 17.3 Delta liquid clustering support
- `a05033865` - Complete DBR 17.3 Delta liquid clustering support

Old-to-new commit mapping after the autosquash:

- `cabf93a45` -> `eda0aa172` (`Add DBR 17.3 Delta OPTIMIZE support`)
- `8b3bf155c` -> `9ae539a3b` (`Add DBR 17.3 Delta auto compaction support`)
- `85629b413` -> `0141241f0` (`Add DBR 17.3 Delta liquid clustering support`)
- `8012072d3` -> `a05033865` (`Complete DBR 17.3 Delta liquid clustering support`)

Current PR split guidance:

- First PR: include through `9ae539a3b`. This gives DBR 17.3 regular OPTIMIZE
  and auto-compaction support with liquid clustering still explicitly outside the
  PR scope.
- Follow-up PR: include `0141241f0` and `a05033865`. These contain liquid
  clustered OPTIMIZE, `OPTIMIZE FULL`, and the deeper liquid write-path work.
- Because local history was rewritten, updating the remote branch requires
  `git push --force-with-lease`.

Milestone status:

- Milestone 1 is complete via the two prerequisite merge commits.
- Milestone 2 is complete and committed in `eda0aa172`.
- Milestone 3 is complete and committed in `9ae539a3b`.
- Milestone 4 is implemented in the two liquid clustering commits,
  `0141241f0` and `a05033865`. Keep it in a separate PR from the regular
  OPTIMIZE/auto-compaction work.
- Milestone 5 is partially complete through targeted test updates. Final build
  and targeted test reruns are still required after the 2026-05-20 autosquash.

Validation completed so far:

- Earlier in this branch, full DBR build passed:

```bash
SKIP_DEP_INSTALL=1 WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
```

- Earlier targeted OPTIMIZE tests passed:

```bash
source jenkins/databricks/setup.sh
source jenkins/databricks/common_vars.sh

bash integration_tests/run_pyspark_from_build.sh \
    --runtime_env=databricks \
    -m "delta_lake" \
    --delta_lake \
    --test_type=$TEST_TYPE \
    -k delta_lake_optimize_table_test.py
```

Result observed: `5 passed`.

- Before the final liquid-clustering completion commit, targeted liquid
  clustering tests were failing (`10 failed, 3 skipped, 13 tests`). Those failures
  drove the follow-up liquid write-path work in `a05033865`. Re-run the liquid
  clustering target before opening or updating the liquid clustering PR.

- After the 2026-05-20 autosquash, the following checks passed:

```bash
git diff --check HEAD~4..HEAD -- delta-lake integration_tests
rg -n "^(<<<<<<<|=======|>>>>>>>)" delta-lake integration_tests
git diff --stat 15a10f6ee HEAD -- delta-lake integration_tests
```

The last command produced no diff, confirming the rewritten branch has the same
tracked `delta-lake/` and `integration_tests/` content as the pre-autosquash
fixup-chain tip.

Important implementation details now committed:

- DBR 17.3 `OptimizeTableCommand` and `OptimizeTableCommandEdge` are registered
  in the Databricks Delta provider and route through shared command meta classes.
- DBR 17.3 has a local `GpuOptimizeTableCommand` / `GpuOptimizeTableCommandEdge`
  and `GpuOptimizeExecutor` implementation based on the Delta 4.x shape.
- Regular OPTIMIZE supports unpartitioned and partitioned compaction and keeps
  unsupported cases on CPU: writable deletion-vector tables, ZORDER, REORG, and
  deletion-vector cleanup.
- `catalogTable` is threaded into DBR 14.3/17.3 GPU optimistic transaction
  constructors so DBR 17.3 OPTIMIZE transactions keep commit routing context.
- Row-tracking commit tags are produced through
  `RowTracking.addPreservedRowTrackingTagIfNotSet` in the DBR 17.3 optimize
  commit path.
- DBR 17.3 auto-compaction uses a `GpuAutoCompact` post-commit hook with the
  `CommittedTransaction` API, reuses `AutoCompactUtils.prepareAutoCompactRequest`,
  and runs GPU compaction through `GpuOptimizeExecutor` when supported.
- Auto-compaction preserves CPU fallback for background auto compaction and
  writable deletion-vector tables, using a shared `GpuDeltaCpuFallback` helper to
  disable RAPIDS around CPU fallback work.
- Liquid clustered OPTIMIZE and `OPTIMIZE FULL` are implemented in the follow-up
  liquid commits. The executor records `clusterBy`, `clusterByAuto`, and `isFull`
  in the Delta operation and tags clustered output files through
  `OptimizeTableStrategy`.
- DBR 17.3 single-column liquid clustering uses an explicit
  `repartitionByRange(...).sortWithinPartitions(...)` workaround because the DBR
  single-column clustering path is routed through a ZORDER-style path that asserts
  when translated directly for GPU execution.
- General liquid write-path completion lives in `a05033865`, including the
  `GpuWriteIntoDelta` and transaction-base changes needed for clustered commit
  metadata and CPU fallback reuse.

Recommended next steps:

1. For the smaller OPTIMIZE/auto-compaction PR, branch or reset to `9ae539a3b`
   and run:

```bash
source jenkins/databricks/setup.sh
source jenkins/databricks/common_vars.sh

bash integration_tests/run_pyspark_from_build.sh \
    --runtime_env=databricks \
    -m "delta_lake" \
    --delta_lake \
    --test_type=$TEST_TYPE \
    -k "delta_lake_optimize_table_test.py or delta_lake_auto_compact_test.py"
```

2. For the liquid clustering follow-up PR, keep `0141241f0` and `a05033865` and
   run:

```bash
source jenkins/databricks/setup.sh
source jenkins/databricks/common_vars.sh

bash integration_tests/run_pyspark_from_build.sh \
    --runtime_env=databricks \
    -m "delta_lake" \
    --delta_lake \
    --test_type=$TEST_TYPE \
    -k "delta_lake_liquid_clustering_test.py or delta_lake_optimize_table_test.py"
```

3. Before either PR is pushed, rerun the DBR build on the final branch state:

```bash
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
```

## Branch Setup

1. Start from `delta_db173_OPTIMIZE_AUTO_COMPACT`.
2. Merge the PR #14787 branch first.
   - This brings in native DBR 17.3 deletion-vector read support.
3. Merge the PR #14810 branch second.
   - This brings in DBR 17.3 DELETE and UPDATE support.
4. Resolve conflicts without rebasing.
   - The repo guidance says not to rebase during review because it can disturb reviewer
     comment context.

No additional snapshot commit is needed for milestone 1 beyond the merge commits
themselves. If either merge fast-forwards, there may be no separate merge commit.

## Implementation Milestones

### Milestone 1: Merge Prerequisite PR Branches

Status: complete. See `62765f8b9` and `0c1b73f69`.

Scope:

- Merge #14787.
- Merge #14810.
- Resolve conflicts.

Validation:

```bash
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh
```

Commit policy:

- No extra snapshot commit beyond the merge commit or fast-forward result.
- Any merge commit should be signed off.

### Milestone 2: OPTIMIZE Command Wiring and Regular Compaction

Status: complete and committed in `eda0aa172`.

Scope:

- Add DBR 17.3 command wiring for OPTIMIZE.
- Register `OptimizeTableCommand`.
- Investigate and likely register `OptimizeTableCommandEdge`.
- Add DBR 17.3 command meta classes.
- Add DBR 17.3 GPU OPTIMIZE command and executor.
- Support regular compaction for:
  - unpartitioned tables
  - partitioned tables
  - partition predicate OPTIMIZE
- Keep unsupported cases on CPU with explicit tagging:
  - ZORDER
  - writable deletion-vector tables
  - unsupported `DeltaOptimizeContext` or reorg cases
  - liquid clustering, which is handled by the follow-up liquid clustering commits

Implementation shape:

- Use OSS Delta 4.x `GpuOptimizeExecutor` as the primary model.
- Use DBR 14.3 code as Databricks package/API reference only.
- Keep DBR 17.3 implementation local initially.
- Do not extract common DBR 14.3/17.3 code unless overlap is clearly small and safe.

Validation:

```bash
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh

source jenkins/databricks/setup.sh
source jenkins/databricks/common_vars.sh

bash integration_tests/run_pyspark_from_build.sh \
    --runtime_env=databricks \
    -m "delta_lake" \
    --delta_lake \
    --test_type=$TEST_TYPE \
    -k delta_lake_optimize_table_test.py
```

Commit policy:

- Commit this milestone only after build and targeted OPTIMIZE tests pass.
- Use `git commit -s`.

### Milestone 3: GPU Auto Compaction Hook

Status: complete and committed in `9ae539a3b`.

Scope:

- Replace the current CPU auto-compaction hook only when GPU auto-compaction is supported.
- Use the DBR 17.3 `PostCommitHook.run(SparkSession, CommittedTransaction)` API.
- Use `CommittedTransaction` fields such as:
  - `deltaLog`
  - `catalogTable`
  - `postCommitSnapshot`
  - `committedActions`
- Reuse DBR 17.3 `AutoCompactUtils.prepareAutoCompactRequest`.
- Preserve CPU fallback for unsupported auto-compaction cases.
- Do not silently skip auto-compaction where CPU behavior previously worked.

Validation:

```bash
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh

source jenkins/databricks/setup.sh
source jenkins/databricks/common_vars.sh

bash integration_tests/run_pyspark_from_build.sh \
    --runtime_env=databricks \
    -m "delta_lake" \
    --delta_lake \
    --test_type=$TEST_TYPE \
    -k delta_lake_auto_compact_test.py
```

Commit policy:

- Commit this milestone only after build and targeted auto-compaction tests pass.
- Use `git commit -s`.

### Milestone 4: Liquid Clustering Support or Explicit Fallback

Status: implemented in `0141241f0` and completed in `a05033865`. Keep this
milestone in a separate liquid clustering PR from the regular OPTIMIZE/auto-compaction PR.

Why liquid clustering matters:

- Liquid clustering is physically maintained through OPTIMIZE.
- Supporting clustered table creation is not enough. The OPTIMIZE path must preserve
  clustering metadata and file tags correctly.

Scope:

- Validate OPTIMIZE on liquid-clustered tables.
- Validate `OPTIMIZE FULL` on liquid-clustered tables.
- Confirm whether DBR 17.3 GPU write paths can handle `isLiquidClustering` without
  falling back to CPU.
- Confirm clustering tags and row-tracking behavior are preserved.
- If the GPU path is not safe, keep liquid-clustered OPTIMIZE on CPU with explicit
  explain tagging.

Validation:

```bash
WITH_DEFAULT_UPSTREAM_SHIM=0 ./jenkins/databricks/build.sh

source jenkins/databricks/setup.sh
source jenkins/databricks/common_vars.sh

bash integration_tests/run_pyspark_from_build.sh \
    --runtime_env=databricks \
    -m "delta_lake" \
    --delta_lake \
    --test_type=$TEST_TYPE \
    -k delta_lake_liquid_clustering_test.py
```

Commit policy:

- Before opening or updating the liquid clustering PR, rerun the DBR build and
  targeted liquid clustering tests on the rewritten branch.
- Use `git commit -s` for any further commits.

### Milestone 5: Final Test Updates and Cleanup

Status: partially complete through targeted test updates; final validation is still pending after the 2026-05-20 autosquash.

Scope:

- Update integration tests to unskip Databricks OPTIMIZE coverage selectively.
- Add or adjust fallback tests for:
  - ZORDER
  - writable deletion vectors
  - disabled GPU Delta writes
  - unsupported liquid clustering cases, if any remain
- Keep diffs minimal.
- Avoid unrelated refactoring.

Validation:

Run affected Delta test files:

```bash
source jenkins/databricks/setup.sh
source jenkins/databricks/common_vars.sh

bash integration_tests/run_pyspark_from_build.sh \
    --runtime_env=databricks \
    -m "delta_lake" \
    --delta_lake \
    --test_type=$TEST_TYPE \
    -k "delta_lake_optimize_table_test.py or delta_lake_auto_compact_test.py or delta_lake_liquid_clustering_test.py or delta_lake_test.py or delta_lake_delete_test.py or delta_lake_update_test.py"
```

Commit policy:

- Commit final test updates and cleanup separately if they are substantial.
- Use `git commit -s`.

## Common Code Decision

Do not start by moving DBR 14.3 and DBR 17.3 OPTIMIZE code into a shared directory.

Reason:

- DBR 14.3 uses an older transaction-driven shape.
- DBR 17.3 is closer to OSS Delta 4.x:
  - `DeltaOptimizeContext`
  - `OptimizeTableStrategy`
  - `CommittedTransaction`
  - `AutoCompactRequest`
  - `OptimizeTableCommandEdge`
  - `OPTIMIZE FULL`
  - newer liquid clustering and row-tracking behavior

Potential common code can be reconsidered after DBR 17.3 builds and tests pass.
Likely shareable pieces would be small helpers for bin grouping, metrics, or stats.
Likely non-shareable pieces are command meta, post-commit hook logic, transaction
creation, auto-compaction request handling, and liquid clustering integration.

## Risk Controls

- Build after each meaningful Scala milestone.
- Run targeted tests immediately after each milestone.
- Commit only after the milestone build and targeted tests pass.
- Keep CPU fallback for unsupported cases.
- Do not include liquid clustering in the smaller OPTIMIZE/auto-compaction PR; keep it in the follow-up PR and validate `OPTIMIZE` / `OPTIMIZE FULL` there.
- Keep writable deletion-vector OPTIMIZE on CPU initially.
- Preserve existing CPU auto-compaction behavior when GPU auto-compaction is unsupported.
- Use signed-off commits with `git commit -s`.

