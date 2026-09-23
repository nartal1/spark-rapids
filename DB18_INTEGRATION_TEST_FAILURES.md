# Databricks 18.3 Integration-Test Progress and Failures

This is a cumulative checkpoint of the Databricks 18.3 integration run. It is regenerated from the available logs on each checkpoint so failures and completed test files are not duplicated.

## Snapshot

- Snapshot time: 2026-10-02 22:30:42 UTC
- Runtime: Databricks 18.3, Spark 4.1.0, Java 21
- Shim: `spark410db183`
- Test type: `nightly`
- Current public test checkout: `cf54d1b4bd735ffc459ad865ea56d2562f8a1f92` (`db_18_support`) plus the authorized DB18-only Python-runner command-header fix. The original build patch SHA-256 is `ac401629c61620b041ea91dbb7f1779efa8f47a0a3746eeb04aa6a1f37e3072e`; UDF-validation source identities, including subsequent whitespace-only cleanup, are pinned by runtime manifest SHA-256 `f91b87c149574e59a23e0b1afd7f732db46be5f6d9729557385cd080ffeaa6e7`. After full UDF completion, a test-only protected-access repair changed the regression-suite hash to `849a19ef4ec33f2962a87d922b99f74f1b4998e6dfeb19e3ee79d016269cb1f8`; production sources and loaded UDF artifacts did not change. Historical non-UDF results were obtained before this fix and are not claimed as reruns on the corrected artifact.
- Current private shim checkout revision: `15bb08c0fc5f4177496191bbb808c760d4caaac4` (`db_18_support`)
- Current recovery build state: the October 2 private upstream `spark400` and DBR `spark410db183` builds passed; the corrected public build (`WITH_DEFAULT_UPSTREAM_SHIM=0`, `SKIP_DEP_INSTALL=0`) passed all 20 modules and packaging, and test setup passed. The dependency-install and optional-variable runner failures are non-test recovery history, detailed below.
- Current private DBR JAR SHA-256: `c6d1eac2a5d06562ea297cac7f0afe87c08f126695ccb218386bfc68f491c159`
- Current public distribution JAR SHA-256: `0eb1240d995f877913e95f13638d04931c5fdd308fcd085e0ff7605f3721b1da`
- Current public integration-test JAR SHA-256: `2d32ce7c4fb4288b8b8f5e2a2c070a218354b18d0e47e12b98fca54ac31bef29`
- Current test state: integration coverage is complete, not all-green. The corrected public build passed all 20 modules and packaging, both scalar/aggregate gates passed, and full corrected-source `udf_test.py` reached exact 146-node JUnit/report coverage at 22:23:53 UTC: 97 passed, 36 failed, 4 skipped, 9 xfailed, zero errors/XPASS. Its pytest exit is 1; validation runner `79635` exited 0 because all coverage/evidence checks succeeded, not because every test passed. The new command-header regression suite compiled and passed both tests separately. The historical 39-failure UDF entry is now superseded exactly once. Cumulative counts are 110 logical phase/file invocations, 108 filenames, 32,803 outcomes, and 117 failures; old-artifact attempts and smoke gates are excluded.
- Historical September build state: the DB18-only public build used `WITH_DEFAULT_UPSTREAM_SHIM=0`, `SKIP_DEP_INSTALL=1` and passed all 20 modules and packaging after its documented setup/packaging failures. Historical private/public distribution/public integration-test hashes were respectively `70f4b117e593073dd56e8af7e1d1758114f49c094b40650e277fb195901052de`, `8ca005680471fa9ad1175b214f4db27076a574a7b83345cd1bc91428ce0cf190`, and `6c3be5cb27e00d9fe678be1551019e14ba24b05e7a62afe01a987b33e190f444`; they are not the current rebuilt artifacts.
- First-run state: complete; PID `569029` exited after the PyArrow phase.
- First clean main/cache continuation state: three files completed under PID `874466`; its incomplete `arithmetic_ops_test.py` evidence is retained as diagnostic history but is superseded by the complete rerun below.
- Historical September continuation state: the original main/cache runner is terminal under PID `19995`, one Python file at a time against exact checkout `cf54d1b4b` with `TEST_PARALLEL=0`. September setup reached terminal exit 0 at 16:39:35 UTC. All fifty-three resumed clean-main file processes through `window_function_test.py` are terminal. The separate alternate-cache `cache_test.py` invocation is also terminal with 615 passed tests. The outer runner exited 1 at 03:04:40 UTC because its completed plan contains earlier pytest failures; this aggregate exit is not an additional test failure. A separate full targeted `udf_test.py` rerun started under PID `1304177` at 03:11:57 UTC to resolve the original run's 107-test coverage gap and was confirmed alive at 03:12:16 UTC with seed `1790565130`. That September cluster then became unreachable: three bounded SSH attempts failed before login, including an explicit 10-second connection timeout at 03:45 UTC. Its evidence was subsequently lost; no terminal targeted-UDF result is available, so those partial outcomes remain excluded. This is not the current October cluster/runner status.
- First-run evidence directory: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z`
- First clean main/cache evidence directory: `/home/ubuntu/spark-rapids/db18-clean-main-cache-results/20260923T072607Z`
- Second-cluster recovered clean main/cache evidence directory (historical and unavailable after cluster replacement): `/home/ubuntu/cudf-spark/db18-clean-main-cache-results/20260923T184559Z`
- Fresh recovered clean main/cache evidence directory: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z`
- Fresh resume-runner SHA-256: `ccb7cc9de372448b0938cb590d8d04efdbb1c389dc68b6764a4f7eae33d55a50`
- Clean-main marker expression: `not delta_lake and not shuffle_test and not pyarrow_test`; the 15 completed Delta files and `parquet_pyarrow_test.py` are omitted from the file plan, while non-shuffle coverage in shared files remains eligible.
- Delta Lake random seed: `1790135081` (the invalid main/cache attempts used different seeds)
- Multithreaded-shuffle random seed: `1790144460`
- PyArrow random seed: `1790144768`
- Recovered arithmetic random seed: `1790189207`
- Recovered AST random seed: `1790192277`
- Recovered collection-operations random seed: `1790194588`
- Recovered datasource-v2 read random seed: `1790197601`
- Fresh recovered join random seed: `1790527195`
- Fresh recovered JSON-fuzz random seed: `1790532195`
- Fresh recovered JSON-matrix random seed: `1790532251`
- Fresh recovered JSON random seed: `1790532755`
- Fresh recovered JSON-tuple random seed: `1790534651`
- Fresh recovered Kudo-dump random seed: `1790534747`
- Fresh recovered limit random seed: `1790534823`
- Fresh recovered logic random seed: `1790535106`
- Fresh recovered map random seed: `1790535205`
- Fresh recovered misc-expression random seed: `1790536264`
- Fresh recovered misc random seed: `1790536358`
- Fresh recovered mortgage random seed: `1790536422`
- Fresh recovered no-op-write random seed: `1790536522`
- Fresh recovered ORC-cast random seed: `1790536572`
- Fresh recovered ORC random seed: `1790536804`
- Fresh recovered ORC-write random seed: `1790540857`
- Fresh recovered Parquet-testing random seed: `1790541365`
- Fresh recovered Parquet random seed: `1790541413`
- Fresh recovered Parquet-write random seed: `1790547938`
- Fresh recovered private optimizer aggregate-pushdown random seed: `1790548770`
- Fresh recovered private optimizer standard-deviation-decomposition random seed: `1790548856`
- Fresh recovered private optimizer skewed-BHJ random seed: `1790548928`
- Fresh recovered private optimizer shared-subquery-scan random seed: `1790549002`
- Fresh recovered project-literal-alias random seed: `1790549083`
- Fresh recovered project-presplit random seed: `1790549159`
- Fresh recovered Protobuf-data-generator random seed: `1790549247`
- Fresh recovered Protobuf random seed: `1790549321`
- Fresh recovered partition-column-pruning random seed: `1790549395`
- Fresh recovered Py4J-workaround random seed: `1790549700`
- Fresh recovered nightly-select random seed: `1790549751`
- Fresh recovered random-expression random seed: `1790550363`
- Fresh recovered range random seed: `1790550449`
- Fresh recovered reduced-IT-selection random seed: `1790550515`
- Fresh recovered reduced-test-matrix-helper random seed: `1790550563`
- Fresh recovered regexp-no-Unicode random seed: `1790550611`
- Fresh recovered regexp random seed: `1790550661`
- Fresh recovered repartition random seed: `1790550902`
- Fresh recovered row-based-UDF random seed: `1790552116`
- Fresh recovered row-conversion random seed: `1790552199`
- Fresh recovered sample random seed: `1790552335`
- Fresh recovered scan-default-values random seed: `1790552549`
- Fresh recovered schema-evolution random seed: `1790552648`
- Fresh recovered sort random seed: `1790552777`
- Fresh recovered string random seed: `1790555351`
- Fresh recovered string-type random seed: `1790555599`
- Fresh recovered struct random seed: `1790555730`
- Fresh recovered subquery random seed: `1790555943`
- Fresh recovered time-window random seed: `1790556197`
- Fresh recovered cuDF-UDF random seed: `1790556509`
- Fresh recovered UDF random seed: `1790556557`
- Fresh recovered URL random seed: `1790560253`
- Fresh recovered variant random seed: `1790560373`
- Fresh recovered window-function random seed: `1790560497`
- Fresh recovered alternate-cache random seed: `1790563559`
- Targeted UDF rerun random seed: `1790565130`

## Phase status

### October 2 cluster recovery history

At the start of October 2 recovery, the restarted DBR 18.3 cluster was reachable but previous cluster-local repos, build artifacts, and test evidence were lost. All September cluster paths below are historical and unavailable; their already-reviewed terminal results remain valid except for the explicitly superseded UDF entry. No complete replacement UDF result had survived at recovery start. The timestamped pre-fix history below records that pending scope; the later completed header-fix validation section resolves it.

The private repo was cloned into `/home/ubuntu/spark-rapids-private` from a verified Git bundle at `15bb08c0fc5f4177496191bbb808c760d4caaac4` because internal GitLab DNS is unavailable. The public repo in `/home/ubuntu/spark-rapids` was restored at the previously tested `cf54d1b4bd735ffc459ad865ea56d2562f8a1f92` on local branch `db_18_support`; the rolling report branch is preserved separately. No source fix was added during recovery.

The private upstream and DB18 builds completed at 17:58:28 UTC. The first public build failed before compilation because `SKIP_DEP_INSTALL=1` omitted additional public DBR artifacts; its diagnostic log is preserved separately. The corrected public build enabled dependency installation with `SKIP_DEP_INSTALL=0`, retained `WITH_DEFAULT_UPSTREAM_SHIM=0`, passed all 20 modules, and completed packaging at 18:13:12 UTC. Python test setup completed at 18:13:38 UTC.

The recovery runner then stopped while sourcing optional environment variables under `set -u`. A corrected continuation resumed directly at testing without repeating builds or setup. PID `18906` started the full UDF invocation at 18:14:15 UTC with Java 21, `TESTS=udf_test.py`, `SPARK_SHIM_VER=spark410db183`, `TEST_PARALLEL=0`, nightly, and the original clean-main marker expression. At the historical 18:15:15 UTC capture, it was alive and Spark confirmed Java 21.0.12, Spark 4.1.0, Scala 2.13, and the DB18 shim. It collected 146 tests with seed `1790964877` but had no terminal record yet. Counts at that earlier capture were unchanged: 110 invocations, 32,696 outcomes, 120 failures. Its later exit and final corrected-source replacement are recorded below.

The first node, `udf_test.py::test_pandas_math_udf[Byte][DATAGEN_SEED=1790964877, TZ=UTC]`, has not produced a terminal outcome. At 18:25:56 UTC, GPU Arrow Python tasks 0–3 in stage 1 each reported a 600-second idle timeout while their workers remained alive. Earlier diagnostics include a SafeSpark UDF environment-installer `ClassCastException` at 18:15:41 UTC, inability to start the UC securable server due to denied access to `/databricks/sparkconnect/creds` at 18:15:47 UTC, and a resource scheduling warning at 18:15:50 UTC. These are observed diagnostics, not established causes or additional pytest failures. The active invocation is left intact; only complete replacement coverage can supersede the original 39-failure watchdog cascade and its 107-test gap.

At the 18:56:29 UTC checkpoint, the same four workers continue to report idle warnings, most recently at 18:55:56 UTC. The hanging detector reported that requesting executors is unsupported by the current scheduler through 18:55:05 UTC. At 18:44:53 UTC, the deadlock detector found no deadlock but warned that a component was likely hanging (and noted that the detector can produce false positives). No continuation exit record, terminal UDF stage row, or UDF JUnit result exists. These ongoing diagnostics do not add terminal failures or resolve selected-test coverage; the run is not considered complete.

At the 19:26:24 UTC capture, all three processes had exited. The stage ledger and continuation exit record both prove exit 16 at 19:16:26 UTC. At 19:15:55 UTC, the RAPIDS watchdog cancelled job 1 after 3,600 seconds and initiated driver shutdown. JUnit `scala2.13/integration_tests/target/run_dir-20261002181437-3RAa/TEST-pytest-1790964877124941557.xml` records 36 failed named cases, zero errors, and zero skips; it also contains an anonymous, outcome-free `<testcase time="0.022"/>` placeholder that is not a passed test. The first Byte case reports `SPARK_JOB_CANCELLED`; the next 35 failures report a stopped SparkContext. Of 146 collected cases, 110 have no named terminal outcome. This attempt remains separate diagnostic evidence: do not add its 36 failures or count the anonymous placeholder, and do not supersede the historical 39-failure UDF cascade until complete replacement coverage is available.

Resume only UDF coverage, using one fresh Spark/pytest process per selected node with the same source, artifacts, Java, serial execution, marker expression, and original job timeout. Preserve the full 146-node collection and original randomization; prioritize the 110 nodes lacking outcomes, then independently rerun the 35 shutdown-cascade nodes. The first Byte timeout is a genuine terminal observation and need not be repeated merely to recover coverage. Reconcile exactly one meaningful outcome per selected node across these isolated runs before replacing the historical UDF totals. Do not skip or weaken any test, modify tested source, or rerun completed non-UDF phases.

### October 2 isolated UDF continuation history (stopped)

The reviewed pre-fix isolated runner used PID `39303` with evidence in `/home/ubuntu/db18-udf-isolated-20261002`. Its full collection validated 146 distinct selected nodes, all 36 prior decorated IDs, and the partition of 110 missing cases plus 35 shutdown-cascade cases to rerun independently. Datagen and OOM seeds were `1790964877`; each case used a fresh serial Spark/pytest process with the original marker expression and timeout. Pre-fix source/artifact identities were unchanged. The selector ran after normal full-collection marker/random-OOM processing; evidence checks distinguished XFAIL/XPASS and stopped on ambiguity. This runner was intentionally stopped before the authorized fix; it is not the current execution or resume plan.

At the 19:56:29 UTC capture, isolated case 1 is `udf_test.py::test_single_aggregate_udf_more_types[Timestamp][DATAGEN_SEED=1790964877, TZ=UTC, APPROXIMATE_FLOAT]`, started at 19:40:31 UTC. Its log proves 146 collected, 145 deselected, and one selected. The machine report records only setup passed, not a completed call. At 19:51:44 UTC, `GpuWindowArrowPythonRunner` reported a 600-second idle timeout for task 0 in stage 3 (TID 9), with its worker still alive. The hanging-task detector continues to report unchanged task metrics since 19:41:46 UTC. These are diagnostics, not a terminal failure or an established cause. No runner exit, completed-case ledger, or completion record exists. The collection-only JUnit is not execution coverage.

At the 20:26:31 UTC checkpoint, PID `39303` remains alive and the same first isolated case is current. Its machine report still contains only setup passed; no runner exit, completed-case ledger, or completion record exists. The same window Arrow Python worker repeated its 600-second idle warning at 20:21:45 UTC. At 20:10:53 UTC the deadlock detector found no deadlock but warned of a likely hanging component, with its false-positive caveat; executor requests remain unsupported through 20:26:01 UTC. No new terminal outcome is added. Leave the active process intact and retain the original job timeout; the cause is not yet established.

At the 20:56:33 UTC capture, isolated case 1 has terminal ledger evidence: exit 16 at 20:41:45 UTC, with exactly one named JUnit failure and matching machine call-failed report. The node is `udf_test.py::test_single_aggregate_udf_more_types[Timestamp][DATAGEN_SEED=1790964877, TZ=UTC, APPROXIMATE_FLOAT]`. Its `SPARK_JOB_CANCELLED` failure follows the watchdog cancellation of job 1 at 20:41:43 UTC after 3,600 seconds, while the window Arrow Python task remained idle. Evidence: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20261002194041-Lcvx/TEST-pytest-1790970041264857565.xml`, `node-001.log`, `node-001-reports.jsonl`, and `completed.tsv`. This is one meaningful replacement failure, not a cascade or an additional baseline failure. The underlying hang cause remains unestablished.

The runner automatically advanced to isolated case 2 at 20:41:45 UTC: `udf_test.py::test_group_aggregate_udf[Byte][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]`. It is alive with setup passed only and no terminal call result. At 20:53:15 UTC, four `GpuWindowArrowPythonRunner` tasks in stage 6 (TIDs 20–23) reported 600-second idle timeouts with their workers alive. These are partial diagnostics and are excluded from replacement outcomes. No runner exit or completion record exists.

At the 21:26:32 UTC checkpoint, PID `39303` is alive and case 2 remains current. The ledger still contains only case 1's terminal failure; case 2's machine report is setup passed only. No runner exit or completion record exists. All four stage-6 window Arrow Python workers repeated 600-second idle warnings at 21:23:16 UTC; executor requests remain unsupported through 21:22:18 UTC. There is no new terminal outcome or established cause. The active process and original timeout are unchanged; replacement coverage remains 2 of 146, with 144 outcomes pending.

Historical progress at 21:26:32 UTC was one isolated terminal failure plus one retained Byte timeout, or 2 of 146 meaningful pre-fix outcomes. Those interim results were never merged into baseline totals. They are now diagnostic history only; the completed corrected-source full run below supersedes the historical UDF entry. Do not restart the old 145-node isolated plan or retain old-artifact timeouts as fixed-source results.

- Isolated status/evidence: `runner.pid`, `runner.exit` when present, `current.json`, `plan.json`, `pending.json`, `retained-timeout.json`, `completed.tsv` when present, `complete.json` when complete, `collection.log`, `node-NNN.log`, `node-NNN-reports.jsonl`, and per-node JUnit paths recorded in the ledger
- Runner SHA-256 (`db18_udf_isolated.py`): `cedbd3ce110d3fcc2349358b4becd7d4bb6906ca29eee1095d67661e6fb25f04`
- Selector SHA-256 (`db18_udf_select.py`): `3d8925f8cf0c67ca020c902b16ac048c45fe79464254f93a24a64f06cf391b60`
- Launcher SHA-256 (`start_db18_udf_isolated.sh`): `a559ffc30fdaf6b2ef07a79fb361b29bc6d0ccb030b37a5eefbcaade442d80c5`
- Launcher log: `/home/ubuntu/db18-udf-isolated-20261002/launcher.log`

### October 2 authorized DB18 command-header fix and validation

The preceding recovery/isolated-run paragraphs are timestamped pre-fix history. Isolated case 2, `test_group_aggregate_udf[Byte][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]`, reached terminal watchdog failure with exit 16 at 21:43:43 UTC. Case 3, `test_group_aggregate_udf[Short]`, began at 21:43:43 UTC but was intentionally interrupted around 21:46 UTC after the user requested fixing the DB18 header before further coverage. The verified runner process group was terminated and no old runner/JVM remained at the later build check. Both terminal old-artifact isolated failures, the earlier Byte scalar timeout, and the interrupted Short case remain diagnostic history, not corrected-source outcomes or additions to cumulative counts.

Installed DB18 bytecode and Python worker protocol show that `BasePythonRunner.Writer` writes `runnerConf` and `evalConf` before calling `writeCommand`. The former GPU runners sent a second configuration map inside that command, where the worker expects logger fields and UDF metadata. The fix exposes Arrow settings through `runnerConf` and writes only native UDF command metadata for the scalar, grouped-map, window/aggregate, and cogroup DB18 runners. Older shim implementations are preserved. A new DB18 regression suite compares all four real writer command bodies with native `PythonUDFRunner.writeUDFs` output and checks configuration placement.

The Java 17 DB18-only public clean-package build passed all 20 modules. Private artifacts, test setup, and previously completed non-UDF phases were not rebuilt/rerun. Regression-harness history: reactor `test` failed before the suite because classified intermodule JARs attach at `package`; Java 21 verification hit the legacy style checker's SecurityManager limitation; Java 17 verification reported three pre-existing style findings outside this fix (`GpuBatchScanExec.scala`, `GpuBatchScanExecSuite.scala`, and import order in `GpuOptimisticTransaction.scala`). No checker was disabled and those files were not changed. Explicit reactor suite selection failed in upstream modules that do not contain the tests-module suite; package discovery then lacked native DBR logging dependencies (`MessageFactory`). The new suite's first compilation also exposed protected `runnerConf` access through its base type; this test defect was fixed with reflective access to each concrete override, without changing production sources. Direct compilation and execution against built production classes plus native DBR/test/JNI dependencies then passed exactly two tests, zero failed/aborted, at 22:25:57 UTC. A first direct launch missing the existing cuDF JNI dependency was retained as harness history. Normal Maven reactor package/test compilation then passed all 17 selected modules at 22:28:04 UTC (tests skipped in that compilation run, not claimed as executions). The two executed regression tests used Java 17/native DBR classpath, while all integration checks used Java 21. Global verify remains blocked by the three documented pre-existing findings. No launch/check issue is added to integration-test counts.

Validation evidence is `/home/ubuntu/db18-udf-header-fix-20261002`. The reviewed harness freezes the original 146 decorated node IDs and datagen/OOM seeds `1790964877`, verifies both repository base HEADs and source/harness/artifact hashes before and after each phase, and uses fresh serial Spark/pytest processes. First it runs `test_pandas_math_udf[Byte]`, then `test_group_aggregate_udf[Byte]`; each gate requires exactly one real named pass, matching machine pytest reports, and exit 0. Only then does it run full `udf_test.py` with the unchanged marker expression and original timeout. Gate attempts and the full replacement use isolated logs; proven gate summaries can be revalidated on resume. A partial full-file attempt is not merged into replacement totals. Complete replacement requires exactly the frozen 146-node JUnit/report set, retaining all six outcome categories and rejecting anonymous outcome-bearing cases.

The scalar gate selected `test_pandas_math_udf[Byte][DATAGEN_SEED=1790964877, TZ=UTC]` and recorded one pass with exit 0 at 22:12:36 UTC. The aggregate gate selected `test_group_aggregate_udf[Byte][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` and recorded one pass with exit 0 at 22:14:10 UTC. Both runs collected the full 146-node seeded plan before deselecting 145, compare GPU with CPU, and have matching named JUnit/machine reports. This confirms the two formerly timed-out paths can execute with the corrected header. Full `udf_test.py` ran from 22:14:12 to terminal validation at 22:23:53 UTC and reconciled exactly 146 named outcomes: 97 passed, 36 failed, 4 skipped, 9 xfailed, zero errors/XPASS. Both smoke cases also passed in the full run; their smoke outcomes are not counted again. Its 36 failures are window-data framing errors, not a watchdog shutdown cascade. `complete.json` confirms coverage completeness; the pytest process exited 1 while the evidence validator exited 0.

The old baseline's 39 UDF failures are removed exactly once and replaced with the complete 146 outcomes: `32,696 - 39 + 146 = 32,803`; failures are `120 - 39 + 36 = 117`. Logical phase/file coverage remains 110 invocations across 108 filenames because this is replacement of one UDF entry, not an additional logical file. The two smoke subprocesses, full replacement subprocess, diagnostic old attempts, and two unit tests are tracked separately, not inflated into the integration totals. All 92 clean-main files, 15 Delta files, shuffle/PyArrow phases, and alternate-cache phase have terminal evidence; no selected UDF case lacks an outcome.

### October 2 incomplete UDF attempt: observed failed nodes

All nodes below are prefixed by `udf_test.py::`. The first failed on the job watchdog; the other 35 failed after the SparkContext stopped. This incomplete October pre-fix attempt is diagnostic history and excluded from current totals; completed corrected-source coverage is recorded separately.

- `test_pandas_math_udf[Byte][DATAGEN_SEED=1790964877, TZ=UTC]`
- `test_pandas_math_udf[Short][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM]`
- `test_pandas_math_udf[Integer][DATAGEN_SEED=1790964877, TZ=UTC]`
- `test_pandas_math_udf[Long][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM]`
- `test_iterator_math_udf[Byte][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM]`
- `test_iterator_math_udf[Short][DATAGEN_SEED=1790964877, TZ=UTC]`
- `test_iterator_math_udf[Integer][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM]`
- `test_iterator_math_udf[Long][DATAGEN_SEED=1790964877, TZ=UTC]`
- `test_pandas_scalar_udf_nested_type[Array(Byte)][DATAGEN_SEED=1790964877, TZ=UTC]`
- `test_pandas_scalar_udf_nested_type[Array(Short)][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM]`
- `test_pandas_scalar_udf_nested_type[Array(Integer)][DATAGEN_SEED=1790964877, TZ=UTC]`
- `test_pandas_scalar_udf_nested_type[Array(Long)][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM]`
- `test_pandas_scalar_udf_nested_type[Array(Float)][DATAGEN_SEED=1790964877, TZ=UTC]`
- `test_pandas_scalar_udf_nested_type[Array(Double)][DATAGEN_SEED=1790964877, TZ=UTC]`
- `test_pandas_scalar_udf_nested_type[Array(String)][DATAGEN_SEED=1790964877, TZ=UTC]`
- `test_pandas_scalar_udf_nested_type[Array(Boolean)][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM]`
- `test_pandas_scalar_udf_nested_type[Array(Date)][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM]`
- `test_pandas_scalar_udf_nested_type[Array(Timestamp)][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM]`
- `test_pandas_scalar_udf_nested_type[Array(Array(Short))][DATAGEN_SEED=1790964877, TZ=UTC]`
- `test_pandas_scalar_udf_nested_type[Array(Array(String))][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM]`
- `test_pandas_scalar_udf_nested_type[Array(Struct(['child0', Byte],['child1', String],['child2', Float]))][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM]`
- `test_pandas_scalar_udf_nested_type[Struct(['child0', Byte],['child1', Short],['child2', Integer],['child3', Long],['child4', Float],['child5', Double],['child6', String],['child7', Boolean],['child8', Date],['child9', Timestamp])][DATAGEN_SEED=1790964877, TZ=UTC]`
- `test_pandas_scalar_udf_nested_type[Struct(['child0', Array(Short)],['child1', Struct(['child0', Byte],['child1', Short],['child2', Integer],['child3', Long],['child4', Float],['child5', Double],['child6', String],['child7', Boolean],['child8', Date],['child9', Timestamp])])][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM]`
- `test_single_aggregate_udf[Byte][DATAGEN_SEED=1790964877, TZ=UTC, APPROXIMATE_FLOAT]`
- `test_single_aggregate_udf[Short][DATAGEN_SEED=1790964877, TZ=UTC, APPROXIMATE_FLOAT]`
- `test_single_aggregate_udf[Integer][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
- `test_single_aggregate_udf[Long][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
- `test_single_aggregate_udf_more_types[Byte][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
- `test_single_aggregate_udf_more_types[Short][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
- `test_single_aggregate_udf_more_types[Integer][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
- `test_single_aggregate_udf_more_types[Long][DATAGEN_SEED=1790964877, TZ=UTC, APPROXIMATE_FLOAT]`
- `test_single_aggregate_udf_more_types[Float][DATAGEN_SEED=1790964877, TZ=UTC, APPROXIMATE_FLOAT]`
- `test_single_aggregate_udf_more_types[Double][DATAGEN_SEED=1790964877, TZ=UTC, APPROXIMATE_FLOAT]`
- `test_single_aggregate_udf_more_types[String][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
- `test_single_aggregate_udf_more_types[Boolean][DATAGEN_SEED=1790964877, TZ=UTC, APPROXIMATE_FLOAT]`
- `test_single_aggregate_udf_more_types[Date][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`

October 2 pre-fix recovery artifact SHA-256 values (the corrected-header public artifacts are listed in the current snapshot above):

- Private DB18 JAR: `c6d1eac2a5d06562ea297cac7f0afe87c08f126695ccb218386bfc68f491c159`
- Public distribution JAR: `5694f0ed0b3733741fc176a1837fa28956199bc3f6b14114c44d8366eb30c8ee`
- Public integration-test JAR: `4143c1f52ce060529783dd62bdac6113ea4c3f601c63c9b9471b749047dd6826`

- Recovery evidence directory: `/home/ubuntu/db18-recovery-20261002`
- Recovery runner: `/home/ubuntu/recover_db18_20261002.sh`
- Corrected UDF continuation: `/home/ubuntu/continue_db18_udf_20261002.sh`
- UDF continuation PID/exit records: `/home/ubuntu/db18-recovery-20261002/udf-continuation.pid`, `/home/ubuntu/db18-recovery-20261002/udf-continuation.exit`
- Corrected continuation launch log: `/home/ubuntu/db18-udf-continuation-20261002.attempt2.launch.log`
- Runner launch log: `/home/ubuntu/db18-recovery-20261002.launch.log`
- Runner PID/exit records: `/home/ubuntu/db18-recovery-20261002/runner.pid`, `/home/ubuntu/db18-recovery-20261002/runner.exit`
- Stage records: `/home/ubuntu/db18-recovery-20261002/current-stage.tsv`, `/home/ubuntu/db18-recovery-20261002/completed-stages.tsv`
- Build/setup/test logs: `private-build.log`, `public-build.log`, `setup.log`, and `udf_test.log` under the recovery evidence directory
- Failed first public attempt: `/home/ubuntu/db18-recovery-20261002/public-build.attempt1.log`
- Artifact hashes: `/home/ubuntu/db18-recovery-20261002/artifact-sha256.txt`
- Private Git bundle SHA-256: `7e63e427db22ded9cb1a3318e4288ae3c49dfb8333f3b7afe9cf1e4bdec4032f`
- Public test-head Git bundle SHA-256: `989b845c554bb056be7687e5b6e911c3a6240302aa416c75125266a03211e2d9`

The phase table combines preserved September evidence with the completed October UDF replacement. Invalid/lost attempts are explicitly historical and excluded. The new exact 146-node replacement resolves the former 107-test coverage gap; no integration continuation is pending.

| Phase | Status | Evidence at this checkpoint |
| --- | --- | --- |
| Setup | Complete, passed | Original setup exited 0 at 03:41:59 UTC; fresh recovered setup exited 0 at 16:39:35 UTC |
| Environment initialization | Complete, passed | Exit 0; DBR 18.3 / Spark 4.1.0 / `spark410db183` validated |
| Original main attempt | Historical invalid collection, superseded | Exit 2; fixed `udf_test.py` `NameError`; excluded from counts and replaced by completed clean-main coverage |
| Original cache attempt | Historical invalid collection, superseded | Exit 2 on the same fixed `NameError`; excluded and replaced by completed cache coverage |
| Delta Lake | Complete, failed | Exit 1; 03:44:33–06:20:39 UTC; all 947 selected tests reached terminal outcomes, with 30 failures |
| Multithreaded shuffle | Complete, passed | Exit 0; 06:20:39–06:26:00 UTC; all 85 selected tests passed |
| PyArrow | Complete, passed | Exit 0; 06:26:00–06:54:48 UTC; all 144 selected tests reached terminal outcomes with no failures |
| Private recovery build | Complete, passed | Exact private revision `15bb08c0f`; upstream `spark400` and DBR `spark410db183` builds succeeded after bundle-based restoration |
| Public recovery build | Complete, passed | Exact public revision `cf54d1b4b`; corrected DB18-only build passed all 20 modules and packaging |
| Clean main continuation | Complete, contains failures | All 92 planned files have terminal coverage; complete October UDF evidence replaces the original 39-outcome watchdog invocation |
| Clean cache continuation | Complete, passed | Separate `cache_test.py` invocation with the alternate cache serializer reached a terminal ledger row with exit 0; all 615 selected tests passed |
| September targeted UDF rerun | Historical interrupted/lost evidence | PID `1304177` was lost with its cluster before terminal evidence; no outcomes counted |
| October corrected-source UDF | Complete, failed | All 146 selected nodes reconciled: 97 passed, 36 failed, 4 skipped, 9 xfailed; both smoke gates passed separately and are excluded from cumulative counts |

## Cumulative terminal outcome counts

These counts include completed Delta, multithreaded-shuffle, PyArrow, clean-main, and alternate-cache coverage. The invalid initial main/cache collection attempts, superseded incomplete arithmetic attempt, and original 39-outcome incomplete UDF attempt are excluded. The complete corrected-source 146-case UDF replacement is counted once; smoke and unit tests are not included.

| Outcome | Count |
| --- | ---: |
| Passed | 30,461 |
| Failed | 117 |
| Error | 0 |
| Skipped | 1,046 |
| Expected failure (`XFAIL`) | 826 |
| Unexpected pass (`XPASS`) | 353 |
| Total observed | 32,803 |

## Delta Lake terminal outcome counts

These counts are from the terminal pytest summary and JUnit XML.

| Outcome | Count |
| --- | ---: |
| Passed | 567 |
| Failed | 30 |
| Skipped | 188 |
| Expected failure (`XFAIL`) | 131 |
| Unexpected pass (`XPASS`) | 31 |
| Total observed | 947 |

## Delta Lake failures

The descriptions below are based on the terminal pytest summary and JUnit XML.

1. `delta_lake_liquid_clustering_test.py::test_delta_rtas_sql_liquid_clustering`
   - GPU override raises `IllegalArgumentException` because `OverwriteByExpressionExecV1` is not columnar.
2. `delta_lake_liquid_clustering_test.py::test_delta_append_sql_liquid_clustering`
   - GPU override raises `IllegalArgumentException` because `DataWritingCommandExec` is not columnar.
3. `delta_lake_liquid_clustering_test.py::test_delta_insert_overwrite_static_sql_liquid_clustering`
   - GPU override raises the same non-columnar `DataWritingCommandExec` exception.
4. `delta_lake_liquid_clustering_test.py::test_delta_append_df_liquid_clustering`
   - GPU override raises the same non-columnar `DataWritingCommandExec` exception.
5. `delta_lake_liquid_clustering_test.py::test_delta_insert_overwrite_df_liquid_clustering[overwrite_mode=STATIC]`
   - GPU override raises the same non-columnar `DataWritingCommandExec` exception.
6. `delta_lake_merge_test.py::test_delta_merge_not_matched_by_source_schema_evolution_db173`
   - The GPU merge job aborts in `GpuRapidsProcessDeltaMergeJoinIterator` with a cuDF concatenate type mismatch (`Type mismatch in columns to concatenate`).
7. `delta_lake_optimize_table_test.py::test_delta_optimize_clustered_table[False]`
   - CPU/GPU Delta-log parity fails at the `add` action because the generated `LIQUID_METADATA_ID` tag values differ.
8. `delta_lake_optimize_table_test.py::test_delta_optimize_clustered_table[True]`
   - The deletion-vector variant has the same CPU/GPU `LIQUID_METADATA_ID` mismatch.
9. `delta_lake_optimize_table_test.py::test_delta_optimize_clustered_table_gpu_write`
   - CPU/GPU Delta-log parity fails because the `add.tags.LIQUID_METADATA_ID` values differ.
10. `delta_lake_optimize_table_test.py::test_delta_optimize_clustered_table_gpu_write_with_deletion_vectors`
    - The deletion-vector GPU-write variant has the same `LIQUID_METADATA_ID` parity mismatch.
11. `delta_lake_optimize_table_test.py::test_delta_optimize_clustered_table_gpu_write_repeated`
    - Repeated OPTIMIZE fails CPU/GPU Delta-log parity on differing `LIQUID_METADATA_ID` values.
12. `delta_lake_optimize_table_test.py::test_delta_optimize_clustered_row_tracking_table_gpu_write`
    - Row-tracking GPU-write coverage fails CPU/GPU Delta-log parity on differing `LIQUID_METADATA_ID` values.
13. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[False-dv_default-no_mapping]`
    - `GpuOverrideUtil` aborts on a non-columnar `OverwriteByExpressionExecV1` while applying GPU overrides.
14. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[False-dv_false-no_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
15. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[False-dv_true-no_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
16. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[False-dv_default-name_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
17. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[False-dv_default-id_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
18. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[True-dv_default-no_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
19. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[True-dv_false-no_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
20. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[True-dv_true-no_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
21. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[True-dv_default-name_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
22. `delta_lake_write_test.py::test_delta_db173_native_managed_ctas_rtas[True-dv_default-id_mapping]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
23. `delta_lake_write_test.py::test_delta_db173_native_sql_ctas_rtas[optimize_off-aqe_off]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
24. `delta_lake_write_test.py::test_delta_db173_native_sql_ctas_rtas[optimize_off-aqe_on]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
25. `delta_lake_write_test.py::test_delta_db173_native_sql_ctas_rtas[optimize_on-aqe_on]`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
26. `delta_lake_write_test.py::test_delta_db173_native_sql_ctas_rtas_legacy_optimized_write`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
27. `delta_lake_write_test.py::test_delta_db173_native_sql_ctas_rtas_empty_input`
    - Fails with the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
28. `delta_lake_write_test.py::test_delta_db173_native_sql_ctas_rtas_existing_table_branches`
    - The existing-table branch first reports Hive incompatible-column types, then the same non-columnar `OverwriteByExpressionExecV1` GPU-override exception.
29. `delta_lake_write_test.py::test_delta_rtas_truncate_capability`
    - The RTAS path reports Hive incompatible-column types and then aborts GPU override on non-columnar `OverwriteByExpressionExecV1`.
30. `delta_lake_write_test.py::test_delta_overwrite_schema_evolution_arrays[False]`
    - CPU/GPU Delta-log `commitInfo.operationParameters` differ: CPU records `canMergeSchema: true`, while GPU omits it.

## Completed Delta Lake Python files

A file is marked complete only after the sequential pytest log advances to a later Python file. Failed, skipped, and expected-failure outcomes still count as executed coverage.

| Python file | Observed outcomes |
| --- | --- |
| `delta_lake_auto_compact_test.py` | 15 passed |
| `delta_lake_catalog_managed_test.py` | 25 skipped |
| `delta_lake_clustered_reads_test.py` | 2 passed |
| `delta_lake_delete_test.py` | 49 passed, 6 skipped, 8 xfailed |
| `delta_lake_liquid_clustering_test.py` | 6 passed, 5 failed, 2 skipped |
| `delta_lake_low_shuffle_merge_test.py` | 62 skipped |
| `delta_lake_merge_test.py` | 124 passed, 1 failed, 19 skipped, 78 xfailed, 4 xpassed |
| `delta_lake_optimize_table_test.py` | 8 passed, 6 failed |
| `delta_lake_reorg_liquid_clustering_test.py` | 2 skipped |
| `delta_lake_reorg_table_test.py` | 9 skipped |
| `delta_lake_test.py` | 213 passed, 51 skipped, 13 xpassed |
| `delta_lake_time_travel_test.py` | 9 passed |
| `delta_lake_update_test.py` | 25 passed, 5 skipped, 20 xfailed, 3 xpassed |
| `delta_lake_write_test.py` | 105 passed, 18 failed, 7 skipped, 25 xfailed, 1 xpassed |
| `delta_zorder_test.py` | 11 passed, 10 xpassed |

Completed Delta-file count: **15**.

## Completed multithreaded-shuffle Python files

| Python file | Observed outcomes |
| --- | --- |
| `hash_aggregate_test.py` | 85 passed |

## Completed PyArrow Python files

| Python file | Observed outcomes |
| --- | --- |
| `parquet_pyarrow_test.py` | 143 passed, 1 xpassed |

## Completed clean-main Python files

| Python file | Observed outcomes |
| --- | --- |
| `allow_non_gpu_conditional_marker_test.py` | 27 passed |
| `allow_non_gpu_conditional_scope_test.py` | 7 passed |
| `aqe_test.py` | 18 passed, 5 skipped |
| `arithmetic_ops_test.py` | 1,429 passed, 4 failed, 37 skipped, 24 xfailed, 1 xpassed |
| `array_test.py` | 660 passed, 73 skipped, 2 xfailed |
| `assert_in_tests_test.py` | 2 passed |
| `asserts_regression_test.py` | 2 passed |
| `ast_test.py` | 182 passed, 2 failed, 1 xpassed |
| `avro_test.py` | 44 skipped |
| `cache_test.py` | 595 passed, 20 skipped |
| `cast_test.py` | 505 passed, 14 skipped, 18 xpassed |
| `cmp_test.py` | 443 passed |
| `collection_ops_test.py` | 344 passed, 5 failed |
| `col_size_exceeding_cudf_limit_test.py` | 72 passed |
| `conditionals_test.py` | 250 passed |
| `cpu_bridge_test.py` | 46 passed, 2 skipped |
| `csv_test.py` | 918 passed, 160 skipped, 40 xfailed, 8 xpassed |
| `data_gen_test.py` | 3 passed |
| `datasourcev2_read_test.py` | 5 passed, 1 failed, 1 skipped |
| `datasourcev2_write_test.py` | 10 passed |
| `date_time_test.py` | 3,006 passed, 3 skipped, 4 xfailed, 4 xpassed |
| `decimal_precision_over_max_test.py` | 1 passed |
| `dpp_test.py` | 105 passed |
| `expand_exec_test.py` | 16 passed |
| `explain_mode_test.py` | 2 passed |
| `explain_test.py` | 6 passed |
| `fastparquet_compatibility_test.py` | 39 passed, 16 xfailed, 8 xpassed |
| `generate_expr_test.py` | 404 passed |
| `get_json_test.py` | 54 passed |
| `grouping_sets_test.py` | 8 passed |
| `group_partitions_test.py` | 4 passed, 2 skipped |
| `hash_aggregate_test.py` | 1,766 passed, 90 skipped, 293 xfailed, 8 xpassed; 85 shuffle-marked tests deselected |
| `hashing_test.py` | 74 passed |
| `higher_order_functions_test.py` | 68 passed |
| `hive_delimited_text_test.py` | 143 passed, 6 xfailed |
| `hive_parquet_write_test.py` | 14 passed, 2 skipped |
| `hive_write_test.py` | 28 passed, 3 skipped |
| `hyper_log_log_plus_plus_test.py` | 123 skipped |
| `inset_test.py` | 8 passed |
| `join_test.py` | 2,301 passed, 29 skipped |
| `json_fuzz_test.py` | 1 skipped |
| `json_matrix_test.py` | 920 passed, 72 xfailed, 43 xpassed |
| `json_test.py` | 2,714 passed, 14 skipped, 87 xfailed, 153 xpassed |
| `json_tuple_test.py` | 13 passed |
| `kudo_dump_test.py` | 1 passed |
| `limit_test.py` | 253 passed |
| `logic_test.py` | 29 passed |
| `map_test.py` | 710 passed, 5 skipped |
| `misc_expr_test.py` | 8 passed |
| `misc_test.py` | 1 passed |
| `mortgage_test.py` | 1 passed |
| `noop_write_test.py` | 4 skipped |
| `orc_cast_test.py` | 185 passed, 1 skipped |
| `orc_test.py` | 2,316 passed, 1 failed, 107 skipped, 62 xfailed, 62 xpassed |
| `orc_write_test.py` | 126 passed, 1 failed, 12 xfailed, 11 xpassed |
| `parquet_testing_test.py` | 4 skipped |
| `parquet_test.py` | 3,745 passed, 66 skipped |
| `parquet_write_test.py` | 385 passed, 3 skipped |
| `private_optimizer_agg_pushdown_test.py` | 1 passed |
| `private_optimizer_decompose_stddev_test.py` | 1 passed |
| `private_optimizer_skewed_bhj_join_test.py` | 1 failed, 1 skipped |
| `private_optimizer_subquery_shared_scan_test.py` | 1 passed |
| `project_lit_alias_test.py` | 2 passed |
| `project_presplit_test.py` | 3 passed |
| `protobuf_data_gen_test.py` | 5 passed |
| `protobuf_test.py` | 2 passed |
| `prune_partition_column_test.py` | 128 passed, 4 skipped |
| `py4j_workaround_test.py` | 5 passed |
| `qa_nightly_select_test.py` | 722 passed |
| `rand_test.py` | 8 passed |
| `range_test.py` | 8 passed |
| `reduced_it_selection_test.py` | 5 passed |
| `reduced_test_matrix_helper_test.py` | 2 passed |
| `regexp_no_unicode_test.py` | 3 skipped |
| `regexp_test.py` | 94 passed, 1 failed, 1 skipped |
| `repart_test.py` | 455 passed |
| `row-based_udf_test.py` | 1 passed, 1 xpassed |
| `row_conversion_test.py` | 32 passed |
| `sample_test.py` | 105 passed |
| `scan_default_values_test.py` | 4 passed |
| `schema_evolution_test.py` | 2 passed |
| `sort_test.py` | 959 passed, 64 xfailed |
| `string_test.py` | 154 passed, 3 skipped, 1 xpassed |
| `string_type_test.py` | 18 passed |
| `struct_test.py` | 63 passed |
| `subquery_test.py` | 46 passed |
| `time_window_test.py` | 73 passed |
| `udf_cudf_test.py` | 11 skipped |
| `udf_test.py` | Corrected-source October 2 replacement: 97 passed, 36 failed, 4 skipped, 9 xfailed; all 146 selected cases have outcomes; supersedes the incomplete September 39-failure entry |
| `url_test.py` | 41 passed |
| `variant_test.py` | 1 passed, 35 failed, 11 skipped |
| `window_function_test.py` | 1,041 passed, 7 skipped, 4 xfailed, 2 xpassed |

## Completed alternate-cache Python files

| Python file | Observed outcomes |
| --- | --- |
| `cache_test.py` | 615 passed |

## Current execution state

- The main-file and alternate-cache runner completed its plan and wrote exit record `1` at 03:04:40 UTC because earlier test invocations failed. The separate alternate-cache invocation itself exited 0 after all 615 selected tests passed.
- The separate full targeted `udf_test.py` rerun started at 03:11:57 UTC under PID `1304177` and was last confirmed alive at 03:12:16 UTC. It had established seed `1790565130` but had not yet emitted a visible collection summary.
- The preceding two bullets describe September's completed runner and lost targeted attempt; those paths are historical and unavailable. The October 2 cluster is reachable; the header-fix build, scalar/aggregate gates, and two command-header unit tests passed. Validation runner `79635` is terminal with a complete 146-node replacement. No integration coverage remains pending, although 117 cumulative failed cases and the documented pre-existing global style findings remain unresolved.

### Terminal corrected-source UDF window failures

The complete October 2 JUnit records the following 36 failed nodes, prefixed by `udf_test.py::`. Each fails on the GPU execution path while `ArrowStreamGroupSerializer.load_stream` reads an invalid dataframe-count marker (`INVALID_NUMBER_OF_DATAFRAMES_IN_GROUP`). The observed counts are 2,013,265,920; 268,435,456; or 134,283,264, as listed per node. This is evidence of remaining window input-framing incompatibility; its exact correction is not established by the header fix. It is not a new timeout/shutdown cascade, and no test was omitted to obtain coverage.

- `test_window_aggregate_udf[No_Partition-Byte][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf[No_Partition-Short][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf[No_Partition-Integer][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf[No_Partition-Long][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf[Unbounded-Byte][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf[Unbounded-Short][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf[Unbounded-Integer][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf[Unbounded-Long][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf[Unbounded_Following-Byte][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf[Unbounded_Following-Short][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf[Unbounded_Following-Integer][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf[Unbounded_Following-Long][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf[Lower_Upper-Byte][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf[Lower_Upper-Short][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf[Lower_Upper-Integer][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf[Lower_Upper-Long][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf_array_from_python[No_Partition-Byte][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf_array_from_python[No_Partition-Short][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf_array_from_python[No_Partition-Integer][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf_array_from_python[Unbounded-Byte][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf_array_from_python[Unbounded-Short][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf_array_from_python[Unbounded-Integer][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `2013265920`.
- `test_window_aggregate_udf_array_from_python[Unbounded_Following-Byte][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf_array_from_python[Unbounded_Following-Short][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf_array_from_python[Unbounded_Following-Integer][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf_array_from_python[Lower_Upper-Byte][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf_array_from_python[Lower_Upper-Short][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf_array_from_python[Lower_Upper-Integer][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf_array_input[No_Partition-Byte][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf_array_input[No_Partition-Integer][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf_array_input[Unbounded-Byte][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf_array_input[Unbounded-Integer][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `268435456`.
- `test_window_aggregate_udf_array_input[Unbounded_Following-Byte][DATAGEN_SEED=1790964877, TZ=UTC, IGNORE_ORDER]` — invalid group count `134283264`.
- `test_window_aggregate_udf_array_input[Unbounded_Following-Integer][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `134283264`.
- `test_window_aggregate_udf_array_input[Lower_Upper-Byte][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `134283264`.
- `test_window_aggregate_udf_array_input[Lower_Upper-Integer][DATAGEN_SEED=1790964877, TZ=UTC, INJECT_OOM, IGNORE_ORDER]` — invalid group count `134283264`.

Evidence: `full-udf-summary.json`, `full-udf-attempt-001/reports.jsonl`, and JUnit `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20261002221420-uwds/TEST-pytest-1790979260702170131.xml`. The complete summary SHA-256 is `8e81c3b3d1447fea6a0cb14e52f5bbe319f005522f413d8313f8d9bd01d7bdc9`; completion-record SHA-256 is `f736fbfef8a658ec2c130aec362700cfb9658109aa4cbf281cf0c2daf6586edb`.

### Historical UDF watchdog failures (superseded; excluded from current totals)

Historical September evidence: the first UDF case stalled four GPU Arrow Python workers until the RAPIDS 3,600-second job watchdog cancelled job 1 and shut down the driver JVM. JUnit recorded 39 `Py4JJavaError` failures and left 107 selected tests without outcomes. Those 39 nodes are preserved below as diagnostic history, but have been replaced in current totals by the complete October 2 UDF run. This was one watchdog-triggered shutdown cascade, not 39 independent correctness mismatches or additional current failures.

Affected JUnit node IDs:

- `udf_test.py::test_pandas_math_udf[Byte][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM]`
- `udf_test.py::test_pandas_math_udf[Short][DATAGEN_SEED=1790556557, TZ=UTC]`
- `udf_test.py::test_pandas_math_udf[Integer][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM]`
- `udf_test.py::test_pandas_math_udf[Long][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM]`
- `udf_test.py::test_iterator_math_udf[Byte][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM]`
- `udf_test.py::test_iterator_math_udf[Short][DATAGEN_SEED=1790556557, TZ=UTC]`
- `udf_test.py::test_iterator_math_udf[Integer][DATAGEN_SEED=1790556557, TZ=UTC]`
- `udf_test.py::test_iterator_math_udf[Long][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(Byte)][DATAGEN_SEED=1790556557, TZ=UTC]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(Short)][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(Integer)][DATAGEN_SEED=1790556557, TZ=UTC]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(Long)][DATAGEN_SEED=1790556557, TZ=UTC]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(Float)][DATAGEN_SEED=1790556557, TZ=UTC]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(Double)][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(String)][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(Boolean)][DATAGEN_SEED=1790556557, TZ=UTC]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(Date)][DATAGEN_SEED=1790556557, TZ=UTC]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(Timestamp)][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(Array(Short))][DATAGEN_SEED=1790556557, TZ=UTC]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(Array(String))][DATAGEN_SEED=1790556557, TZ=UTC]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Array(Struct(['child0', Byte],['child1', String],['child2', Float]))][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Struct(['child0', Byte],['child1', Short],['child2', Integer],['child3', Long],['child4', Float],['child5', Double],['child6', String],['child7', Boolean],['child8', Date],['child9', Timestamp])][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM]`
- `udf_test.py::test_pandas_scalar_udf_nested_type[Struct(['child0', Array(Short)],['child1', Struct(['child0', Byte],['child1', Short],['child2', Integer],['child3', Long],['child4', Float],['child5', Double],['child6', String],['child7', Boolean],['child8', Date],['child9', Timestamp])])][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM]`
- `udf_test.py::test_single_aggregate_udf[Byte][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf[Short][DATAGEN_SEED=1790556557, TZ=UTC, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf[Integer][DATAGEN_SEED=1790556557, TZ=UTC, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf[Long][DATAGEN_SEED=1790556557, TZ=UTC, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf_more_types[Byte][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf_more_types[Short][DATAGEN_SEED=1790556557, TZ=UTC, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf_more_types[Integer][DATAGEN_SEED=1790556557, TZ=UTC, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf_more_types[Long][DATAGEN_SEED=1790556557, TZ=UTC, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf_more_types[Float][DATAGEN_SEED=1790556557, TZ=UTC, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf_more_types[Double][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf_more_types[String][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf_more_types[Boolean][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf_more_types[Date][DATAGEN_SEED=1790556557, TZ=UTC, APPROXIMATE_FLOAT]`
- `udf_test.py::test_single_aggregate_udf_more_types[Timestamp][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
- `udf_test.py::test_group_aggregate_udf[Byte][DATAGEN_SEED=1790556557, TZ=UTC, IGNORE_ORDER]`
- `udf_test.py::test_group_aggregate_udf[Short][DATAGEN_SEED=1790556557, TZ=UTC, INJECT_OOM, IGNORE_ORDER]`

### Terminal variant failures

All 35 variant failures occur during GPU-plan override because DBR 18.3 leaves a plan node non-columnar where the test expects the variant path to be executable or to fall back safely.

The following 23 nodes fail on non-columnar `ProjectExec`:

- `variant_test.py::test_parquet_variant_try_get_string[v1][DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_string[v2][DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[x-tinyint][DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[x-smallint][DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[x-int][DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[x-bigint][DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[s-tinyint][DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[s-smallint][DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[s-int][DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[s-bigint][DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[i-int][DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[i-bigint][DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[l-bigint][DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[m-tinyint][DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[m-smallint][DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[m-int][DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_targets[m-bigint][DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_nested_object_path[DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_null_variant_rows[DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_null_and_missing_fields[DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_integral_boundaries[DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_aggregate[DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT, ALLOW_NON_GPU(HashAggregateExec,ShuffleExchangeExec)]`
- `variant_test.py::test_parquet_variant_try_get_heterogeneous_values[DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, IGNORE_ORDER({'local': True}), INCOMPAT]`

The following seven nodes fail on non-columnar `ShuffleExchangeExec`:

- `variant_test.py::test_parquet_nested_struct_variant_try_get[DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, INCOMPAT]`
- `variant_test.py::test_parquet_variant_pass_through_filter_project[DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT, ALLOW_NON_GPU(Or,IsNull,GreaterThanOrEqual)]`
- `variant_test.py::test_parquet_variant_try_get_direct_filter[DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT, ALLOW_NON_GPU(And,IsNotNull,GreaterThan)]`
- `variant_test.py::test_parquet_variant_try_get_before_shuffle[DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_if[DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_array_paths[v1][DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT]`
- `variant_test.py::test_parquet_variant_try_get_array_paths[v2][DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, INCOMPAT]`

The following five fallback nodes fail on non-columnar `ColumnarToRowExec`:

- `variant_test.py::test_variant_try_get_non_literal_path_falls_back[DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, INCOMPAT, ALLOW_NON_GPU(ProjectExec,VariantGet)]`
- `variant_test.py::test_variant_try_get_quoted_path_falls_back[DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT, ALLOW_NON_GPU(ProjectExec,VariantGet)]`
- `variant_test.py::test_variant_get_strict_mode_falls_back[DATAGEN_SEED=1790560373, TZ=UTC, INJECT_OOM, INCOMPAT, ALLOW_NON_GPU(ProjectExec,VariantGet)]`
- `variant_test.py::test_variant_try_get_unsupported_target_type_falls_back[DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT, ALLOW_NON_GPU(ProjectExec,VariantGet)]`
- `variant_test.py::test_variant_try_get_cpu_bridge_disabled_falls_back[DATAGEN_SEED=1790560373, TZ=UTC, INCOMPAT, ALLOW_NON_GPU(ProjectExec,VariantGet)]`

### Terminal regexp failure

1. `regexp_test.py::test_regexp_replace_unicode_support[DATAGEN_SEED=1790550661, TZ=UTC, INJECT_OOM]`
   - CPU and GPU disagreed for `regexp_replace(a, TEST\\b, PROD, 1)` on a string containing a Hangul character: CPU produced `PROD휠`, while GPU retained `TEST휠`. This is a Unicode word-boundary behavior mismatch.

### Terminal private-optimizer failure

1. `private_optimizer_skewed_bhj_join_test.py::test_optimize_skewed_bhj_join_skips_on_databricks_executor_broadcast[DATAGEN_SEED=1790548928, TZ=UTC]`
   - The DBR executor-broadcast coverage expected the streamed-side skew rewrite to be skipped, but the rule-enabled GPU physical plan still contained `GpuCustomShuffleReader coalesced and skewed`. The required executor-broadcast and streamed-shuffle markers were present, so the assertion failed specifically because the forbidden skew marker remained.

### Terminal ORC failures

1. `orc_test.py::test_orc_not_support_timestamp_ltz[DATAGEN_SEED=1790536804, TZ=UTC, INJECT_OOM]`
   - The assertion expected the pre-Spark-4.2 text `ParseException`, but the GPU read raised a `QueryExecutionException` whose message says GPU ORC cannot convert a file `timestamp with local time zone` to reader `timestamp`; the expected substring was absent.
2. `orc_write_test.py::test_orc_do_not_lowercase_columns[DATAGEN_SEED=1790540857, TZ=UTC, INJECT_OOM, IGNORE_ORDER]`
   - The test expected the older Spark 4.x/DBR wording `Key \`acol\` is not exists.`, while DBR 18.3 returned the corrected structured-error text `[KEY_NOT_EXISTS] Key \`acol\` does not exist.`.

### Terminal arithmetic failures

The complete rerun reproduced the same four test parameterizations seen in the interrupted session, under a new random seed. All four are CPU/GPU result mismatches for large-magnitude inverse hyperbolic inputs where the GPU result becomes positive or negative infinity.

1. `arithmetic_ops_test.py::test_acosh[Double][DATAGEN_SEED=1790189207, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
   - GPU returned `inf` where CPU returned `496.1282635955343`.
2. `arithmetic_ops_test.py::test_asinh[Double][DATAGEN_SEED=1790189207, TZ=UTC, APPROXIMATE_FLOAT]`
   - GPU returned `inf` where CPU returned `496.1282635955343`.
3. `arithmetic_ops_test.py::test_asinh[Float][DATAGEN_SEED=1790189207, TZ=UTC, APPROXIMATE_FLOAT]`
   - GPU returned `-inf` where CPU returned `-43.836294594133975`.
4. `arithmetic_ops_test.py::test_asinh[Decimal(12,2)][DATAGEN_SEED=1790189207, TZ=UTC, INJECT_OOM, APPROXIMATE_FLOAT]`
   - GPU returned `-inf` where CPU returned `-23.631791156016195`.

### Terminal AST failures

Both AST failures are the same inverse-hyperbolic overflow class as the arithmetic failures: GPU produced infinity for a large finite CPU result.

1. `ast_test.py::test_asinh[(Double, False)][DATAGEN_SEED=1790192277, TZ=UTC, APPROXIMATE_FLOAT]`
   - GPU returned `inf` where CPU returned `576.5135014024883`.
2. `ast_test.py::test_acosh[(Double, True)][DATAGEN_SEED=1790192277, TZ=UTC, APPROXIMATE_FLOAT]`
   - GPU returned `inf` where CPU returned `576.5135014024883`.

### Terminal collection-operations failures

All five cases received the expected illegal-sequence exception, but the assertion searched for the legacy text `Illegal sequence boundaries`. DBR 18.3 instead emits the structured error prefix `[ILLEGAL_SEQUENCE_BOUNDARIES]` followed by a detailed step/start/stop explanation.

1. `collection_ops_test.py::test_sequence_illegal_boundaries[Short-Short-Short][DATAGEN_SEED=1790194588, TZ=UTC]`
   - Expected legacy error text was absent from the structured illegal-boundaries exception.
2. `collection_ops_test.py::test_sequence_illegal_boundaries[Long-Long-Long][DATAGEN_SEED=1790194588, TZ=UTC, INJECT_OOM]`
   - Expected legacy error text was absent from the structured illegal-boundaries exception.
3. `collection_ops_test.py::test_sequence_illegal_boundaries[Byte-Byte-Byte][DATAGEN_SEED=1790194588, TZ=UTC, INJECT_OOM]`
   - Expected legacy error text was absent from the structured illegal-boundaries exception.
4. `collection_ops_test.py::test_sequence_illegal_boundaries[Integer-Integer-Integer0][DATAGEN_SEED=1790194588, TZ=UTC, INJECT_OOM]`
   - Expected legacy error text was absent from the structured illegal-boundaries exception.
5. `collection_ops_test.py::test_sequence_illegal_boundaries[Integer-Integer-Integer1][DATAGEN_SEED=1790194588, TZ=UTC, INJECT_OOM]`
   - Expected legacy error text was absent from the structured illegal-boundaries exception.

### Terminal datasource-v2 read failure

1. `datasourcev2_read_test.py::test_arrow_source_pandas_udf[DATAGEN_SEED=1790197601, TZ=UTC, ALLOW_NON_GPU(BatchScanExec)]`
   - Spark cancelled job 20 after the RAPIDS integration-test job reached its 3,600-second timeout. The two GPU Arrow Python tasks had stopped making metric progress and repeatedly logged 600-second worker-idle timeouts; the underlying cause remains undetermined.

Completed phase/file invocation count across valid and clean phases: **110** (15 Delta Lake, 1 multithreaded shuffle, 1 PyArrow, 92 clean-main invocations, and 1 alternate-cache invocation). This represents **108 distinct Python filenames** because `hash_aggregate_test.py` completed two disjoint selections (85 shuffle-marked tests and 2,157 clean-main tests) and `cache_test.py` completed both clean-main and alternate-cache-serializer selections.

Terminal pytest-failure count across all valid completed evidence: **117 of 32,803 outcomes** (30 Delta, 4 arithmetic, 2 AST, 5 collection-operations, 1 datasource-v2 read, 2 ORC, 1 private-optimizer, 1 regexp, 36 corrected-source UDF window-framing, and 35 variant failures; 0.36%).

## Harness and infrastructure observations

- During the September 27 recovery, the fresh cluster could not resolve the internal GitLab host, so the exact private revision was restored from a verified Git bundle. This was recovery infrastructure work, not a pytest failure.
- The September 27 public bootstrap stopped before Maven because its requested Maven 3.8.1 symlink conflicted with the private build's Maven 3.6.3 symlink. Repointing `/usr/local/bin/mvn` to 3.8.1 resolved the setup issue. This attempt did not execute integration tests.
- The next September 27 public attempt passed all 20 DBR 18.3 modules, then failed only during an unnecessary default `spark350` packaging pass because the private `spark350` artifact was absent. That recovery's corrected DB18-only rerun used `WITH_DEFAULT_UPSTREAM_SHIM=0` and `SKIP_DEP_INSTALL=1`; all 20 modules and packaging passed. Neither packaging attempt is counted as a pytest failure. The separate October 2 recovery required `SKIP_DEP_INSTALL=0`, as documented above.
- At 04:44-04:45 UTC on the previous cluster, two bounded SSH attempts failed before login with `Network is unreachable`. The cluster then shut down and its local directories were lost. This is historical infrastructure-access failure, not a pytest failure; no counts or completion claims were advanced from the incomplete prior `join_test.py` attempt.
- At 03:44-03:45 UTC during the September targeted UDF rerun, three bounded SSH attempts to that former cluster failed before login; the final attempt used an explicit 10-second connection timeout and reported that port 2200 timed out. This is historical infrastructure-access failure, not a pytest failure. The lost targeted attempt contributes no partial outcome; the October cluster is reachable and its complete replacement is counted separately.
- The first DB18 cluster became unreachable after its 07:58:30 UTC capture and its local directories were lost. The fresh cluster recovery rebuilt both exact DB18 branches before testing resumed.
- The recovered `arithmetic_ops_test.py` run reached a terminal summary and JUnit result for all 1,495 selected tests, so the earlier partial run is no longer in the resume scope and is not double-counted.
- `array_test.py` logged an expected Spark task failure for unequal `MapData` key/value lengths during negative-path coverage, but pytest completed all 735 selected tests without a failure; it is not counted as a test failure.
- All 44 selected `avro_test.py` cases were skipped by runtime markers; the file completed successfully and remains counted as completed coverage.
- `datasourcev2_read_test.py` reached terminal JUnit evidence for all seven selected tests after the 3,600-second Spark job watchdog cancelled the stalled `test_arrow_source_pandas_udf` job. The file has 5 passed, 1 failed, and 1 skipped outcome and no longer remains in the resume scope. The runner recovered automatically and continued to later files.
- `date_time_test.py` logged expected exception-path Spark task failures, including arithmetic overflow cases, but its terminal pytest summary and JUnit contain no failed tests: all 3,017 selected tests reached passed, skipped, xfailed, or xpassed outcomes.
- `fastparquet_compatibility_test.py` logged expected exception-path Spark task failures, but its terminal pytest summary and JUnit contain no failed tests: all 63 selected tests reached passed, xfailed, or xpassed outcomes.
- The clean-main `hash_aggregate_test.py` invocation logged expected negative-path Spark task failures, but its terminal pytest summary and JUnit contain no failed tests: all 2,157 selected tests reached passed, skipped, xfailed, or xpassed outcomes.
- The fresh `join_test.py` rerun supersedes the prior incomplete attempt and reached terminal evidence for all 2,330 selected tests: 2,301 passed and 29 skipped, with no failure.
- Forty-seven of the fifty-three fresh completed-file processes through `window_function_test.py` logged a closed-stream logging error after their terminal pytest summary and JUnit write; `orc_test.py`, `schema_evolution_test.py`, `udf_test.py`, `url_test.py`, `variant_test.py`, and `window_function_test.py` are the exceptions. All 18,617 fresh terminal outcomes were already recorded, so this is harness shutdown noise rather than lost test evidence.
- The alternate-cache `cache_test.py` process logged the same closed-stream logging error after its terminal summary and JUnit write. All 615 selected tests had already passed and were recorded, so this is harness shutdown noise rather than lost test evidence.
- The outer continuation runner exited 1 after completing all planned main and alternate-cache invocations because its aggregate status retained earlier pytest failures. The exit does not represent an additional test or infrastructure failure.
- Historical September `udf_test.py` collected 146 tests, but the watchdog shutdown produced only 39 failure outcomes and left 107 cases without outcomes. That incomplete entry was counted in earlier checkpoints, not as 39 independent defects. It is now superseded and excluded from current totals because October 2 corrected-source coverage reconciles all 146 selected cases. Neither the earlier October full-file/isolated diagnostics nor smoke gates are counted again.
- All four `parquet_testing_test.py` cases were skipped because none of the expected parquet-testing fixture directories contained the upstream data files. The file is terminal and counted as skipped coverage; this is missing test-data coverage rather than a pytest failure.
- The initial main and cache attempts failed collection because the two new DB18 UDF decorators referenced `is_databricks_version` without importing it. Commit `ad3341d06` adds the missing import. The Delta phase subsequently collected all 32,659 discoverable items and selected 947 without that error, confirming the collection fix.
- The PyArrow log contains three startup-time `ERROR` messages: a Spark Connect gRPC Unix-socket permission denial, a transient `BlockManagerMasterEndpoint` null-pointer error while re-registering a null block manager, and a SafeSpark UC securable-server permission failure. Spark continued and all 144 selected PyArrow tests reached terminal outcomes, so none is counted as a pytest failure.
- Some merge tests emit background auto-compaction `DBR_FILE_NOT_EXIST` errors after temporary files are removed. Tests surrounding the observed messages have continued to pass; these messages are not counted as failures unless the final pytest result says otherwise.
- After the Delta terminal summary and JUnit write, shutdown logging reported a closed stream and inability to create a logging file. This occurred after all 947 results were recorded and is tracked as harness noise, not a lost test result.
- PyArrow shutdown logged the same closed-stream error after its terminal summary and JUnit write. All 144 results had already been recorded, so this is also harness noise rather than a lost result.

## Resume plan

All planned integration coverage is terminal. If the cluster stops after this checkpoint, preserve this report; do not launch ordinary continuation or recreate completed phases merely to regain logs.

1. Do not rerun the fifteen completed Delta Python files for coverage continuation; the Delta phase is complete. Rerun its failed tests separately only for diagnosis or verification.
2. Do not rerun the completed multithreaded-shuffle invocation of `hash_aggregate_test.py`; all 85 shuffle-selected tests passed. This is distinct from the completed 2,157-test clean-main invocation included in step 4.
3. Do not rerun `parquet_pyarrow_test.py`; the PyArrow phase is complete and all 144 selected tests reached terminal outcomes without a failure.
4. Do not rerun the ninety-two terminal clean-main files for ordinary coverage continuation. The completed October 2 UDF replacement resolves the former 107-test gap; all 146 selected UDF cases have meaningful terminal outcomes.
5. Do not rerun the separate alternate-serializer `cache_test.py` invocation; all 615 selected tests passed and reached a terminal ledger row. This invocation is distinct from the completed clean-main `cache_test.py` result.
6. Retain the new terminal summaries/JUnit references and the immutable manifest captured before full-run completion. Do not restart runner `79635`, old isolated runner `39303`, or lost September runner `1304177`. Any future rerun should target explicit failure diagnosis/fix validation, not missing coverage. Source changes require new artifact identities; do not merge mixed-artifact partial results or double-count smoke tests.

## Completion and level-of-effort assessment

All requested phases have terminal selected-test evidence, including the final UDF replacement. This means coverage complete, not release-ready: 117 cumulative cases failed, 1,046 were skipped, and 826 were expected failures. Historical non-UDF failures still need verification/triage under subsequent fixes; this narrowly scoped header fix did not rerun them or claim to repair them.

The command-header defect was a localized four-runner shim change, validated by a 20-module build, two native-byte regression tests, CPU/GPU scalar and aggregate smoke comparisons, and full seeded UDF execution. Remaining UDF work is a separate window-stream framing family (36 failures across three test functions), not 36 independent fixes; the exact stream correction and its interaction with bounded/unbounded frames need investigation. Treat that as a medium-effort compatibility task requiring native writer/serializer comparison and targeted window checks before another full UDF validation. Variant failures form another substantial runtime-compatibility group; Delta, arithmetic/AST, collection operations, private optimizer, ORC, regex and datasource/UDF diagnostics should be triaged by their evidence-backed families. A reliable calendar estimate is not justified without that diagnosis. The three pre-existing global style findings remain an independent CI/review prerequisite; they were not silently disabled or absorbed into this source change.

## Evidence locations

### Current DB18 header-fix evidence

- Directory: `/home/ubuntu/db18-udf-header-fix-20261002`
- Build: `build.pid`, `build.exit`, `build.log`, `source.diff`, `source-sha256.txt`, `artifact-sha256.txt`; full original patch `/home/ubuntu/db18-udf-header-fix.patch` includes added sources omitted by tracked-only `source.diff`
- Validation identity: `runtime-manifest.json` (SHA-256 `f91b87c149574e59a23e0b1afd7f732db46be5f6d9729557385cd080ffeaa6e7`); original frozen plan `/home/ubuntu/db18-udf-isolated-20261002/plan.json` (SHA-256 `7a46b654c66054ac6cedd92a9dba39277976e39736eeb3ae605a4750a0621c31`)
- Validator/selector/launcher hashes: `3f6f774a6a7b347c1b1e41ce712b679d30fb1437a8a647d175ff625656818309`, `b0f8ec1702dae0d74e76a0e4a53c59ec2a3a58a629c9d9c97028c38a957a6146`, `003ae94e759504509710762b77c0aed084ea51e213e49cb80c84d739af068b00`
- Validation: `runner.pid`, `runner.exit` when terminal, `runner.launch.log`, `current.json`, `completed.tsv`, `scalar-gate-summary.json`, `aggregate-gate-summary.json`, `full-udf-summary.json`, `complete.json`; per-phase `*-attempt-NNN/test.log`, `reports.jsonl`, and `process-exit.json`; JUnit paths are recorded in summaries/ledger
- Regression/build-harness diagnostics: `unit.log`, `unit.exit` when terminal, `unit-reactor-test-phase.log`, `unit-dependencies-java21.log`, `unit-verify-preexisting-style.log`, `unit-upstream-missing-suite.log`, and their terminal exit records
- Final regression evidence: `GpuPythonRunnerCommandSuite.scala` (SHA-256 `849a19ef4ec33f2962a87d922b99f74f1b4998e6dfeb19e3ee79d016269cb1f8`), `unit-native.log`, `unit-native-result.json` (SHA-256 `f3f00076f34c37950f623f7b55fef8ec9cfa3ba0a365c1630a52e461c02d726e`), generated `TEST-org.apache.spark.sql.rapids.execution.python.shims.GpuPythonRunnerCommandSuite.xml`, successful `unit-compile.log`/`unit-compile.exit`; earlier protected-access and missing-JNI diagnostics are retained separately

### Historical evidence from replaced clusters

These paths are preserved from the prior reviewed checkpoint. The cluster-local files are no longer accessible after the cluster replacement; the outcome details above retain the evidence captured before that loss.

- Phase ledger: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z/phase-status.tsv`
- Terminal Delta log: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z/delta.log`
- Delta JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923034441-XRiS/TEST-pytest-1790135081138085498.xml`
- Terminal multithreaded-shuffle log: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z/multithreaded_shuffle.log`
- Multithreaded-shuffle JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923062100-32Su/TEST-pytest-1790144460955804202.xml`
- Terminal PyArrow log: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z/pyarrow.log` (SHA-256 `3f3f360997505b262eac79138a78c840d87d0f1b3d7fb5221570a3ccd1965104`)
- PyArrow JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923062608-CYhd/TEST-pytest-1790144768734234991.xml` (SHA-256 `4fd412620d74f494c7958a9e648e4d3b8efb44fa77b19e08d2f1527382b641f0`)
- Invalid main log: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z/main.log`
- Invalid cache log: `/home/ubuntu/spark-rapids/db18-integration-results/20260923T034132Z/cache.log`
- Main collection JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923034220-M060/TEST-pytest-1790134940877003166.xml`
- Cache collection JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923034333-Sqz9/TEST-pytest-1790135013130855509.xml`
- First clean main/cache file ledger: `/home/ubuntu/spark-rapids/db18-clean-main-cache-results/20260923T072607Z/file-status.tsv`
- Completed clean-main JUnits:
  - `allow_non_gpu_conditional_marker_test.py`: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923072640-toIc/TEST-pytest-1790148400849689189.xml`
  - `allow_non_gpu_conditional_scope_test.py`: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923072733-5T0I/TEST-pytest-1790148453429816357.xml`
  - `aqe_test.py`: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260923072823-NWiX/TEST-pytest-1790148503506090068.xml`

### Historical second-cluster recovery evidence

These paths were available through the 04:12:39 UTC inspection on the second cluster. That cluster later shut down and its local directories were lost, so the paths are historical and unavailable. The reviewed outcome details above preserve the terminal evidence captured before the loss.

- Recovered clean main/cache file ledger: `/home/ubuntu/cudf-spark/db18-clean-main-cache-results/20260923T184559Z/file-status.tsv`
- Recovered clean main/cache completed-file ledger: `/home/ubuntu/cudf-spark/db18-clean-main-cache-results/20260923T184559Z/completed-files.tsv`
- Recovered clean main/cache current-file record: `/home/ubuntu/cudf-spark/db18-clean-main-cache-results/20260923T184559Z/current-file.tsv`
- Terminal recovered arithmetic log: `/home/ubuntu/cudf-spark/db18-clean-main-cache-results/20260923T184559Z/main__arithmetic_ops_test.log`
- Recovered arithmetic JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923184647-t6Ig/TEST-pytest-1790189207445451310.xml`
- Recovered array JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923191420-TAwl/TEST-pytest-1790190860659267943.xml`
- Recovered assert-in-tests JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923193558-WJ26/TEST-pytest-1790192158027611928.xml`
- Recovered asserts-regression JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923193653-ktGw/TEST-pytest-1790192213632422363.xml`
- Recovered AST JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923193757-UeLU/TEST-pytest-1790192277511836399.xml`
- Recovered Avro JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923194144-h4Js/TEST-pytest-1790192504818999516.xml`
- Recovered clean-main `cache_test.py` JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923194235-Zx6x/TEST-pytest-1790192555646291250.xml`
- Recovered cast JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923195712-1oU0/TEST-pytest-1790193432101879397.xml`
- Recovered comparison JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923200636-NyWl/TEST-pytest-1790193996059915811.xml`
- Recovered collection-operations JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923201628-d6yu/TEST-pytest-1790194588730920290.xml`
- Recovered column-size-limit JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923202735-gW1Y/TEST-pytest-1790195255212472073.xml`
- Recovered conditionals JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923203109-Pdsv/TEST-pytest-1790195469132739240.xml`
- Recovered CPU-bridge JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923204749-49mk/TEST-pytest-1790196469929328686.xml`
- Recovered CSV JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923205039-3n3d/TEST-pytest-1790196639053468621.xml`
- Recovered data-generator JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923210547-XTSe/TEST-pytest-1790197547244007844.xml`
- Recovered datasource-v2 read JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923210641-pldf/TEST-pytest-1790197601972642595.xml`
- Recovered datasource-v2 write JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923220758-cRgC/TEST-pytest-1790201278080333314.xml`
- Recovered date/time JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923221048-Knme/TEST-pytest-1790201448707253541.xml`
- Recovered decimal-precision JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923224556-7xu7/TEST-pytest-1790203556402048683.xml`
- Recovered DPP JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923224716-JPZJ/TEST-pytest-1790203636302282427.xml`
- Recovered expand-exec JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923230300-XOp1/TEST-pytest-1790204580882184478.xml`
- Recovered explain-mode JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923230457-dxw6/TEST-pytest-1790204697574062104.xml`
- Recovered explain JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923230623-pqQ8/TEST-pytest-1790204783695326619.xml`
- Recovered Fastparquet-compatibility JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923230811-PFfL/TEST-pytest-1790204891299208880.xml`
- Recovered generate-expression JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260923231038-lp4l/TEST-pytest-1790205038925595671.xml`
- Recovered get-JSON JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924000616-qqTw/TEST-pytest-1790208376473789666.xml`
- Recovered grouping-sets JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924000823-sTCP/TEST-pytest-1790208503769344577.xml`
- Recovered group-partitions JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924001008-02hv/TEST-pytest-1790208608166938885.xml`
- Recovered clean-main hash-aggregate JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924001104-6M3s/TEST-pytest-1790208664084095880.xml`
- Recovered hashing JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924031325-O8mG/TEST-pytest-1790219605428709700.xml`
- Recovered higher-order-functions JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924031651-f4zb/TEST-pytest-1790219811079076279.xml`
- Recovered Hive-delimited-text JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924031919-tot0/TEST-pytest-1790219959461927943.xml`
- Recovered Hive-Parquet-write JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924032443-xCIR/TEST-pytest-1790220283769915196.xml`
- Recovered Hive-write JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924032932-9eUR/TEST-pytest-1790220572140964904.xml`
- Recovered HLL++ JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924033324-WOj0/TEST-pytest-1790220804217328334.xml`
- Recovered inset JUnit: `/home/ubuntu/cudf-spark/scala2.13/integration_tests/target/run_dir-20260924033417-7ONa/TEST-pytest-1790220857429187838.xml`

### Historical September recovered-cluster evidence (unavailable)

The September cluster was unreachable at its final capture and was later replaced. These paths are historical and unavailable, not current October evidence. The previously reviewed terminal details for setup, all fifty-three resumed clean-main file processes, and alternate-cache remain preserved. The lost September targeted-UDF attempt had no terminal evidence and is excluded; October's complete replacement uses the new paths above.

- Fresh evidence directory: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z`
- File-status ledger: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/file-status.tsv`
- Completed-file ledger: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/completed-files.tsv`
- Current-file record: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/current-file.tsv`
- Full 92-file clean-main plan: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/all-clean-main-files.txt`
- Preserved 39-file historical-completion list: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/historical-completed-clean-main-files.txt`
- Fresh 53-file resume list: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main-files-resumed.txt`
- Terminal setup log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/setup.log`
- Terminal join log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__join_test.log`
- Terminal join JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927163955-XJ8y/TEST-pytest-1790527195856538615.xml`
- Terminal JSON-fuzz log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__json_fuzz_test.log`
- Terminal JSON-fuzz JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927180315-GJrI/TEST-pytest-1790532195123409207.xml`
- Terminal JSON-matrix log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__json_matrix_test.log`
- Terminal JSON-matrix JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927180411-WjqQ/TEST-pytest-1790532251487079441.xml`
- Terminal JSON log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__json_test.log`
- Terminal JSON JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927181235-G0AR/TEST-pytest-1790532755241711674.xml`
- Terminal JSON-tuple log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__json_tuple_test.log`
- Terminal JSON-tuple JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927184411-U4Kp/TEST-pytest-1790534651133741339.xml`
- Terminal Kudo-dump log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__kudo_dump_test.log`
- Terminal Kudo-dump JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927184547-zz3K/TEST-pytest-1790534747945172661.xml`
- Terminal limit log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__limit_test.log`
- Terminal limit JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927184703-yox3/TEST-pytest-1790534823767119967.xml`
- Terminal logic log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__logic_test.log`
- Terminal logic JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927185146-7NgA/TEST-pytest-1790535106862999044.xml`
- Terminal map log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__map_test.log`
- Terminal map JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927185325-DYQB/TEST-pytest-1790535205948650246.xml`
- Terminal misc-expression log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__misc_expr_test.log`
- Terminal misc-expression JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927191104-5apB/TEST-pytest-1790536264356828421.xml`
- Terminal misc log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__misc_test.log`
- Terminal misc JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927191238-I9My/TEST-pytest-1790536358902178916.xml`
- Terminal mortgage log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__mortgage_test.log`
- Terminal mortgage JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927191342-0MS1/TEST-pytest-1790536422142605369.xml`
- Terminal no-op-write log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__noop_write_test.log`
- Terminal no-op-write JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927191522-5TRL/TEST-pytest-1790536522711169414.xml`
- Terminal ORC-cast log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__orc_cast_test.log`
- Terminal ORC-cast JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927191612-Efja/TEST-pytest-1790536572941775171.xml`
- Terminal ORC log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__orc_test.log`
- Terminal ORC JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927192004-BLlD/TEST-pytest-1790536804346640559.xml`
- Terminal ORC-write log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__orc_write_test.log`
- Terminal ORC-write JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927202737-ca5I/TEST-pytest-1790540857882112728.xml`
- Terminal Parquet-testing log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__parquet_testing_test.log`
- Terminal Parquet-testing JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927203605-d5gJ/TEST-pytest-1790541365234205700.xml`
- Terminal Parquet log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__parquet_test.log`
- Terminal Parquet JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927203653-kCvH/TEST-pytest-1790541413596963812.xml`
- Terminal Parquet-write log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__parquet_write_test.log`
- Terminal Parquet-write JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927222538-f2yK/TEST-pytest-1790547938039129672.xml`
- Terminal private-optimizer aggregate-pushdown log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__private_optimizer_agg_pushdown_test.log`
- Terminal private-optimizer aggregate-pushdown JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927223930-zRQm/TEST-pytest-1790548770086478319.xml`
- Terminal private-optimizer standard-deviation-decomposition log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__private_optimizer_decompose_stddev_test.log`
- Terminal private-optimizer standard-deviation-decomposition JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927224056-K9Jt/TEST-pytest-1790548856267703057.xml`
- Terminal private-optimizer skewed-BHJ log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__private_optimizer_skewed_bhj_join_test.log`
- Terminal private-optimizer skewed-BHJ JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927224208-ExZY/TEST-pytest-1790548928949157449.xml`
- Terminal private-optimizer shared-subquery-scan log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__private_optimizer_subquery_shared_scan_test.log`
- Terminal private-optimizer shared-subquery-scan JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927224322-rjfZ/TEST-pytest-1790549002959625321.xml`
- Terminal project-literal-alias log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__project_lit_alias_test.log`
- Terminal project-literal-alias JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927224443-jUbe/TEST-pytest-1790549083674099514.xml`
- Terminal project-presplit log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__project_presplit_test.log`
- Terminal project-presplit JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927224559-QrHJ/TEST-pytest-1790549159205937550.xml`
- Terminal Protobuf-data-generator log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__protobuf_data_gen_test.log`
- Terminal Protobuf-data-generator JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927224727-Ql5a/TEST-pytest-1790549247296150474.xml`
- Terminal Protobuf log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__protobuf_test.log`
- Terminal Protobuf JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927224841-Ekfb/TEST-pytest-1790549321332015306.xml`
- Terminal partition-column-pruning log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__prune_partition_column_test.log`
- Terminal partition-column-pruning JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927224955-lN8t/TEST-pytest-1790549395093716032.xml`
- Terminal Py4J-workaround log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__py4j_workaround_test.log`
- Terminal Py4J-workaround JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927225500-zRn7/TEST-pytest-1790549700886105604.xml`
- Terminal nightly-select log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__qa_nightly_select_test.log`
- Terminal nightly-select JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927225551-McHs/TEST-pytest-1790549751227058473.xml`
- Terminal random-expression log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__rand_test.log`
- Terminal random-expression JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927230603-Za20/TEST-pytest-1790550363130662470.xml`
- Terminal range log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__range_test.log`
- Terminal range JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927230729-BYt7/TEST-pytest-1790550449188860851.xml`
- Terminal reduced-IT-selection log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__reduced_it_selection_test.log`
- Terminal reduced-IT-selection JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927230835-Yx4q/TEST-pytest-1790550515758774670.xml`
- Terminal reduced-test-matrix-helper log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__reduced_test_matrix_helper_test.log`
- Terminal reduced-test-matrix-helper JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927230923-8WVX/TEST-pytest-1790550563437877159.xml`
- Terminal regexp-no-Unicode log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__regexp_no_unicode_test.log`
- Terminal regexp-no-Unicode JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927231011-prGQ/TEST-pytest-1790550611098986143.xml`
- Terminal regexp log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__regexp_test.log`
- Terminal regexp JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927231101-ud0i/TEST-pytest-1790550661878225906.xml`
- Terminal repartition log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__repart_test.log`
- Terminal repartition JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927231502-Cotq/TEST-pytest-1790550902211646723.xml`
- Terminal row-based-UDF log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__row-based_udf_test.log`
- Terminal row-based-UDF JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927233516-XT7s/TEST-pytest-1790552116307929366.xml`
- Terminal row-conversion log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__row_conversion_test.log`
- Terminal row-conversion JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927233639-9pt2/TEST-pytest-1790552199612132416.xml`
- Terminal sample log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__sample_test.log`
- Terminal sample JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927233855-Az3M/TEST-pytest-1790552335821843193.xml`
- Terminal scan-default-values log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__scan_default_values_test.log`
- Terminal scan-default-values JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927234229-e6td/TEST-pytest-1790552549332288458.xml`
- Terminal schema-evolution log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__schema_evolution_test.log`
- Terminal schema-evolution JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927234408-7qMW/TEST-pytest-1790552648615053795.xml`
- Terminal sort log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__sort_test.log`
- Terminal sort JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260927234617-JfOz/TEST-pytest-1790552777367270058.xml`
- Terminal string log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__string_test.log`
- Terminal string JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260928002911-skAX/TEST-pytest-1790555351340906810.xml`
- Terminal string-type log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__string_type_test.log`
- Terminal string-type JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260928003319-qghW/TEST-pytest-1790555599388646641.xml`
- Terminal struct log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__struct_test.log`
- Terminal struct JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260928003530-QwSF/TEST-pytest-1790555730798819860.xml`
- Terminal subquery log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__subquery_test.log`
- Terminal subquery JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260928003903-u169/TEST-pytest-1790555943029547686.xml`
- Terminal time-window log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__time_window_test.log`
- Terminal time-window JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260928004317-CTIU/TEST-pytest-1790556197013702227.xml`
- Terminal cuDF-UDF log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__udf_cudf_test.log`
- Terminal cuDF-UDF JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260928004829-pxmL/TEST-pytest-1790556509029675855.xml`
- Terminal UDF log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__udf_test.log`
- Terminal UDF JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260928004917-srpc/TEST-pytest-1790556557016878355.xml`
- Terminal URL log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__url_test.log`
- Terminal URL JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260928015053-PakO/TEST-pytest-1790560253364813942.xml`
- Terminal variant log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__variant_test.log`
- Terminal variant JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260928015253-Rogj/TEST-pytest-1790560373850862521.xml`
- Terminal window-function log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/main__window_function_test.log`
- Terminal window-function JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260928015457-heDs/TEST-pytest-1790560497101121105.xml`
- Terminal alternate-cache log: `/home/ubuntu/spark-rapids/db18-resume-results/20260927T163910Z/cache__cache_test.log`
- Terminal alternate-cache JUnit: `/home/ubuntu/spark-rapids/scala2.13/integration_tests/target/run_dir-20260928024559-KS4V/TEST-pytest-1790563559785648252.xml`
- Resume launcher log: `/home/ubuntu/db18_resume_runner.launch.log`
- Runner PID record: `/home/ubuntu/db18_resume_runner.pid`
- Runner exit record when terminal: `/home/ubuntu/db18_resume_runner.exit`
- Targeted UDF evidence directory: `/home/ubuntu/spark-rapids/db18-udf-rerun-results/20260928T031157Z`
- Targeted UDF status record: `/home/ubuntu/spark-rapids/db18-udf-rerun-results/20260928T031157Z/status.tsv`
- Targeted UDF log: `/home/ubuntu/spark-rapids/db18-udf-rerun-results/20260928T031157Z/udf_test.log`
- Targeted UDF launcher log: `/home/ubuntu/db18_udf_runner.launch.log`
- Targeted UDF PID record: `/home/ubuntu/db18_udf_runner.pid`
- Targeted UDF exit record when terminal: `/home/ubuntu/db18_udf_runner.exit`
- Targeted UDF runner script: `/home/ubuntu/db18_targeted_udf_runner.sh` (SHA-256 `30fa07a0d6335649ca6b50de38d63da6e2788fd4caa96c2d12d4a55ab9f4dd5a`)
- Private build log: `/home/ubuntu/db18-private-build.log`
- First public bootstrap attempt log: `/home/ubuntu/db18-public-build.attempt1.log`
- Second public build/packaging attempt log: `/home/ubuntu/db18-public-build.attempt2.log`
- Corrected successful public DB18-only build log: `/home/ubuntu/db18-public-build.log`
