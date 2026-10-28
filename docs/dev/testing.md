---
layout: page
title: Testing
nav_order: 2
parent: Developer Overview
---
An overview of testing can be found within the repository at:
* [Unit tests](https://github.com/NVIDIA/cudf-spark/tree/main/tests#readme)
* [Integration testing](https://github.com/NVIDIA/cudf-spark/tree/main/integration_tests#readme)


## Databricks premerge integration tests

For source-built premerge jobs, Databricks Runtime 14.3, 17.3, and 18.3 use the same
test-mode selection in
`jenkins/databricks/test.sh`:

* `CI_PART1` runs upstream Spark smoke checks and the first group of integration tests.
* `CI_PART2` runs the balanced test files, cache tests, a dedicated Delta Lake suite,
  multithreaded shuffle tests, and PyArrow tests.
* `DEFAULT` runs both groups in one invocation, including the dedicated Delta Lake suite.
* `DELTA_LAKE_ONLY` runs the dedicated Delta Lake suite.

Ordinary integration invocations skip Delta-marked cases unless `--delta_lake` is supplied.
The dedicated suite selects `-m delta_lake` and supplies `--delta_lake`; it runs once in
`DEFAULT`, `CI_PART2`, or `DELTA_LAKE_ONLY`, and does not run in `CI_PART1`.

CI retains native Spark readiness checks and, where required, upstream Spark readiness
checks before product tests. Explicit `SPARK_READINESS_ONLY=1` runs only the required
readiness checks; it requires the CI startup hook and a supported test mode. A successful
readiness-only invocation does not establish integration-test success.
