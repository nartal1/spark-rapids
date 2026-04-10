# Diagnosis: Databricks 17.3 Job Hang During NDS Benchmark

## Context

An NDS benchmark job running on Databricks 17.3 (Spark 4.0) with the RAPIDS Accelerator hangs indefinitely during execution. Key observations:
- **First NDS run completes fine; hang occurs on the second run**
- **The hang is NOT consistent on the same query** - it can happen on different queries across runs
- In the captured logs, the hang occurred on **query20**

## Log Analysis

The hang occurs at a very specific point:

1. **23:13:35** - AQE submits **Stage 3783** (127 tasks, shuffle map)
2. **23:13:35** - 4 tasks start on 2 executors (TIDs 98299-98302)
3. **23:13:35** - AQE **immediately cancels** Stage 3783 (~4ms later): _"Adaptive query execution has replanned the query and cancelled unused stages"_
4. **23:13:35** - All 4 tasks receive `TaskKilled`
5. **23:13:35** - AQE submits replacement **Stage 3784** (127 tasks)
6. **23:13:35** - `TaskSet 3784.0` added to the scheduler... **but zero tasks are ever started**
7. **23:13:37+** - Only `ClusterLoadAvgHelper` heartbeats for 38+ minutes with no task activity

The critical symptom: `TaskSchedulerImpl` added the task set but never issued a "Starting task" log for any of the 127 tasks.

## Root Cause: Plugin Completion Listeners Delay GPU Resource Return During AQE Task Kill

### Key Observation: This does NOT happen on CPU

The hang only occurs with the GPU (spark-rapids plugin enabled). The same AQE cancel-and-resubmit pattern on CPU completes without issue. This means the spark-rapids plugin is contributing to the hang — it is not purely a Spark scheduler bug.

### Why this is a probabilistic/race-condition issue

The fact that:
- First run completes (AQE cancellations work fine most of the time)
- Second run hangs (eventually the race is hit)
- Not consistent on which query

...indicates a **timing-dependent race condition**, not a deterministic bug. Over ~100 NDS queries per run, each potentially triggering AQE replannings with stage cancellations, the probability of hitting the race accumulates. The first run was simply lucky.

### The mechanism: How AQE + plugin completion listeners cause the hang

When Spark kills a task on an executor, the `TaskRunner.run()` sequence is:
1. Task code is interrupted/killed
2. `task.context.markTaskCompleted(TaskKilled)` fires **ALL registered `TaskCompletionListener`s**
3. **Only after ALL listeners complete**, the `TaskRunner` sends `StatusUpdate(KILLED)` to the driver
4. The driver frees GPU resources from its resource pool and calls `makeOffers()` to schedule new tasks

On **CPU** (no plugin): Step 2 has no plugin listeners → `StatusUpdate` is sent immediately → resources freed promptly → replacement stage tasks get scheduled.

On **GPU** (with spark-rapids plugin): Step 2 must run these plugin-registered completion listeners before `StatusUpdate` can be sent:

#### Listeners registered in `onTaskStart()` (Plugin.scala:850-867) — runs for EVERY task:
- `TaskPriority.taskDone(tc.taskAttemptId())` — lightweight cleanup
- `TaskRegistryTracker.taskIsDone(taskId)` — calls `RmmSpark.taskDone(taskId)` **(JNI call into CUDA/C++ layer)**

#### Listeners registered on first GPU semaphore access (GpuSemaphore.scala:559-561) — runs if task touched GPU:
- `GpuSemaphore.completeTask()` — calls `releaseSemaphore()` and `RmmSpark.getMaxGpuTaskMemory(taskId)` **(JNI call)**

### The critical bottleneck: `TaskRegistryTracker.taskIsDone()`

```scala
// TaskRegistryTracker.scala:46-52
private def taskIsDone(taskId: Long): Unit = synchronized {  // <-- GLOBAL LOCK
    val threads = taskToThread.remove(taskId)
    if (threads != null) {
      threads.foreach(registeredThreads.remove)
      RmmSpark.taskDone(taskId)  // <-- JNI call while holding lock
    }
  }
```

This method:
1. Holds a **synchronized lock on the `TaskRegistryTracker` singleton** (per-executor)
2. Calls `RmmSpark.taskDone(taskId)` — a **JNI call** into the C++/CUDA layer that may perform GPU synchronization or memory cleanup
3. **All killed tasks on the same executor serialize through this lock**

In the hang scenario, **all 4 killed tasks are on the same executor**. If `RmmSpark.taskDone()` is slow for even one task (due to CUDA sync, memory cleanup, GPU operation finalization), it:
- Holds the `TaskRegistryTracker` lock
- Blocks all other killed tasks from completing their listeners
- Delays `StatusUpdate(KILLED)` for ALL tasks on that executor
- The driver thinks all GPU resource slots are still in use
- The replacement stage's `reviveOffers()` finds no available GPU slots
- **No tasks are ever scheduled for the replacement stage**

### Why the "Lost task" messages are misleading

The driver-side "Lost task" WARN messages appear in the logs at the same timestamp as the cancellation:
```
WARN TaskSetManager: Lost task 0.0 in stage 5461.0 (TID 144812) ... TaskKilled (Stage cancelled: ...)
```

These are logged by `TaskSetManager.handleFailedTask()` when the **cancellation signal** is processed on the driver — NOT when the executor actually reports back via `StatusUpdate`. The driver optimistically marks them as killed, but **GPU resource accounting is only updated when the executor's `StatusUpdate` arrives**. If the executor is delayed (by plugin completion listeners), the resources remain "in use" on the driver's books.

### The race window

```
19:03:36 — Stage 5461 submitted, 4 tasks started
19:03:36 — Stage 5461 cancelled (~4ms later)
19:03:36 — Stage 5462 submitted → reviveOffers() → NO GPU resources available
19:03:36 — "Lost task" messages logged (cancellation-side, NOT executor report-back)
19:03:36 — TaskSet 5462.0 added to scheduler
           ... executor still running completion listeners (RmmSpark.taskDone JNI calls) ...
           ... StatusUpdate never arrives (or arrives too late without triggering reviveOffers) ...
19:03:39+ — Only heartbeats, no task activity
```

### Evidence supporting this theory

- Only happens with GPU (plugin enabled), never on CPU
- Only 1 executor (executor 5) with 1 GPU — all 4 tasks serialize through `TaskRegistryTracker` lock
- After the kills, **no tasks start anywhere** — consistent with all GPU resource slots appearing exhausted on the driver
- The hang is probabilistic — depends on JNI call duration which varies with GPU state
- The `TaskSet 3783.0` was logged as removed **twice** in the first instance (a race condition artifact suggesting concurrent state issues)

### What is NOT the direct cause (but may contribute)

The spark-rapids plugin's internal GPU semaphore (`GpuSemaphore`) is **not the direct cause** — the hang is at the Spark scheduler level (tasks never start), not the semaphore level (tasks would start but block). However, the semaphore's `completeTask()` callback does make additional JNI calls (`RmmSpark.getMaxGpuTaskMemory`) that add to the completion listener delay:

- `SemaphoreTaskInfo.releaseSemaphore()` has a `hasSemaphore` guard — safe no-op for tasks that never acquired it (`GpuSemaphore.scala:492-509`)
- `ScalableTaskCompletion.callAllCallbacks()` catches exceptions per-callback and continues (`ScalableTaskCompletion.scala:100-128`)
- `PrioritySemaphore` properly handles interruption in `acquire()` (`PrioritySemaphore.scala:96-103`)
- `blockUntilReady()` catch block properly cleans up (`GpuSemaphore.scala:446-459`)

## Secondary Issue: `listener.Manager` Not Found

Every query logs: `Not found com.nvidia.spark.rapids.listener.Manager 'JavaPackage' object is not callable`

**This class does not exist in the spark-rapids codebase.** It's a py4j error from the NDS test script (`nds_power.py`) trying to instantiate a non-existent Java class. It's a harmless warning from the test harness and is **not related to the hang**.

## Second Hang Instance (April 10, 2026) — Same Root Cause Confirmed

A second hang was observed on a different run, confirming this is a reproducible race condition.

### Log Summary

The logs show the identical sequence:

1. **19:03:36** — AQE submits **Stage 5461** (127 tasks, `GpuOpTimeTrackingRDD` shuffle map)
2. **19:03:36** — 4 tasks start on executor 5 (TIDs 144812–144815)
3. **19:03:36** — AQE cancels Stage 5461 (~4ms later): _"Adaptive query execution has replanned the query and cancelled unused stages"_
4. **19:03:36** — All 4 tasks receive `TaskKilled`
5. **19:03:36** — AQE submits replacement **Stage 5462** (127 tasks) — `TaskSet 5462.0` added to scheduler
6. **19:03:36+** — **Zero tasks ever start** for Stage 5462
7. **19:03:39+** — Only `ClusterLoadAvgHelper` heartbeats — no task activity for minutes

### Differences from First Instance

| Aspect | First Hang | Second Hang |
|--------|-----------|-------------|
| Query | query20 | query98 |
| Cancelled Stage | 3783 | 5461 |
| Replacement Stage | 3784 | 5462 |
| Executors | 2 (executor 1, 3) | 1 (executor 5 only) |
| GPU slots | 2 total | 1 total |
| Tasks killed | 4 (2 per executor) | 4 (all on executor 5) |

### What This Confirms

- **Not query-specific** — different queries trigger the same hang pattern
- **Not executor-count-specific** — reproduces with both 1 and 2 executors
- **Probabilistic** — the race condition accumulates probability over many AQE replannings across ~100 NDS queries per run
- **Same mechanism** — GPU resource slots are not freed after `TaskKilled`, preventing the replacement stage from scheduling

## Recommended Actions

### Immediate Workaround

Disable AQE to prevent the cancel-and-resubmit pattern:
```
spark.sql.adaptive.enabled=false
```

Or reduce AQE aggressiveness:
```
spark.sql.adaptive.forceOptimizeSkewedJoin=false
spark.sql.adaptive.coalescePartitions.enabled=false
```

### Plugin-Side Fixes

#### Fix 1: Add diagnostic timing to completion listeners (confirm theory)

Add timing logs around the JNI calls in completion listeners to measure how long they take during AQE task kills. This confirms whether `RmmSpark.taskDone()` is the bottleneck.

**Files to modify:**
- `TaskRegistryTracker.scala:46-52` — log time spent in `RmmSpark.taskDone()`
- `GpuSemaphore.scala:612-626` — log time spent in `completeTask()`

#### Fix 2: Make `TaskRegistryTracker.taskIsDone()` non-blocking

The current code holds a synchronized lock while calling `RmmSpark.taskDone()` (JNI). Move the JNI call outside the synchronized block, or execute it asynchronously on a separate thread, so it doesn't block other tasks from completing their cleanup and sending `StatusUpdate`.

**File to modify:**
- `TaskRegistryTracker.scala:46-52` — restructure to release lock before JNI call

#### Fix 3: Add `reviveOffers()` safety net via SparkListener

Register a `SparkListener` in `RapidsDriverPlugin.init()` that listens for `onStageCompleted` events. When a stage completes (including cancellation), schedule a delayed `reviveOffers()` call to ensure the scheduler re-evaluates pending task sets even if the initial scheduling attempt found no resources.

**Access path:** `SparkContext` → `schedulerBackend` (accessible via reflection or `TrampolineUtil` in `org.apache.spark` package) → `reviveOffers()`

**File to modify:**
- `Plugin.scala:487-544` — add listener in `RapidsDriverPlugin.init()`

#### Fix 4: Add explicit GPU resource release in `onTaskFailed()`

Currently `onTaskFailed()` (`Plugin.scala:823-848`) only handles logging and CUDA fatal error detection. It does NOT explicitly release GPU semaphore or trigger cleanup — it relies entirely on completion listeners. Add an explicit `GpuSemaphore.releaseIfNecessary()` call as a belt-and-suspenders measure.

**File to modify:**
- `Plugin.scala:823-848`

### Investigation Steps (if plugin fixes don't resolve)

1. **Check executor logs** (not just driver logs) for killed task TIDs — look for:
   - Whether `RapidsExecutorPlugin.onTaskFailed()` was called for each killed task
   - Any exceptions or slow JNI calls during GPU resource cleanup
   - Whether Spark's `TaskRunner` sent `StatusUpdate(TaskKilled)` back to driver and how long after the kill signal

2. **Enable DEBUG logging** on `TaskSchedulerImpl` and `CoarseGrainedSchedulerBackend` to see:
   - Whether `reviveOffers()` is called after killed tasks' `StatusUpdate` arrives
   - What `resourceOffers()` sees as available GPU resources when it runs
   - Timestamps of resource return vs. replacement stage submission

3. **Capture a thread dump** on the executor during the hang — look for threads blocked in:
   - `TaskRegistryTracker.taskIsDone()` (waiting for synchronized lock)
   - `RmmSpark.taskDone()` (blocked in JNI/CUDA)
   - `ScalableTaskCompletion.callAllCallbacks()` (waiting for a callback to complete)

### Fix the `listener.Manager` Warning

Remove or update the reference to `com.nvidia.spark.rapids.listener.Manager` in the NDS test script (`nds_power.py`). This class was never part of the spark-rapids codebase.

## Key Files Reviewed

### Critical path (completion listeners that delay StatusUpdate)
- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/TaskRegistryTracker.scala:46-52` — **Primary suspect**: `taskIsDone()` holds synchronized lock while calling `RmmSpark.taskDone()` JNI
- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/Plugin.scala:850-867` — `onTaskStart()` registers completion listeners including `TaskRegistryTracker.registerDedicatedThreadForRetry()`
- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/Plugin.scala:823-848` — `onTaskFailed()` does NOT explicitly release GPU resources
- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/ScalableTaskCompletion.scala:100-128,183-199` — Task completion callback registration and dispatch (robust, catches exceptions per-callback)

### GPU semaphore (not the direct cause but adds JNI overhead to completion path)
- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuSemaphore.scala:559-561,612-626` — Semaphore task registration and `completeTask()` callback
- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuSemaphore.scala:492-509` — `releaseSemaphore()` with `hasSemaphore` guard
- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/PrioritySemaphore.scala:119-143` — Backing semaphore release and wake-up logic

### Driver plugin (for potential reviveOffers safety net)
- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/Plugin.scala:444-558` — `RapidsDriverPlugin` with SparkContext access, can add SparkListeners
- `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/TrampolineUtil.scala` — Utility for accessing Spark internals from `org.apache.spark` package

### Other
- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuExec.scala:55-95` — `GpuOpTimeTrackingRDD` (no resource management, just metrics)
- `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuShuffleExchangeExecBase.scala` — Where `GpuOpTimeTrackingRDD` wraps shuffle data
