/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.rapids

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.util.Clock

/** Common type from which 20x-24x open-source Delta Lake implementations derive. */
abstract class GpuOptimisticTransactionBase(
    deltaLog: DeltaLog,
    catalog: Option[CatalogTable],
    snapshot: Snapshot,
    rapidsConf: RapidsConf)
    (implicit clock: Clock)
    extends AbstractGpuOptimisticTransactionBase(deltaLog, catalog, snapshot, rapidsConf)(clock) {

  import org.apache.spark.sql.delta.constraints.{Constraint, DeltaInvariantCheckerExec}
  import org.apache.spark.sql.execution.SparkPlan

  /**
   * Delta 20x-24x implementation - uses Seq[Constraint] for DeltaInvariantCheckerExec
   */
  override protected def addInvariantChecks(plan: SparkPlan, constraints: Seq[Constraint]): SparkPlan = {
    val cpuInvariants =
      DeltaInvariantCheckerExec.buildInvariantChecks(plan.output, constraints, plan.session)
    GpuCheckDeltaInvariant.maybeConvertToGpu(cpuInvariants, rapidsConf) match {
      case Some(gpuInvariants) =>
        val gpuPlan = convertToGpu(plan)
        GpuDeltaInvariantCheckerExec(gpuPlan, gpuInvariants)
      case None =>
        val cpuPlan = convertToCpu(plan)
        // Delta 20x-24x expects Seq[Constraint]
        DeltaInvariantCheckerExec(cpuPlan, constraints)
    }
  }
}
