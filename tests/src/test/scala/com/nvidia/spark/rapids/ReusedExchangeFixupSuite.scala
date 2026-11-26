/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.execution.{RangeExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange,
  ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode}
import org.apache.spark.sql.rapids.execution.GpuBroadcastExchangeExec

class ReusedExchangeFixupSuite extends SparkQueryCompareTestSuite {
  /**
   * Tests reuse exchange fixup. Ideally this would be an integration test, but
   * attempts to reproduce the failure to reuse were not successful at smaller scales.
   */
  private def fixupExchangeReuseTest(spark: SparkSession): Unit = {
    val range = RangeExec(Range(1, 2, 1, Some(1)))
    val broadcast1 = BroadcastExchangeExec(
      HashedRelationBroadcastMode(range.output),
      range)
    val join1 = BroadcastHashJoinExec(
      range.output,
      broadcast1.output,
      Inner,
      BuildRight,
      None,
      range,
      broadcast1)

    val broadcast2 = BroadcastExchangeExec(
      HashedRelationBroadcastMode(range.output),
      range)
    val join2 = BroadcastHashJoinExec(
      range.output,
      broadcast2.output,
      Inner,
      BuildRight,
      None,
      range,
      broadcast2)
    val plan = UnionExec(Seq(join1, join2))
    val overrides = GpuOverrides()
    val transitionOverrides = new GpuTransitionOverrides
    var updatedPlan = overrides(plan)
    updatedPlan = transitionOverrides(updatedPlan)
    val reused = updatedPlan.find {
      case _: ReusedExchangeExec => true
      case _ => false
    }
    assert(reused.isDefined)
  }

  test("fixupExchangeReuse") {
    val conf = new SparkConf()
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.exchange.reuse", "true")
        .set(RapidsConf.ENABLE_AQE_EXCHANGE_REUSE_FIXUP.key, "true")
    withGpuSparkSession(fixupExchangeReuseTest, conf)
  }

  private val reuseFixupConf = new SparkConf()
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.exchange.reuse", "true")
      .set(RapidsConf.ENABLE_AQE_EXCHANGE_REUSE_FIXUP.key, "true")

  /** A broadcast hash join of a range with a broadcast of that range. */
  private def broadcastJoin(range: RangeExec): SparkPlan = {
    val broadcast = BroadcastExchangeExec(HashedRelationBroadcastMode(range.output), range)
    BroadcastHashJoinExec(range.output, broadcast.output, Inner, BuildRight, None, range,
      broadcast)
  }

  private def toGpuPlan(plan: SparkPlan): SparkPlan =
    new GpuTransitionOverrides()(GpuOverrides()(plan))

  private def reusedExchanges(plan: SparkPlan): Seq[ReusedExchangeExec] =
    plan.collectWithSubqueries { case r: ReusedExchangeExec => r }

  /**
   * Checks that the original of every reused exchange is in the same plan. Databricks'
   * ReuseExchangeAndSubquery looks each one up among the exchanges of the plan it rewrites and
   * fails with "key not found" otherwise.
   */
  private def assertReusesOnlyOwnExchanges(plan: SparkPlan): Unit = {
    val own = plan.collectWithSubqueries { case e: Exchange => e }
    reusedExchanges(plan).foreach { r =>
      assert(own.exists(_ eq r.child),
        s"reused exchange ${r.child.nodeName} is not in the plan:\n$plan")
    }
  }

  private def noReuseAcrossPlansTest(spark: SparkSession): Unit = {
    // Plans built from the same RangeExec have the same attribute ids and so equal canonical
    // forms, as when the same query is planned twice.
    val range = RangeExec(Range(1, 2, 1, Some(1)))
    val first = toGpuPlan(broadcastJoin(range))
    val second = toGpuPlan(broadcastJoin(range))
    Seq(first, second).foreach { plan =>
      assertReusesOnlyOwnExchanges(plan)
      assert(reusedExchanges(plan).isEmpty, s"unexpected reused exchange:\n$plan")
      assert(plan.find(_.isInstanceOf[GpuBroadcastExchangeExec]).isDefined,
        s"the broadcast is not on the GPU:\n$plan")
    }
  }

  private def reuseWithinPlanTest(spark: SparkSession): Unit = {
    // An equal plan is still alive, so its exchange is in ExchangeMappingCache.
    val range = RangeExec(Range(1, 2, 1, Some(1)))
    val earlier = toGpuPlan(broadcastJoin(range))
    val plan = toGpuPlan(UnionExec(Seq(broadcastJoin(range), broadcastJoin(range))))
    assert(reusedExchanges(plan).size == 1, s"expected one reused exchange:\n$plan")
    assertReusesOnlyOwnExchanges(plan)
    assert(earlier.find(_.isInstanceOf[GpuBroadcastExchangeExec]).isDefined)
  }

  test("fixupExchangeReuse does not reuse an exchange of another plan") {
    withGpuSparkSession(noReuseAcrossPlansTest, reuseFixupConf)
  }

  test("fixupExchangeReuse reuses within a plan while an equal plan is alive") {
    withGpuSparkSession(reuseWithinPlanTest, reuseFixupConf)
  }
}
