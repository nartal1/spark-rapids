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

/*** spark-rapids-shim-json-lines
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.classic.{SparkSession, Strategy}
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

/**
 * Example "SparkShims" implementation.
 * The "applyOverridesToExtensions" method is called reflectively from ShimLoader.
 * It uses the methods below (filled in with calls to ShimReflectionUtils).
 */
object SQLExecPluginShims {

  // This is the main entry point that your ShimLoader calls reflectively:
  def applyOverridesToExtensions(extensions: SparkSessionExtensions): Unit = {
    // Inject columnar overrides
    extensions.injectColumnar { sparkSession =>
      newColumnarOverrideRules()
    }
    // Inject GPU query stage prep rule
    extensions.injectQueryStagePrepRule { sparkSession =>
      newGpuQueryStagePrepOverrides()
    }
    // Inject your planner strategy
    extensions.injectPlannerStrategy { _ =>
      newStrategyRules()
    }
  }

  /**
   * Creates an instance of "com.nvidia.spark.rapids.ColumnarOverrideRules"
   * using ShimReflectionUtils.
   */
  private def newColumnarOverrideRules(): ColumnarRule = {
    // If you have multiple versions (Spark 3 vs Spark 4) referencing different classes,
    // you can still point them to the same class name (if it's identical across versions).
    ShimReflectionUtils.newInstanceOf("com.nvidia.spark.rapids.ColumnarOverrideRules")
  }

  /**
   * Creates an instance of "com.nvidia.spark.rapids.GpuQueryStagePrepOverrides"
   * via reflection.
   */
  private def newGpuQueryStagePrepOverrides(): Rule[SparkPlan] = {
    ShimReflectionUtils.newInstanceOf("com.nvidia.spark.rapids.GpuQueryStagePrepOverrides")
  }

  /**
   * Creates an instance of "com.nvidia.spark.rapids.StrategyRules"
   * using ShimReflectionUtils, cast to Strategy.
   */
  private def newStrategyRules(): Strategy = {
    ShimReflectionUtils.newInstanceOf("com.nvidia.spark.rapids.StrategyRules")
  }
}