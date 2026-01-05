/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.NamedExpression

/**
 * Databricks 17.3 version where WindowInPandasExec was renamed to ArrowWindowPythonExec.
 * See: https://github.com/apache/spark/commit/23a19e6b5b0
 * 
 * This trait overrides getWindowExpressions to handle the case where
 * WindowInPandasExec doesn't exist (renamed to ArrowWindowPythonExec).
 * Since the old exec won't be used in DB 17.3, this method shouldn't be called.
 */
trait WindowInPandasShims {
  def getWindowExpressions(winPy: Any): Seq[NamedExpression] = {
    // WindowInPandasExec was renamed in DB 17.3, so this shouldn't be called
    throw new UnsupportedOperationException(
      "WindowInPandasExec was renamed to ArrowWindowPythonExec in Databricks 17.3")
  }
}
