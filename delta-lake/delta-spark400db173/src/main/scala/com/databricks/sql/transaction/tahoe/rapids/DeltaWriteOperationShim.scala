/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe.DeltaOperations
import com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaEdge

/** DBR 17.3 uses the legacy Delta write operation metadata. */
private[rapids] object DeltaWriteOperationShim {
  def getOperation(
      _cpuWrite: WriteIntoDeltaEdge,
      operation: DeltaOperations.Write): DeltaOperations.Write = operation
}
