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
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "400"}
{"spark": "401"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.jni.BloomFilter

/**
 * Provides the BloomFilter version to use based on the Spark version.
 * Spark 4.0 and earlier use V1 format.
 * This shim provides the default (V1) behavior.
 */
object BloomFilterVersionShim {
  /** 
   * Returns the BloomFilter version to use when creating new bloom filters.
   * V1 for Spark 4.0 and earlier, V2 for Spark 4.1+.
   */
  val bloomFilterVersion: Int = BloomFilter.VERSION_1
  
  /**
   * V1 header size is 12 bytes (version, numHashes, numLongs).
   * V2 header size is 16 bytes (version, numHashes, seed, numLongs).
   */
  val headerSize: Int = 12
}

