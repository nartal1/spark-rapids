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
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.jni.BloomFilter

/**
 * Provides the BloomFilter version to use for Spark 4.1+.
 * Spark 4.1 introduced BloomFilter V2 format (SPARK-47547) which fixes
 * int32 truncation issues for larger bloom filters.
 */
object BloomFilterVersionShim {
  /** 
   * Returns the BloomFilter version to use when creating new bloom filters.
   * V2 for Spark 4.1+ which fixes int32 truncation issues.
   */
  val bloomFilterVersion: Int = BloomFilter.VERSION_2
  
  /**
   * V2 header size is 16 bytes (version, numHashes, seed, numLongs).
   */
  val headerSize: Int = 16
}

