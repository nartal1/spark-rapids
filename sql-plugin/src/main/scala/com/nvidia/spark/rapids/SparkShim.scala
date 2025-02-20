package com.nvidia.spark.rapids

import org.apache.spark.sql.SparkSessionExtensions

trait SparkShim {

  /**
   * Inject version-specific rules, strategies, etc. into SparkSessionExtensions.
   *
   * Implementation is in each version's SparkShims.scala (Spark3 or Spark4).
   */
  def applyOverridesToExtensions(extensions: SparkSessionExtensions): Unit

}
