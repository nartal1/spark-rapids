/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.profiling

import java.io.FileWriter

import org.apache.hadoop.fs.Path
import org.rogach.scallop.ScallopOption
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap, Map}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * A profiling tool to parse Spark Event Log
 */
object ProfileMain extends Logging {
  /**
   * Entry point from spark-submit running this as the driver.
   */
  def main(args: Array[String]) {
    val sparkSession = ProfileUtils.createSparkSession
    val exitCode = mainInternal(sparkSession, new ProfileArgs(args))
    if (exitCode != 0) {
      System.exit(exitCode)
    }
  }

  /**
   * Entry point for tests
   */
  def mainInternal(sparkSession: SparkSession, appArgs: ProfileArgs): Int = {

    // This tool's output log file name
    val logFileName = "rapids_4_spark_tools_output.log"

    // Parsing args
    val eventlogPaths = appArgs.eventlog()
    val filterN = appArgs.filterCriteria
    val matchEventLogs = appArgs.matchEventLogs
    val outputDirectory = appArgs.outputDirectory().stripSuffix("/")

    // Create the FileWriter and sparkSession used for ALL Applications.
    val fileWriter = new FileWriter(s"$outputDirectory/$logFileName")
    logInfo(s"Output directory:  $outputDirectory")

    // Get the event logs required to process
    lazy val allPaths = processAllPaths(filterN, matchEventLogs, eventlogPaths)

    // If compare mode is on, we need lots of memory to cache all applications then compare.
    // Suggest only enable compare mode if there is no more than 10 applications as input.
    if (appArgs.compare()) {
      // Create an Array of Applications(with an index starting from 1)
      val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      var index: Int = 1
      for (path <- allPaths.filter(p => !p.getName.contains("."))) {
        apps += new ApplicationInfo(appArgs, sparkSession, fileWriter, path, index)
        index += 1
      }

      //Exit if there are no applications to process.
      if (apps.isEmpty) {
        logInfo("No application to process. Exiting")
        return 0
      }
      processApps(apps, generateDot = false)
      // Show the application Id <-> appIndex mapping.
      for (app <- apps) {
        logApplicationInfo(app)
      }
    } else {
      // This mode is to process one application at one time.
      var index: Int = 1
      for (path <- allPaths.filter(p => !p.getName.contains("."))) {
        // This apps only contains 1 app in each loop.
        val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
        val app = new ApplicationInfo(appArgs, sparkSession, fileWriter, path, index)
        apps += app
        logApplicationInfo(app)
        processApps(apps, appArgs.generateDot())
        app.dropAllTempViews()
        index += 1
      }
    }

    logInfo(s"Output log location:  $outputDirectory/$logFileName")

    fileWriter.flush()
    fileWriter.close()

    /**
     * Function to process ApplicationInfo. If it is in compare mode, then all the eventlogs are
     * evaluated at once and the output is one row per application. Else each eventlog is parsed one
     * at a time.
     */
    def processApps(apps: ArrayBuffer[ApplicationInfo], generateDot: Boolean): Unit = {
      if (appArgs.compare()) { // Compare Applications
        logInfo(s"### A. Compare Information Collected ###")
        val compare = new CompareApplications(apps)
        compare.compareAppInfo()
        compare.compareExecutorInfo()
        compare.compareRapidsProperties()
      } else {
        val collect = new CollectInformation(apps)
        logInfo(s"### A. Information Collected ###")
        collect.printAppInfo()
        collect.printExecutorInfo()
        collect.printRapidsProperties()
        if (generateDot) {
          collect.generateDot()
        }
      }

      logInfo(s"### B. Analysis ###")
      val analysis = new Analysis(apps)
      analysis.jobAndStageMetricsAggregation()
      val sqlAggMetricsDF = analysis.sqlMetricsAggregation()

      if (!sqlAggMetricsDF.isEmpty) {
        fileWriter.write(s"### C. Qualification ###\n")
        new Qualification(apps, sqlAggMetricsDF)
      } else {
        logInfo(s"Skip qualification part because no sqlAggMetrics DataFrame is detected.")
      }
    }

    def logApplicationInfo(app: ApplicationInfo) = {
      logInfo("========================================================================")
      logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
      logInfo("========================================================================")
    }

    0
  }

  /**
   * Function to evaluate the event logs to be processed.
   *
   * @param eventDir       directory containing the event logs
   * @param filterNLogs    number of event logs to be selected
   * @param matchlogs      keyword to match file names in the directory
   * @param eventLogsPaths Array of event log paths
   * @return event logs to be processed
   */
  def processAllPaths(
      filterNLogs: ScallopOption[String],
      matchlogs: ScallopOption[String],
      eventLogsPaths: List[String]): Seq[Path] = {

    val allPaths: ArrayBuffer[Path] = ArrayBuffer[Path]()
    val allPathsWithTimestamp: Map[Path, Long] = Map.empty[Path, Long]

    for (pathString <- eventLogsPaths) {
      val (paths, pathsWithTimestamp) = ProfileUtils.stringToPath(pathString)
      if (paths.nonEmpty) {
        allPaths ++= paths
        allPathsWithTimestamp ++= pathsWithTimestamp
      }
    }

    // Filter the eventlogs to be processed based on the criteria. If it is not provided in the
    // command line, then return all the event logs processed above.
    if (matchlogs.isDefined || filterNLogs.isDefined) {
      var sortedResult: LinkedHashMap[Path, Long] = LinkedHashMap.empty[Path, Long]
      if (filterNLogs.isDefined) {
        val numberofEventLogs = filterNLogs.toOption.get.split("-")(0).toInt
        val criteria = filterNLogs.toOption.get.split("-")(1)
        if (criteria.equals("newest")) {
          sortedResult = LinkedHashMap(allPathsWithTimestamp.toSeq.sortWith(_._2 > _._2): _*)
        } else if (criteria.equals("oldest")) {
          sortedResult = LinkedHashMap(allPathsWithTimestamp.toSeq.sortWith(_._2 < _._2): _*)
        } else {
          logError("Criteria should be either newest or oldest")
          System.exit(1)
        }

        if (matchlogs.isDefined) {
          val filteredPath =
            sortedResult.map(_._1).toSeq.filter(a => a.toString.contains(matchlogs.toOption.get))
          val finalResult = filteredPath.take(numberofEventLogs)
          finalResult
        } else {
          sortedResult.map(_._1).toSeq.take(numberofEventLogs)
        }
      } else { // if only match criteria is provided.
        allPaths.filter(a => a.toString.contains(matchlogs.toOption.get))
      }
    } else { // send all event logs for processing
      allPaths
    }
  }
}
