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

package org.apache.spark.sql.rapids.tool

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.qualification.QualificationArgs
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging

class AppFilterImpl(
    numRows: Int,
    hadoopConf: Configuration,
    timeout: Option[Long],
    nThreads: Int) extends Logging {

  private val filteredeventLogs = new ConcurrentLinkedQueue[AppFilterReturnParameters]()
  // default is 24 hours
  private val waitTimeInSec = timeout.getOrElse(60 * 60 * 24L)

  private val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true).setNameFormat("qualTool" + "-%d").build()
  logInfo(s"Threadpool size is $nThreads")
  private val qualFilter = Executors.newFixedThreadPool(nThreads, threadFactory)
      .asInstanceOf[ThreadPoolExecutor]

  private class QualifyThread(path: EventLogInfo) extends Runnable {
    def run: Unit = filterEventLog(path, numRows, hadoopConf)
  }

  def filterEventLogs(
      allPaths: Seq[EventLogInfo],
      appArgs: QualificationArgs): Seq[EventLogInfo] = {
    allPaths.foreach { path =>
      try {
        qualFilter.submit(new QualifyThread(path))
      } catch {
        case e: Exception =>
          logError(s"Unexpected exception submitting log ${path.eventLog.toString}, skipping!", e)
      }
    }
    // wait for the threads to finish processing the files
    qualFilter.shutdown()
    if (!qualFilter.awaitTermination(waitTimeInSec, TimeUnit.SECONDS)) {
      logError(s"Processing log files took longer then $waitTimeInSec seconds," +
          " stopping processing any more event logs")
      qualFilter.shutdownNow()
    }
    var eventlog = filteredeventLogs.asScala.map(x => x.eventlog).toSeq

    // This will be required to do the actual filtering
    val allSumfilteredEventLogs = filteredeventLogs.asScala.map(x => (x.appInfo, x.eventlog)).toSeq

    if (appArgs.applicationName.isDefined) {
      val applicationN = appArgs.applicationName
      val finalEventLogs = allSumfilteredEventLogs.map { case (optQualInfo, y) =>
        optQualInfo.get.appName match {
          case a if a.equals(applicationN.getOrElse("")) =>
            logWarning(s"matched on ${optQualInfo.get.appName} and $applicationN")
            y
          case _ => logWarning(s"didnt' match on ${optQualInfo.get.appName}")
        }
      }
      eventlog = finalEventLogs.asInstanceOf[Seq[EventLogInfo]]
    }
    eventlog
  }

  case class AppFilterReturnParameters(
      appInfo: Option[ApplicationStartInfo],
      eventlog: EventLogInfo)

  private def filterEventLog(
      path: EventLogInfo,
      numRows: Int,
      hadoopConf: Configuration): Unit = {

    val startAppInfo = new FilterAppInfo(numRows, path, hadoopConf)
    val appInfo = AppFilterReturnParameters(startAppInfo.appInfo, path)
    filteredeventLogs.add(appInfo)
  }
}
