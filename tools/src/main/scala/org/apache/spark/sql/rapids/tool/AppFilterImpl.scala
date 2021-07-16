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

import java.util.Calendar
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadPoolExecutor, TimeUnit}
import scala.collection.JavaConverters._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.EventLogPathProcessor.logError
import com.nvidia.spark.rapids.tool.qualification.QualificationArgs
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

import scala.collection.mutable.LinkedHashMap

class AppFilterImpl(
    numRows: Int,
    hadoopConf: Configuration,
    timeout: Option[Long],
    nThreads: Int) extends Logging {

  private val appsForFiltering = new ConcurrentLinkedQueue[AppFilterReturnParameters]()
  // default is 24 hours
  private val waitTimeInSec = timeout.getOrElse(60 * 60 * 24L)

  private val NEGATE = "~"

  private val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true).setNameFormat("qualAppFilter" + "-%d").build()
  logInfo(s"Threadpool size is $nThreads")
  private val qualFilterthreadPool = Executors.newFixedThreadPool(nThreads, threadFactory)
      .asInstanceOf[ThreadPoolExecutor]

  private class FilterThread(path: EventLogInfo) extends Runnable {
    def run: Unit = filterEventLog(path, numRows, hadoopConf)
  }

  def filterEventLogs(
      allPaths: Seq[EventLogInfo],
      appArgs: QualificationArgs): Seq[EventLogInfo] = {
    allPaths.foreach { path =>
      try {
        qualFilterthreadPool.submit(new FilterThread(path))
      } catch {
        case e: Exception =>
          logError(s"Unexpected exception submitting log ${path.eventLog.toString}, skipping!", e)
      }
    }
    // wait for the threads to finish processing the files
    qualFilterthreadPool.shutdown()
    if (!qualFilterthreadPool.awaitTermination(waitTimeInSec, TimeUnit.SECONDS)) {
      logError(s"Processing log files took longer then $waitTimeInSec seconds," +
          " stopping processing any more event logs")
      qualFilterthreadPool.shutdownNow()
    }

    // This will be required to do the actual filtering
    val apps = appsForFiltering.asScala

    val filterAppName = appArgs.applicationName.getOrElse("")
    val filterCriteria = appArgs.filterCriteria.getOrElse("")

    if (appArgs.applicationName.isSupplied && filterAppName.nonEmpty) {
      val filtered = if (filterAppName.startsWith(NEGATE)) {
        // remove ~ before passing it into the containsAppName function
        apps.filterNot(app => containsAppName(app, filterAppName.substring(1)))
      } else {
        apps.filter(app => containsAppName(app, filterAppName))
      }
      filtered.map(_.eventlog).toSeq
    } else if (appArgs.startAppTime.isSupplied) {
      val msTimeToFilter = AppFilterImpl.parseAppTimePeriodArgs(appArgs)
      val filtered = apps.filter { app =>
        val appStartOpt = app.appInfo.map(_.startTime)
        if (appStartOpt.isDefined) {
          appStartOpt.get >= msTimeToFilter
        } else {
          false
        }
      }
      filtered.map(_.eventlog).toSeq
    } else if (appArgs.filterCriteria.isSupplied && filterCriteria.nonEmpty) {
      if (filterCriteria.endsWith("-overall")) {
        val filteredInfo = filterCriteria.split("-")
        val numberofEventLogs = filteredInfo(0).toInt
        val criteria = filteredInfo(1)
        val filtered = if (criteria.equals("oldest")) {
          apps.toSeq.sortBy(_.appInfo.get.startTime)
        } else if (criteria.equals("newest")) {
          apps.toSeq.sortBy(_.appInfo.get.startTime).reverse
        } else {
          logError("Criteria should be either newest-overall or oldest-overall")
          Seq[AppFilterReturnParameters]()
        }
        filtered.map(_.eventlog).take(numberofEventLogs)
      } else {
        val distinctAppNameMap = apps.groupBy(_.appInfo.get.appName)
        //TODO : filter N eventlogs per app Name
        //TEMPORARILY RETURNING ALL EVENTLOGS
        apps.map(x => x.eventlog).toSeq
      }
    } else {
      apps.map(x => x.eventlog).toSeq
    }
  }

  private def containsAppName(app: AppFilterReturnParameters, filterAppName: String): Boolean = {
    val appNameOpt = app.appInfo.map(_.appName)
    if (appNameOpt.isDefined) {
      appNameOpt.get.contains(filterAppName)
    } else {
      // in complete log file
      false
    }
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
    appsForFiltering.add(appInfo)
  }
}

object AppFilterImpl {

  def parseAppTimePeriodArgs(appArgs: QualificationArgs): Long = {
    if (appArgs.startAppTime.isSupplied) {
      val appStartStr = appArgs.startAppTime.getOrElse("")
      parseAppTimePeriod(appStartStr)
    } else {
      0L
    }
  }

  // parse the user provided time period string into ms.
  def parseAppTimePeriod(appStartStr: String): Long = {
    val timePeriod = raw"(\d+)([h,d,w,m]|min)?".r
    val (timeStr, periodStr) = appStartStr match {
      case timePeriod(time, null) =>
        (time, "d")
      case timePeriod(time, period) =>
        (time, period)
      case _ =>
        throw new IllegalArgumentException(s"Invalid time period $appStartStr specified, " +
          "time must be greater than 0 and valid periods are min(minute),h(hours)" +
          ",d(days),w(weeks),m(months).")
    }
    val timeInt = try {
      timeStr.toInt
    } catch {
      case ne: NumberFormatException =>
        throw new IllegalArgumentException(s"Invalid time period $appStartStr specified, " +
          "time must be greater than 0 and valid periods are min(minute),h(hours)" +
          ",d(days),w(weeks),m(months).")
    }

    if (timeInt <= 0) {
      throw new IllegalArgumentException(s"Invalid time period $appStartStr specified, " +
        "time must be greater than 0 and valid periods are min(minute),h(hours)" +
        ",d(days),w(weeks),m(months).")
    }
    val c = Calendar.getInstance
    periodStr match {
      case "min" =>
        c.add(Calendar.MINUTE, -timeInt)
      case "h" =>
        c.add(Calendar.HOUR, -timeInt)
      case "d" =>
        c.add(Calendar.DATE, -timeInt)
      case "w" =>
        c.add(Calendar.WEEK_OF_YEAR, -timeInt)
      case "m" =>
        c.add(Calendar.MONTH, -timeInt)
      case _ =>
        throw new IllegalArgumentException(s"Invalid time period $appStartStr specified, " +
          "time must be greater than 0 and valid periods are min(minute),h(hours)" +
          ",d(days),w(weeks),m(months).")
    }
    c.getTimeInMillis
  }

}
