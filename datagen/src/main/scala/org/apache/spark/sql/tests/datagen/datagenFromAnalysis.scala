

package org.apache.spark.sql.tests.datagen

import net.jcazevedo.moultingyaml._
import scala.io.Source

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types._

object ColumnStatsProtocol extends DefaultYamlProtocol {
  implicit val columnStatsFormat = yamlFormat11(ColumnStats)
}

object DataGenerationApp {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    if (args.length < 1) {
      System.err.println("Usage: DataGenerationApp <yaml_file_path> <output_directory>")
      System.exit(1)
    }

    val yamlFilePath = args(0)
    val outputDirectory = if (args.length >= 2) args(1) else "./output" // Default directory

    // Read the statistics from the file
    val tableColumnStats = readTableColumnStats(yamlFilePath)

    // Initialize SparkSession
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DataGeneration")
      .getOrCreate()

    try {
      val dbGen = new DBGen()

      tableColumnStats.foreach { case (tableName, columnStats) =>
        val numRows = columnStats.head._2.total_count

        // Define the schema
        val fields = columnStats.map { case (columnName, stats) =>
          val dataType = stats.data_type.toLowerCase match {
            case "int" | "integer" => IntegerType
            case "double" | "float" => DoubleType
            case _ => throw new IllegalArgumentException(s"" +
              s"Unsupported data type: ${stats.data_type}")
          }
          StructField(columnName, dataType, nullable = false)
        }.toSeq

        val schema = StructType(fields)

        val tableGen = dbGen.addTable(tableName, schema, numRows)

        columnStats.foreach { case (columnName, stats) =>
          val columnGen = tableGen(columnName)
          val dataType = stats.data_type.toLowerCase

          val minValue = stats.min
          val maxValue = stats.max

          // Configure the column generator
          dataType match {
            case "int" | "integer" =>
              columnGen.setValueRange(minValue.toInt, maxValue.toInt)

              // Set distribution
              stats.distribution.toLowerCase match {
                case "uniform" =>
                  // Uniform distribution between min and max
                  columnGen.setSeedMapping(FlatDistribution())
                case "normal" | "unknown" =>
                  columnGen.setSeedMapping(
                    NormalDistribution(stats.mean, stats.stddev).withColumnConf(
                      columnGen.dataGen.conf.forSeedRange(minValue.toLong, maxValue.toLong)
                    )
                  )
                case "exponential" =>
                  columnGen.setSeedMapping(
                    ExponentialDistribution(
                      target = stats.max, stdDev = stats.stddev).withColumnConf(
                      columnGen.dataGen.conf.forSeedRange(minValue.toLong, maxValue.toLong)
                    )
                  )
                case "distinct" =>
                  columnGen.setSeedMapping(DistinctDistribution()
                  )
                case _ =>
                  throw new IllegalArgumentException(
                    s"Unsupported distribution: ${stats.distribution}")
              }

              // Set the distinct count (cardinality) if applicable
//              if (stats.distinct_count > 0) {
//                columnGen.setSeedRange(1, stats.distinct_count)
//              }

            case "double" | "float" =>
              // Set distribution
              stats.distribution.toLowerCase match {
                case "uniform" =>
                  columnGen.setValueGen(DoubleGenFunc())

                case "normal"| "unknown" =>
                  columnGen.setValueGen(NormalDoubleGenFunc(stats.mean, stats.stddev))

                case "exponential" =>
                  columnGen.setValueGen(ExponentialDoubleGenFunc(
                    target = maxValue, stdDev = stats.stddev))
                case "distinct" =>
                  columnGen.setSeedMapping(
                    DistinctDistribution(numTableRows = numRows).withColumnConf(
                      columnGen.dataGen.conf.forSeedRange(1, stats.distinct_count)
                    )
                  )
                case _ =>
                  throw new IllegalArgumentException(s"Unsupported distribution: " +
                    s"${stats.distribution}")
              }

            case _ =>
              throw new IllegalArgumentException(s"Unsupported data type: ${dataType}")
          }
        }

        // Generate the data
        val df = tableGen.toDF(spark)
//        if(tableName == "store_sales") {
//          val groupedDF = df.groupBy("ss_sold_time_sk").count()
//          println(s"COUNT for $tableName -  ${groupedDF.count()}")
//          //show in descending order
//          groupedDF.orderBy(groupedDF("count").desc).show()
//        }
//        df.show()

        // Save the DataFrame
        df.write.mode("overwrite").parquet(s"$outputDirectory/$tableName.parquet")
        System.out.println(s"Writing to ${outputDirectory}/$tableName.parquet}")
      }
    } catch {
      case e: Exception =>
        System.err.println(s"Error during data generation: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  // Function to read the statistics from the file
  def readTableColumnStats(filePath: String): Map[String, Map[String, ColumnStats]] = {
    import ColumnStatsProtocol._
    val source = Source.fromFile(filePath)
    val yamlStr = source.mkString
    source.close()

    val yamlAst = yamlStr.parseYaml

    val result = yamlAst.asYamlObject.fields.collect {
      case (YamlString(tableName), columnsYaml) =>
        val columnsMap = columnsYaml.asYamlObject.fields.collect {
          case (YamlString(columnName), statsYaml) =>
            val stats = statsYaml.convertTo[ColumnStats]
            (columnName, stats)
        }.toMap
        (tableName, columnsMap)
    }.toMap
    result
  }
}



// Define a case class to hold column statistics
case class ColumnStats(
    data_type: String,
    distinct_count: Long,
    distribution: String,
    kurtosis: Double,
    max: Double,
    mean: Double,
    min: Double,
    skewness: Double,
    stddev: Double,
    total_count: Long,
    `type`: String
)

// Implementations of missing generator functions

// Normal distribution generator for Double
case class NormalDoubleGenFunc(mean: Double, stdDev: Double,
    mapping: LocationToSeedMapping = null) extends GeneratorFunction {
  override def apply(rowLoc: RowLocation): Any = {
    val r = DataGen.getRandomFor(rowLoc, mapping)
    val value = r.nextGaussian() * stdDev + mean
    value
  }

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    NormalDoubleGenFunc(mean, stdDev, mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalStateException("Value ranges are not supported for normal double yet")
}

// Exponential distribution generator for Double
case class ExponentialDoubleGenFunc(target: Double, stdDev: Double,
    mapping: LocationToSeedMapping = null) extends GeneratorFunction {
  override def apply(rowLoc: RowLocation): Any = {
    val r = DataGen.getRandomFor(rowLoc, mapping)
    val g = r.nextDouble()
    val logged = math.log(1.0 - g)
    val adjusted = logged * stdDev
    val value = adjusted + target
    value
  }

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    ExponentialDoubleGenFunc(target, stdDev, mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalStateException("Value ranges are not supported for exponential double yet")
}
