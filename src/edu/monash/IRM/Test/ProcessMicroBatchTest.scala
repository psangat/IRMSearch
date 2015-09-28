package edu.monash.IRM.Batch

/**
 * Created by Prajwol Sangat on 21/08/15.
 */

import edu.monash.IRM.Common.{MapInput, Utils}
import edu.monash.IRM.GeoHash.GeoHash
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ProcessMicroBatchTest {
  val jsonBuilder = new StringBuilder
  val calculateDistance = udf { (lat: String, lon: String) => GeoHash.getDistance(lat.toDouble, lon.toDouble) }
  var lattitude = 0.0
  var longitude = 0.0

  def main(args: Array[String]): Unit = {
    if (args.length < 0) {
      System.err.println("Usage: ProcessMicroBatch <master> <input_directory>")
      System.exit(1)
    }
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.executor.instances", "3")
      .set("spark.executor.memory", "1g")
      .set("spark.executor.cores", "1")
      .set("spark.task.cpus", "1")
      .set("spark.driver.memory", "1g")


    //.setJars(Seq(SparkContext.jarOfClass(this.getClass).get))
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(800))
    val sqc = new SQLContext(sc)


    val gpsLookUpTable = MapInput.cacheMappingTables(sc, sqc).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val broadcastTable = sc.broadcast(gpsLookUpTable)
    val rdd = sc.textFile("file:///home/sparkusr/filesDump/tf34_20150727_0503_MLPA_ALLOUTL.CSV")
    if (!rdd.partitions.isEmpty) {
      val header = rdd.first().split(",")
      val rowsWithoutHeader = Utils.dropHeader(rdd).repartition((rdd.count() / 3).toInt)
      jsonBuilder.append("[")
      rowsWithoutHeader.foreachPartition { partition =>
        partition.foreach {
          row =>
            println("----------------------Row--------------------------------------")
            println(row)
            println("------------------------Row------------------------------------")
            jsonBuilder.append("{")
            println("{")
            val singleRowArray = row.split(",")
            (header, singleRowArray).zipped
              .foreach { (x, y) => println(x, y)
              jsonBuilder.append(convertToStringBasedOnDataType(x, y))
              if (x.equals("GPSLat")) {
                lattitude = y.toDouble
              }
              else if (x.equals("GPSLon")) {
                longitude = y.toDouble

                val selectedRow = broadcastTable.value
                  .filter("geoCode LIKE '" + GeoHash.subString(lattitude, longitude) + "%'")
                  .withColumn("Distance", calculateDistance(col("Lat"), col("Lon")))
                  .orderBy("Distance")
                  .select("TrackKM", "TrackName").take(1)
                if (selectedRow.length != 0) {
                  jsonBuilder.append(convertToStringBasedOnDataType("TrackKm", selectedRow(0).get(0)))
                  jsonBuilder.append(convertToStringBasedOnDataType("TrackName", selectedRow(0).get(1)))
                }
                else {
                  jsonBuilder.append(convertToStringBasedOnDataType("TrackKm", "NULL"))
                  jsonBuilder.append(convertToStringBasedOnDataType("TrackName", "NULL"))
                }

              }
            }
            println("}")
            jsonBuilder.setLength(jsonBuilder.length - 1)
            jsonBuilder.append("},")
        }
      }
      jsonBuilder.setLength(jsonBuilder.length - 1)
      jsonBuilder.append("]")

      println(jsonBuilder.toString())
      sc.parallelize(Seq(jsonBuilder.toString)).saveAsTextFile("file:///home/sparkusr/outputDirectory")
    }
  }

  def convertToStringBasedOnDataType(x: String, obj: Any): String = {
    if (obj.isInstanceOf[String] || obj.isInstanceOf[Char]) {
      return "\"" + x + "\": \"" + obj.toString.trim + "\","
    }
    else {
      return "\"" + x + "\": " + obj.toString.trim + ","

    }
  }


}


