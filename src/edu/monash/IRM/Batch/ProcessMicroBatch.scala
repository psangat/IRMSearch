package edu.monash.IRM.Batch

/**
 * Created by Prajwol Sangat on 21/08/15.
 */

import java.util.Calendar

import edu.monash.IRM.Common.{MapInput, Utils}
import edu.monash.IRM.GeoHash.GeoHash
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ProcessMicroBatch {
  var jsonBuilder = new StringBuilder
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
    /*.set("spark.executor.instances", "3")
    .set("spark.executor.memory", "18g")
    .set("spark.executor.cores", "9")
    .set("spark.task.cpus", "1")
    .set("spark.driver.memory", "10g")*/

    //.setJars(Seq(SparkContext.jarOfClass(this.getClass).get))
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(25))
    val sqc = new SQLContext(sc)
    val gpsLookUpTable = MapInput.cacheMappingTables(sc, sqc).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val broadcastTable = sc.broadcast(gpsLookUpTable)

    ssc.textFileStream("hdfs://localhost:9000/inputDirectory/")
      .foreachRDD { rdd =>

      if (!rdd.partitions.isEmpty) {
        //MapInput.mapInputToJSON(rdd, sc, gpsLookUpTable)
        jsonBuilder.append("[")
        val header = rdd.first().split(",")
        val rowsWithoutHeader = Utils.dropHeader(rdd).repartition(4)
        //"local[*]" "hdfs://localhost:9000/localdir"

        rowsWithoutHeader.foreachPartition { partition =>
          partition.foreach {

            row =>
              jsonBuilder.append("{")
              val singleRowArray = row.split(",")
              (header, singleRowArray).zipped
                .foreach { (x, y) =>
                jsonBuilder.append(convertToStringBasedOnDataType(x, y))
                // GEO Hash logic here
                if (x.equals("GPSLat") || x.equals("Lat")) {
                  lattitude = y.toDouble
                }
                else if (x.equals("GPSLon") || x.equals("Lon")) {
                  longitude = y.toDouble
                  if (x.equals("Lon")) {
                    // This section is used to convert GPS Look Up to GPS LookUP with Hash
                    jsonBuilder.append(convertToStringBasedOnDataType("geoCode", GeoHash.encode(lattitude, longitude)))
                  }
                  else {
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
              }
              jsonBuilder.setLength(jsonBuilder.length - 1)
              jsonBuilder.append("},")
          }
        }

        jsonBuilder.setLength(jsonBuilder.length - 1)
        jsonBuilder.append("]")
        // Uncomment when you need to create GeoHashLookUpTable From Scratch
        // sc.parallelize(Seq(jsonBuilder.toString)).repartition(1).saveAsTextFile("file:///home/sparkusr/GpsLookUpWithHash/")
        sc.parallelize(Seq(jsonBuilder.toString)).repartition(1).saveAsTextFile("hdfs://localhost:9000/outputDirectory")
        val hadoopconf = new Configuration
        val hdfsURI = "hdfs://localhost:9000"
        val today = Calendar.getInstance().getTime.toGMTString.replaceAll(" ", "_").replaceAll(":", "_")

        hadoopconf.set("fs.defaultFS", hdfsURI)
        val hdfs = FileSystem.get(hadoopconf)
        //Create output stream to HDFS file
        val srcPath = new Path("hdfs://localhost:9000/outputDirectory/part-00000")
        //val destPath = new Path("file:///mnt/outputFiles/" + today)
        val destPath = new Path("file:///home/sparkusr/dataLake/" + today)
        if (hdfs.exists(srcPath)) {
          println("=========================Saving to the Local System===================================")
          hdfs.copyToLocalFile(false, srcPath, destPath, true)
          jsonBuilder.clear()
        }
      }
    }
    sys.addShutdownHook {
      ssc.stop(true, true)
    }

    ssc.start()
    ssc.awaitTermination()
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


