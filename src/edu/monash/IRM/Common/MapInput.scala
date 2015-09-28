package edu.monash.IRM.Common

import java.util.Calendar

import edu.monash.IRM.GeoHash.GeoHash
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by Prajwol Sangat on 21/08/15.
 */

object MapInput {
  val jsonBuilder = new StringBuilder
  val calculateDistance = udf { (lat: String, lon: String) => GeoHash.getDistance(lat.toDouble, lon.toDouble) }
  var lattitude = 0.0
  var longitude = 0.0

  def cacheMappingTables(sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    //file:///home/sparkusr/supportfiles/GeoHashLookUpTable
    val allRows = sc.textFile("hdfs://localhost:9000/supportFiles/GeoHashLookUpTable")
    sqlContext.read.json(allRows).registerTempTable("GeoHashLookUpTable")
    val gpsLookUpTable = sqlContext.sql(" SELECT Lat, Lon, geoCode, TrackKM, TrackCode, TrackName, SubTrackCode, SubTrackName  FROM GeoHashLookUpTable ")
    return gpsLookUpTable
  }

  def mapInputToJSON(rdd: RDD[String], sc: SparkContext, gpsLookUpTable: DataFrame = null): Unit = {

    val header = rdd.first().split(",")
    val rowsWithoutHeader = Utils.dropHeader(rdd).collect()
    //"local[*]" "hdfs://localhost:9000/localdir"
    jsonBuilder.append("[")

    for (i <- 0 until rowsWithoutHeader.length) {
      jsonBuilder.append("{")
      val singleRowArray = rowsWithoutHeader(i).split(",")
      var obje = (header, singleRowArray).zipped
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

            val row = gpsLookUpTable.filter("geoCode LIKE '" + GeoHash.subString(lattitude, longitude) + "%'")
              .withColumn("Distance", calculateDistance(col("Lat"), col("Lon")))
              .orderBy("Distance")
              .select("TrackKM", "TrackName").take(1)
            if (row.length != 0) {
              jsonBuilder.append(convertToStringBasedOnDataType("TrackKm", row(0).get(0)))
              jsonBuilder.append(convertToStringBasedOnDataType("TrackName", row(0).get(1)))
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
    val destPath = new Path("file:///mnt/outputFiles/" + today)
    //val destPath = new Path("file:///home/sparkusr/dataLake/" + today)
    if (hdfs.exists(srcPath)) {
      println("=========================Saving to the Local System===================================")
      hdfs.copyToLocalFile(false, srcPath, destPath, true)
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
