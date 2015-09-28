package edu.monash.IRM.Test

import java.io.File
import java.util.regex.Pattern

import edu.monash.IRM.Common.Utils
import edu.monash.IRM.GeoHash.GeoHash
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sparkusr on 13/09/15.
 */
object ProcessBatch {
  val jsonBuilder = new StringBuilder
  val calculateDistance = udf { (lat: String, lon: String) => GeoHash.getDistance(lat.toDouble, lon.toDouble) }
  var lattitude = 0.0
  var longitude = 0.0

  def main(args: Array[String]) {
    val pattern = Pattern.compile("[0-9]{1,4}_(.*?)_(.*?)_ALLOUT(.*?).CSV")
    //val dirName = "/mnt/AllFiles"
    val dirName = "/home/sparkusr/filesbk"
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)

    val sc = new SparkContext(conf)
    val sqc = new SQLContext(sc)
    val allRows = sc.textFile("file:///home/sparkusr/supportfiles/GeoHashLookUpTable")
    sqc.read.json(allRows).registerTempTable("GeoHashLookUpTable")
    val gpsLookUpTable = sqc.sql(" SELECT Lat, Lon, geoCode, TrackKM, TrackCode, TrackName, SubTrackCode, SubTrackName  FROM GeoHashLookUpTable ").cache()
    val broadCastTable = sc.broadcast(gpsLookUpTable)
    val filesList = new File(dirName).listFiles
    filesList.foreach {
      file =>
        val patternMatcher = pattern.matcher(file.getName)
        if (patternMatcher.find()) {

          val rdd = sc.textFile(file.toString)
          val header = rdd.first().split(",")
          val rowsWithoutHeader = Utils.dropHeader(rdd)
          //"local[*]" "hdfs://localhost:9000/localdir"
          jsonBuilder.append("[")
          rowsWithoutHeader.foreach { row =>
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
                  val selectedRow = broadCastTable.value
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
          jsonBuilder.setLength(jsonBuilder.length - 1)
          jsonBuilder.append("]")
          // Uncomment when you need to create GeoHashLookUpTable From Scratch
          // sc.parallelize(Seq(jsonBuilder.toString)).repartition(1).saveAsTextFile("file:///home/sparkusr/GpsLookUpWithHash/")
          sc.parallelize(Seq(jsonBuilder.toString)).repartition(1).saveAsTextFile("file:///home/sparkusr/outputDirectory")
        }
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
