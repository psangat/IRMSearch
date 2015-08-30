package edu.monash.IRM.Common

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by Prajwol Sangat on 21/08/15.
 */

case class GPSLookUpTable
(
  Number: Int,
  Lat: Double,
  Lon: Double,
  TrackKM: Double,
  TrackCode: Int,
  TrackName: String,
  SubTrackCode: Int,
  SubTrackName: String
  )


object MapInput {
  val jsonBuilder = new StringBuilder
  var lattitude = 0.0


  def cacheMappingTables(sc: SparkContext): DataFrame = {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val allRows = sc.textFile("file:///home/sparkusr/supportfiles/GPS_Lookup_Table.csv")
    val gpsLookUpTable = Utils.dropHeader(allRows)
      .map(_.split(",")
      .map(_.trim))
      .map(p => GPSLookUpTable(
      Utils.convertToInt(p(0)),
      Utils.convertToDouble(p(1)),
      Utils.convertToDouble(p(2)),
      Utils.convertToDouble(p(3)),
      Utils.convertToInt(p(4)),
      p(5),
      Utils.convertToInt(p(6)),
      p(7))).toDF()

    return gpsLookUpTable
  }

  def convertToStringBasedOnDataType(x: String, obj: Any): String = {
    if (obj.isInstanceOf[String] || obj.isInstanceOf[Char]) {
      return "\"" + x + "\": \"" + obj.toString.trim + "\","
    }
    else {
      return "\"" + x + "\": " + obj.toString.trim + ","

    }
  }

  def mapInputToJSON(rdd: RDD[String], sc: SparkContext, gPSLookUpTable: DataFrame): Unit = {

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
        if (x.equals("GPSLat")) {
          lattitude = y.toDouble
        }
        else if (x.equals("GPSLon")) {
          /* val rowGPSLookUp = gpsLookUpTable.filter("Lat = " + lattitude + "AND Lon = " + y.toDouble).select("TrackKM", "TrackCode", "TrackName").first()
           if (rowGPSLookUp.size > 0) {
             sb.append ("\"" + "TrackKM" + "\": \"" + rowGPSLookUp(0) + "\",")
             sb.append ("\"" + "TrackName" + "\": \"" + rowGPSLookUp(2) + "\",")
           }*/
        }
      }
      jsonBuilder.setLength(jsonBuilder.length - 1)
      jsonBuilder.append("},")
    }
    jsonBuilder.setLength(jsonBuilder.length - 1)
    jsonBuilder.append("]")
    sc.parallelize(Seq(jsonBuilder.toString)).repartition(1).saveAsTextFile("file:///home/sparkusr/outputFiles/")
  }

}
