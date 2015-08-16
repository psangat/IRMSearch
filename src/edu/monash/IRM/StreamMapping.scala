package edu.monash.IRM

import java.io.StringReader
import java.text.DecimalFormat
import java.util.regex.Pattern

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Prajwol Sangat on 16/08/15.
 */

case class TimeLookUpTable
(
  SomatTime: Double,
  UTCMonth: Double,
  UTCDay: Double,
  UTCHour: Double,
  UTCMinute: Double,
  UTCSecond: Double
  )

object StreamMapping {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("IRM Stream Mapping")
      .setMaster("local[*]")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    streamProcessing(sc)
  }

  /*
   * This function looks for gpsCL files in streams and maps corresponding raw data files
   * to create a jSon output.
   *
   * */
  def streamProcessing(sc: SparkContext): Unit = {
    //"file:///home/sparkusr/supportfiles/542_20150617_2131_gpsCL.CSV"
    val pattern = Pattern.compile("[0-9]{1,4}_(.*?)_(.*?)_gpsCL.CSV")
    val scc = new StreamingContext(sc, Seconds(20))
    scc.textFileStream("hdfs://localhost:9000/localdir/").foreachRDD {
      singleRDD =>
        val debugString = singleRDD.toDebugString
        val matcher = pattern.matcher(debugString)
        if (matcher.find()) {
          val fileName = matcher.group().substring(0, matcher.group().lastIndexOf("gpsCL"))
          val sqlContext = new SQLContext(sc)
          import sqlContext.implicits._
          val csv1 = singleRDD.map { line => val reader = new CSVReader(new StringReader(line))
            reader.readNext()
          }
          val timeLookUpTable = csv1.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
            .map { row => (row(0), row(10), row(11), row(12), row(13), row(14))}
            .map(row => row.toString().replace("(", "").replace(")", ""))
            .map(_.split(",")
            .map(_.trim))
            .map(p => TimeLookUpTable(
            Utils.convertToDouble(p(0)),
            Utils.convertToDouble(p(1)),
            Utils.convertToDouble(p(2)),
            Utils.convertToDouble(p(3)),
            Utils.convertToDouble(p(4)),
            Utils.convertToDouble(p(5))
          )).toDF()


          val sb = new StringBuilder
          var lattitude = 0.0
          val lines = sc.textFile("hdfs://localhost:9000/localdir/" + fileName + "MLDD_ALLOUTE.CSV")
          val header = lines.first().split(",")
          val rowsWithoutHeader = Utils.dropHeader(lines).collect()
          val df = new DecimalFormat("###")
          sb.append("{" + "\"" + "data\"" + ":[{")
          //var json = "{" + "\"" + "data\"" + ":[{"
          for (i <- 0 until rowsWithoutHeader.length) {
            sb.append("\"" + i + "\"" + ":{" + "")
            val singleRowArray = rowsWithoutHeader(i).split(",")
            (header, singleRowArray).zipped
              .foreach { (x, y) =>
              sb.append("\"" + x + "\": \"" + y.trim + "\",")
              if (x.equals("SomatTime")) {
                val rowTimeLookUp = timeLookUpTable.filter("SomatTime = " + y.trim)
                  .select("UTCMonth", "UTCDay", "UTCHour", "UTCMinute", "UTCSecond").first()
                if (rowTimeLookUp.size > 0) {
                  val formattedRow = (df.format(rowTimeLookUp(0))
                    + "-" + df.format(rowTimeLookUp(1))
                    + " " + df.format(rowTimeLookUp(2))
                    + ":" + df.format(rowTimeLookUp(3))
                    + ":" + df.format(rowTimeLookUp(4))) //.foreach(print)
                  sb.append("\"" + "UTCTime" + "\": \"" + formattedRow + "\",")
                }
              }
              else if (x.equals("GPSLat")) {
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
            sb.setLength(sb.length - 1)
            sb.append("},")
          }
          sb.setLength(sb.length - 1)
          sb.append("}]}")
          print(sb.toString)
        }
    }

    scc.start()
    scc.awaitTermination()

  }

}
