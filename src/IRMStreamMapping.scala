import java.io.StringReader
import java.text.DecimalFormat

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

/**
 * Created by sparkusr on 31/07/15.
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

case class TimeLookUpTable
(

  SomatTime: Double,
  UTCMonth: Double,
  UTCDay: Double,
  UTCHour: Double,
  UTCMinute: Double,
  UTCSecond: Double
  )

object IRMStreamMapping {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("IRM Stream Mapping")
      .setMaster("local[*]")
      .set("spark.hadoop.validateOutputSpecs", "false")
      //.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    val gpsLookUpTable = gpsLookUp(sc).persist(StorageLevel.MEMORY_AND_DISK)
    val timeLookUpTable = timeLookUp(sc).persist(StorageLevel.MEMORY_AND_DISK)
    //val lines = sc.textFile("hdfs://localhost:9000/localdir/")
    //val lines = sc.textFile("file:///home/sparkusr/datafiles/542_20150617_2131_MLDD_ALLOUTE.CSV")

    val ssc = new StreamingContext(sc, Seconds(30))
    ssc.textFileStream("hdfs://localhost:9000/localdir/")

      .foreachRDD {
      singleRDD =>
        val header = singleRDD.first().split(",")
        val rowsWithoutHeader = IRMUtils.dropHeader(singleRDD).collect()
        val sqlContext = new SQLContext(sc)
        val df = new DecimalFormat("###")
        // start creating JSON

        var json = "{" + "\"" + "data\"" + ":[{"
        for (i <- 0 until 3 /*rowsWithoutHeader.length*/ ) {
          var lattitude = ""
          json += "\"" + i + "\"" + ":{" + ""
          val singleRowArray = rowsWithoutHeader(i).split(",")
          (header, singleRowArray).zipped
            .foreach { (x, y) =>
            json += ("\"" + x + "\": \"" + y.trim + "\",")
            if (x.equals("SomatTime")) {
              val rowTimeLookUp = timeLookUpTable.filter("SomatTime = " + y.trim)
                .select("UTCMonth", "UTCDay", "UTCHour", "UTCMinute", "UTCSecond").first()
              if (rowTimeLookUp.size > 0) {
                val formattedRow = (df.format(rowTimeLookUp(0))
                  + "-" + df.format(rowTimeLookUp(1))
                  + " " + df.format(rowTimeLookUp(2))
                  + ":" + df.format(rowTimeLookUp(3))
                  + ":" + df.format(rowTimeLookUp(4))) //.foreach(print)
                json += ("\"" + "UTCTime" + "\": \"" + formattedRow + "\",")
              }
            }
            else if (x.equals("GPSLat")) {
              lattitude = y.trim
            }
            else if (x.equals("GPSLon")) {
              /* val rowGPSLookUp = gpsLookUpTable.filter("Lat = " + lattitude + "AND Lon = " + y.trim)
           .select("TrackKM","TrackCode","TrackName").first()//.foreach(println)
           //val formattedRow =
           if (rowGPSLookUp.size > 0) {
             json += ("\"" + "TrackKM" + "\": \"" + rowGPSLookUp(0) + "\",")
             json += ("\"" + "TrackName" + "\": \"" + rowGPSLookUp(2) + "\",")
           }
           */

            }
          }
          json = json.substring(0, json.lastIndexOf(","))
          json += "},"
        }
        json = json.substring(0, json.lastIndexOf(","))
        json += "}]}"
        // end creating JSON
        print(json)

    }
    ssc.start()
    ssc.awaitTermination()
  }

  def streamProcessing(conf: SparkConf, gpsLookUpTable: DataFrame, timeLookUpTable: DataFrame): Unit = {
    val sc = new SparkContext(conf)
    try {

      //val ssc = new StreamingContext(conf, Seconds(50))
      //val lines = ssc.textFileStream("hdfs://localhost:9000/localdir/")

      val lines = sc.textFile("file:///home/sparkusr/datafiles/542_20150617_2131_MLDD_ALLOUTE.CSV")
      val header = lines.first().split(",")
      val rowsWithoutHeader = IRMUtils.dropHeader(lines).collect()
      gpsLookUpTable.show()
      timeLookUpTable.show()
      // val valuesInEachRow = rowsWithoutHeader.map(_.split(",")).take(100)//.collect()

      val sqlContext = new SQLContext(sc)

      //gpsLookUpTable.show()
      //timeLookUpTable.show()
      gpsLookUpTable.registerTempTable("GPSLookUpTable")
      timeLookUpTable.registerTempTable("TimeLookUpTable")
      var json = "{" + "\"" + "data\"" + ":[{"
      for (i <- 0 until rowsWithoutHeader.length) {
        json += "\"" + i + "\"" + ":{" + ""
        val singleRowArray = rowsWithoutHeader(i).split(",")
        (header, singleRowArray).zipped.foreach { (x, y) =>
          json += ("\"" + x + "\": \"" + y.trim + "\",")
          if (x.equals("SomatTime")) {
            val output = sqlContext.sql("select UTCMonth,UTCDay,UTCHour,UTCMinute,UTCSecond  from TimeLookUpTable where SomatTime = " + y.trim)
            output.foreach(println)
          }
        }
        json = json.substring(0, json.lastIndexOf(","))
        json += "},"
      }
      json = json.substring(0, json.lastIndexOf(","))
      json += "}]}"
      //rowsWithoutHeader.foreach {
      //json += "{" + ""
      // rows => rows.split(",").foreach { row => (header, row).zipped.foreach((x, y) => print(x, y))
      //.zipped
      //.foreach((x, y) => json += ("\"" + x + "\": \"" + y + "\",")
      //println(x,y)
      //)
      //}
      // }
      /* valuesInEachRow.foreach { rows => (header, rows)
        .zipped
        .foreach((x, y) => json += ("\"" + x + "\": \"" + y.trim + "\",")
          //println(x,y)
        )
      }*/
      //json = json.substring(0, json.lastIndexOf(","))
      //json += " }]}"

      print(json)


      //lines.print()
      //ssc.start()
      //ssc.awaitTermination()
      //}

    }
    catch {
      case e: Exception => e.printStackTrace()
        println("Error Happened")
    }
    finally {
      sc.stop()
    }
  }

  def gpsLookUp(sc: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val allRows = sc.textFile("file:///home/sparkusr/supportfiles/GPS_Lookup_Table.csv")
    val gpsLookUpTable = IRMUtils.dropHeader(allRows)
      .map(_.split(",")
      .map(_.trim))
      .map(p => GPSLookUpTable(
      IRMUtils.convertToInt(p(0)),
      IRMUtils.convertToDouble(p(1)),
      IRMUtils.convertToDouble(p(2)),
      IRMUtils.convertToDouble(p(3)),
      IRMUtils.convertToInt(p(4)),
      p(5),
      IRMUtils.convertToInt(p(6)),
      p(7))).toDF()

    return gpsLookUpTable
    //gpsLookUpTable.registerTempTable("GPSLookUpTable")
    //dataWithoutHeader.show()
  }

  def timeLookUp(sc: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val allRows = sc.textFile("file:///home/sparkusr/supportfiles/542_20150617_2131_gpsCL.CSV")
    val csv1 = allRows.map { line => val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    }
    val timeLookUpTable = csv1.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map { row => (row(0), row(10), row(11), row(12), row(13), row(14))
    }
      .map(row => row.toString().replace("(", "").replace(")", ""))
      .map(_.split(",")
      .map(_.trim))
      .map(p => TimeLookUpTable(
      IRMUtils.convertToDouble(p(0)),
      IRMUtils.convertToDouble(p(1)),
      IRMUtils.convertToDouble(p(2)),
      IRMUtils.convertToDouble(p(3)),
      IRMUtils.convertToDouble(p(4)),
      IRMUtils.convertToDouble(p(5))
    )).toDF()


    return timeLookUpTable
  }


}
