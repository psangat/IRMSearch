import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

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
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    // val gpsLookUpTable = gpsLookUp(sc)
    // val timeLookUpTable = loadGPSCL(sc)
    //val lines = sc.textFile("hdfs://localhost:9000/localdir/")
    // lines.take(10).foreach(println)
    streamProcessing(conf)

  }

  def streamProcessing(conf: SparkConf): Unit = {
    try {

      //val ssc = new StreamingContext(conf, Seconds(50))
      //val lines = ssc.textFileStream("hdfs://localhost:9000/localdir/")
      val sc = new SparkContext(conf)
      val lines = sc.textFile("file:///home/sparkusr/datafiles/542_20150617_2131_MLDD_ALLOUTE.CSV")
      val header = lines.first().split(",")
      val rowsWithoutHeader = IRMUtils.dropHeader(lines)
      val valuesInEachRow = rowsWithoutHeader.map(_.split(",")).take(1) //.collect()
      println(header.length)
      //println(valuesInEachRow.length)
      /*var json = "{ "
      for (i <- 0 until header.length) {
        for (j <- 0 until valuesInEachRow(0).length) {
          if (i == j) {
            //println("Value of i %i, j %i: ", i, j)
            json += "\"" + header(i) + "\": \"" + valuesInEachRow(i) + "\""
            //else json += "\"" + header(i) + "\" : \"" + valuesInEachRow(i) + "\","
          }
        }


      }
      json += " }"

      print(json)*/
      valuesInEachRow.foreach { rows => (header,rows)
         .zipped
         .foreach((x,y) => ( x + ":" + y)
         //println(x,y)
         )}


      /*for(var i = 0; i < header.length; i++) {
      (i + 1) == key.length ? json += "\"" + header(i) + "\" : \"" + value[i] + "\"" : json += "\"" + key[i] + "\" : \"" + value[i] + "\",";
    }
    json += " }";*/


      // header.foreach(print)

      //lines.
      //JSONObject obj = new JSONObject(lines)
      //val wordCounts = lines.flatMap(_.split(" ")).countByValue()

      //lines.print()
      //ssc.start()
      //ssc.awaitTermination()
    }
    catch {
      case e: Exception => e.printStackTrace()
        println("Error Happened")
    }
    finally {

    }
  }

  def gpsLookUp(sc: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val allRows = sc.textFile("file:///home/sparkusr/datafiles/GPS_Lookup_Table.csv")
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
      p(7))).toDF().persist(StorageLevel.MEMORY_AND_DISK)
    return gpsLookUpTable
    //gpsLookUpTable.registerTempTable("GPSLookUpTable")
    //dataWithoutHeader.show()
  }

  def timeLookUp(sc: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val allRows = sc.textFile("file:///home/sparkusr/datafiles/542_20150617_2131_gpsCL.CSV")
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
    )).toDF().persist(StorageLevel.MEMORY_AND_DISK)

    return timeLookUpTable
    //timeLookUpTable.registerTempTable("TimeLookUpTable")
  }


}
