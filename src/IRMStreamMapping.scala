import java.io.StringReader
import java.text.DecimalFormat

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
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
  //var df = null
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

    val ssc = new StreamingContext(sc, Seconds(700))
    var counter = 1
    var lattitude = 0.0
    val sb = new StringBuilder
    ssc.textFileStream("hdfs://localhost:9000/localdir/")
      .foreachRDD {
      singleRDD =>
        val header = singleRDD.first().split(",")
        val rowsWithoutHeader = IRMUtils.dropHeader(singleRDD).collect()
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
          //json = json.substring(0, json.lastIndexOf(","))
          sb.append("},")
        }
        sb.setLength(sb.length - 1)
        sb.append("}]}")
        print(sb.toString)
        //sc.parallelize(json).repartition(1).saveAsTextFile("hdfs://localhost:9000/output/" + "TestOutput" + counter)
        //sc.parallelize(json).repartition(1).saveAsTextFile("file:///home/ubuntu/outputfolder/" + "TestOutput" + counter)
        //sc.parallelize(sb.toString).repartition(1).saveAsTextFile("file:///home/sparkusr/outputfolder/" + "TestOutput" + counter)

        counter = counter + 1
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def gpsLookUp(sc: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //val allRows = sc.textFile("file:///home/sparkusr/supportfiles/GPS_Lookup_Table.csv")
    val allRows = sc.textFile("file:///home/ubuntu/filesDump/GPS_Lookup_Table.csv")
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
    //val allRows = sc.textFile("file:///home/sparkusr/supportfiles/542_20150617_2131_gpsCL.CSV")
    val allRows = sc.textFile("file:///home/ubuntu/filesDump/542_20150617_2131_gpsCL.CSV")

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
