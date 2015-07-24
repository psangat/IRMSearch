import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sparkusr on 20/07/15.
 */
case class PIRD(
                 time: Double,
                 speed: Double,
                 loadEmpty: Int,
                 km: Double,
                 lat: Double,
                 lon: Double,
                 track: Int,
                 snd1: Double,
                 snd2: Double,
                 snd3: Double,
                 snd4: Double,
                 couplerForce: Double,
                 lateralAccelerometer: Double,
                 accLeft: Double,
                 accRight: Double,
                 bounceFront: Double,
                 bounceRear: Double,
                 rockFront: Double,
                 rockRear: Double
                 )

object IRMRangeSearch {

  def main(args: Array[String]) {
    if (args.length  < 0 ) {
      println("Search Query expected as Input")
    }
    else {
      val startTime = System.currentTimeMillis()
      val conf = new SparkConf()
        .setAppName("IRM Search")
        .setMaster("local[*]")

      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val pird = sc.textFile("file:///mnt/datafiles/datasource.csv")
        .map(_.split(",")
        .map(_.trim))
        //.mapPartitionsWithIndex { (index, rows) => if (index == 0) rows.drop(1) else rows }
        .map(p => PIRD(
        convertToDouble(p(0)),
        convertToDouble(p(1)),
        convertToInt(p(2)),
        convertToDouble(p(3)),
        convertToDouble(p(4)),
        convertToDouble(p(5)),
        convertToInt(p(6)),
        convertToDouble(p(7)),
        convertToDouble(p(8)),
        convertToDouble(p(9)),
        convertToDouble(p(10)),
        convertToDouble(p(11)),
        convertToDouble(p(12)),
        convertToDouble(p(13)),
        convertToDouble(p(14)),
        convertToDouble(p(15)),
        convertToDouble(p(16)),
        convertToDouble(p(17)),
        convertToDouble(p(18))
      )
        ).toDF()
      pird.registerTempTable("pird")
      val output = sqlContext.sql(args(0))
      output.foreach(println)
      sc.stop()
      val endTime = System.currentTimeMillis()

      println("Total time taken: " + ((endTime - startTime) / 1000D) + " Secs")
    }
  }

  def convertToDouble(string: String): Double = {
    return string.trim.toDouble
  }

  def convertToInt(string: String): Int = {
    return string.trim.toInt
  }
}
