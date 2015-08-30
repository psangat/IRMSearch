package edu.monash.IRM.Batch

import edu.monash.IRM.Common.Utils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Prajwol Sangat on 16/08/15.
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

object BatchSearch {

  def main_old(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: BatchSearch <master> <input_file> <query> <output_directory>")
      System.exit(1)
    }
    val startTime = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName(this.getClass.getCanonicalName)
      .setMaster(args(0))
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //val inputFile = "file:///home/sparkusr/datafiles/pird.csv"
    val data = sc.textFile(args(1))
    val dataWithoutHeader = Utils.dropHeader(data)
      .map(_.split(",")
      .map(_.trim))
      .map(p => PIRD(
      Utils.convertToDouble(p(0)),
      Utils.convertToDouble(p(1)),
      Utils.convertToInt(p(2)),
      Utils.convertToDouble(p(3)),
      Utils.convertToDouble(p(4)),
      Utils.convertToDouble(p(5)),
      Utils.convertToInt(p(6)),
      Utils.convertToDouble(p(7)),
      Utils.convertToDouble(p(8)),
      Utils.convertToDouble(p(9)),
      Utils.convertToDouble(p(10)),
      Utils.convertToDouble(p(11)),
      Utils.convertToDouble(p(12)),
      Utils.convertToDouble(p(13)),
      Utils.convertToDouble(p(14)),
      Utils.convertToDouble(p(15)),
      Utils.convertToDouble(p(16)),
      Utils.convertToDouble(p(17)),
      Utils.convertToDouble(p(18))
    )
      ).toDF()
    dataWithoutHeader.registerTempTable("pird")
    //val output = sqlContext.sql(args(0))
    sqlContext.sql(args(2)).map(rdd => rdd).saveAsObjectFile(args(3))
    sc.stop()
    val endTime = System.currentTimeMillis()

    println("Total time taken: " + ((endTime - startTime) / 1000D) + " Secs")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(this.getClass.getCanonicalName)
      .setMaster("local[2]")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.json("file:///home/sparkusr/output/part*").printSchema()
    //data.foreach(println)
  }

}
