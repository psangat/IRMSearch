
import org.apache.spark.rdd.RDD
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

    if (args.length < 0) {
      println("Search Query expected as Input")
    }
    else {
      val startTime = System.currentTimeMillis()
      val conf = new SparkConf()
        .setAppName("IRM Search")
        .setMaster("local[*]")
        .set("spark.hadoop.validateOutputSpecs", "false")

      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._
      // val inputFile = sc.textFile("file:///mnt/datafiles/datasource.csv").
      val inputFile = "file:///home/sparkusr/datafiles/pird.csv"
      val data = sc.textFile(inputFile)
      val dataWithoutHeader = IRMUtils.dropHeader(data)
        .map(_.split(",")
        .map(_.trim))
        .map(p => PIRD(
        IRMUtils.convertToDouble(p(0)),
        IRMUtils.convertToDouble(p(1)),
        IRMUtils.convertToInt(p(2)),
        IRMUtils.convertToDouble(p(3)),
        IRMUtils.convertToDouble(p(4)),
        IRMUtils.convertToDouble(p(5)),
        IRMUtils.convertToInt(p(6)),
        IRMUtils.convertToDouble(p(7)),
        IRMUtils.convertToDouble(p(8)),
        IRMUtils.convertToDouble(p(9)),
        IRMUtils.convertToDouble(p(10)),
        IRMUtils.convertToDouble(p(11)),
        IRMUtils.convertToDouble(p(12)),
        IRMUtils.convertToDouble(p(13)),
        IRMUtils.convertToDouble(p(14)),
        IRMUtils.convertToDouble(p(15)),
        IRMUtils.convertToDouble(p(16)),
        IRMUtils.convertToDouble(p(17)),
        IRMUtils.convertToDouble(p(18))
      )
        ).toDF()
      dataWithoutHeader.registerTempTable("pird")
      //val output = sqlContext.sql(args(0))
      val output = sqlContext.sql("select * from pird where track = 2").repartition(10)
      val outputFolder = "file:///home/sparkusr/outputfolder"

      output.map(row => row).saveAsTextFile(outputFolder)
      /*output.foreachPartition {
        partition => partition
          val serverSocket = new ServerSocket(4020)
          serverSocket.setSoTimeout(10000)
          val server = serverSocket.accept()
          val toClient =
            new PrintWriter(server.getOutputStream(), true)
          //val conn = server.accept()
          // val out = new PrintWriter()(conn.getOutputStream)
          partition.foreach(record => toClient.println(record))
          serverSocket.close()
      }*/

      sc.stop()
      val endTime = System.currentTimeMillis()

      println("Total time taken: " + ((endTime - startTime) / 1000D) + " Secs")
    }
  }




}
