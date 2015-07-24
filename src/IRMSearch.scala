import org.apache.spark.{SparkContext, SparkConf}
//import org.apache.spark.sql.hive.HiveContext
/**
 * Created by sparkusr on 20/07/15.
 */
object IRMSearch {
  def main (args: Array[String]) {
    if (args.length == 5)
      {

        println("Search word expected as Input")
      }
    else {
      val startTime = System.currentTimeMillis()
      val conf = new SparkConf()
        .setAppName("IRM Search")
        .setMaster("local[*]")
        .set("spark.executor.memory","10g")

      val sc = new SparkContext(conf)
     // val hiveCtx = new HiveContext(sc)
      //hiveCtx.sql()
      val rows = sc.textFile("file:///home/sparkusr/datafiles/*")
      val selectedRows1 = rows.filter(_.contains("test2")).cache()
      val selectedRows2 = rows.filter(_.contains("2012-10-15")).cache()
      val intersectedRow = selectedRows1.intersection(selectedRows2).cache()
      val totalCount = intersectedRow.count().toInt
      intersectedRow.take(totalCount).foreach(println)
      sc.stop()
      val endTime = System.currentTimeMillis()

      println("Total time taken: " + ((endTime-startTime) / 1000D) + " Secs")
    }
  }
}
