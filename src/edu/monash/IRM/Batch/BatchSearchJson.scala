package edu.monash.IRM.Batch

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sparkusr on 28/08/15.
 */
object BatchSearchJson {
  def main(args: Array[String]) {
    if (args.length < 0) {
      System.err.println("Usage: BatchSearchJson <query> ")
      System.exit(1)
    }
    val startTime = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName(this.getClass.getCanonicalName)
      .setMaster("local[*]")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val stringRDD = sc.textFile("file:///home/sparkusr/searchDrive/*")

    sqlContext.read.json(stringRDD).registerTempTable("testJson")
    sqlContext.sql(args(0)).toDF().show()
  }


}
