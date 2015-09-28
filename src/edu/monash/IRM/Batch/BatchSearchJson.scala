package edu.monash.IRM.Batch

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sparkusr on 28/08/15.
 */
object BatchSearchJson {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: BatchSearchJson <input files> <query> <master>")
      System.exit(1)
    }
    //val to_day = Calendar.getInstance().getTime
    //val startTime = to_day.getTime
    val startTime = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName(this.getClass.getCanonicalName)
      .setMaster(args(2))
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.executor.instances", "3")
      .set("spark.executor.memory", "10g")
      .set("spark.executor.cores", "5")
      .set("spark.task.cpus", "1")
      .set("spark.driver.memory", "10g")
    //"select * from testJson where TrackKm = 'NULL'"
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //"file:///home/sparkusr/dataLake/*"
    val stringRDD = sc.textFile(args(0)).cache() //.persist(StorageLevel.MEMORY_ONLY)

    sqlContext.read.json(stringRDD).registerTempTable("testJson")
    sqlContext.sql(args(1)).show()
    val endTime = System.currentTimeMillis()
    println("===================Time Taken================== ")
    println()
    println(TimeUnit.MILLISECONDS.toSeconds(endTime - startTime) + " Seconds")
    println()
    println("================================================ ")
  }


}
