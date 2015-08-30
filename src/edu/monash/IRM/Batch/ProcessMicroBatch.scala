package edu.monash.IRM.Batch

/**
 * Created by Prajwol Sangat on 21/08/15.
 */

import edu.monash.IRM.Common.MapInput
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ProcessMicroBatch {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: ProcessMicroBatch <master> <input_directory>")
      System.exit(1)
    }
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName(this.getClass.getCanonicalName)
      .set("spark.hadoop.validateOutputSpecs", "false")
    //.setJars(Seq(SparkContext.jarOfClass(this.getClass).get))
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(50))
    val sqc = new SQLContext(sc)


    val gpsLookUpTable = MapInput.cacheMappingTables(sc).persist(StorageLevel.MEMORY_AND_DISK_SER)

    ssc.textFileStream(args(1))
      .foreachRDD { rdd =>
      MapInput.mapInputToJSON(rdd, sc, gpsLookUpTable)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
