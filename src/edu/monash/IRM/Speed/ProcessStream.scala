package edu.monash.IRM.Speed

/**
 * Created by Prajwol Sangat on 21/08/15.
 */

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ProcessStream {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: ProcessStream <master> <hostname> <port>")
      System.exit(1)
    }

    //Configure the Streaming Context
    val sparkConf = new SparkConf()
      .setMaster(args(0))
      .setAppName(this.getClass.getCanonicalName)

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint(".")
    // Create the DStream from data sent over the network
    val dStream = ssc.socketTextStream(args(1), args(2).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    // Will the network stream have headers??
    // No right, its http not ftp.
    // then how to produce json key value??
    //dStream.transform(rdd => MapInput.mapInputToJSON(rdd))
  }

}
