package edu.monash.IRM.Speed

import com.mongodb.casbah.Imports._
import com.mongodb.util.JSON
import edu.monash.IRM.Common._
import edu.monash.IRM.GeoHash.GeoHash
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json._

/**
 * Created by psangat on 29/10/15.
 */
object ProcessMicroBatchStreams {
  //Logger.getLogger("org").setLevel(Level.WARN)
  //Logger.getLogger("akka").setLevel(Level.WARN)
  val calculateDistance = udf { (lat: String, lon: String) => GeoHash.getDistance(lat.toDouble, lon.toDouble) }
  val DB_NAME = "IRT"
  val COLLECTION_NAME = "sensordata"
  val records = Array[String]()

  def main(args: Array[String]): Unit = {
    if (args.length < 0) {
      System.err.println("Usage: ProcessMicroBatchStreams <master> <input_directory>")
      System.exit(1)
    }
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getCanonicalName)
      .set("spark.hadoop.validateOutputSpecs", "false")
    /*.set("spark.executor.instances", "3")
    .set("spark.executor.memory", "18g")
    .set("spark.executor.cores", "9")
    .set("spark.task.cpus", "1")
    .set("spark.driver.memory", "10g")*/

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(60))
    val sqc = new SQLContext(sc)
    val gpsLookUpTable = MapInput.cacheMappingTables(sc, sqc).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val broadcastTable = sc.broadcast(gpsLookUpTable)

    ssc.textFileStream("hdfs://master:9000/inputDirectory/Dir1/")
      .foreachRDD { rdd =>
      if (!rdd.partitions.isEmpty) {
        rdd.coalesce(2) // using repartion(4) here causes broadcast variable to have no values
          .foreachPartition {
          partition =>

            partition.foreach {
              row =>
                val items = row.split("\n")
                items.foreach { item =>
                  if (!item.isEmpty()) {
                    val jsonObject = new JSONObject(item)
                    /*val latitude = jsonObject.getDouble(Constants.LATITUDE)
                    val longitude = jsonObject.getDouble(Constants.LONGITUDE)
                    val selectedRow = broadcastTable.value
                      .filter("geoCode LIKE '" + GeoHash.subString(latitude, longitude) + "%'")
                      .withColumn("Distance", calculateDistance(col("Lat"), col("Lon")))
                      .orderBy("Distance")
                      .select(Constants.TRACK_KM, Constants.TRACK_NAME).take(1)

                    if (selectedRow.length != 0) {
                      jsonObject.put(Constants.TRACK_KM, selectedRow(0).get(0))
                      jsonObject.put(Constants.TRACK_NAME, selectedRow(0).get(1))
                    }
                    else {
                      jsonObject.put(Constants.TRACK_KM, "NULL")
                      jsonObject.put(Constants.TRACK_NAME, "NULL")
                    }*/
                    jsonObject.put(Constants.TRACK_KM, "NULL")
                    jsonObject.put(Constants.TRACK_NAME, "NULL")
                    val client = MongoClient()
                    val mongoColl = client(DB_NAME)(COLLECTION_NAME)
                    val record = JSON.parse(jsonObject.toString()).asInstanceOf[DBObject]
                    mongoColl.insert(record)
                    client.close()
                  }
                }
            }
        }
      }
    }
    sys.addShutdownHook {
      ssc.stop(true, true)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
