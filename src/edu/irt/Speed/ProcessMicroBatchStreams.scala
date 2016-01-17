package edu.irt.Speed

import java.io.FileReader
import java.util.{ArrayList, Scanner}

import com.mongodb.casbah.Imports._
import com.mongodb.util.JSON
import edu.irt.Common.GeoHash._
import edu.irt.Common.{ClosestLocation, Constants, GeoHash}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

import scala.collection.JavaConversions._
import scala.collection.immutable.TreeMap

/**
 * Created by psangat on 18/10/15.
 */
object ProcessMicroBatchStreams {

  val EARTH_RADIUS = 6371
  val records = Array[String]()

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: ProcessMicroBatchStreams <master> <input_directory> <file_path_to_GPS_LookUp_Table>")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName(this.getClass.getCanonicalName)
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val treeMap = loadMappingTree(args(2))
    val broadcastTable = sc.broadcast(treeMap)
    // "hdfs://master:9000/inputDirectory/Dir1/"
    ssc.textFileStream(args(1))
      .foreachRDD { rdd =>
      if (!rdd.partitions.isEmpty) {
        rdd.repartition(14)
          .foreachPartition {
          partition =>
            val client = MongoConnection(Constants.HOST_NAME)
            val mongoColl = client(Constants.DB_NAME)(Constants.COLLECTION_NAME)
            partition.foreach {
              row =>
                val items = row.split("\n")
                items.foreach { item =>
                  if (!item.isEmpty()) {
                    val jsonObject = new JSONObject(item)
                    val latitude = jsonObject.getDouble(Constants.LATITUDE)
                    val longitude = jsonObject.getDouble(Constants.LONGITUDE)
                    val listCloseLatLon = new ArrayList[ClosestLocation]()
                    var srefDistance = 1000.0
                    val StartLatitudeLongitude = latitude + "," + longitude
                    val geoCode = encode(latitude, longitude, 8)
                    if (broadcastTable.value.contains(geoCode)) {
                      broadcastTable.value.get(geoCode).get.foreach {
                        _closestLocation =>
                          val sLatLon2 = _closestLocation.lat + "," + _closestLocation.lon
                          val distance = CalculateDistance(StartLatitudeLongitude,
                            sLatLon2)
                          if (distance < srefDistance) {
                            srefDistance = distance
                            listCloseLatLon.add(0, _closestLocation)
                          }
                      }
                      jsonObject.put(Constants.TRACK_KM, listCloseLatLon.get(0).trackKM)
                      jsonObject.put(Constants.TRACK_NAME, listCloseLatLon.get(0).trackName)
                      val record = JSON.parse(jsonObject.toString()).asInstanceOf[DBObject]
                      mongoColl.insert(record)
                    }
                    else {
                      jsonObject.put(Constants.TRACK_KM, "NULL")
                      jsonObject.put(Constants.TRACK_NAME, "NULL")
                      val record = JSON.parse(jsonObject.toString()).asInstanceOf[DBObject]
                      mongoColl.insert(record)
                    }
                  }
                }
            }
            client.close()
        }
      }
    }
    sys.addShutdownHook {
      ssc.stop(true, true)
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def CalculateDistance(LatLon1: String, LatLon2: String): Double = {
    val lat1 = (LatLon1.split(",")(0)).toDouble
    val lon1 = (LatLon1.split(",")(1)).toDouble
    val lat2 = (LatLon2.split(",")(0)).toDouble
    val lon2 = (LatLon2.split(",")(1)).toDouble
    val latDistance = (lat2 - lat1).toRadians
    val lonDistance = (lon2 - lon1).toRadians
    val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(lat1.toRadians) * Math.cos(lat2.toRadians) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val distance = EARTH_RADIUS * c
    return distance
  }

  def loadMappingTree(filePath: String): TreeMap[String, ArrayList[ClosestLocation]] = {
    var treeMap = TreeMap.empty[String, ArrayList[ClosestLocation]]
    val _scanner = new Scanner(new FileReader(filePath))
    _scanner.nextLine()
    while (_scanner.hasNextLine()) {
      // process the line.
      // Number	Lat	Lon	TrackKM	TrackCode	TrackName	SubTrackCode	SubTrackCode
      val values = _scanner.nextLine().split(",")
      val lat = values(1)
      val lon = values(2)
      val trackKM = values(3)
      val trackCode = values(4)
      val trackName = values(5)
      val subTrackCode = values(6)
      val subTrackName = values(7)
      val geoCode = encode(lat.toDouble, lon.toDouble, 8)
      if (!treeMap.contains(geoCode)) {
        val _arrayList = new ArrayList[ClosestLocation]
        _arrayList.add(ClosestLocation(lat, lon, trackKM, trackCode, trackName, subTrackCode, subTrackName, geoCode))
        treeMap += (geoCode -> _arrayList)
      } else {
        treeMap.get(geoCode).get.add(ClosestLocation(lat, lon, trackKM, trackCode, trackName, subTrackCode, subTrackName, geoCode))
      }
    }
    _scanner.close()
    return treeMap
  }

}
