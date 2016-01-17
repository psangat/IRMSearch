package edu.irt.Speed

import java.net.ServerSocket
import java.util.ArrayList

import com.mongodb.casbah.Imports._
import com.mongodb.util.JSON
import edu.irt.Common.GeoHash._
import edu.irt.Common.{ClosestLocation, Constants, GeoHash}
import edu.irt.Speed.ProcessMicroBatchStreams._
import org.json.JSONObject

import scala.collection.JavaConversions._
import scala.io._


/**
 * Created by psangat on 17/01/16.
 */


object StreamServer {
  val server = new ServerSocket(Constants.PORT_NO)

  def main(args: Array[String]) {
    System.err.println("Connected to the server ...")
    if (args.length < 1) {
      System.err.println("Usage: StreamServer <file_path_to_GPS_LookUp_Table>")
      System.err.println("Exiting the program ...")
      System.exit(1)
    }
    streamInsert(args(0))
  }

  def streamInsert(filePath: String): Unit = {
    val treeMap = loadMappingTree(filePath)
    val client = MongoConnection(Constants.HOST_NAME)
    val mongoColl = client(Constants.DB_NAME)(Constants.COLLECTION_NAME)
    while (true) {
      val s = server.accept()
      val in = new BufferedSource(s.getInputStream()).getLines()
      in.foreach { item =>
        val jsonObject = new JSONObject(item)
        println(jsonObject.toString())
        val gpsObject = jsonObject.getJSONObject(Constants.GPS)
        if (!gpsObject.toString().equals("{}")) {
          val latitude = gpsObject.getDouble(Constants.LATITUDE)
          val longitude = gpsObject.getDouble(Constants.LONGITUDE)
          val listCloseLatLon = new ArrayList[ClosestLocation]()
          var srefDistance = 1000.0
          val StartLatitudeLongitude = latitude + "," + longitude
          val geoCode = encode(latitude, longitude, 8)
          if (treeMap.contains(geoCode)) {
            treeMap.get(geoCode).get.foreach {
              _closestLocation =>
                val sLatLon2 = _closestLocation.lat + "," + _closestLocation.lon
                val distance = ProcessMicroBatchStreams.CalculateDistance(StartLatitudeLongitude,
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
        else {
          System.err.println("GPS is NULL")
        }
      }
      s.close()
    }
  }
}
