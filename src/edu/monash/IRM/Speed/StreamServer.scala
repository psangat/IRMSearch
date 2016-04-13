package edu.monash.IRM.Speed

/**
 * Created by psangat on 14/12/15.
 */

import java.io._
import java.net._
import java.util.ArrayList

import com.mongodb.casbah.Imports._
import com.mongodb.util.JSON
import edu.monash.IRM.Common.Constants
import edu.monash.IRM.GeoHash.{ClosestLocation, GeoHash}
import org.json.JSONObject

import scala.collection.JavaConversions._
import scala.io._

object StreamServer {
  val DB_NAME = "IRT"
  val COLLECTION_NAME = "sensordata"
  val server = new ServerSocket(9999)
  val recordsList = new ArrayList[String]()
  val RECORDS_SIZE = 10
  var count = 1

  def main(args: Array[String]) {
    System.err.print("Connected to the server")
    // fileInsert()
    streamInsert()
    System.err.print("Disconnected from the server")

  }

  def streamInsert(): Unit = {
    val treeMap = ProcessMicroBatchStreams.loadMappingTree()
    val client = MongoConnection("localhost")
    val mongoColl = client(DB_NAME)(COLLECTION_NAME)
    while (true) {
      val s = server.accept()
      val in = new BufferedSource(s.getInputStream()).getLines()
      in.foreach { item =>
        val jsonObject = new JSONObject(item)
        println(jsonObject.toString())

        val gpsObject = jsonObject.getJSONObject("gps")
        if (!gpsObject.toString().equals("{}")) {
          val latitude = gpsObject.getDouble(Constants.LATITUDE)
          val longitude = gpsObject.getDouble(Constants.LONGITUDE)
          val listCloseLatLon = new ArrayList[ClosestLocation]()
          var srefDistance = 1000.0
          val StartLatitudeLongitude = latitude + "," + longitude
          val geoCode = GeoHash.encode(latitude, longitude, 8)
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
          System.err.println("gps is NULL")
        }
      }
      s.close()
    }

  }

  def fileInsert(): Unit = {
    while (true) {
      val s = server.accept()
      val in = new BufferedSource(s.getInputStream()).getLines()

      in.foreach { record => recordsList.add(record) }
      if (recordsList.size() >= RECORDS_SIZE) {
        val pw = new PrintWriter(new File("/Users/psangat/Dropbox/IRTTestFiles/OutputTestFiles/SampleFile" + count + ".json"))
        recordsList.foreach { record =>
          pw.println(record)
        }
        pw.close
        recordsList.clear()
        count = count + 1

      }
      s.close()
    }
  }


}
