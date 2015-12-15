package edu.monash.IRM.GeoHash

import java.io._
import java.util._

import scala.collection.JavaConversions._
import scala.collection.immutable.TreeMap
import scala.util.control.Breaks._


/**
 * Created by psangat on 19/11/15.
 */

case class ClosestLocation(lat: String,
                           lon: String,
                           trackKM: String,
                           trackCode: String,
                           trackName: String,
                           subTrackCode: String,
                           subTrackName: String,
                           geoCode: String)

object ClosestLatLon {
  val EARTH_RADIUS = 6371
  val listCloseLatLon = new ArrayList[ClosestLocation]()
  var treeMap = TreeMap.empty[String, ArrayList[ClosestLocation]]
  var dubTreeMap = TreeMap.empty[String, ClosestLocation]

  def main(args: Array[String]) {


    val startTime = System.currentTimeMillis()
    // Locate the nearest points on the database
    val br = new Scanner(
      new FileReader("/Users/psangat/Dropbox/IRTTestFiles/GPS_Lookup_Table.csv"))
    var line = ""
    br.nextLine()
    breakable {
      while (br.hasNextLine()) {
        // process the line.
        // Number	Lat	Lon	TrackKM	TrackCode	TrackName	SubTrackCode	SubTrackCode
        val values = br.nextLine().split(",")
        val id = values(0)
        val lat = values(1)
        val lon = values(2)
        val trackKM = values(3)
        val trackCode = values(4)
        val trackName = values(5)
        val subTrackCode = values(6)
        val subTrackName = values(7)
        val geoCode = GeoHash.encode(lat.toDouble, lon.toDouble, 8)
        val geoCode1 = GeoHash.encode(lat.toDouble, lon.toDouble)
        if (!treeMap.contains(geoCode)) {
          val arraylist = new ArrayList[ClosestLocation]
          arraylist.add(ClosestLocation(lat, lon, trackKM, trackCode, trackName, subTrackCode, subTrackName, geoCode1))
          treeMap += (geoCode -> arraylist)
        } else {
          treeMap.get(geoCode).get.add(ClosestLocation(lat, lon, trackKM, trackCode, trackName, subTrackCode, subTrackName, geoCode1))
        }
        //break
      }
    }
    br.close()
    treeMap.foreach(println)
    val endTime = System.currentTimeMillis()
    println("Time Taken to build Tree & List: " + (((endTime - startTime) / 1000) % 60) + " Seconds")
    searchTree()
    //listCloseLatLon.foreach { item => println(item.geoCode + " " + item.lat + " " + item.lon) }

  }

  def searchTree(): Unit = {
    val br = new Scanner(
      new FileReader("/Users/psangat/Dropbox/IRTTestFiles/542_20150617_2131_MLDD_ALLOUTL.CSV"))
    var line = ""
    var errCounter = 0
    var counter = 0
    br.nextLine()
    breakable {
      while (br.hasNextLine()) {
        val values = br.nextLine().split(",")

        val lat = values(22)
        val lon = values(23)
        val trackKM = values(1)
        val trackName = values(14)
        var srefDistance = 1000.0
        val StartLatitudeLongitude = lat + "," + lon

        val geoCode = GeoHash.encode(lat.toDouble, lon.toDouble, 8)

        val startTime = System.currentTimeMillis()
        if (treeMap.contains(geoCode)) {
          //println(treeMap.get(geoCode))
          treeMap.get(geoCode).get.foreach {
            item =>
              val sLatLon2 = item.lat + "," + item.lon
              val distance = CalculateDistance(StartLatitudeLongitude,
                sLatLon2)
              if (distance < srefDistance) {
                //println("Lat: " + lat + " Lon: " + lon + " TrackKM: " + trackKM + " TrackCode: " + trackCode + " TrackName: " + trackName)
                srefDistance = distance
                listCloseLatLon.add(0, item)
              }
          }
          if (listCloseLatLon.get(0).trackName.equals(trackName)) {
            println("Lat: " + listCloseLatLon.get(0).lat + " Lat In: " + lat + " Lon: " + listCloseLatLon.get(0).lon + " Lon In: " + lon + " TrackKM: " + listCloseLatLon.get(0).trackKM + " TrackKM In: " + trackKM + " TrackCode: " + listCloseLatLon.get(0).trackCode + " TrackName: " + listCloseLatLon.get(0).trackName + " TrackName In: " + trackName)
            counter += 1
          }
          else {
            System.err.println("Lat: " + listCloseLatLon.get(0).lat + " Lat In: " + lat + " Lon: " + listCloseLatLon.get(0).lon + " Lon In: " + lon + " TrackKM: " + listCloseLatLon.get(0).trackKM + " TrackKM In: " + trackKM + " TrackCode: " + listCloseLatLon.get(0).trackCode + " TrackName: " + listCloseLatLon.get(0).trackName + " TrackName In: " + trackName)
            errCounter += 1
          }
          val endTime = System.currentTimeMillis()
          //println("Time Taken for Tree: " + (((endTime - startTime) / 1000) % 60) + " Seconds")
          println("Time Taken for Tree: " + (endTime - startTime) + " milliSeconds")
          println("Total errors in mapping: " + errCounter + "/" + (counter + errCounter))
        }
        else {
          System.err.println("Search Keywords does not exists")
        }
        //break

      }
    }
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

}
