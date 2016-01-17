package edu.irt.Common

import edu.irt.Common.Base32._
import edu.irt.Common.Calate._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sparkusr on 31/08/15.
 */

case class ClosestLocation(lat: String,
                           lon: String,
                           trackKM: String,
                           trackCode: String,
                           trackName: String,
                           subTrackCode: String,
                           subTrackName: String,
                           geoCode: String)

/** GeoHash encoding/decoding as per http://en.wikipedia.org/wiki/Geohash */
object GeoHash {

  // Aliases, utility functions
  type Bounds = (Double, Double)
  val LAT_RANGE = (-90.0, 90.0)
  val LON_RANGE = (-180.0, 180.0)
  val EARTH_RADIUS = 6371
  // in KM
  var lat1 = -21.20810084
  var lon1 = 117.04907981

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)
      .set("spark.hadoop.validateOutputSpecs", "false")
    val encoded = hashCode("-20.8218252".toDouble, "116.777518".toDouble) // encode("-20.82182528".toDouble, "116.7775181".toDouble)
    println("Encoded Value: " + encoded)
    //createGeoHashMappedFile(conf)
    //CheckPrecisionMapping(conf)
    //println("Decoded Value: " + decode(encoded))
    //-64162521
  }

  def hashCode(lat: Double, lon: Double): Int = {
    (lat.toString + lon.toString).hashCode
  }

  def subString(lat: Double, lon: Double) = encode(lat, lon).substring(0, 8) // change precision here

  def calculateDistance(lat: String, lon: String) = getDistance(lat.toDouble, lon.toDouble)

  def CheckPrecisionMapping(conf: SparkConf): Unit = {
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val allRows = sc.textFile("file:///home/sparkusr/supportfiles/GeoHashLookUpTable")
    //lat1 = -20.59653531
    //lon1 = 117.17342463
    val encoded = encode(lat1, lon1)
    sqlContext.read.json(allRows).registerTempTable("GeoHashLookUpTable")
    sqlContext.udf.register("calculateDistance", calculateDistance _)
    val df = sqlContext.sql(

      " SELECT Lat, Lon, geoCode, calculateDistance(Lat,Lon) AS Distance, TrackKM, TrackCode, TrackName, SubTrackCode, SubTrackName"
        + " FROM GeoHashLookUpTable"
      //+ " WHERE geoCode LIKE '" + subString(lat1, lon1) + "%'"
      //+ " ORDER BY Distance ASC"
    )

    df.filter("geoCode LIKE '" + subString(lat1, lon1) + "%'")
      .orderBy("Distance").show()
    // df.sqlContext.sql("select Lat,Lon from ")
    // .toDF().show()//.first()
    //printf(df.toString)
  }

  def getDistance(lat0: Double, lon0: Double): Double = {
    val dLng = (lon0 - lon1).toRadians
    val dLat = (lat0 - lat1).toRadians

    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(lat0.toRadians) * Math.cos(lat1.toRadians) * Math.sin(dLng / 2) * Math.sin(dLng / 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * EARTH_RADIUS
    return c
  }

  def createGeoHashMappedFile(conf: SparkConf): Unit = {

    //.setJars(Seq(SparkContext.jarOfClass(this.getClass).get))
    val sc = new SparkContext(conf)
    val lines = sc.textFile("file:///home/sparkusr/supportfiles/GPS_Lookup_Table.csv")
    //MapInput.mapInputToJSON(lines, sc)


  }

  /**
   * Encode lat/long as a base32 geohash.
   *
   * Precision (optional) is the number of base32 chars desired; default is 12, which gives precision well under a meter.
   */
  def encode(lat: Double, lon: Double, precision: Int = 12): String = {
    // scalastyle:ignore
    require(lat in LAT_RANGE, "Latitude out of range")
    require(lon in LON_RANGE, "Longitude out of range")
    require(precision > 0, "Precision must be a positive integer")
    val rem = precision % 2 // if precision is odd, we need an extra bit so the total bits divide by 5
    val numbits = (precision * 5) / 2
    val latBits = findBits(lat, LAT_RANGE, numbits)
    val lonBits = findBits(lon, LON_RANGE, numbits + rem)
    val bits = intercalate(lonBits, latBits)
    bits.grouped(5).map(toBase32).mkString // scalastyle:ignore
  }

  /**
   * Decode a base32 geohash into a tuple of (lat, lon)
   */
  def decode(hash: String): (Double, Double) = {
    require(isValid(hash), "Not a valid Base32 number")
    val (odd, even) = extracalate(toBits(hash))
    val lon = mid(decodeBits(LON_RANGE, odd))
    val lat = mid(decodeBits(LAT_RANGE, even))
    (lat, lon)
  }

  private def mid(b: Bounds) = (b._1 + b._2) / 2.0

  private def decodeBits(bounds: Bounds, bits: Seq[Boolean]) =
    bits.foldLeft(bounds)((acc, bit) => if (bit) (mid(acc), acc._2) else (acc._1, mid(acc)))

  private def findBits(part: Double, bounds: Bounds, p: Int): List[Boolean] = {
    if (p == 0) Nil
    else {
      val avg = mid(bounds)
      if (part >= avg) true :: findBits(part, (avg, bounds._2), p - 1) // >= to match geohash.org encoding
      else false :: findBits(part, (bounds._1, avg), p - 1)
    }
  }

  implicit class BoundedNum(x: Double) {
    def in(b: Bounds): Boolean = x >= b._1 && x <= b._2
  }

}