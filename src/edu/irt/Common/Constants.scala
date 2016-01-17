package edu.irt.Common

/**
 * Created by psangat on 29/10/15.
 */
object Constants {
  val LATITUDE = "latitude"
  val LONGITUDE = "longitude"
  val TRACK_KM = "TrackKM"
  val TRACK_NAME = "TrackName"
  val GPS = "gps"


  // STREAM SERVER CONSTANTS
  val PORT_NO = 9999

  // "/mnt/AllFiles/GPS_Lookup_Table.csv"
  // val GPS_LOOKUP_FILE_PATH = "/Users/psangat/Dropbox/IRTTestFiles/GPS_Lookup_Table.csv"
  //"local[*]" "hdfs://master:9000/inputDirectory/Dir1/" "/Users/psangat/Dropbox/IRTTestFiles/GPS_Lookup_Table.csv"

  // MONGO DB CONSTANTS
  val HOST_NAME = "localhost"
  val DB_NAME = "IRT"
  val COLLECTION_NAME = "sensordata"
}
