package edu.monash.IRM.Mongo

import org.json._

/**
 * Created by psangat on 23/10/15.
 */

case class SensorData(
                       iSegment: BigInt
                       , kmh: Double
                       , CarOrient: Double
                       , EorL: Double
                       , minSND: Double
                       , maxSND: Double
                       , Rock: Double
                       , Bounce: Double
                       , minCFA: Double
                       , maxCFA: Double
                       , accR3: Double
                       , accR4: Double
                       , Direction: String
                       , minCFB: Double
                       , maxCFB: Double
                       , LATACC: Double
                       , maxBounce: Double
                       , PipeA: Double
                       , PipeB: Double
                       , GPSLat: Double
                       , GPSLon: Double

                       )

object InsertToMongo {

  val mongo = MongoFactory

  def Insert(sensorData: JSONObject): Unit = {
    //mongo.collection.insert(sensorData)
  }


}
