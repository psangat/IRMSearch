package edu.monash.IRM.Mongo

/**
 * Created by psangat on 23/10/15.
 */

import com.mongodb.casbah.MongoConnection

object MongoFactory {

  val connection = MongoConnection(SERVER)
  val collection = connection(DATABASE)(COLLECTION)
  private val SERVER = "localhost"
  private val PORT = 27017
  private val DATABASE = "portfolio"
  private val COLLECTION = "stocks"

}
