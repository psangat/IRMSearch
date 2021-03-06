package edu.monash.IRM.Mongo

/**
 * Created by psangat on 23/10/15.
 */


import com.mongodb.casbah.Imports._

case class Stock(symbol: String, price: Double)

object Common {
  /**
   * Convert a Stock object into a BSON format that MongoDb can store.
   */
  def buildMongoDbObject(stock: Stock): MongoDBObject = {
    val builder = MongoDBObject.newBuilder
    builder += "symbol" -> stock.symbol
    builder += "price" -> stock.price
    builder.result
  }

}
