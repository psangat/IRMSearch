package edu.monash.IRM.Mongo

import com.mongodb.casbah.Imports._


/**
 * Created by psangat on 19/10/15.
 */
case class Person(
                   name: String,
                   address: String,
                   phone: String,
                   zipcode: String
                   )

object TestForMongo {
  val DB_NAME = "test"
  val COLLECTION_NAME = "persons"

  def main(args: Array[String]) {
    /*val host = "mongodb://localhost:27017"

    val mongoClient = new MongoClient(host)
    val database = mongoClient.getDatabase("test")
    val collection = database.getCollection("restaurants")

   println(collection.find().first())*/
    val mongoConn = MongoConnection()
    val mongoDB = mongoConn(DB_NAME)
    val mongoColl = mongoDB(COLLECTION_NAME)
    // Displaying all records
    println("============Displaying all Records==============")
    mongoColl.find().foreach(println)

    val person = Person("Sangat", "Chabahil", "9876543", "678")
    val newObj = MongoDBObject("name" -> person.name,
      "address" -> person.address,
      "phone" -> person.phone,
      "zipcode" -> person.zipcode)

    //// Adding the record
    //mongoColl.insert(newObj)
    //println("==After adding==")
    // mongoColl.find().foreach(println)

    // finding a record

    println("============Finding a Record==============")
    val q = MongoDBObject("name" -> "Sangat")
    mongoColl.findOne(q).foreach { x =>
      // do some work if you found the user...
      println("Found a user! %s".format(x("name")))
    }
  }

}
