package edu.monash.IRM.Batch

/**
 * Created by psangat on 19/10/15.
 */


object SparkMongo extends App {
  /*
    // Set up the configuration for reading from MongoDB.
    val mongoConfig = new Configuration()
    // MongoInputFormat allows us to read from a live MongoDB instance.
    // We could also use BSONFileInputFormat to read BSON snapshots.
    // MongoDB connection string naming a collection to read.
    // If using BSON, use "mapred.input.dir" to configure the directory
    // where the BSON files are located instead.
    mongoConfig.set("mongo.input.uri",
      "mongodb://localhost:27017/test.restaurants")

    val sparkConf = new SparkConf()
    val sc = new SparkContext("local", "SparkMongoExample", sparkConf)

    // Create an RDD backed by the MongoDB collection.
    val documents = sc.newAPIHadoopRDD(
      mongoConfig, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject]) // Value type

    // Create a separate Configuration for saving data back to MongoDB.
    val outputConfig = new Configuration()
    outputConfig.set("mongo.output.uri",
      "mongodb://localhost:27017/test.newrestaurants")

    // Save this RDD as a Hadoop "file".
    // The path argument is unused; all documents will go to "mongo.output.uri".
    /*documents.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Any],
      classOf[Any],
      classOf[MongoOutputFormat[Any, Any]],
      outputConfig)*/
  */

}
