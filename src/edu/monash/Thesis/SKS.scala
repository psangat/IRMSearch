package edu.monash.Thesis

/**
 * Created by psangat on 22/05/15.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._

object SKS {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  var EDB = Map[String, String]()
  var xSet = Set[Int]()

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: Single Keyword Search <master> <input_file> <search word>")
      System.exit(1)
    }
    val t1 = System.currentTimeMillis
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName(this.getClass.getCanonicalName)
      .set("spark.executor.instances", "3")
      .set("spark.executor.memory", "4g")
      .set("spark.executor.cores", "1")
      .set("spark.task.cpus", "1")
      .set("spark.driver.memory", "4g")
    val sc = new SparkContext(conf)

    val output = sc.wholeTextFiles(args(1))
    val words = Common.calc_W(output)
    val DB = Common.calc_DB(output)
    setup(DB, words, sc)
    if (EDB.size > 0) {
      val keys = search_client("K", args(2))
      val docLocation = search_server(keys._1.toString, keys._2.toString)
      if (docLocation.size <= 0) {
        println("The searched keyword does not exist")
      }
      else {
        println("Files found in: ")
        docLocation.foreach(println)
      }
    }
    else {
      println("EDB setup has failed")
    }
    val t2 = System.currentTimeMillis()
    println("Time for run :" + (t2 - t1) * 0.001 + " secs")
  }

  def setup(DB: RDD[(String, String)], words: Array[String], sc: SparkContext): Int = {
    var k1, k2, xTrap = 0
    words.foreach {
      word =>
        k1 = Common.hash("hash1", "K", word)
        k2 = Common.hash("hash2", "K", word)
        xTrap = Common.hash("F", "KX", word)
        var c = 0
        DB.lookup(word)(0).split(" ; ").foreach {
          id =>
            val label = Common.hash("F", k1.toString, c.toString)
            val d = Common.encrypt(k2.toString, id)
            c += 1
            EDB += (label.toString -> d)
            xSet += Common.hash("F", xTrap.toString, id)
        }
    }
    return EDB.size
  }

  def search_client(keyWord: String, word: String): (Int, Int) = {
    return new Tuple2(Common.hash("hash1", keyWord, word), Common.hash("hash2", keyWord, word))
  }

  def search_server(K1: String, K2: String): Set[String] = {
    var c = 0
    var results = Set[String]()
    while (EDB.contains(Common.hash("F", K1, c.toString).toString)) {
      val encDocLocation = EDB.apply(Common.hash("F", K1, c.toString).toString)
      val docLocation = Common.decrypt(K2, encDocLocation)
      c = c + 1
      results += docLocation.toString
    }
    return results
  }


}
