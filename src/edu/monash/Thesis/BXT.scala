package edu.monash.Thesis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._
import scala.util.control.Breaks._
/**
 * Created by psangat on 15/06/15.
 */
object BXT {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  var EDB = Map[String, String]()
  var xSet = Set[Int]()
  var xTraps = Set[String]()

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: Basic Cross Tags <master> <input_file> <two search word>")
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
      val keys = searchClient("K", Array(args(2), args(3)))
      val docLocation = searchServer(keys._1.toString, keys._2.toString)
      if (docLocation.size <= 0) {
        println("The searched keyword does not exist.")
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
    //using scala map to store edb
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

  def searchClient(keyWord: String, ws: Array[String]): (Int, Int) = {
    ws.foreach { w =>
      if (w != leastCommonWord(ws))
        xTraps += Common.hash("F", "KX", w).toString
    }
    return new Tuple2(Common.hash("hash1", keyWord, leastCommonWord(ws)), Common.hash("hash2", keyWord, leastCommonWord(ws)))
  }

  def leastCommonWord(ws: Array[String]): String = {
    return ws(0)
  }

  def searchServer(K1: String, K2: String): Set[String] = {
    var c = 0
    var results = Set[String]()
    while (EDB.contains(Common.hash("F", K1, c.toString).toString)) {
      val encDocLocation = EDB.apply(Common.hash("F", K1, c.toString).toString)
      val docLocation = Common.decrypt(K2, encDocLocation)

      //begin new section
      var hasAllKeyWords = true
      breakable {
        xTraps.foreach {
          xTrap =>
            if (!xSet.contains(Common.hash("F", xTrap, docLocation))) {
              hasAllKeyWords = false
              break
            }
        }
      }
      if (hasAllKeyWords)
        results += docLocation.toString
      c = c + 1
      results += docLocation.toString
    }
    return results
  }

}
