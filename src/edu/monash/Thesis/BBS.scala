package edu.monash.Thesis

import java.util.ArrayList

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
 * Created by psangat on 15/10/15.
 */
object BBS {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  var EDB = Map[String, String]()
  var xSet = Set[Int]()
  var xTraps = Set[String]()


  def setup(DB: RDD[(String, String)], words: Array[String], sc: SparkContext): Int = {
    var stag, ke, xTrap = 0
    //using scala map to store edb
    words.foreach {
      word =>
        stag = Common.hash("hash1", "K", word)
        ke = Common.hash("hash2", "K", word)
        xTrap = Common.hash("F", "KX", word)
        var c = 0
        DB.lookup(word)(0).split(" ; ").foreach {
          id =>
            val label = Common.hash("F", stag.toString, c.toString)
            val e = Common.encrypt(ke.toString, id)
            c += 1
            EDB += (label.toString -> e)
            xSet += Common.hash("F", xTrap.toString, id)
        }
    }
    //sc.parallelize(EDB.toIndexedSeq).saveAsTextFile("/Users/mac/Applications/output")
    return EDB.size
  }

  def chooseStags(node: Node): (ArrayList[(Int, Int)], Int) = {
    if (node.isLeaf) {
      val stagsKes = new ArrayList[(Int, Int)]
      stagsKes.add((Common.hash("hash1", "K", node.value), Common.hash("hash2", "K", node.value)))
      return (stagsKes, node.weight)
    }
    //return (new ArrayList[(Int,Int)], node.weight)
    else if (node.operation.equalsIgnoreCase("NOT")) {
      val stagsKes = new ArrayList[(Int, Int)]
      stagsKes.add((-1, -1))
      return (stagsKes, 100)

    }
    else if (node.operation.equalsIgnoreCase("AND")) {
      var stagsMinKes = new ArrayList[(Int, Int)]
      var costMin = 100
      node.children.foreach { child =>
        val value = chooseStags(child)
        if (value._2 < costMin) {
          stagsMinKes = value._1
          costMin = value._2
        }
      }
      return (stagsMinKes, costMin)
    }
    else if (node.operation.equalsIgnoreCase("OR")) {
      val allStags = new ArrayList[(Int, Int)]
      var allCosts = 0
      node.children.foreach {
        child =>
          val value = chooseStags(child)
          allStags.add(value._1(0))
          allCosts = allCosts + value._2
      }
      return (allStags, allCosts)

    }
    val stagsKes = new ArrayList[(Int, Int)]
    stagsKes.add((-1, -1))
    return (stagsKes, 100)
  }

  def clientSearch(node: Node): Set[String] = {
    val stags = chooseStags(node)
    val exprForm = BBTree.replaceAllLeaves(node)
    return searchServer(stags._1, exprForm)
  }

  def evaluateExpression(node: Node, id: String): Boolean = {
    if (node.isLeaf)
      return xSet.contains(Common.hash("F", node.value.toString, id))
    else if (node.operation.equalsIgnoreCase("NOT"))
      return !evaluateExpression(node.children(0), id)
    else if (node.operation.equalsIgnoreCase("AND")) {
      node.children.foreach {
        child =>
          if (!evaluateExpression(child, id))
            return false
      }
      return true
    }
    else if (node.operation.equalsIgnoreCase("OR")) {
      node.children.foreach {
        child =>
          if (evaluateExpression(child, id))
            return true
      }
      return false
    }
    return false
  }

  def searchServer(stags: ArrayList[(Int, Int)], exprForm: Node): Set[String] = {
    var results = Set[String]()
    stags.foreach { stag =>
      var c = 0
      while (EDB.contains(Common.hash("F", stag._1.toString, c.toString).toString)) {
        val e = EDB.apply(Common.hash("F", stag._1.toString, c.toString).toString)
        val id = Common.decrypt(stag._2.toString, e)
        if (evaluateExpression(exprForm, id)) {
          results += id
        }
        c = c + 1
      }
    }
    return results
  }

  def main(args: Array[String]) {
    val t1 = System.currentTimeMillis
    val conf = new SparkConf()
      .setMaster("local[2]") // using 4 cores. change the int value to increase or decrease the cores used
      .setAppName("BBS Implementation")
      .set("spark.executor.memory", "2g") // 2GB of RAM assigned for spark
    val sc = new SparkContext(conf)

    val output = sc.wholeTextFiles("/Users/psangat/Dropbox/testfiles/file*") // location of the input files
    val words = Common.calc_W(output)
    val DB = Common.calc_DB(output)
    setup(DB, words, sc)
    if (EDB.size > 0) {
      val files = clientSearch(BBTree.createTree())
      if (files.size <= 0) {
        println("The searched keyword does not exist")
      }
      else {
        println("Files found in: ")
        files.foreach(println)
      }
    }
    else {
      println("EDB setup has failed")
    }
    val t2 = System.currentTimeMillis()
    println("Time for run :" + (t2 - t1) * 0.001 + " secs")
  }
}
