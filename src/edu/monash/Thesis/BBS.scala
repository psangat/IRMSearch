package edu.monash.Thesis

import java.util.ArrayList

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
            val d = Common.encrypt(ke.toString, DB.lookup(word).toString)
            c += 1
            EDB += (label.toString -> d)
            xSet += Common.hash("F", xTrap.toString, id)
        }
    }
    //sc.parallelize(EDB.toIndexedSeq).saveAsTextFile("/Users/mac/Applications/output")
    return EDB.size
  }

  def chooseStags(node: Node): (ArrayList[Int], Int) = {
    if (node.isLeaf)
      return (new ArrayList[Int](Common.hash("F", "K", node.value)), node.weight)
    else if (node.operation.equalsIgnoreCase("NOT"))
      return (new ArrayList[Int](-1), 100)
    else if (node.operation.equalsIgnoreCase("AND")) {
      var stagsMin = -1
      var costMin = 100
      node.children.foreach { child =>
        val value = chooseStags(child)
        if (value._2 < costMin) {
          stagsMin = value._1(0)
          costMin = value._2
        }
      }
      return (new ArrayList[Int](stagsMin), costMin)
    }
    else if (node.operation.equalsIgnoreCase("OR")) {
      val allStags: ArrayList[Int] = new ArrayList[Int]()
      var allCosts = 0
      node.children.foreach {
        child =>
          val value = chooseStags(child)
          allStags.add(value._1(0))
          allCosts = allCosts + value._2
      }
      return (allStags, allCosts)

    }
    return (new ArrayList[Int](-1), 100)
  }

  def clientSearch(node: Node): Unit = {
    chooseStags(node)
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

  def searchServer(stags: ArrayList[(Int, Int)], exprForm: Node): Unit = {
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
}
