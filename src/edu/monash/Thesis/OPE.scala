package edu.monash.Thesis

import java.util.ArrayList

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
 * Created by psangat on 16/10/15.
 */

case class Interval(
                     lv: Int, // left value
                     rv: Int // right value
                     )

object OPE {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  val ptIntervals = new ArrayList[Interval]
  val ctIntervals = new ArrayList[Interval]
  val EDB = new ArrayList[(Double, Int)]
  val seclectedList = new ArrayList[(Double, Int)]
  var globalXPrime = 0.0
  var sValue = 0.0

  // used to search the values between specified minimum and maximum value
  def search(min: Int, max: Int): Unit = {
    val encValuesA = encrypt(min)
    val minCtA = globalXPrime - sValue
    val idxA = indexOfInverval(minCtA, ctIntervals)
    val encValuesB = encrypt(max)
    val maxCtB = globalXPrime
    val idxB = indexOfInverval(maxCtB, ctIntervals)

    EDB.foreach {
      item =>
        val idxPrime = indexOfInverval(item._1, ctIntervals)
        if (idxA <= idxPrime && idxPrime <= idxB) {
          seclectedList.add(item)
        }
        if (idxPrime == idxA) {
          if (item._2 == encValuesA._2 && item._1 < encValuesA._1) {
            seclectedList.remove(item)
          }
          if (item._2 < encValuesA._2) {
            seclectedList.remove(item)
          }
        }
        if (idxPrime == idxB) {
          if (item._2 == encValuesB._2 && item._1 > encValuesB._1) {
            seclectedList.remove(item)

          }
          if (item._2 > encValuesB._2) {
            seclectedList.remove(item)
          }
        }
    }
    println("========= Selected List =============")
    seclectedList.foreach { item => decrypt(item._1, item._2) }

  }

  // Creates the encrypted data base used to search
  def setup(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: Parallel Order Preserving Encryption <master> <input_file> <search range>")
      System.exit(1)
    }
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
    val plainTexts = Common.calc_W(output)
    scala.util.Sorting.quickSort(plainTexts)
    createPlainTextRange(plainTexts(0).toInt, plainTexts.last.toInt)
    println("================Plain Text Range=====================")
    ptIntervals.foreach(println)
    createCipherTextRange()
    println("================Cipher Text Range=====================")
    ctIntervals.foreach(println)

    println("First: " + plainTexts(0).toInt + "\n Last: " + plainTexts.last.toInt)
    plainTexts.foreach { num =>
      val encValue = encrypt(num.toInt)
      EDB.add(encValue)
      println(num + " => " + encValue)
    }
    println("Total items added: " + EDB.size())
  }

  // creates a random plain text range
  def createPlainTextRange(first: Int, last: Int): Unit = {

    var ptrv = 0
    var ptlv = 0
    while (ptrv < last) {
      if (ptrv == 0) {
        ptlv = first - 5
        ptrv += ptlv + Common.getRandomInteger(5, 10)
        ptIntervals.add(new Interval(ptlv, ptrv))
        ptlv = ptrv
      }
      else {
        ptrv += Common.getRandomInteger(10, 20)
        ptIntervals.add(new Interval(ptlv, ptrv))
        ptlv = ptrv
      }
    }
  }

  // creates a random cipher text range
  def createCipherTextRange(): Unit = {
    var ctlv = 0
    var ctrv = 0
    ptIntervals.foreach {
      interval =>
        if (ctlv == 0) {
          ctlv = interval.lv + 100
          ctrv += ctlv + Common.getRandomInteger(25, 40)
          ctIntervals.add(new Interval(ctlv, ctrv))
          ctlv = ctrv
        }
        else {
          ctrv += Common.getRandomInteger(25, 40)
          ctIntervals.add(new Interval(ctlv, ctrv))
          ctlv = ctrv
        }
    }
  }

  // returns xPrime and value of b
  def encrypt(x: Int): (Double, Int) = {
    val ptlv = getPlainTextRange(0).lv
    val ptrv = getPlainTextRange(0).rv

    if (x <= ptrv && x > ptlv) {
      val ctlv = getCipherTextRange(0).lv
      val ctrv = getCipherTextRange(0).rv
      val s = (ctrv - ctlv) / (ptrv - ptlv).toDouble
      globalXPrime = ctlv + s * (x.toInt - ptlv)
      sValue = -s / 2.1
      var xPrime = ctlv + s * (x.toInt - ptlv)
      val n1 = Common.getRandomDouble(-s / 2.1, 0.0)
      xPrime += n1
      return (xPrime, -1)
    }
    else {
      var index = 0
      ptIntervals.foreach { interval =>

        if (x <= interval.rv && x > interval.lv) {
          val ptlv = interval.lv
          val ptrv = interval.rv
          val ctRange = getCipherTextRange(index)
          val ctRangeMinOne = getCipherTextRange(index - 1)
          val ctlvMinOne = ctRangeMinOne.lv
          val ctrv = ctRange.rv
          val s = (ctrv - ctlvMinOne) / (ptrv - ptlv).toDouble
          globalXPrime = ctlvMinOne + s * (x.toInt - ptlv)
          sValue = -s / 2.1
          var xPrime = ctlvMinOne + s * (x.toInt - ptlv)
          val n1 = Common.getRandomDouble(-s / 2.1, 0.0)
          xPrime += n1
          var b = -1
          if (xPrime <= ctRange.rv && xPrime > ctRange.lv)
            b = 0
          else if (xPrime <= ctRangeMinOne.rv && xPrime > ctRangeMinOne.lv)
            b = 1
          return (xPrime, b)
        }
        index += 1
      }
    }
    return (0.0, -1)
  }

  // returns index of interval in which number belongs
  def indexOfInverval(num: Double, intervals: ArrayList[Interval]): Int = {
    var index = 0
    intervals.foreach { interval =>
      if (num <= interval.rv && num > interval.lv) {
        return index
      }
      index = index + 1
    }
    return -1
  }

  // returns index of EDB in which the number belongs
  def indexOf(number: Double): Int = {
    EDB.foreach {
      item =>
        println("Floor of Item: " + item._1.toInt)
        if (item._1.toInt == number.toInt) {

          return EDB.indexOf(item)
        }
    }
    return -1
  }

  // returns the plain text range based on index
  def getPlainTextRange(index: Int): Interval = {
    return ptIntervals.get(index)
  }

  // returns the cipher text range based on index
  def getCipherTextRange(index: Int): Interval = {
    return ctIntervals.get(index)
  }

  // decrypts the cipher text values to get plain text value
  def decrypt(xPrime: Double, b: Int): Unit = {
    val ctlv = getCipherTextRange(0).lv
    val ctrv = getCipherTextRange(0).rv

    if (xPrime <= ctrv && xPrime > ctlv && b == -1) {
      val ptlv = getPlainTextRange(0).lv
      val ptrv = getPlainTextRange(0).rv
      val s = (ctrv - ctlv) / (ptrv - ptlv).toDouble
      val x = Math.round((xPrime - (ctlv - s * ptlv)) / s)
      println(x)
    }
    else {
      var index = 0
      ctIntervals.foreach { interval =>

        if (xPrime <= interval.rv && xPrime > interval.lv) {
          if (b == 0) {
            val ptlv = getPlainTextRange(index).lv
            val ptrv = getPlainTextRange(index).rv
            val ctlvminus = getCipherTextRange(index - 1).lv
            val s = (interval.rv - ctlvminus) / (ptrv - ptlv).toDouble
            val x = Math.round((xPrime - (ctlvminus - s * ptlv)) / s)
            println(x)
          }
          else if (b == 1) {
            val ptlv = getPlainTextRange(index + 1).lv
            val ptrv = getPlainTextRange(index + 1).rv
            val ctrvplus = getCipherTextRange(index + 1).rv
            val s = (ctrvplus - interval.lv) / (ptrv - ptlv).toDouble
            val x = Math.round((xPrime - (interval.lv - s * ptlv)) / s)
            println(x)
          }
        }
        index += 1
      }
    }
  }

  def main(args: Array[String]) {
    val t1 = System.currentTimeMillis
    setup(args)
    search(args(2).toInt, args(3).toInt)
  }
}
