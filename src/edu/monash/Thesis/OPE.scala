package edu.monash.Thesis

import java.util.ArrayList

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
  val ptIntervals = new ArrayList[Interval]
  val ctIntervals = new ArrayList[Interval]

  def main(args: Array[String]) {
    val t1 = System.currentTimeMillis
    setup()
  }

  def setup(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]") // using 4 cores. change the int value to increase or decrease the cores used
      .setAppName("OPE Implementation")
      .set("spark.executor.memory", "2g") // 2GB of RAM assigned for spark
    val sc = new SparkContext(conf)

    val output = sc.wholeTextFiles("/Users/psangat/Dropbox/testfiles/OPE/file*") // location of the input files
    val plainTexts = Common.calc_W(output)
    scala.util.Sorting.quickSort(plainTexts)
    createPlainTextRange(plainTexts(0).toInt, plainTexts.last.toInt)
    createCipherTextRange()
    plainTexts.foreach { num =>
      println("=======================")
      println(num)
      val encValue = encrypt(num.toInt)
      println(encValue)
      decrypt(encValue._1, encValue._2)
      println("=======================")
    }
    /*plainTexts.foreach {
      number =>
        println(number)
        if (number.toInt > rv) {
          if (rv == 0) {
            lv = number.toInt - 5
            rv += number.toInt + Common.getRandomInteger(5, 10)
            ptIntervals.add(new Interval(lv, rv))
            lv = rv
          }
          else {
            var rand = 0
            while (!(number.toInt > lv && number.toInt <= rv)) {
              rand = Common.getRandomInteger(5, 10)
              ptIntervals.add(new Interval(lv, rand + rv))
            }
            rv += rand
            lv = rv
          }

        }
    }*/

    /*println("Plain Intervals")
    ptIntervals.foreach {
      interval => println(interval)
    }
    println("Cipher Intervals")
    ctIntervals.foreach(println)*/

  }

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

  def encrypt(x: Int): (Double, Int) = {
    val ptlv = getPlainTextRange(0).lv
    val ptrv = getPlainTextRange(0).rv

    if (x <= ptrv && x > ptlv) {
      val ctlv = getCipherTextRange(0).lv
      val ctrv = getCipherTextRange(0).rv
      val s = (ctrv - ctlv) / (ptrv - ptlv).toDouble
      var xPrime = ctlv + s * (x.toInt - ptlv)
      val n1 = Common.getRandomDouble(-s / 2.1, s / 2.0)
      xPrime += n1
      println(xPrime)
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
          var xPrime = ctlvMinOne + s * (x.toInt - ptlv)
          val n1 = Common.getRandomDouble(-s / 2.1, s / 2.0)
          xPrime += n1
          println(xPrime)
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

  def decrypt(xPrime: Double, b: Int): Unit = {
    val ctlv = getCipherTextRange(0).lv
    val ctrv = getCipherTextRange(0).rv

    if (xPrime <= ctrv && xPrime > ctlv && b == -1) {
      val ptlv = getPlainTextRange(0).lv
      val ptrv = getPlainTextRange(0).rv
      val s = (ctrv - ctlv) / (ptrv - ptlv).toDouble
      val x = Math.round((xPrime - (ctlv - s * ptlv)) / s)
      print(x)
    }
    else {
      var index = 0
      ctIntervals.foreach { interval =>

        if (xPrime <= interval.rv && xPrime > interval.lv) {
          if (b == 0) {
            val ptlv = getPlainTextRange(index).lv
            val ptrv = getPlainTextRange(index).rv
            val s = (interval.rv - interval.lv) / (ptrv - ptlv).toDouble
            val x = Math.round((xPrime - (ctlv - s * ptlv)) / s)
            print(x)
            index += 1
          }
          else if (b == 1) {
            val ptlv = getPlainTextRange(index + 1).lv
            val ptrv = getPlainTextRange(index + 1).rv
            val ctlvplus = getCipherTextRange(index + 1).rv
            val s = (interval.rv - interval.lv) / (ptrv - ptlv).toDouble
            val x = Math.round((xPrime - (ctlv - s * ptlv)) / s)
            print(x)
            index += 1
          }
        }
      }
    }
  }

  def getPlainTextRange(index: Int): Interval = {
    return ptIntervals.get(index)
  }

  def getCipherTextRange(index: Int): Interval = {
    return ctIntervals.get(index)
  }

}
