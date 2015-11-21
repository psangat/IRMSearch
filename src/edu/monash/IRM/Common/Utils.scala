package edu.monash.IRM.Common

import java.io._

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
 * Created by Prajwol Sangat on 31/07/15.
 */
object Utils {

  def main(args: Array[String]) {
    generateLookUpTable("")
  }

  def generateLookUpTable(fileName: String): Unit = {
    val reader = new CSVReader(new FileReader("/Users/psangat/Dropbox/IRTTestFiles/GPS_Lookup_Table.csv"))
    var row = Array[String]()
    val tableBuilder = new StringBuilder()
    row = reader.readNext()
    row(row.length + 1) = "GeoCode"
    while ((row = reader.readNext()) != null) {

      for (i <- 0 until row.length) {
        System.out.println("Cell column index: " + i)
        System.out.println("Cell Value: " + row(i))
        System.out.println("-------------")
        //val lat = row ()
      }

    }

  }

  // returns the random double in the range
  def getRandomDouble(min: Double, max: Double): Double = {
    val random = new Random()
    return min + (max - min) * random.nextDouble()
  }

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((index, rows) => {
      if (index == 0) {
        rows.drop(1)
      }
      else {
        rows
      }


    })
  }

  def convertToDouble(string: String): Double = {
    return string.toDouble
  }

  def convertToInt(string: String): Int = {
    return string.trim.toInt
  }

}
