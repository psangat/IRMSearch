package edu.monash.IRM.Common

import org.apache.spark.rdd.RDD

/**
 * Created by Prajwol Sangat on 31/07/15.
 */
object Utils {

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
