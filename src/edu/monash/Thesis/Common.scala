package edu.monash.Thesis

/**
 * Created by psangat on 15/10/15.
 */

import java.io.File

import org.apache.commons.codec.binary.{Base64, StringUtils}
import org.apache.spark.rdd._

import scala.util.Random

object Common {

  // gets the list of file in the directory
  def getListOfFiles(directory: String): List[File] = {
    val d = new File(directory)
    if (d.exists && d.isDirectory)
      d.listFiles.filter(_.isFile).toList
    else
      List[File]()
  }

  // returns the random integer in the range
  def getRandomInteger(min: Int, max: Int): Int = {

    val random = new Random()
    return random.nextInt((max - min) + 1) + min
  }

  // returns the random double in the range
  def getRandomDouble(min: Double, max: Double): Double = {
    val random = new Random()
    return min + (max - min) * random.nextDouble()
  }

  // Splits the contents of file to individual words
  def calc_W(output: RDD[(String, String)]): Array[String] = {
    val words = output.flatMap { case (filePath, line) => line.replaceAll("\n", " ").split(" ") }
    val wordsCount: Long = words.count()
    val sortedWords: Array[String] = words.top(wordsCount.toInt)
    return sortedWords
  }

  // Creates the database of words and the corresponding files
  def calc_DB(output: RDD[(String, String)]): RDD[(String, String)] = {

    val db = output.flatMap { case (filePath, line) => line.replaceAll("\n", " ").split(" ") map { word => (word, filePath.substring(filePath.lastIndexOf("/") + 1, filePath.length)) } }
      .distinct
      .reduceByKey { case (f1, f2) => f1 + " ; " + f2 }
    return db
  }

  // encrypt the plaintext using the key
  def encrypt(key: String, plainText: String): String = {

    val message = (key + ":" + plainText) //.getBytes("UTF-8")
    return Base64.encodeBase64String(StringUtils.getBytesUtf8(message)) //DatatypeConverter.printBase64Binary(message)
  }

  // returns the hash
  def hash(keyWord: String, key: String, word: String): Int = return (keyWord + key + word).hashCode()

  // decrypt the ciphertext using the key
  def decrypt(key: String, cipherText: String): String = {
    val decoded = StringUtils.newStringUtf8(Base64.decodeBase64(cipherText)) // DatatypeConverter.parseBase64Binary(cipherText)
    val keyNdata = decoded.split(":") //new String(decoded, "UTF-8").split(":")
    if (keyNdata(0).equals(key)) {
      return keyNdata(1)
    }
    else {
      throw new NullPointerException()
    }
  }


}
