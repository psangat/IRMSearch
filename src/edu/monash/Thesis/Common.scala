package edu.monash.Thesis

/**
 * Created by psangat on 15/10/15.
 */

import org.apache.commons.codec.binary.{Base64, StringUtils}
import org.apache.spark.rdd._

object Common {


  // Splits the contents of file to individual words
  def calc_W(output: RDD[(String, String)]): Array[String] = {
    val words = output.flatMap { case (filePath, line) => line.replaceAll("\n", " ").split(" ") }
    val wordsCount: Long = words.count()
    val sortedWords: Array[String] = words.top(wordsCount.toInt)
    sortedWords.foreach(println)
    return sortedWords
  }

  // Creates the database of words and the corresponding files
  def calc_DB(output: RDD[(String, String)]): RDD[(String, String)] = {

    val db = output.flatMap { case (filePath, line) => line.replaceAll("\n", " ").split(" ") map { word => (word, filePath) } }
      .distinct
      .reduceByKey { case (f1, f2) => f1 + " ; " + f2 }
    db.foreach(println)
    return db
  }

  // returns the hash
  def hash(keyWord: String, key: String, word: String): Int = return (keyWord + key + word).hashCode()

  // encrypt the plaintext using the key
  def encrypt(key: String, plainText: String): String = {

    val message = (key + ":" + plainText) //.getBytes("UTF-8")
    return Base64.encodeBase64String(StringUtils.getBytesUtf8(message)) //DatatypeConverter.printBase64Binary(message)
  }

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
