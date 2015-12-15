package edu.monash.IRM.Speed

/**
 * Created by psangat on 14/12/15.
 */

import java.io._
import java.net._
import java.util.ArrayList

import scala.collection.JavaConversions._
import scala.io._

object StreamServer {
  val server = new ServerSocket(9999)
  val recordsList = new ArrayList[String]()
  val RECORDS_SIZE = 10
  var count = 1

  def main(args: Array[String]) {

    while (true) {
      val s = server.accept()
      val in = new BufferedSource(s.getInputStream()).getLines()

      in.foreach { record => recordsList.add(record) }
      //recordsList.add(in)
      if (recordsList.size() >= RECORDS_SIZE) {
        val pw = new PrintWriter(new File("/Users/psangat/Dropbox/IRTTestFiles/OutputTestFiles/SampleFile" + count + ".json"))
        recordsList.foreach { record =>
          pw.println(record)
        }
        pw.close
        recordsList.clear()
        count = count + 1

      }
      s.close()
    }

  }

}
