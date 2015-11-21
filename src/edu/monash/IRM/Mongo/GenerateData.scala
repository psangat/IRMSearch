package edu.monash.IRM.Mongo

/**
 * Created by psangat on 23/10/15.
 * Client Application to generate test data
 * The generated data will be stored to MongoDB using InsertToMongo
 */

import java.io._
import java.net._

object GenerateData {

  def main(args: Array[String]) {

    while (true) {
      val s = new Socket(InetAddress.getByName("localhost"), 9999)
      // lazy val in = new BufferedSource(s.getInputStream()).getLines()
      val out = new PrintStream(s.getOutputStream())

      out.println("Hello, world")
      out.flush()
      //println("Received: " + in.next())

      s.close()
    }

  }


}
