package edu.irt.Test

/**
 * Created by psangat on 17/01/16.
 */

import java.io._
import java.net._

object StreamClient {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: StreamClient <hostname> <port>")
      System.exit(1)
    }
    sendContinuousStream(args)
  }

  def sendContinuousStream(args: Array[String]): Unit = {
    val s = new Socket(InetAddress.getByName(args(0)), args(1).toInt)
    val out = new PrintStream(s.getOutputStream())
    var count = 1
    while (true) {
      println("Number of records sent: " + count)
      out.println("{\"iSegment\":   242,\"SomatTime\": 11534.000,\"kmh\":     0.900,\"CarOrient\":     0.900,\"EorL\":     0.000,\"minSND\":     0.000,\"maxSND\":     0.000,\"Rock\":     0.000,\"Bounce\":     0.000,\"minCFA\":     0.000,\"maxCFA\":     0.000,\"accR3\":     0.000,\"accR4\":     0.000,\"Direction\":\"ToMine    \",\"minCFB\":    0.0000,\"maxCFB\":    0.0000,\"LATACC\":        0,\"maxBounce\":     0.000,\"PipeA\":     0.000,\"PipeB\":     0.000,\"gps\":{\"latitude\":      -20.6047562700,\"longitude\":      117.1677482500}}")
      out.flush()
      count = count + 1
    }
    s.close()


  }
}
