package edu.monash.IRM.Speed

/**
 * Created by psangat on 14/12/15.
 */

import java.io._
import java.net._

object StreamClient {

  def main(args: Array[String]) {

    for (i <- 0 until 2000) {
      val s = new Socket(InetAddress.getByName("localhost"), 9999)
      val out = new PrintStream(s.getOutputStream())

      out.println("{\"iSegment\":   242,\"SomatTime\": 11534.000,\"kmh\":     0.900,\"CarOrient\":     0.900,\"EorL\":     0.000,\"minSND\":     0.000,\"maxSND\":     0.000,\"Rock\":     0.000,\"Bounce\":     0.000,\"minCFA\":     0.000,\"maxCFA\":     0.000,\"accR3\":     0.000,\"accR4\":     0.000,\"Direction\":\"ToMine    \",\"minCFB\":    0.0000,\"maxCFB\":    0.0000,\"LATACC\":        0,\"maxBounce\":     0.000,\"PipeA\":     0.000,\"PipeB\":     0.000,\"GPSLat\":      -20.6047562700,\"GPSLon\":      117.1677482500}")
      out.flush()

      s.close()
    }
  }

}
