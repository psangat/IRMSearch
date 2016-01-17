package edu.irt.Test

import java.io.{File, FileReader, PrintWriter}
import java.util.regex.Pattern
import java.util.{ArrayList, Scanner}

import scala.collection.JavaConversions._
import scala.util.control.Breaks._

/**
 * Created by psangat on 4/12/15.
 */
object DataSynthesizer {
  val recordsList = new ArrayList[String]()

  def main(args: Array[String]) {
    val pattern = Pattern.compile("[0-9]{1,4}_(.*?)_(.*?)_ALLOUT(.*?).CSV")

    val dirName = "/mnt/AllFiles"
    //val dirName = "/Users/psangat/Dropbox/IRTTestFiles/InputTestFiles"

    var count = 0
    val filesList = new File(dirName).listFiles
    filesList.foreach {
      file =>
        val patternMatcher = pattern.matcher(file.getName)
        if (patternMatcher.find()) {
          val br = new Scanner(
            new FileReader(file.toString))
          br.nextLine()
          breakable {
            while (br.hasNextLine()) {
              val values = br.nextLine().split(",")

              recordsList.add("{\"iSegment\":%s,\"SomatTime\":%s,\"kmh\":%s,\"CarOrient\":%s,\"EorL\":%s,\"minSND\":%s,\"maxSND\":%s,\"Rock\":%s,\"Bounce\":%s,\"minCFA\":%s,\"maxCFA\":%s,\"accR3\":%s,\"accR4\":%s,\"Direction\":\"%s\",\"minCFB\":%s,\"maxCFB\":%s,\"LATACC\":%s,\"maxBounce\":%s,\"PipeA\":%s,\"PipeB\":%s,\"latitude\":%s,\"longitude\":%s}"
                .format(values(0), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(15), values(16), values(17), values(18).replace(".", ""), values(19), values(20), values(21), values(22), values(23)))
              if (recordsList.size() >= 20000) {
                count = count + 1
                printFile(count)
              }
            }
          }
        }
    }
  }

  def printFile(count: Int): Unit = {
    //
    val pw = new PrintWriter(new File("/mnt/outputFiles/SampleFile" + count + ".json"))
    //val pw = new PrintWriter(new File("/Users/psangat/Dropbox/IRTTestFiles/OutputTestFiles/SampleFile" + count + ".json"))
    recordsList.foreach { record =>
      pw.println(record)
    }
    pw.close
    recordsList.clear()
    //System.exit(1)
  }

}
