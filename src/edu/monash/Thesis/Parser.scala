package edu.monash.Thesis

/**
 * Created by psangat on 2/10/15.
 */

import java.io._

import edu.jhu.nlp.wikipedia._

object Parser {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: Parse Wikipedia XML Files <input file path> <output file path>")
      System.exit(1)

    }
    val wxp = WikiXMLParserFactory.getDOMParser(args(0))
    val printWriter = new PrintWriter(new File(args(1)))
    val stringBuilder = new StringBuilder()
    try {
      println("=========Started Parsing=============")
      wxp.parse()
      val it = wxp.getIterator()
      while (it.hasMorePages()) {
        val page = it.nextPage()
        stringBuilder.append(page.getText())
      }
      printWriter.write(stringBuilder.toString())
      printWriter.close()
      println("============End Parsing=============")

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
