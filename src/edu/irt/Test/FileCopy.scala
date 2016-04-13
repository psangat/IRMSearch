package edu.irt.Test

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Created by psangat on 17/01/16.
 */
object FileCopy {

  val pattern = Pattern.compile("SampleFile[0-9]{1,10}.json")
  //val dirName = "/mnt/outputFiles"
  val dirName = "/Users/psangat/Dropbox/IRTTestFiles/OutputTestFiles/"

  def main(args: Array[String]) {
    while (true) {
      fileCopySimulationv1()
      TimeUnit.SECONDS.sleep(5)
    }
  }

  def fileCopySimulationv1(): Unit = {

    val filesList = new File(dirName).listFiles
    filesList.foreach {
      file =>
        val patternMatcher = pattern.matcher(file.getName)
        if (patternMatcher.find()) {
          val hadoopconf = new Configuration
          val hdfsURI = "hdfs://master:9000"
          hadoopconf.set("fs.defaultFS", hdfsURI)
          val hdfs = FileSystem.get(hadoopconf)
          val srcPath = new Path(file.toString)
          val destPath = new Path("hdfs://master:9000/inputDirectory/Dir1/" + file.getName)
          hdfs.moveFromLocalFile(srcPath, destPath)
          println(file.getName + " Copied")
        }
    }
  }

}
