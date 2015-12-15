import java.io.File
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Created by sparkusr on 4/09/15.
 */
object FileCopy {
  val pattern = Pattern.compile("[0-9]{1,4}_(.*?)_(.*?)_ALLOUT(.*?).CSV")
  val dirName = "/mnt/inputFiles"
  //val dirName = "/home/sparkusr/filesbk"

  def main(args: Array[String]) {
    val filePath = "file:///Users/psangat/Dropbox/IRTTestFiles/OutputTestFiles/SampleFile.json"
    //val filePath = "file:///Users/psangat/Dropbox/IRTTestFiles/1015_20150720_1634_MLDD_ALLOUTE.json"
    //val filePath = "file:///mnt/datafiles/1015_20150720_1634_MLDD_ALLOUTE_new.json"
    //fileCopySimulationv3(filePath)

    while (true) {
      //fileCopySimulationv1()
      TimeUnit.SECONDS.sleep(25)
    }
  }

  def fileCopySimulationv3(filePath: String): Unit = {
    val hadoopconf = new Configuration
    val hdfsURI = "hdfs://master:9000"
    hadoopconf.set("fs.defaultFS", hdfsURI)
    val hdfs = FileSystem.get(hadoopconf)
    val srcPath = new Path(filePath)
    for (i <- 0 until 100000) {
      val destPath1 = new Path("hdfs://master:9000/inputDirectory/Dir1/" + "outputFile" + i)
      hdfs.copyFromLocalFile(false, srcPath, destPath1)
      println("[Directory 1] : File " + (i + 1) + " Copied\n Going to sleep ...")
      val destPath2 = new Path("hdfs://master:9000/inputDirectory/Dir2/" + "outputFile" + i)
      //hdfs.copyFromLocalFile(false, srcPath, destPath2)
      println("[Directory 2] : File " + (i + 1) + " Copied\n Going to sleep ...")
      TimeUnit.SECONDS.sleep(10)
    }
  }

  def fileCopySimulationv1: Unit = {

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

  def fileCopySimulationv6(): Unit = {
    //val filePath = "file:///mnt/datafiles/tf34_20150727_0503_MLPA_ALLOUTL.CSV"
    val filePath = "file:///home/sparkusr/filesDump/tf34_20150727_0503_MLPA_ALLOUTL.CSV"
    val hadoopconf = new Configuration
    val hdfsURI = "hdfs://localhost:9000"
    hadoopconf.set("fs.defaultFS", hdfsURI)
    val hdfs = FileSystem.get(hadoopconf)
    val srcPath = new Path(filePath)
    for (i <- 0 until 5000) {
      val destPath = new Path("hdfs://localhost:9000/inputDirectory6/" + "outputFile" + i)
      hdfs.copyFromLocalFile(false, srcPath, destPath)
      println("[Directory 6] : FIle Copied \n Going to sleep ...")
      TimeUnit.SECONDS.sleep(25)
    }
  }


}
