import java.io.File
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Created by sparkusr on 4/09/15.
 */
object FileCopy {
  def main(args: Array[String]) {
    //fileCopySimulationv2()
    fileCopySimulationv3()
    //fileCopySimulationv4()
    //ileCopySimulationv5()
    //fileCopySimulationv6()
  }

  def fileCopySimulationv1: Unit = {
    val pattern = Pattern.compile("[0-9]{1,4}_(.*?)_(.*?)_ALLOUT(.*?).CSV")
    val dirName = "/mnt/AllFiles"
    //val dirName = "/home/sparkusr/filesbk"
    val filesList = new File(dirName).listFiles
    filesList.foreach {
      file =>
        val patternMatcher = pattern.matcher(file.getName)
        if (patternMatcher.find()) {
          val hadoopconf = new Configuration
          val hdfsURI = "hdfs://localhost:9000"
          hadoopconf.set("fs.defaultFS", hdfsURI)
          val hdfs = FileSystem.get(hadoopconf)
          val srcPath = new Path(file.toString)
          val destPath = new Path("hdfs://localhost:9000/inputDirectory/" + file.getName)
          hdfs.copyFromLocalFile(false, srcPath, destPath)
          println(file.getName + " Copied\n Going to sleep for 10 Minutes...")
          TimeUnit.MINUTES.sleep(10)

        }
    }
  }

  def fileCopySimulationv2(): Unit = {
    val filePath = "file:///mnt/datafiles/tf34_20150727_0503_MLPA_ALLOUTL.CSV"
    //val filePath = "file:///home/sparkusr/filesDump/tf34_20150727_0503_MLPA_ALLOUTL.CSV"
    val hadoopconf = new Configuration
    val hdfsURI = "hdfs://localhost:9000"
    hadoopconf.set("fs.defaultFS", hdfsURI)
    val hdfs = FileSystem.get(hadoopconf)
    val srcPath = new Path(filePath)
    for (i <- 0 until 900000000) {
      val destPath = new Path("hdfs://localhost:9000/inputDirectory/" + "outputFile" + i)
      hdfs.copyFromLocalFile(false, srcPath, destPath)
      println("[Directory 2] : FIle Copied\n Going to sleep ...")
      TimeUnit.SECONDS.sleep(25)
    }
  }

  def fileCopySimulationv3(): Unit = {
    //val filePath = "file:///mnt/datafiles/tf34_20150727_0503_MLPA_ALLOUTL.CSV"
    val filePath = "file:///home/sparkusr/filesDump/tf34_20150727_0503_MLPA_ALLOUTL.CSV"
    val hadoopconf = new Configuration
    val hdfsURI = "hdfs://localhost:9000"
    hadoopconf.set("fs.defaultFS", hdfsURI)
    val hdfs = FileSystem.get(hadoopconf)
    val srcPath = new Path(filePath)
    for (i <- 0 until 100000) {
      val destPath = new Path("hdfs://localhost:9000/inputDirectory/" + "outputFile" + i)
      hdfs.copyFromLocalFile(false, srcPath, destPath)
      println("[Directory 3] : FIle Copied\n Going to sleep ...")
      TimeUnit.SECONDS.sleep(25)
    }
  }

  def fileCopySimulationv4(): Unit = {
    //val filePath = "file:///mnt/datafiles/tf34_20150727_0503_MLPA_ALLOUTL.CSV"
    val filePath = "file:///home/sparkusr/filesDump/tf34_20150727_0503_MLPA_ALLOUTL.CSV"
    val hadoopconf = new Configuration
    val hdfsURI = "hdfs://localhost:9000"
    hadoopconf.set("fs.defaultFS", hdfsURI)
    val hdfs = FileSystem.get(hadoopconf)
    val srcPath = new Path(filePath)
    for (i <- 0 until 5000) {
      val destPath = new Path("hdfs://localhost:9000/inputDirectory4/" + "outputFile" + i)
      hdfs.copyFromLocalFile(false, srcPath, destPath)
      println("[Directory 4] : FIle Copied\n Going to sleep ...")
      TimeUnit.SECONDS.sleep(25)
    }
  }

  def fileCopySimulationv5(): Unit = {
    //val filePath = "file:///mnt/datafiles/tf34_20150727_0503_MLPA_ALLOUTL.CSV"
    val filePath = "file:///home/sparkusr/filesDump/tf34_20150727_0503_MLPA_ALLOUTL.CSV"
    val hadoopconf = new Configuration
    val hdfsURI = "hdfs://localhost:9000"
    hadoopconf.set("fs.defaultFS", hdfsURI)
    val hdfs = FileSystem.get(hadoopconf)
    val srcPath = new Path(filePath)
    for (i <- 0 until 5000) {
      val destPath = new Path("hdfs://localhost:9000/inputDirectory5/" + "outputFile" + i)
      hdfs.copyFromLocalFile(false, srcPath, destPath)
      println("[Directory 5] : FIle Copied\n Going to sleep ...")
      TimeUnit.SECONDS.sleep(25)
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
