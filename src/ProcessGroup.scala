import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Created by sparkusr on 27/07/15.
 */
@serializable object ProcessGroup {
  val hadoopConf = new Configuration()
  val fs = FileSystem.get(hadoopConf)
  def apply(groupName: String, records: Iterable[String]): Unit = {
    val outFileStream = fs.create(new Path("/output_dir/" + groupName))
    records.map(_ + "\n").foreach(outFileStream.writeUTF)
    outFileStream.close()
  }
}
