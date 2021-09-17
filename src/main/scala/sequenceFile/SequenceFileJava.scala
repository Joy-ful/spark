package sequenceFile

import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, SequenceFile}
import org.apache.hadoop.util.ReflectionUtils


object SequenceFileJava {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration
    val HDFS_PATH: String = "hdfs:10.10.13.179:9870"
    val fs = FileSystem.get(URI.create(HDFS_PATH), conf)
    //file:\D:\临时目录\input            /test/spark
    val path = "/test/spark"
    val output = new Path("/test/test_spark")
    val writer = SequenceFile.createWriter(fs, conf, output, classOf[BytesWritable], classOf[BytesWritable], SequenceFile.CompressionType.NONE)
    val fileStatuses = fs.listStatus(new Path(path))

    //num.set(bytes1);
    for (fileStatus <- fileStatuses) { //byte fileText = new Byte(fileStatus.getPath().toString());
      val name = fileStatus.getPath.getName
      val bytes1 = IOUtils.toByteArray(name)
      System.out.println("--------" + bytes1.length)
      val b = new BytesWritable(bytes1)
      //System.out.println("文件地址："+fileText);
      val in = fs.open(new Path(fileStatus.getPath.toString))
      val bytes = IOUtils.toByteArray(in)
      in.read(bytes)
      val value = new BytesWritable(bytes)
      System.out.println("key的值：" + b)
      System.out.println("key的长度：" + b.getLength)
      System.out.println("value的值：" + value)
      System.out.println("value的长度：" + value.getLength)
      //long length = writer.getLength();
      System.out.println("*******************************************")
      writer.append(b, value)
    }
    writer.close()
    System.out.println("-----------------------------------------------------------------------------------------------")
    val path1 = new Path("/test/test_spark")
    val reader = new SequenceFile.Reader(fs, path1, conf)
    val keys = ReflectionUtils.newInstance(reader.getKeyClass, conf).asInstanceOf[BytesWritable]
    val values = ReflectionUtils.newInstance(reader.getValueClass, conf).asInstanceOf[BytesWritable]
    val pos = reader.getPosition
    System.out.println("头：" + pos)
    System.out.println("******************")
    while (reader.next(keys, values)) {
      System.out.println("文件指针：" + reader.getPosition())
      System.out.println("key的值：" + keys)
      System.out.println("key的长度：" + keys.getLength())
      System.out.println("value的值：" + values)
      System.out.println("value的长度：" + values.getLength())
      System.out.println("--------------------------------------------------")

      reader.close()
    }
  }
}