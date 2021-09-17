package demo1

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object HDFSUtils {

  def main(args: Array[String]): Unit = {
    val status = uploadFile("D:\\临时目录\\input", "/test/", "file1.txt")
    if (status) println("上传成功！") else println("上传失败")
  }

  /**
   * 本地文件上传到 hdfs
   *
   * @param localDirectory 本地目录
   * @param hdfsDirectory  hdfs目录
   * @param fileName       文件名称
   * @return true：上传成功  flase：上传失败
   */
  def uploadFile(localDirectory: String, hdfsDirectory: String, fileName: String): Boolean = {

    val conf = new Configuration
    val HDFS_PATH: String = "hdfs:node1:9870"
    val fileSystem = FileSystem.get(URI.create(HDFS_PATH), conf)

    val localFullPath = localDirectory + "/" + fileName
    val hdfsFullPath = hdfsDirectory + "/" + fileName

    val localPath = new Path(localFullPath)
    val hdfspath = new Path(hdfsDirectory)
    val hdfsfilepath = new Path(hdfsFullPath)

    val status1 = new File(localFullPath).isFile
    val status2 = fileSystem.isDirectory(hdfspath)
    val status3 = fileSystem.exists(hdfsfilepath)
    println(status1, status2, !status3)

    // 本地文件存在,hdfs目录存在,hdfs文件不存在(防止文件覆盖)
    if (status1 && status2 && !status3) {
      fileSystem.copyFromLocalFile(false, false, localPath, hdfsfilepath)
      return true
    }
    false
  }

}