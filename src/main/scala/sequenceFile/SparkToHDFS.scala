package sequenceFile


import org.apache.hadoop.io.{BytesWritable, NullWritable, SequenceFile, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkToHDFS {
  def main(args: Array[String]): Unit = {

    //val spark: SparkSession = SparkSession.builder().master("local[*]").appName("soparktest").getOrCreate()
    //读取时必须指定SequenceFile kv的泛型,不然会报错的
    //r: RDD[(BytesWritable, BytesWritable)]
    val config = new SparkConf().setAppName("DataSourceTest").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")
    val file: RDD[(String, String)] = sc.wholeTextFiles("file:\\D:\\临时目录\\input\\*", 1)
    //file.foreach(println)
    //file.saveAsSequenceFile("/test/spark3")
    val files: RDD[(BytesWritable, BytesWritable)] = sc.sequenceFile[BytesWritable, BytesWritable]("/test/spark3")
    files.foreach(println)
    /* //key
     val linesRDD1: RDD[String] = filesRDD.flatMap(_._1.split(","))
     //value
     val linesRDD: RDD[String] = filesRDD.map(_._2)
     /*linesRDD1.foreach(println)
     linesRDD.foreach(println)*/
     val data =List(linesRDD1,linesRDD)
     val dataRDD2: RDD[(String, String)] = sc.parallelize(data)*/
    //filesRDD.saveAsSequenceFile("/test/testspark")
    //rdd.saveAsSequenceFile("data/dir1")
    /*val rdd: RDD[(BytesWritable, BytesWritable)] = sc.sequenceFile[BytesWritable, BytesWritable]("/test/testspark")
    val keys = rdd.keys
    val values = rdd.values
    keys.foreach(println)*/

    //keys.foreach(println)
  }
}