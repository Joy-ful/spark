package demo1

import org.apache.hadoop.io.BytesWritable
import org.apache.spark.api.java.JavaSparkContext.fromSparkContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark sql query hdfs file")
    val sc = new SparkContext(conf)
    val line = sc.textFile("/test/test.txt")
    val worf = line.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    worf.foreach(println)
    val listRDD = sc.parallelize(List(1,2,3,4,5))
    val result = listRDD.map(x => (x,1))
    result.foreach(println)

    val list = List("ss", "rdd", "egerg", 324, 123)
    val r = sc.makeRDD(list, 1)
    r.saveAsObjectFile("hdfs:/your/path/list")

/*    val file = sc.sequenceFile[BytesWritable]("hdfs:/your/path/list/part-00000")
    val bw = file.take(1).apply(0)._2
    val bs = bw.getBytes

    import java.io._
    val bis = new ByteArrayInputStream(bs)
    val ois = new ObjectInputStream(bis)
    val rdd1 = sc.parallelize(1 to 100)
    val functionToInt = rdd1.reduce(_+_)
    println(functionToInt)
    val s1 = Seq(1, 2, 3, 4, 5)
    s1.reverse.foreach(println)*/


  }
}
