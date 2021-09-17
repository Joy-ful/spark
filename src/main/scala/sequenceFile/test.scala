package sequenceFile

import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SequenceFileTest")
    conf.setMaster("local[3]")
    val sc = new SparkContext(conf)
    val data = List(("ABC", 1), ("BCD", 2), ("CDE", 3), ("DEF", 4), ("FGH", 5))
    val rdd = sc.parallelize(data, 1)
    val dir = "/test/tests" + System.currentTimeMillis()
    rdd.saveAsSequenceFile(dir)
    val rdd2 = sc.sequenceFile[String, Int](dir + "/part-00000")
    println(rdd2.collect().map(elem => (elem._1 + ", " + elem._2)).toList)
    sc.stop()
    //1 创建配置文件对象
  }
}
