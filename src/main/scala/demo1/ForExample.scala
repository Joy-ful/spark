package demo1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType};


object ForExample {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("SequenceFileTest").setMaster("local[3]")

    val sc = new SparkContext(sparkconf)
    val sqlContext = new SQLContext(sc)
    val people = sc.textFile("")
    val scheamString = "name age"
    val schema =
      StructType(
        scheamString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val value: RDD[Row] = people.map(_.split(" ")).map(p => Row(p(0), p(1).trim))
    val frame = sqlContext
      .createDataFrame(value, schema)
    frame.registerTempTable("prople")
    val results = sqlContext
      .sql("select * from people")

    results.rdd.map(t => "Name: " + t(0)).collect().foreach(println)

    var df: DataFrame = sqlContext.read.load("")
    df.select("name").write.save("")



  }
}
