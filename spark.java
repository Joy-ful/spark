# spark

spark_test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * spark 一行转多行
  */

object logAnalysis_catalina {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val root = this.getClass.getResource("/")
    val conf = new SparkConf().setAppName("logAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val sourceRdd = sc.textFile(root + "catalina/catalina*.log")
    //用于临时保存要合并的行
    var log = new ArrayBuffer[String]()
    val preprocessRDD = sourceRdd
      .map(line => {
        var tlog = " "
        //合并两行
        if (log.length < 2) {
          log += line
          if (log.length == 2) {
            if (log(0).contains(": ")) tlog = log(1) + " " + log(0)
            else tlog = log(0) + " " + log(1)
            log = new ArrayBuffer[String]()
          }
        }
        //合并后的行
        tlog
      })
      .filter(!_.equals(" "))
    import sqlContext.implicits._
    val logDF = preprocessRDD.toDF()
    logDF.show()

    import com.databricks.spark.csv._
    logDF.repartition(1).saveAsCsvFile(root + "catalina.csv")

  }

}

