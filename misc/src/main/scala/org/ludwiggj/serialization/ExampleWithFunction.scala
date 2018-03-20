package org.ludwiggj.serialization

import org.apache.spark.sql.SparkSession

object ExampleWithFunction {

  object Spark {
    val sparkSession = SparkSession
      .builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    val sc = sparkSession.sparkContext
  }

  def main(args: Array[String]): Unit = {
    new Testing().doIT
  }

  class Testing {

    val rddList = Spark.sc.parallelize(List(1, 2, 3))

    def doIT = {
      //again calling the fucntion someFunc
      val after = rddList.map(someFunc)
      //this will crash (spark lazy)
      after.collect().map(println)
    }

    val someFunc = (a: Int) => a + 1
  }
}