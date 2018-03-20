package org.ludwiggj.serialization

import org.apache.spark.sql.SparkSession

object ExampleWithClass {

  def main(args: Array[String]): Unit = {
    new Testing().doIT
  }

  class Testing {
    val sparkSession = SparkSession
      .builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val list = List(1, 2, 3)
    val rddList = sc.parallelize(list)

    def doIT = {
      //again calling the fucntion someFunc
      val after = rddList.map(someFunc(_))
      //this will crash (spark lazy)
      after.collect().map(println(_))
    }

    def someFunc(a: Int) = a + 1
  }

}