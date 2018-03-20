package org.ludwiggj.serialization

import org.apache.spark.sql.SparkSession

object ExampleWithObject {

  def main(args: Array[String]): Unit = {

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

    def someFunc(a: Int) = a + 1

    //calling function outside closure
    val after = rddList.map(someFunc)

    after.collect().map(println(_))
  }
}