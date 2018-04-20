package org.ludwiggj

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Transformations {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val sc = spark.sparkContext

    val numbers: RDD[Int] = sc.parallelize(10 to 50 by 10)

    println("Numbers:")
    numbers.foreach(x => println(x))

    val numbersSquared: RDD[Int] = numbers.map(num => num * num)

    println("\nSquared Numbers:")
    numbersSquared.foreach(x => println(x))

    println("\nReversed stringy numbers:")
    val reversed: RDD[String] = numbersSquared.map(x => x.toString.reverse)
    reversed.foreach(x => println(x))

    println("\nFirst 4:")
    reversed.top(4).foreach(println)
  }
}