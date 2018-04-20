package org.ludwiggj

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DoubleFunctions {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val clientIdFile = s"$resourcesDir/client-ids.log"

    val sc = spark.sparkContext
    val lines: RDD[String] = sc.textFile(clientIdFile)

    val flatmappedIds: RDD[String] = lines.flatMap(_.split(","))
    val intIds: RDD[Int] = flatmappedIds.map(_.toInt)

    println(s"Client ids: ${intIds.collect().mkString(" ")}")

    println(s"Sum: ${intIds.sum}")
    println(s"Mean: ${intIds.mean}")
    println(s"Variance: ${intIds.variance}")
    println(s"Standard Deviation: ${intIds.stdev}")

    val stats = intIds.stats()

    println(s"Min: ${stats.min}")
    println(s"Max: ${stats.max}")
    println(s"Count: ${stats.count}")
    println(s"Sum: ${stats.sum}")
    println(s"Mean: ${stats.mean}")
    println(s"Variance: ${stats.variance}")
    println(s"Standard Deviation: ${stats.stdev}")

    // histograms
    println(s"Histogram on (1,50,100): ${intIds.histogram(Array(1.0, 50.0, 100.0)).mkString(" ")}")
    val (r, c) = intIds.histogram(3)
    println(s"Histogram on 3 equally spaced regions, (${r.mkString(",")}) = (${c.mkString(",")})")
  }
}