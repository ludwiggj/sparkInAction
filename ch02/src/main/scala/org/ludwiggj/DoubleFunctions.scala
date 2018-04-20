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

    println(intIds.collect().mkString(" "))

    println(intIds.sum)
    println(intIds.mean)
    println(intIds.variance)
    println(intIds.stdev)

    val stats = intIds.stats()

    println(stats.min)
    println(stats.max)
    println(stats.count)
    println(stats.sum)
    println(stats.mean)
    println(stats.variance)
    println(stats.stdev)

    // histograms
    println(intIds.histogram(Array(1.0, 50.0, 100.0)).mkString(" "))
    val (r, c) = intIds.histogram(3)
    println(r.mkString(" "))
    println(c.mkString(" "))
  }
}