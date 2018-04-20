package org.ludwiggj

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MoreTransformations {
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

    val idsStr: RDD[Array[String]] = lines.map(_.split(","))
    idsStr.foreach(println)

    val collectedIds: Array[Array[String]] = idsStr.collect()
    collectedIds.foreach(x => println(x.mkString(" ")))

    val flatmappedIds: RDD[String] = lines.flatMap(_.split(","))
    val collectedFlatmappedIds: Array[String] = flatmappedIds.collect()
    collectedFlatmappedIds.foreach(println)

    val intIds: RDD[Int] = flatmappedIds.map(_.toInt)
    intIds.collect().foreach(println)

    val uniqueIds: RDD[Int] = intIds.distinct()
    uniqueIds.collect().foreach(println)

    println(s"${intIds.count()} txs, with ${uniqueIds.count()} distinct")

    // sample - transformation
    val s1 = uniqueIds.sample(withReplacement = false, 0.3)
    println(s"${s1.count()} elements sampled without replacement: ${s1.collect().mkString(" ")}")

    val s2 = uniqueIds.sample(withReplacement = true, 0.75)
    println(s"${s2.count()} elements sampled with replacement: ${s2.collect().mkString(" ")}")

    // takeSample - action
    val s3 = uniqueIds.takeSample(withReplacement = false, 5)
    println(s"${s3.size} elements takeSampled without replacement: ${s3.mkString(" ")}")

    val s4 = uniqueIds.takeSample(withReplacement = true, 5)
    println(s"${s4.size} elements takeSampled with replacement: ${s4.mkString(" ")}")

    val s5 = uniqueIds.take(5)
    println(s"${s5.size} elements taked: ${s5.mkString(" ")}")
  }
}