package org.ludwiggj.ex_03

import org.apache.spark.rdd.RDD
import org.ludwiggj.sparkSession

object GlomExample {

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(noOfNodes = Some(3))

    val list = List.fill(500)(scala.util.Random.nextInt(100))
    val rdd: RDD[Int] = spark.sparkContext.parallelize(list, numSlices = 30)

    // 500 random numbers distributed across 30 partitions
    println(s"RDD partitions = ${rdd.partitions.size}")
    println(rdd.collect().mkString(", "))

    // An array of 30 Int arrays, each containing all elements from a single partition
    val glommedRdd: RDD[Array[Int]] = rdd.glom()
    println(s"Glommed RDD partitions = ${glommedRdd.partitions.size}")
    glommedRdd.collect().map(_.mkString(", ")).foreach(println)

    // Collapse into single partition, yielding a single array
    println("Collapsed into single partition then glommed")
    rdd.repartition(1).glom().collect().map(_.mkString(", ")).foreach(println)

    // Do without the glom
    println("Collapsed into single partition, not glommed")
    println(rdd.repartition(1).collect().mkString(", "))

    // Pause so we can look at the jobs/stages via Spark UI
    System.in.read()
    spark.stop()
  }
}