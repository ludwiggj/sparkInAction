package org.ludwiggj

object GlomExample {

  def main(args: Array[String]): Unit = {

    val spark = sparkSessionBuilder.getOrCreate()

    val list = List.fill(500)(scala.util.Random.nextInt(100))
    val rdd = spark.sparkContext.parallelize(list, 30)

    println(rdd.partitions.size)
    println(rdd.collect().mkString(", "))

    // An array of 30 Int arrays, each containing all elements from a single partition
    val glommedRdd = rdd.glom()
    println(glommedRdd.partitions.size)
    glommedRdd.collect().map(_.mkString(", ")).foreach(println)

    // Collapse into single partition, yielding a single array
    rdd.repartition(1).glom().collect().map(_.mkString(", ")).foreach(println)
  }
}