package org.ludwiggj

object Cartesian {
  def main(args: Array[String]): Unit = {
    val spark = sparkSessionBuilder.getOrCreate()

    val sc = spark.sparkContext

    val rdd1 = sc.parallelize(List(7,8,9))
    val rdd2 = sc.parallelize(List(1,2,3))

    rdd1.cartesian(rdd2).collect().foreach(println(_))

    rdd1.cartesian(rdd2).filter(p => p._1 % p._2 == 0).collect().foreach(println(_))
  }
}

