package org.ludwiggj

object Zipping {
  def main(args: Array[String]): Unit = {
    val spark = sparkSession(noOfNodes = Some(3))

    val sc = spark.sparkContext

    val rdd1 = sc.parallelize(List(7,8,9))
    val rdd2 = sc.parallelize(List("a1","a2","a3"))

    rdd1.zip(rdd2).collect().foreach(println(_))

    val rdd3 = sc.parallelize(1 to 10, 10)
    val rdd4 = sc.parallelize((1 to 8).map(x => "n" + x), 10)

    rdd3.zipPartitions(rdd4, true)((iter1, iter2) => {
      iter1.zipAll(iter2, -1, "empty").map(
        {
          case(x1, x2) => x1 + "-" + x2
        }
      )
    }
    ).collect().map(println(_))
  }
}

