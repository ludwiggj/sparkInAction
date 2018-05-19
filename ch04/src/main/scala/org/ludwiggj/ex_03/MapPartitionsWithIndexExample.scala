package org.ludwiggj.ex_03

import org.apache.spark.rdd.RDD
import org.ludwiggj.sparkSession

object MapPartitionsWithIndexExample {

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(noOfNodes = Some(3))

    // def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) ⇒ Iterator[U], preservesPartitioning: Boolean = false)
    //                              (implicit arg0: ClassTag[U]): RDD[U]

    // Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original
    // partition. preservesPartitioning indicates whether the input function preserves the partitioner, which should be
    // false unless this is a pair RDD and the input function doesn't modify the keys.

    // In this example, we add partition no to each element of an RDD

    val rdd: RDD[String] = spark.sparkContext.parallelize(
      List("yellow", "red", "blue", "cyan", "black"), numSlices = 3
    )

    println(rdd.partitions.length)

    val mappedRDD: RDD[String] = rdd.mapPartitionsWithIndex {
      // 'index' represents the partition number
      // 'iterator' to iterate through all elements in the partition
      (index, iterator) => {
        val myList = iterator.toList

        println(s"$myList called in Partition -> $index")

        // In a normal user case, we will do the initialization(ex : initializing database)
        // before iterating through each element
        myList.map(x => x + " -> " + index).iterator
      }
    }

    mappedRDD.collect().foreach(println)

    // Pause so we can look at the jobs/stages via Spark UI
//    System.in.read()
//    spark.stop()
  }
}