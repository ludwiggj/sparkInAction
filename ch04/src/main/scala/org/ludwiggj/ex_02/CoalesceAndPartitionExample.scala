package org.ludwiggj.ex_02

import org.ludwiggj.deleteDirIfExists
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ludwiggj.outputDataDir
import org.ludwiggj.sparkSession

object CoalesceAndPartitionExample {

  def displayPartitionCount(df: DataFrame): Unit ={
    println(s"No of partitions = ${df.rdd.partitions.size}")
  }

  def writePartitionData(df: DataFrame, partitionId: String): Unit = {
    val dfPath = s"$outputDataDir\\partitions_df$partitionId"
    deleteDirIfExists(dfPath)
    df.write.mode("overwrite").csv(dfPath)
  }

  def dataFrame(spark: SparkSession): DataFrame = {
    val oneToSixteen = (1 to 16).toList
    import spark.implicits._

    println(s"spark.default.parallelism: [${spark.conf.getOption("spark.default.parallelism")}]")
    println(s"No of cores: ${Runtime.getRuntime().availableProcessors()}")

    val dataFrame = oneToSixteen.toDF
    displayPartitionCount(dataFrame)
    dataFrame
  }

  // See https://hackernoon.com/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4
  def main(args: Array[String]): Unit = {

    // Master[*], no parallelism set
    // No of partitions defaults to number of cores
    var spark = sparkSession()
    writePartitionData(dataFrame(spark), "1_8")
    spark.stop()

    // Master[3], no parallelism set
    // No of partitions = 3
    spark = sparkSession(noOfNodes = Some(3))
    writePartitionData(dataFrame(spark), "1_3")
    spark.stop()

    // Master[*], parallelism set to 5
    // No of partitions = 5
    spark = sparkSession(parallelism = Some(5))
    writePartitionData(dataFrame(spark), "1_5")
    spark.stop()

    // Master[4], parallelism set to 6
    // No of partitions = 6 (parallelism trumps!)
    spark = sparkSession(noOfNodes = Some(4), parallelism = Some(6))
    writePartitionData(dataFrame(spark), "1_6")
    spark.stop()
/*
    // (1) coalesce combines existing partitions to avoid a full shuffle

    // The coalesce method reduces the number of partitions in a DataFrame
    // This algorithm is fast in certain situations because it minimizes data movement.
    val df2 = dataFrame1.coalesce(2)
    displayPartitionCount(df2)
    writePartitionData(df2, "partitions_df2")

    // You can try to increase the number of partitions with coalesce, but it won’t work
    // The coalesce algorithm changes the number of nodes by moving data from some partitions to existing partitions.
    // This algorithm obviously cannot increase the number of partitions
    val df3 = dataFrame1.coalesce(12)
    displayPartitionCount(df3)

    // (2) The repartition algorithm does a full shuffle of the data and creates equal sized partitions of data

    // The repartition method can be used to either increase or decrease the number of partitions in a DataFrame
    // The repartition algorithm does a full data shuffle and equally distributes the data among the partitions.
    // It does not attempt to minimize data movement like the coalesce algorithm.
    val df4 = dataFrame1.repartition(2)
    displayPartitionCount(df4)
    writePartitionData(df4, "partitions_df4")

    // The repartition method can be used to increase the number of partitions as well
    // The repartition method does a full shuffle of the data, so the number of partitions can be increased
    val df5 = dataFrame1.repartition(12)
    displayPartitionCount(df5)
    writePartitionData(df5, "partitions_df5")

    // Repartition by column

    val people = List(
      (10, "blue"),
      (13, "red"),
      (15, "blue"),
      (99, "red"),
      (67, "blue")
    )
    val peopleDf = people.toDF("age", "color")
    displayPartitionCount(peopleDf)
    writePartitionData(peopleDf, "partitions_peopleDf")

    val colourDf = peopleDf.repartition($"color")
    displayPartitionCount(colourDf)

    // When partitioning by a column, Spark will create a minimum of 200 partitions by default.
    // This example will have two partitions with data and 198 empty partitions.

    // Partition 00091
    // 13,red
    // 99,red
    // Partition 00168
    // 10,blue
    // 15,blue
    // 67,blue
    */
  }
}