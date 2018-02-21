package org.ludwiggj

import org.apache.spark.sql.DataFrame

object CoalesceAndPartitionExample {

  def displayPartitionCount(df: DataFrame) ={
    println(s"No of partitions = ${df.rdd.partitions.size}")
  }

  def writePartitionData(df: DataFrame, dfDir: String) = {
    val dfPath = s"$outputDataDir\\$dfDir"
    df.write.mode("overwrite").csv(dfPath)
  }

  def main(args: Array[String]): Unit = {

    val spark = sparkSessionBuilder.getOrCreate()

    val oneToSixteen = (1 to 16).toList
    import spark.implicits._

    val df1 = oneToSixteen.toDF
    displayPartitionCount(df1)
    writePartitionData(df1, "partitions_df1")

    // (1) coalesce combines existing partitions to avoid a full shuffle

    // The coalesce method reduces the number of partitions in a DataFrame
    // This algorithm is fast in certain situations because it minimizes data movement.
    val df2 = df1.coalesce(2)
    displayPartitionCount(df2)
    writePartitionData(df2, "partitions_df2")

    // You can try to increase the number of partitions with coalesce, but it won’t work
    // The coalesce algorithm changes the number of nodes by moving data from some partitions to existing partitions.
    // This algorithm obviously cannot increase the number of partitions
    val df3 = df1.coalesce(12)
    displayPartitionCount(df3)

    // (2) The repartition algorithm does a full shuffle of the data and creates equal sized partitions of data

    // The repartition method can be used to either increase or decrease the number of partitions in a DataFrame
    // The repartition algorithm does a full data shuffle and equally distributes the data among the partitions.
    // It does not attempt to minimize data movement like the coalesce algorithm.
    val df4 = df1.repartition(2)
    displayPartitionCount(df4)
    writePartitionData(df4, "partitions_df4")

    // The repartition method can be used to increase the number of partitions as well
    // The repartition method does a full shuffle of the data, so the number of partitions can be increased
    val df5 = df1.repartition(12)
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
  }
}
