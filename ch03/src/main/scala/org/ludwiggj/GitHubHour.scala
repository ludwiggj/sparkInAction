package org.ludwiggj

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.io.Source.fromFile

object GitHubHour {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val inputPath = s"$resourcesDir/github-archive/2015-03-01-0.json"

    // Read data in
    val ghLog: DataFrame = spark.sqlContext.read.json(inputPath)

    println(s"Log has ${ghLog.count()} entries")

    // Filter for push events
    val pushes: DataFrame = ghLog.filter("type = 'PushEvent'")

    println(s"Log has ${pushes.count()} push events")

    // Schema and first five entries
    println("\nHere's the schema:")
    pushes.printSchema()
    println("\nHere's the first 5 entries:")
    pushes.show(5)

    // Group by username
    println("\nGrouped by username:")
    val grouped: DataFrame = pushes.groupBy("actor.login").count
    grouped.show(5)

    // Top 5 pushers
    val ordered: DataFrame = grouped.orderBy(grouped("count").desc)
    println("\nTop 5 pushers:")
    ordered.show(5)

    val empPath = s"$resourcesDir/ghEmployees.txt"
    val employees = Set() ++ (
      for {
        line <- fromFile(empPath).getLines
      } yield line.trim
      )

    println(s"We have ${employees.size} employees")

    val sqlContext = spark.sqlContext

    // Each time there is a shuffle, Spark needs to decide how many partitions
    // the shuffle RDD will have. The default is 200.
    println(s"Shuffle partitions: ${sqlContext.getConf("spark.sql.shuffle.partitions")}")
    sqlContext.setConf("spark.sql.shuffle.partitions", "8")
    println(s"Shuffle partitions: ${sqlContext.getConf("spark.sql.shuffle.partitions")}")

    val sc = spark.sparkContext
    val bcEmployees = sc.broadcast(employees)

    val isEmp: (String => Boolean) = (arg: String) => bcEmployees.value.contains(arg)
    val isEmployee = spark.udf.register("isEmpUdf", isEmp)

    import spark.implicits._

    val filtered: DataFrame = ordered.filter(isEmployee($"login"))
    println("\nTop 20 pushers who work for our company:")
    filtered.show()
  }
}