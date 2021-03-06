package org.ludwiggj

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source.fromFile

object GitHubDay {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val ghLog: DataFrame = spark.sqlContext.read.json(s"$resourcesDir/github-archive/*.json")

    println(s"Log has ${ghLog.count()} entries")

    val pushes: DataFrame = ghLog.filter("type = 'PushEvent'")

    println(s"\nLog has ${pushes.count()} push events")

    val grouped = pushes.groupBy("actor.login").count
    val ordered = grouped.orderBy(grouped("count").desc)

    val empPath = s"$resourcesDir/ghEmployees.txt"
    val employees = Set() ++ (
      for {
        line <- fromFile(empPath).getLines
      } yield line.trim
      )

    val sc = spark.sparkContext
    val bcEmployees = sc.broadcast(employees)

    val isEmp: (String => Boolean) = (arg: String) => bcEmployees.value.contains(arg)
    val isEmployee = spark.udf.register("isEmpUdf", isEmp)

    import spark.implicits._

    val filtered = ordered.filter(isEmployee($"login"))
    println("\nTop 20 employees:")
    filtered.show()
  }
}