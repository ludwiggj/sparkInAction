package org.ludwiggj

import org.apache.spark.sql.SparkSession

import scala.io.Source.fromFile

object GitHubDayParameterised {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .getOrCreate()

    val ghLog = spark.sqlContext.read.json(args(0))
    val pushes = ghLog.filter("type = 'PushEvent'")
    val grouped = pushes.groupBy("actor.login").count
    val ordered = grouped.orderBy(grouped("count").desc)

    val employees = Set() ++ (
      for {
        line <- fromFile(args(1)).getLines
      } yield line.trim
      )

    val sc = spark.sparkContext
    val bcEmployees = sc.broadcast(employees)

    val isEmp = user => bcEmployees.value.contains(user)
    val isEmployee = spark.udf.register("isEmpUdf", isEmp)

    import spark.implicits._

    val filtered = ordered.filter(isEmployee($"login"))

    filtered.write.format(args(3)).save(args(2))
  }
}