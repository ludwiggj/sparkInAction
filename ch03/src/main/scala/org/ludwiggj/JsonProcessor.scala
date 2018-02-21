package org.ludwiggj

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source.fromFile

object JsonProcessor {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val homeDir = System.getenv("HOME") + "\\code"
    val inputPath = homeDir + "\\sparkInAction\\github-archive\\2015-03-01-0.json"
    val ghLog: DataFrame = spark.sqlContext.read.json(inputPath)

    println(s"Log has ${ghLog.count()} entries")

    val pushes = ghLog.filter("type = 'PushEvent'")

    println(s"Log has ${pushes.count()} push events")

    pushes.printSchema()
    pushes.show(5)

    val grouped = pushes.groupBy("actor.login").count
    grouped.show(5)

    val ordered = grouped.orderBy(grouped("count").desc)
    ordered.show(5)

    val empPath = homeDir + "\\sparkInAction\\ch03\\src\\main\\resources\\ghEmployees.txt"
    val employees = Set() ++ (
      for {
        line <- fromFile(empPath).getLines
      } yield line.trim
      )

    println(employees.size)

    val sc = spark.sparkContext
    val bcEmployees = sc.broadcast(employees)

    val isEmp: (String => Boolean) = (arg: String) => bcEmployees.value.contains(arg)
    val isEmployee = spark.udf.register("isEmpUdf", isEmp)

    import spark.implicits._

    val filtered = ordered.filter(isEmployee($"login"))
    filtered.show()
  }
}