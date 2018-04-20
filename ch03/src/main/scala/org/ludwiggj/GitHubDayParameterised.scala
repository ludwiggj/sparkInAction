package org.ludwiggj

import org.apache.spark.sql.SparkSession

import scala.io.Source.fromFile

object GitHubDayParameterised {

  // Run this with the following program arguments:

  // C:\Users\ludwiggj\code\sparkInAction\ch03\src\main\resources\github-archive\*.json
  // C:\Users\ludwiggj\code\sparkInAction\ch03\src\main\resources\ghEmployees.txt
  // C:\Users\ludwiggj\code\sparkInAction\ch03\src\main\resources\out
  // json

  // In one line:
  // C:\Users\ludwiggj\code\sparkInAction\ch03\src\main\resources\github-archive\*.json C:\Users\ludwiggj\code\sparkInAction\ch03\src\main\resources\ghEmployees.txt C:\Users\ludwiggj\code\sparkInAction\ch03\src\main\resources\out json

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("GitHub parameterised")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val sqlContext = spark.sqlContext

    sqlContext.setConf("spark.sql.shuffle.partitions", "8")

    val ghLog = sqlContext.read.json(args(0))
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