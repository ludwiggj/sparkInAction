package scala

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val homeDir = System.getenv("HOME")
    val inputPath = homeDir + "\\sparkInAction\\github-archive\\2015-03-01-0.json"
    val ghLog = spark.sqlContext.read.json(inputPath)

    println(s"Log has ${ghLog.count()} entries")
  }
}