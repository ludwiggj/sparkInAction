package org.ludwiggj

import org.apache.spark.sql.SparkSession

object LicenseProcessor {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val licenseFile = s"${System.getProperty("user.dir")}/ch02/src/main/resources/LICENSE"

    val sc = spark.sparkContext
    val licLines = sc.textFile(licenseFile)
    println(licLines.count())
  }
}