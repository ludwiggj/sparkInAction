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

    val licenseFile = s"$resourcesDir/LICENSE"

    val sc = spark.sparkContext
    val licLines = sc.textFile(licenseFile)
    println(s"No of lines: ${licLines.count()}")

    val licenseLines = licLines.filter(line => line.contains("license"))
    println(s"\nNo of license lines: ${licenseLines.count()}")

    println("\nHere are the lines:")
    licenseLines.foreach(println)
  }
}