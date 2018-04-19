package org

import org.apache.spark.sql.SparkSession

package object ludwiggj {
  val sparkSessionBuilder = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")

  val resourcesDir = s"${System.getProperty("user.dir")}/ch04/src/main/resources"

  val inputDataDir = s"$resourcesDir/input"
  val outputDataDir = s"$resourcesDir/output"
}
