package org

import org.apache.spark.sql.SparkSession

package object ludwiggj {
  val sparkSessionBuilder = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")

  val homeDir = System.getenv("HOME") + "\\code"
  val dataDir = homeDir + "\\sparkInAction\\data\\ch04"
  val inputDataDir = dataDir + "\\input"
  val outputDataDir = dataDir + "\\output"
}
