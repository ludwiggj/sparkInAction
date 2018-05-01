package org

import java.io.File

import org.apache.spark.sql.SparkSession

package object ludwiggj {

  def sparkSession(noOfNodes: Option[Int] = None,
                   parallelism: Option[Int] = None): SparkSession = {
    var sparkBuilder = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")

    sparkBuilder = noOfNodes match {
      case Some(noOfNodesVal) =>
        sparkBuilder.master(s"local[$noOfNodesVal]")
      case None =>
        sparkBuilder.master("local[*]")
    }

    sparkBuilder = parallelism match {
      case Some(parallelismVal) =>
        sparkBuilder.config("spark.default.parallelism", parallelismVal)
      case None =>
        sparkBuilder
    }

    sparkBuilder.getOrCreate()
  }

  val resourcesDir = s"${System.getProperty("user.dir")}/ch04/src/main/resources"

  val inputDataDir = s"$resourcesDir/input"
  val outputDataDir = s"$resourcesDir/output"

  // This only works if directory does not contain any sub-directories
  def deleteDirIfExists(dir: String): Unit = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.foreach(_.delete())
      d.delete()
    }
  }
}
