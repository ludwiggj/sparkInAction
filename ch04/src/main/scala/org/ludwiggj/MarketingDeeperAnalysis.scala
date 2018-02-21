package org.ludwiggj

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MarketingDeeperAnalysis {

  def main(args: Array[String]) {
    val spark = sparkSessionBuilder.getOrCreate()

    val sc = spark.sparkContext

    val tranFile = sc.textFile(inputDataDir + "\\data_transactions.txt")
    val tranData: RDD[Array[String]] = tranFile.map(_.split("#"))

   // Names of products with totals sold
    val txsByProduct: RDD[(Int, Array[String])] = tranData.map(tx => (tx(3).toInt, tx))

    val totalsByProduct = txsByProduct.mapValues(tx => tx(5).toDouble).reduceByKey(_ + _)

    val prodData: RDD[(Int, Array[String])] =
      sc.textFile(inputDataDir + "\\data_products.txt").map(_.split("#")).map(p => (p(0).toInt, p))

    val soldProductTotals = totalsByProduct.join(prodData).collect()

    println(">>>>> soldProductTotals >>>>>")

    soldProductTotals.foreach {
      case (prodId, (total, product)) => println(s"$prodId, $total, [${product.mkString(", ")}]")
    }

    println(s"soldProductTotals (entries) = ${soldProductTotals.size}")

    // List of products, including those that the company didn't sell yesterday
    val allProductTotals = prodData.leftOuterJoin(totalsByProduct).collect()

    println(">>>>> allProductTotals >>>>>")

    allProductTotals.foreach {
      case (prodId, (product, totalOption)) => println(s"$prodId, [${product.mkString(", ")}], $totalOption")
    }

    println(s"allProductTotals (entries) = ${allProductTotals.size}")

    println("Products that didn't sell:")

    allProductTotals.filter { case (_, (_, amount)) => amount.isEmpty }.foreach {
      case (prodId, (product, totalOption)) => println(s"$prodId, [${product.mkString(", ")}], $totalOption")
    }
  }
}