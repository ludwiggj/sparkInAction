package org.ludwiggj

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MarketingDeeperAnalysis {

  def main(args: Array[String]) {
    val spark = sparkSessionBuilder.getOrCreate()

    val sc = spark.sparkContext

    // Load transactions
    // -----------------
    //
    // Each line in data_transactions has the format:
    //                0      1             2            3          4               5
    // transaction date # time # customer ID # product ID # quantity # product price

    val tranFile = sc.textFile(inputDataDir + "\\data_transactions.txt")
    val tranData: RDD[Array[String]] = tranFile.map(_.split("#"))

   // Names of products with totals sold
    val txsByProductId: RDD[(Int, Array[String])] = tranData.map(tx => (tx(3).toInt, tx))

    val totalSoldByProductId = txsByProductId.mapValues(tx => tx(5).toDouble).reduceByKey(_ + _)

    // Load products
    // -----------------
    //
    // Each line in data_products has the format:
    //          0              1                    2                    3
    // product ID # product name # product unit price # quantity available
    val prodData: RDD[(Int, Array[String])] =
      sc.textFile(inputDataDir + "\\data_products.txt").map(_.split("#")).map(p => (p(0).toInt, p))

    val soldProductTotals = totalSoldByProductId.join(prodData).collect()

    println(">>>>> soldProductTotals >>>>>")

    soldProductTotals.foreach {
      case (prodId, (total, product)) => println(s"$prodId, $total, [${product.mkString(", ")}]")
    }

    println(s"soldProductTotals (entries) = ${soldProductTotals.size}")

    // List of all products, including those that didn't sell yesterday
    val allProductTotals = prodData.leftOuterJoin(totalSoldByProductId).collect()

    println(">>>>> allProductTotals >>>>>")

    allProductTotals.foreach {
      case (prodId, (product, totalOption)) => println(s"$prodId, [${product.mkString(", ")}], $totalOption")
    }

    println(s"allProductTotals (entries) = ${allProductTotals.size}\n")

    // List of all products that didn't sell yesterday

    println(">>>>> Products that didn't sell yesterday >>>>>")

    val productsThatDidNotSell = allProductTotals.filter { case (_, (_, amount)) => amount.isEmpty }

    productsThatDidNotSell.foreach {
      case (prodId, (product, totalOption)) => println(s"$prodId, [${product.mkString(", ")}], $totalOption")
    }

    println(s"Products that didn't sell yesterday (entries) = ${productsThatDidNotSell.size}")

    // Now try right outer join

    println(">>>>> Products that didn't sell yesterday (AGAIN) >>>>>")

    val allProductTotalsRight = totalSoldByProductId.rightOuterJoin(prodData).collect()

    val missingProducts = allProductTotalsRight.filter(x => x._2._1 == None).map(x => x._2._2)
    missingProducts.foreach(p => println(p.mkString(", ")))

    println(s"Products that didn't sell yesterday (AGAIN) (entries) = ${missingProducts.size}")

    // Now try a full outer join

    println(">>>>> All product and sales info (full outer join) >>>>>")

    val caboodle = prodData.fullOuterJoin(totalSoldByProductId).collect()

    caboodle.foreach {
      case (prodId, (product, totalOption)) => println(s"$prodId, [${product.map(_.mkString(", "))}], $totalOption")
    }

    val noProductSales = caboodle.filter { case (_, (_, amount)) => amount.isEmpty }
    println(noProductSales.size)
  }
}