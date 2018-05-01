package org.ludwiggj

import org.apache.spark.rdd.RDD

object MarketingDeeperAnalysis {

  def main(args: Array[String]) {
    val spark = sparkSession(noOfNodes = Some(3))

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

    val totalSoldByProductId: RDD[(Int, Double)] = txsByProductId.mapValues(tx => tx(5).toDouble).reduceByKey(_ + _)

    // Load products
    // -----------------
    //
    // Each line in data_products has the format:
    //          0              1                    2                    3
    // product ID # product name # product unit price # quantity available
    val prodData: RDD[(Int, Array[String])] =
    sc.textFile(inputDataDir + "\\data_products.txt").map(_.split("#")).map(p => (p(0).toInt, p))

    val soldProductTotals: RDD[(Int, (Double, Array[String]))] = totalSoldByProductId.join(prodData)

    println(">>>>> soldProductTotals >>>>>")

    soldProductTotals.collect().foreach {
      case (prodId, (total, product)) => println(s"$prodId, $total, [${product.mkString(", ")}]")
    }

    println(s"soldProductTotals (entries) = ${soldProductTotals.collect().size}")

    // List of all products, including those that didn't sell yesterday
    val allProductTotals: Array[(Int, (Array[String], Option[Double]))] = prodData.leftOuterJoin(totalSoldByProductId).collect()

    println(">>>>> allProductTotals >>>>>")

    allProductTotals.foreach {
      case (prodId, (product, totalOption)) => println(s"$prodId, [${product.mkString(", ")}], $totalOption")
    }

    println(s"allProductTotals (entries) = ${allProductTotals.size}\n")

    // List of all products that didn't sell yesterday

    println(">>>>> Products that didn't sell yesterday (leftOuterJoin) >>>>>")

    val productsThatDidNotSell = allProductTotals.filter { case (_, (_, amount)) => amount.isEmpty }

    productsThatDidNotSell.foreach {
      case (prodId, (product, totalOption)) => println(s"$prodId, [${product.mkString(", ")}], $totalOption")
    }

    println(s"Products that didn't sell yesterday (entries) = ${productsThatDidNotSell.size}")

    // Now try right outer join

    println(">>>>> Products that didn't sell yesterday (rightOuterJoin) >>>>>")

    val allProductTotalsRight: Array[(Int, (Option[Double], Array[String]))] = totalSoldByProductId.rightOuterJoin(prodData).collect()

    val missingProducts = allProductTotalsRight.filter(x => x._2._1 == None).map(x => x._2._2)
    missingProducts.foreach(p => println(p.mkString(", ")))

    println(s"Products that didn't sell yesterday (AGAIN) (entries) = ${missingProducts.size}")

    // Now try a full outer join

    println(">>>>> All product and sales info (full outer join) >>>>>")

    val caboodle: Array[(Int, (Option[Array[String]], Option[Double]))] = prodData.fullOuterJoin(totalSoldByProductId).collect()

    caboodle.foreach {
      case (prodId, (productOption, totalOption)) => println(s"$prodId, [${productOption.map(_.mkString(", "))}], $totalOption")
    }

    val noProductSales = caboodle.filter { case (_, (_, amount)) => amount.isEmpty }
    println(s"Number of products that didn't sell: ${noProductSales.size}")

    // Can also use subtractByKey to find the products that haven't sold
    val missingProds = prodData.subtractByKey(totalSoldByProductId).values

    println(">>>>> Products that didn't sell yesterday (subtractByKey) >>>>>")
    missingProds.foreach(p => println(p.mkString(", ")))

    // Cogroups
    println(">>>>> All product and sales info (cogroup) >>>>>")
    val productTotalsCogroup = totalSoldByProductId.cogroup(prodData)

    productTotalsCogroup.foreach(
      x => println(s"Id [${x._1}] Total [${x._2._1.headOption.getOrElse(0)}] Product [${x._2._2.headOption.getOrElse(Array()).mkString(", ")}]")
    )

    val sortedProducts = soldProductTotals.sortBy(_._2._2(1))
    sortedProducts.collect().foreach {
      case (prodId, (total, product)) => println(s"$prodId, $total, [${product.mkString(", ")}]")
    }
  }
}