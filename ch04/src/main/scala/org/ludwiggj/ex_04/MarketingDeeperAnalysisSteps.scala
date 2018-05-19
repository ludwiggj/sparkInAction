package org.ludwiggj.ex_04

import org.apache.spark.rdd.RDD
import org.ludwiggj.{inputDataDir, sparkSession}

object MarketingDeeperAnalysisSteps {

  def main(args: Array[String]) {
    val spark = sparkSession(noOfNodes = Some(3))

    val sc = spark.sparkContext

    // Load transactions
    // -----------------
    //
    // Each line in data_transactions has the format:
    //                0      1             2            3          4               5
    // transaction date # time # customer ID # product ID # quantity # product price

    val txFileRDD = sc.textFile(inputDataDir + "\\data_transactions.txt")
    val txRDD: RDD[Array[String]] = txFileRDD.map(_.split("#"))

    println(s"txRDD partition no = ${txRDD.partitions.length}")

    // Load products
    // -----------------
    //
    // Each line in data_products has the format:
    //          0              1                    2                    3
    // product ID # product name # product unit price # quantity available
    val productRDD: RDD[(Int, Array[String])] = sc
      .textFile(inputDataDir + "\\data_products.txt")
      .map(_.split("#"))
      .map(p => (p(0).toInt, p))

    productRDD.cache()

    println(s"productRDD partition no = ${productRDD.partitions.length}")

    // (0) Job 0 - sold product totals (join)

    // Names of products with totals sold
    val txsByProductIdRDD: RDD[(Int, Array[String])] = txRDD.map(tx => (tx(3).toInt, tx))

    println(s"txsByProductIdRDD partition no = ${txsByProductIdRDD.partitions.length}")

    val totalSoldByProductIdRDD: RDD[(Int, Double)] =
      txsByProductIdRDD.mapValues(tx => tx(5).toDouble).reduceByKey(_ + _)

    println(s"totalSoldByProductIdRDD partition no = ${totalSoldByProductIdRDD.partitions.length}")

    // Now we need to join the data together - use an inner join aka "join"
    val soldProductTotalsRDD: RDD[(Int, (Double, Array[String]))] = totalSoldByProductIdRDD.join(productRDD)

    println(s"soldProductTotalsRDD partition no = ${soldProductTotalsRDD.partitions.length}")

    val soldProductTotals = soldProductTotalsRDD.collect()

    println(s">>>>> There were ${soldProductTotals.length} different products sold >>>>>")

    soldProductTotals.foreach {
      case (prodId, (total, product)) => println(s"$prodId, $total, [${product.mkString(", ")}]")
    }

    // (1) Job 1 - products that didn't sell (leftJoin)

    // List of all products, including those that didn't sell yesterday
    val allProductTotals: Array[(Int, (Array[String], Option[Double]))] =
      productRDD.leftOuterJoin(totalSoldByProductIdRDD).collect()

    println(s">>>>> There were ${allProductTotals.length} different products on sale >>>>>")

    allProductTotals.foreach {
      case (prodId, (product, totalOption)) => println(s"$prodId, [${product.mkString(", ")}], $totalOption")
    }

    // List of all products that didn't sell yesterday
    val productsThatDidNotSell = allProductTotals.filter { case (_, (_, amount)) => amount.isEmpty }

    println(s">>>>> ${productsThatDidNotSell.length} product(s) didn't sell (leftOuterJoin) >>>>>")

    productsThatDidNotSell.foreach {
      case (_, (product, _)) => println(product.mkString(", "))
    }

    // (2) Job 2 - products that didn't sell (rightJoin)

    println(">>>>> Products that didn't sell yesterday (rightOuterJoin) >>>>>")

    val allProductTotalsRight: Array[(Int, (Option[Double], Array[String]))] =
      totalSoldByProductIdRDD.rightOuterJoin(productRDD).collect()

    val missingProducts = allProductTotalsRight.filter(x => x._2._1 == None).map(x => x._2._2)

    println(s">>>>> ${missingProducts.length} product(s) didn't sell (rightOuterJoin) >>>>>")

    missingProducts.foreach(p => println(p.mkString(", ")))

    // (3) Job 3 - full outer join
    println(">>>>> All product and sales info (full outer join) >>>>>")

    val caboodle: Array[(Int, (Option[Array[String]], Option[Double]))] =
      productRDD.fullOuterJoin(totalSoldByProductIdRDD).collect()

    caboodle.foreach {
      case (prodId, (productOption, totalOption)) =>
        println(s"$prodId, ${productOption.map(_.mkString(", "))}, $totalOption")
    }

    val noProductSales = caboodle.filter { case (_, (_, amount)) => amount.isEmpty }
    println(s"Number of products that didn't sell: ${noProductSales.length}")

    // (4) Job 4 - subtractByKey to find products that haven't sold

    val missingProdsRDD = productRDD.subtractByKey(totalSoldByProductIdRDD).values

    println(">>>>> Products that didn't sell yesterday (subtractByKey) >>>>>")
    missingProdsRDD.foreach(p => println(p.mkString(", ")))

    // (5) Job 5 - Cogroups
    println(">>>>> All product and sales info (cogroup) >>>>>")

    // totalSoldByProductIdRDD is RDD[(Int, String)]
    // productRDD is RDD[(Int, Array[String])] (list of products loaded from file)
    val productTotalsCogroupRDD: RDD[(Int, (Iterable[Double], Iterable[Array[String]]))] = totalSoldByProductIdRDD.cogroup(productRDD)

    productTotalsCogroupRDD.foreach(
      x => println(s"Id [${x._1}] Total [${x._2._1.headOption.getOrElse(0)}] Product [${x._2._2.toArray.flatten.mkString(", ")}]")
    )

    // (6) Job 6 - The products that didn't sell
    productTotalsCogroupRDD.filter(_._2._1.isEmpty).foreach(x => println(x._2._2.head.mkString(", ")))

    // (7) Job 7 - create totals and products RDD via a cogroup i.e. the products that sold
    val totalsAndProdsRDD1: RDD[(Int, (Double, Array[String]))] = productTotalsCogroupRDD.filter(
      !_._2._1.isEmpty
    ).mapValues(x => (x._1.head, x._2.head))

    def displayProducts(totalsAndProdsRDD: RDD[(Int, (Double, Array[String]))]): Unit = {
      totalsAndProdsRDD.foreach {
        case (_, (total, prod)) =>
          println(s"${prod.mkString(", ")} sold [£$total]")
      }
    }

    displayProducts(totalsAndProdsRDD1)

    // (8) Job 8 - create totals and products RDD via a cogroup i.e. the products that sold, take 2
    val totalsAndProdsRDD2: RDD[(Int, (Double, Array[String]))] = productTotalsCogroupRDD.filter(
      !_._2._1.isEmpty
    ).map(x => (x._2._2.head(0).toInt, (x._2._1.head, x._2._2.head)))

    displayProducts(totalsAndProdsRDD2)

    /*
    val sortedProducts = soldProductTotalsRDD.sortBy(_._2._2(1))
    sortedProducts.collect().foreach {
      case (prodId, (total, product)) => println(s"$prodId, $total, [${product.mkString(", ")}]")
    }
*/

    // Pause so we can look at the jobs/stages via Spark UI
    System.in.read()
    spark.stop()
  }
}