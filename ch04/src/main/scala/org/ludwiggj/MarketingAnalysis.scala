package org.ludwiggj

import org.apache.spark.rdd.RDD

object MarketingAnalysis {
  def totalAmountSpent(transByCust: RDD[(Int, Array[String])]): Double = {
   transByCust.values.map(tx => tx(5).toDouble).sum()
  }

  val bearProductId = 4
  val barbieShoppingMallPlaysetProductId = 25
  val pyjamasProductId = 63
  val toothbrushProductId = 70
  val dictionaryProductId = 81

  def main(args: Array[String]) {
    val spark = sparkSessionBuilder.getOrCreate()
    val sc = spark.sparkContext

    val tranFile = sc.textFile(inputDataDir + "\\data_transactions.txt")
    val tranData = tranFile.map(_.split("#"))

    // One entry per tx, takes advantage of RDD key/value structure
    // May have multiple lines for same key (customer)
    var transByCust: RDD[(Int, Array[String])] = tranData.map(tx => (tx(2).toInt, tx))

    val noOfUniqueCustomers = transByCust.keys.distinct().count()
    println(s"noOfUniqueCustomers = $noOfUniqueCustomers")

    val totalSpent = totalAmountSpent(transByCust)
    println(s"Total amount spent = $totalSpent")

    // Count number of txs per customer id
    val txsByKey: collection.Map[Int, Long] = transByCust.countByKey()
    println(s"Number of tx's = ${txsByKey.values.sum}")

    // Customer with the most transactions
    val (customerIdMostTxs, purchases) = txsByKey.toSeq.sortBy(_._2).last
    println(s"Customer $customerIdMostTxs bought $purchases things")

    // Get hold of all tx's for that customer
    val txs: Seq[Array[String]] = transByCust.lookup(customerIdMostTxs)
    println(s"Here are the txs:")
    txs.foreach(tx => println(tx.mkString(", ")))

    // Discount any tx where buying 2 or more Barbie Shopping Mall Playsets by 5%
    transByCust = transByCust.mapValues(tx => {
      if (tx(3).toInt == barbieShoppingMallPlaysetProductId && tx(4).toDouble > 1)
        tx(5) = (tx(5).toDouble * 0.95).toString
      tx
    })

    val totalSpentAfterDiscount = totalAmountSpent(transByCust)
    println(s"Total amount spent (after discount) = ${totalSpentAfterDiscount}")
    println(s"Amount discounted = ${totalSpent - totalSpentAfterDiscount}")

    // Find customer who spent the most...

    // (1) Via foldByKey
    val amounts = transByCust.mapValues(t => t(5).toDouble)
    amounts.foreach { case (custId, cash) => println(s"$custId -> $cash") }

    val totals = amounts.foldByKey(0)(_ + _).collect()
    println(totals.mkString(", "))

    val (customerIdBigSpender, bigSpenderCashTotal) = totals.toSeq.sortBy(_._2).last
    println(s"Cust $customerIdBigSpender spent $bigSpenderCashTotal")

    // (2) We can also use reduceByKey
    val bigSpender = amounts.reduceByKey(_ + _).collect().toSeq.sortBy(_._2).last
    println(s"Cust ${bigSpender._1} spent ${bigSpender._2}")

    // REWARD TIME...

    println(s"Number of tx's = ${transByCust.count()}")

    // Add a complimentary toothbrush to customers who bought five or more dictionaries
    // Use a flatmap function - a tx can be converted into 0 .. n txs
    transByCust = transByCust.flatMapValues(tx => {
      if (tx(3).toInt == dictionaryProductId && tx(4).toDouble >= 5) {
        val cloneTx = tx.clone()
        cloneTx(5) = "0.00"
        cloneTx(3) = toothbrushProductId.toString
        cloneTx(4) = "1"
        List(tx, cloneTx)
      } else {
        List(tx)
      }
    })

    println(s"Number of tx's = ${transByCust.count()}")

    // Transaction to award complimentary bear to customer with the most transactions
    val complimentaryBearTx = Array("2015-03-30", "11:59 PM", customerIdMostTxs.toString, bearProductId.toString, "1", "0.00")

    // Transaction to award pyjamas to customer with the most spend
    val bigSpenderTx = Array("2015-03-30", "11:59 PM", customerIdBigSpender.toString, pyjamasProductId.toString, "1", "0.00")

    // Store this in array, which we'll use again later
    var complimentaryTxs = Array(complimentaryBearTx, bigSpenderTx)

    val newTxs = sc.parallelize(complimentaryTxs).map(t => (t(2).toInt, t))

    transByCust = transByCust.union(newTxs)

    println(s"Number of tx's = ${transByCust.count()}")

    // Write back out to file
    transByCust.map(t => t._2.mkString(("#"))).saveAsTextFile(outputDataDir + "\\txsByCustomer")

    // List of all products purchased by each customer

    val prods = transByCust.aggregateByKey(List[String]())(
      (prods, tx) => prods ::: List(tx(3)),
      (prods1, prods2) => prods1 ::: prods2
    )

    prods.foreach {
      case (id, txs) => println(s"CustId $id bought ${txs.mkString(", ")}" )
    }
  }
}