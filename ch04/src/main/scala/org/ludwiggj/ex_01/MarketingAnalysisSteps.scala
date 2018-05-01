package org.ludwiggj.ex_01

import org.ludwiggj.{deleteDirIfExists, inputDataDir, outputDataDir, sparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MarketingAnalysisSteps {
  val bearProductId = 4
  val barbieShoppingMallPlaysetProductId = 25
  val pyjamasProductId = 63
  val toothbrushProductId = 70
  val dictionaryProductId = 81

  private def displayExecutors(sc: SparkContext): Unit = {
    val executors = sc.statusTracker.getExecutorInfos
    println(s"No of executors: ${executors.size}")
    executors.foreach(ex =>
      println(s"  Host: ${ex.host()}, Port: ${ex.port()}, Tasks: ${ex.numRunningTasks()}"))
  }

  private def displaySparkConfig(spark: SparkSession): Unit = {
    // spark.default.parallelism is the default number of partitions in RDDs returned by
    // transformations like join, reduceByKey, and parallelize when not set explicitly
    // by the user.

    // NOTE also the following in SparkContext.scala:
    // Default min number of partitions for Hadoop RDDs when not given by user
    //   * Notice that we use math.min so the "defaultMinPartitions" cannot be higher than 2.
    //   * The reasons for this are discussed in https://github.com/mesos/spark/pull/718; this is a very interesting
    //     and considered discussion about the various trade-offs. Extract: ...the Hadoop InputFormat library will
    //     always create more partitions for large files, and does so already. For example, if you have a HDFS block
    //     size of 64 MB, and you call textFile on a 640 MB file, you'll get 10 partitions, even though
    //     defaultMinSplits is 2. So this is only a question of how we deal with very small files, which may occur
    //     when a user is testing locally, and may be confusing if they see just one thread being used. Note that
    //     even for local files though, InputFormat treats them as having 32 MB blocks (I believe).

    // Note that spark.default.parallelism seems to only be working for raw RDD and is
    // ignored when working with dataframes
    println(s"spark.default.parallelism: [${spark.conf.get("spark.default.parallelism")}]")

    // spark.sql.shuffle.partitions configures the number of partitions that are used when
    // shuffling data for joins or aggregations
    println(s"spark.sql.shuffle.partitions: [${spark.sqlContext.getConf("spark.sql.shuffle.partitions")}]")
  }

  private def transactionsByCustomer(spark: SparkSession, minPartitions: Int): RDD[(Int, Array[String])] = {
    // Each line in the transaction file has following format:
    // date#time#customer ID #product ID#quantity#product price

    val sc = spark.sparkContext

    val txFileRDD: RDD[String] = sc.textFile(inputDataDir + "\\data_transactions.txt", minPartitions)

    println(s"Txs loaded into ${txFileRDD.partitions.length} partitions")

    // One entry per tx, takes advantage of RDD key/value structure
    // May have multiple lines for same key (customer)
    val txsByCustRDD: RDD[(Int, Array[String])] = txFileRDD.map(_.split("#")).map(tx => (tx(2).toInt, tx))

    println(s"Number of tx's = ${txsByCustRDD.values.count()}")

    txsByCustRDD
  }

  private def displayNoOfUniqueCustomers(txsByCustRDD: RDD[(Int, Array[String])]): Unit = {
    val noOfUniqueCustomers = txsByCustRDD.keys.distinct().count()
    println(s"noOfUniqueCustomers = $noOfUniqueCustomers")
  }

  // Count number of txs per customer id
  private def transactionsByKey(txsByCustRDD: RDD[(Int, Array[String])]): collection.Map[Int, Long] = {
    val txsByKey: collection.Map[Int, Long] = txsByCustRDD.countByKey()
    println(s"Number of tx's = ${txsByKey.values.sum}")
    txsByKey
  }

  private def customerWhoMadeTheMostTransactions(txsByKey: collection.Map[Int, Long]): Int = {
    val (customerIdMostTxs, noOfTxs) = txsByKey.toSeq.maxBy(_._2)
    println(s"Customer $customerIdMostTxs bought $noOfTxs things")
    customerIdMostTxs
  }

  // Get hold of all tx's for that customer
  // Note that a lookup transaction will transfer the values to the driver,
  // so you have to make sure that the values will fit in its memory
  private def displayTxsForCustomerId(txsByCustRDD: RDD[(Int, Array[String])], customerId: Int): Unit = {
    val txs: Seq[Array[String]] = txsByCustRDD.lookup(customerId)
    println(s"Here are the txs for customer $customerId:")
    txs.foreach(tx => println(tx.mkString(", ")))
  }

  private def totalAmountSpent(transByCustRDD: RDD[(Int, Array[String])]): Double = {
    transByCustRDD.values.map(tx => tx(5).toDouble).sum()
  }

  private def giveBarbieDiscount(txsByCustRDD: RDD[(Int, Array[String])]): RDD[(Int, Array[String])] = {
    txsByCustRDD.mapValues(tx => {
      if (tx(3).toInt == barbieShoppingMallPlaysetProductId && tx(4).toDouble > 1)
        tx(5) = (tx(5).toDouble * 0.95).toString
      tx
    })
  }

  // Customers who have a toothbrush
  private def customersWithToothbrush(txsByCustRDD: RDD[(Int, Array[String])]): RDD[Int] = {
    txsByCustRDD.filter({ case (_, tx) => tx(3) == toothbrushProductId.toString }).keys
  }

  private def awardComplimentaryToothbrushes(txsByCustRDD: RDD[(Int, Array[String])]): RDD[(Int, Array[String])] = {
    println(s"Now giving a complimentary toothbrush ($toothbrushProductId) to customers "
      + s"who bought five or more dictionaries ($dictionaryProductId)")
    txsByCustRDD.flatMapValues(tx => {
      if (tx(3).toInt == dictionaryProductId && tx(4).toDouble >= 5) {
        // Add a toothbrush
        val cloneTx = tx.clone()
        cloneTx(5) = "0.00"
        cloneTx(3) = toothbrushProductId.toString
        cloneTx(4) = "1"
        List(tx, cloneTx)
      } else {
        List(tx)
      }
    })
  }

  private def toListOfIds(customersRDD: RDD[Int]): String = {
    customersRDD.collect().mkString(", ")
  }

  private def findBiggestSpender(amountsRDD: RDD[(Int, Double)], initValue: Double = 0): (Int, Double) = {
    val totalsRDD: RDD[(Int, Double)] = amountsRDD.foldByKey(initValue)(_ + _)
    totalsRDD.collect().toSeq.maxBy(_._2)
  }

  private def writeToFile(dirName: String, txsByCustRDD: RDD[(Int, Array[String])]): Unit = {
    println(s"About to write txsByCustRDD to file, with ${txsByCustRDD.partitions.length} partitions")
    val targetDir = s"$outputDataDir\\$dirName"

    deleteDirIfExists(targetDir)

    txsByCustRDD.map(t => t._2.mkString("#")).saveAsTextFile(targetDir)
  }

  private def displayProductsPurchasedByEachCustomer(txsByCustRDD: RDD[(Int, Array[String])]): Unit = {
    // Use aggregateByKey to generate a list of all products purchased by each customer

    println(s"Number of partitions before: ${txsByCustRDD.partitions.length}")

    val prods: RDD[(Int, List[String])] = txsByCustRDD.aggregateByKey(List[String]())(
      (prods, tx) => prods ::: List(tx(3)),
      (prods1, prods2) => prods1 ::: prods2
    )

    prods.foreach {
      case (id, txs) => println(s"CustId $id bought ${txs.mkString(", ")}")
    }

    println(s"Number of partitions after: ${prods.partitions.length}")
  }

  // START HERE...
  def main(args: Array[String]): Unit = {
    val spark = sparkSession(noOfNodes = Some(3), parallelism = Some(5))

    displaySparkConfig(spark)

    displayExecutors(spark.sparkContext)

    // Task 1 - Give a complimentary bear doll to the customer who made the most transactions

    // (0) Job 0

    // NOTE: Loading customer txs into 10 partitions
    //
    // If this wasn't specified, it would be set to a minimum of 2, regardless of value of
    // default parallelism (5 in this case)
    //
    // This is because of the following code in SparkContext:
    //
    //
    //

    /**
      * Default min number of partitions for Hadoop RDDs when not given by user
      * Notice that we use math.min so the "defaultMinPartitions" cannot be higher than 2.
      * The reasons for this are discussed in https://github.com/mesos/spark/pull/718
      */

    // def defaultMinPartitions: Int = math.min(defaultParallelism, 2)

    var txsByCustRDD = transactionsByCustomer(spark, minPartitions = 10)
    txsByCustRDD.persist()

    // (1) Job 1

    displayNoOfUniqueCustomers(txsByCustRDD)

    // (2) Job 2 - find customer with most transactions

    val txsByKey = transactionsByKey(txsByCustRDD)
    val customerIdMostTxs = customerWhoMadeTheMostTransactions(txsByKey)

    // (3) Job 3

    // Show the txs for the customer who had the most
    displayTxsForCustomerId(txsByCustRDD, customerIdMostTxs)

    // Transaction to award complimentary bear to customer with the most transactions
    // Will award it later!
    val complimentaryBearTx =
    Array("2015-03-30", "11:59 PM", customerIdMostTxs.toString, bearProductId.toString, "1", "0.00")

    // (4) Job 4

    val totalSpentBeforeDiscounts = totalAmountSpent(txsByCustRDD)
    println(s"Total amount spent (before discounts) = ${totalSpentBeforeDiscounts}")

    // (5) Job 5

    // Task 2 - Give a 5% discount to any tx where buying 2 or more Barbie Shopping Mall Playsets
    // Use mapValues to change a tx whilst keeping the key (customerId) unchanged
    txsByCustRDD = giveBarbieDiscount(txsByCustRDD)

    // Task 3 - Add a complimentary toothbrush to customers who bought five or more dictionaries
    // Use flatmapValues - a tx can be converted into 0 .. n txs

    val customersWithToothbrushBeforeOfferRDD = customersWithToothbrush(txsByCustRDD)

    txsByCustRDD = awardComplimentaryToothbrushes(txsByCustRDD)

    val customersWithToothbrushAfterOfferRDD = customersWithToothbrush(txsByCustRDD)

    val customersReceivingFreeToothbrushRDD =
      customersWithToothbrushAfterOfferRDD.subtract(customersWithToothbrushBeforeOfferRDD)

    println(s"Customers with a toothbrush before: ${toListOfIds(customersWithToothbrushBeforeOfferRDD)}")

    // (6) Job 6

    println(s"Customers with a toothbrush after: ${toListOfIds(customersWithToothbrushAfterOfferRDD)}")

    // (7) Job 7
    println(s"Customers who received a free toothbrush ${toListOfIds(customersReceivingFreeToothbrushRDD)}")

    // (8) Jobs 8 - 14
    //     1 * job for the collect
    //     6 * jobs, 1 each to print the txs of the 6 customers receiving a free toothbrush
    customersReceivingFreeToothbrushRDD.collect().foreach(displayTxsForCustomerId(txsByCustRDD, _))

    // (9) Job 15
    println(s"Number of tx's = ${txsByCustRDD.count()}")

    // (10) Job 16
    // Task 4 - find the customer who spent the most, and give them a pair of pj's

    // (a) Via foldByKey
    val amountsRDD: RDD[(Int, Double)] = txsByCustRDD.mapValues(t => t(5).toDouble)

    val (bigSpenderCustomerId, bigSpenderCashTotal) = findBiggestSpender(amountsRDD)
    println(s"Customer $bigSpenderCustomerId spent the most, $bigSpenderCashTotal")

    // (11) Job 17
    val (fakeBigSpenderCustomerId, fakeBigSpenderCashTotal) = findBiggestSpender(amountsRDD, 15)
    println(s"Fake Customer $fakeBigSpenderCustomerId spent the most(!), $fakeBigSpenderCashTotal")

    // (12) Job 18
    // (b) We can also use reduceByKey
    val bigSpender = amountsRDD.reduceByKey(_ + _).collect().toSeq.maxBy(_._2)
    println(s"Customer ${bigSpender._1} spent the most, ${bigSpender._2}")

    // Transaction to award pyjamas to customer with the most spend
    // Will award it later!
    val bigSpenderTx = Array(
      "2015-03-30", "11:59 PM", bigSpenderCustomerId.toString, pyjamasProductId.toString, "1", "0.00"
    )

    // (13) Job 19
    // Now add the free bear and pj's into the tx list
    val complimentaryTxs = Array(complimentaryBearTx, bigSpenderTx)

    // Map them on customerId, as per main RDD
    val sc = spark.sparkContext
    val newTxsRDD: RDD[(Int, Array[String])] = sc.parallelize(complimentaryTxs).map(t => (t(2).toInt, t))

    // NOTE: txsByCustRDD has 10 partitions
    //       newTxsRDD has 5 partitions

    //       Union has 15 partitions (which is why there are 15 tasks in the stage)
    txsByCustRDD = txsByCustRDD.union(newTxsRDD)
    println(s"Number of tx's = ${txsByCustRDD.count()}")

    // (14) Job 20
    // Write back out to file
    // NOTE - Partitions are unbalanced in this example
    writeToFile("txsByCustomer", txsByCustRDD)

    // (15) Job 21
    //      Repartition to balance these out
    txsByCustRDD = txsByCustRDD.repartition(numPartitions = 5)
    writeToFile("txsByCustomerRepartitioned", txsByCustRDD)

    // (16) Job 22
    val totalSpentAfterDiscounts = totalAmountSpent(txsByCustRDD)
    println(s"Total amount spent  (after discounts) = $totalSpentAfterDiscounts")
    println(s"Total amount spent (before discounts) = $totalSpentBeforeDiscounts")
    println(s"Amount discounted                     = ${totalSpentBeforeDiscounts - totalSpentAfterDiscounts}")

    // (17) Job 23
    displayProductsPurchasedByEachCustomer(txsByCustRDD)

    // Pause so we can look at the jobs/stages via Spark UI
    System.in.read()
    spark.stop()
  }
}