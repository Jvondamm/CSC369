package example
import org.apache.spark.SparkContext._

import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object App {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NameOfApp")
    val sc = new SparkContext(conf)

    val products = product
    val lineItems = lineItem(sc)
    val stores = store(sc)
    val sales = sale(sc)

    // group by product ID, multiply quantity by price for each product to get sum -> (saleID, productID, sum)
    val job1 = lineItems.groupByKey().mapValues(x => x.foldRight(0.0)((y,z) => products(y(2)) * y(3).toInt + z))

    // saleID, date (truncated), time, storeID, customerID, storeID, storeName
    val job2 = sales.join(stores).mapValues(x => Array(x._1(0), x._1(1).substring(0,7), x._1(2), x._1(3), x._1(4), x._2._1, x._2._2))

    // join by saleID, map to -> date, storeID, storeName, sum, group and sort by date,
    // map values where we group by date and get total of sum, then map to (storeName, sum) and collect and print
    job2.join(job1).map(x => (x._2._1(1), (x._2._1(5), x._2._1(6), x._2._2))).groupByKey().sortByKey()
      .mapValues(x => x.groupBy(_._1).mapValues(x => x.toList.map(x => (x._3))).mapValues(_.sum))
      .map(x => (x._1, x._2.toList.sorted)).head(10).foreach(println(_))
    }

    // productID, description, price -> (productID, price)
    def product: Map[String, Double] = {
        return Source.fromFile("product.csv").getLines().toList.map(x =>
        (x.split(",")(0),  x.split(",")(2).toDouble)).toMap
    }

    // lineItemID, salesID, productID, quantity -> (salesID, (lineItemID, salesID, productID, quantity))
    def lineItem(sc : SparkContext): RDD[(String, Array[String])] = {
        return sc.textFile("/user/jvondamm/input/lineItem.csv").map(x =>
        (x.split(",")(1), x.split(",")))
    }

    // storeID, storeName, address, city, ZIP, state, phoneNumber -> (storeID, storeName, city)
    def store(sc: SparkContext): RDD[(String, (String, String))] = {
        return sc.textFile("/user/jvondamm/input/store.csv").map(x =>
        (x.split(",")(0), (x.split(",")(1), x.split(",")(3))))
    }

    // saleID, date, time, storeID, customerID -> (saleID, [saleID, date, time, storeID, customerID])
    def sale(sc: SparkContext): RDD[(String, Array[String])] = {
        sc.textFile("/user/jvondamm/input/sales.csv").map(x =>
        (x.split(",")(0), x.split(",")))
    }
}