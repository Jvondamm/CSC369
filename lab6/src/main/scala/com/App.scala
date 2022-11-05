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

    val products = product(sc)
    val lineItems = lineItem(sc)
    val stores = store(sc)
    val sales = sale(sc)

    // salesID, productID, price*quantity then join by salesID
    // .join sales, .join stores -> storeID, state, salesID, money then groupby storeID, sort by state, and print

    // state, storeID, sales

    }

    // productID, description, price -> (productID, price)
    def product(sc : SparkContext): RDD[(String, Double)] = {
        return sc.textFile("product.csv").map(x =>
        (x.split(", ")(0), x.split(", ")(2).toDouble))
    }

    // lineItemID, salesID, productID, quantity -> (salesID, productID, quantity)
    def lineItem(sc : SparkContext): RDD[(String, String, Double)] = {
        return sc.textFile("lineItem.csv").map(x =>
        (x.split(", ")(1), x.split(", ")(2), x.split(", ")(2)))
    }

    // storeID, storeName, address, city, ZIP, state, phoneNumber -> (storeID, state)
    def store(sc: SparkContext): RDD[(String, String)] = {
        return sc.textFile("store.csv").map(x =>
        (x.split(", ")(0), x.split(", ")(5)))
    }

    // saleID, date, time, storeID, customerID -> (saleID, storeID)
    def sale(sc: SparkContext): RDD[(String, String)] = {
        sc.textFile("sales.csv").map(x =>
        (x.split(", ")(0), x.split(", ")(3)))
    }
}