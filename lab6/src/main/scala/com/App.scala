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
                                    // .setMaster("local[4]")
    val sc = new SparkContext(conf)
    //parameter to setMaster tells us how to distribute
    // the data, e.g., 4 partitions on the localhost
    //don't use setMaster if running on cluster

    /* productID, price */
    val products = product(sc)

    val lineItems = sc.textFile("lineItem.csv")
      .map(x => (x.split(", ")(1), x.split(", ")))
      .groupByKey()
      .mapValues(x => x.foldRight(0.0)((y,z) => products(y(2)) * y(3).toInt + z))
      .foreach(println(_))

    // val sales = sc.textFile("lineItem.csv").map(x =>
    //     (x.split(",")(0), x.split(", ")(1), x.split(", ")(2), x.split(", ")(3).toDouble))
    //     .map(x => (x._2, (products(x._3) * x._4)))
    // val saleTotal = sales.groupBy(x => (x._1, x._2)).mapValues(_.map(_._2).sum)
    // val storeTotal = sc.textFile("sales.csv").map(x =>
    //     (x.split(", ")(0), x.split(", ")(1), x.split(", ")(2), x.split(", ")(3), x.split(", ")(4)))
    //     .map(x => (x._4, saleTotal(x._1))).groupBy(x => (x._1, x._2)).mapValues(_.map(_._2).sum)
    // sc.textFile("store.csv").map(x =>
    //     (x.split(", ")(0), x.split(", ")(1), x.split(", ")(2), x.split(", ")(3), x.split(", ")(4),x.split(", ")(5),x.split(", ")(6)))
    //     .map(x =>(x._6, x._1, saleTotal(x._1))).sortBy(x => (x._1, x._2)).foreach(println(_))
  }

  def product(sc : SparkContext): RDD[(String, Double)] = {
    return sc.textFile("product.csv").map(x => (x.split(", ")(0), x.split(", ")(2).toDouble))
  }
}