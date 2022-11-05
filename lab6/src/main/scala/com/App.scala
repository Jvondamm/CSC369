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

    /* id, item, price */
    val products = sc.textFile("product.csv").map(x =>
        x.split(",")(0) -> "%.2f".format(x.split(",")(2).toDouble).toDouble)
    val saleTotal = sc.textFile("lineItem.csv").map(x =>
        (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3).toInt))
        .map(x => (x._2, (products(x._3) * x._4))).groupBy(x => (x._1, x._2)).mapValues(_.map(_._2).sum)
    val storeTotal = sc.textFile("sales.csv").map(x =>
        (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3), x.split(",")(4)))
        .map(x => (x._4, saleTotal(x._1))).groupBy(x => (x._1, x._2)).mapValues(_.map(_._2).sum)
    sc.textFile("store.csv").map(x =>
        (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3), x.split(",")(4),x.split(",")(5),x.split(",")(6)))
        .map(x =>(x._6, x._1, saleTotal(x._1))).sortBy(x => (x._1, x._2)).foreach(println(_))
  }
}

// object App {
//  def main(args: Array[String]) {
//      Logger.getLogger("org").setLevel(Level.OFF)
//      Logger.getLogger("akka").setLevel(Level.OFF)

//     val conf = new SparkConf().setAppName("NameOfApp")
//                                     // .setMaster("local[4]")
//     val sc = new SparkContext(conf)
//  ...//parameter to setMaster tells us how to distribute
//     // the data, e.g., 4 partitions on the localhost
//    //don't use setMaster if running on cluster

//     /* id, item, price */
//     val products = sc.textFile("lineItem.csv").map(x =>
//         (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3).toInt))

//     /* id, name, address, city, zip, state, phone */
//     val stores = sc.textFile("store.csv").map(x =>
//         (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3), x.split(",")(4),x.split(",")(5),x.split(",")(6)))

//     /* id, date, time, storeid, customerid */
//     val sales = sc.textFile("sales.csv").map(x =>
//         (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3), x.split(",")(4)))

//     /* id, saleid, productid, number */
//     val lineItems = sc.textFile("lineItem.csv").map(x =>
//         (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3).toInt))
//   }
// }