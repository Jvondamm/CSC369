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

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    /* id, item, price */
    val products = sc.textFile("product.csv").map(x =>
        x.split(",")(0) -> "%.2f".format(x.split(",")(2).toDouble).toDouble).foreach(println(_))
 }
}