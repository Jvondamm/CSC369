package example
import scala.io._
import scala.collection._

object App {
  def main(args: Array[String]): Unit = {
    val products = product
    val saleTotal = lineItem.map(x => (x._2, products(x._3) * x._4)).groupBy(_._1).mapValues(_.map(_._2).sum)
    val storeTotal = sales.map(x => (x._4, saleTotal(x._1))).groupBy(_._1).mapValues(_.map(_._2).sum)
    store.map(x =>(x._6, x._1, saleTotal(x._1))).sortBy(_._1).foreach(println(_))
  }

  /* id, name, address, city, zip, state, phone */
  def store: List[(String, String, String, String, String, String, String)] = {
    return Source.fromFile("store.csv").getLines().toList.map(x =>
        (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3), x.split(",")(4),x.split(",")(5),x.split(",")(6)))
  }

  /* id, date, time, storeid, customerid */
  def sales: List[(String, String, String, String, String)] = {
    return Source.fromFile("sales.csv").getLines().toList.map(x =>
        (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3), x.split(",")(4)))
  }

  /* map so we can call it directly with lineItem product */
  /* id, item, price */
  def product: Map[String, Double] = {
    return Source.fromFile("product.csv").getLines().toList.map(x =>
        x.split(",")(0) -> "%.2f".format(x.split(",")(2).toDouble).toDouble).toMap
  }

  /* id, saleid, productid, number */
  def lineItem: List[(String, String, String, Int)] = {
    return Source.fromFile("lineItem.csv").getLines().toList.map(x =>
        (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3).toInt))
  }
}