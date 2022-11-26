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
    val conf = new SparkConf().setAppName("example")
    val sc = new SparkContext(conf)

    val courses = sc.textFile("courses").map(x => (x.split(",")(0), x.split(",")(1).toInt))
    val grades = sc.textFile("grades").map(x => (x.split(",")(1), x.split(",")))
    val students = sc.textFile("students").map(x => (x.split(",")(1), x.split(",")))

    // Q1
    val hardest_num = sc.parallelize(courses.sortBy(_._2, ascending = false).take(1))
    grades.join(hardest_num).map(x => (x._2._1(0), x._2._1(1))).join(students).foreach(println(_))

    // Q2
    grades.join(courses).map(x => (x._2._1(0), x._2._2)).groupByKey()
    .mapValues(x => x.sum.toDouble / x.size.toDouble).rightOuterJoin(p)
    .foreach(x => println(x._2._2(1)+","+x._2._1.getOrElse(0)))

    // Q3
    courses.sortBy(_._2, ascending = false).take(5).foreach(println(_))

    // Q4
    val grades2 = sc.textFile("grades").map(x => (x.split(",")(0), x.split(",")))

    val gpas = grades2.groupByKey().map(x => (x._1, x._2.aggregate((0,0))((x,y)=>
        (x._1 + gpa.getOrElse(y(2).charAt(0), 0), x._2 + 1),(x,y) => (x._1 + y._1, x._2 + y._2))))

    gpas.mapValues(x => x._1.toDouble / x._2.toDouble)
     .rightOuterJoin(students).foreach(x => println(x._2._2(1)+","+x._2._1.getOrElse(0)))
  }
}