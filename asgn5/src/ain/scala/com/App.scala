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

        // Q1
        return sc.textFile("integers").fiter(_ % 3 == 0).map(lambda x: (x[0],1)).reduceByKey(lambda x, y: x+y).foreach(println(x => x._1 +" appears ", x._2 + " times, "))

        // Q2
        val employees = sc.texFile("employee").map(x => x.split(" ")(0), x.split(" "(1)))
        val departments = sc.texFile("department").map(x => x.split(" ")(0), x.split(" "(1)))
        return employees.cartesian(departments).filter(x => x._1._2 == x._2._1).foreach(println(x => x._1._1, x._2._2))


        // Q3
        val students = sc.textFile("grades").map(x => x.split(",")(0), x.split(",")(1), x.split(",", 3)(2).trim().split(",").trim().map(x => x.split(" ")(0)))
        // John Back, 23, A CSC369, B CSC366 -> (John Back, 23, (A, B))

        val func: Map[Char, Int] = Map('A' -> 4, 'B' -> 3, 'C' -> 2, 'D' -> 1, 'F' -> 0)

        def param0 = (accu:Int, v:String) => accu._1 + func.getOrElse(v.charAt(0), 0)
        def param1 = (accu1:(Int, Int), accu2:(Int, Int)) => (accu1._1 + accu2._1, accu1._2 + accu2._2)

        return students.map(x._1, x._2, x._3.aggregate((0,0))(param0, param1)).foreach(println(x._1, x._2, x._3._1.toFloat / x._3._2.toFloat))
    }
}
