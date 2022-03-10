import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object finalProj {
   def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      val conf = new SparkConf().setAppName("finalProj")
                                    // .setMaster("local[4]")
      val sc = new SparkContext(conf)
      val lines = sc.textFile("/user/etruon08/input/animes.csv")
      val intLines = lines.map(line => (line.split(",")(0), line.split(",")(1)))
      .top(10).foreach(println)
   }
}