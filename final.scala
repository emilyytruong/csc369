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
      // read in animes file (animesParsed.csv)
      val animeLines = sc.textFile("/user/etruon08/input/animesParsedSmall.csv")
      // read in noiseWords file (noiseWords.csv)
      val noiseWords = sc.textFile("user/etruon08/input/noiseWords.csv").flatMap(line => line.trim())

      // index 1 = title
      // index 2 = description
      // index 3 = score

      val descWords = animeLines.flatMap(line => line.split(",")(2).trim().split(" ").trim())
      // remove noise words
      val cleanDescWords = descWords.subtract(noiseWords)
      val wordTuples = cleanDescWords.map(word => (word, 1))
      val wordCount = wordTuples.reduceByKey({ (x,y) => x + y }).sortByKey()

      // don't know if we should do this way or the treeMap method
      // if we continue down this path, you would just take the .top(N) of wordCount which are tuples => (word, wordCount)
   }
}