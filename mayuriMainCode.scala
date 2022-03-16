import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection._
import scala.io._
import scala.math._

object App {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("App").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val animeLines = sc.textFile("/Users/mayuriprasad/Documents/Code/csc369/finalproj/animesParsed.csv").persist()
    // uid, title, synopsis, score

    val noiseWords = sc.textFile("/Users/mayuriprasad/Documents/Code/csc369/finalproj/stopwords.txt").map(line => line.trim()).persist()

    // index 1 = title
    // index 2 = description
    // index 3 = score

    val descWords = animeLines.flatMap(line => line.split(",,")(1).split(",")(0).trim().split(" "))
    // remove noise words

    val cleanDescWords = descWords.subtract(noiseWords)
    val wordTuples = cleanDescWords.map(word => (word, 1))
    val top1k = wordTuples.reduceByKey({ (x,y) => x + y }).map{
      case (word, count) => (count, word)
    }.sortByKey(false).take(1000).map(_._2)
    //.map(_._2) // => Array[(count: Int, word: String)]


    // RDD[Array[(String,Int)]]
    // map.reduceByKey
    val countsPerShow = animeLines.map(line=>line.split(",,")(1).split(",")(0).trim().split(" ").map(word=>(word,1))
      // Array[(word, 1)]
      .groupBy(_._1) // => (word, Array[(word, 1), (word, 1)...])
      .map(_._2.reduce({(x,y) => (x._1, x._2+y._2)})).toList).persist() //RDD[Map(String, Int)]
    //.foreach(_.foreach(println))

    val vectors = countsPerShow.map(item => (top1k)
      .map{ x => (x,  item.toMap.getOrElse(x, 0)) }.map(_._2)) // =>  RDD[Array[(String, Int)]],
    // where each array represents a show and the (word,count) for each show, the .map(_._2) keeps only the count (vector of counts)

    val docCounts = countsPerShow.map(item => (top1k)
      .map{ x => (x,  if (item.toMap.contains(x)) { 1 } else { 0 })}.map(_._2)).reduce((x,y) => (x,y).zipped.map(_ + _))

    //vectors.foreach(x=> println(x.mkString))
    //top1k words = Array, <"world", "secondWord", ... >
    //vectors = RDD of Arrays, where each array is a show: [count of "world", count of "secondWord", ...]
    //docCounts = Array [doc count of "world", doc count of "secondWord", ...] VERY SLOW TO GENERATE

    val scaledVectors = vectors.map(show => if (show.max != 0) {show.indices.map(i => (show(i) / show.max) * (log10(19311 / docCounts(i)) / log10(2.0)))} else {show.toVector})
    //RDD of Vectors, where each vector has the scaled value for the word: [scaled val of "world, scaled val of "secondWord", etc]

    // *** Next steps: ***
    // 1. Divide data into training and testing
    // 2. Figure out vector similarity calculation logic between shows
    // 3. Predict ratings for testing shows based on this
  }
}
