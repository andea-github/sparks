package demo.app

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author admin 2020-5-25
 */
object WordCount extends App {
  val inputPath = "file:///"
  private val conf = new SparkConf()
  conf.setMaster("local").setAppName("WordCount")
  val sc = new SparkContext(conf)
  val lines = sc.textFile(inputPath.concat("C:/tatas/spark/").concat("words.txt"))
  val wordCount = lines.flatMap(line => line.split(" "))
    .map(w => (w, 1)).reduceByKey((v1, v2) => v1 + v2)
  wordCount.collect()
  wordCount.foreach(println)
}
