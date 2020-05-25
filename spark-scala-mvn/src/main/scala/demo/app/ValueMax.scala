package demo.app

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 求最大、最小值
 *
 * @author admin 2020-5-25
 */
object ValueMax extends App {
  val inputPath = "file:///".concat("C:/tatas/spark/")
  val conf = new SparkConf().setMaster("local").setAppName("ValueMax")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val lines = sc.textFile(inputPath.concat("max_value.txt"))
  val result = lines.filter(_.trim().length > 0)
    .map(line => ("key", line.trim.toInt))
    .groupByKey()
    .map(p => {
      // Integer.MIN_VALUE, Integer.MAX_VALUE
      var min = 0
      var max = 0
      for (num <- p._2) {
        if (num > max) max = num
        if (num < min) min = num
      }
      (max, min)
    }).collect.foreach(p => println("max = %s min = %d".format(p._1, p._2)))
}
