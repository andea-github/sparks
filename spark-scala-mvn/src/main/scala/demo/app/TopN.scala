package demo.app

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 数据格式：sid,value
 * 1,150
 * 2,99
 * 3,320
 * 4,100
 * 5,520
 * TopN 个 value
 *
 * @author admin 2020-5-25
 */
object TopN extends App {
  val inputPath = "file:///".concat("C:/tatas/spark/")
  val conf = new SparkConf().setMaster("local").setAppName("TopN")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val lines = sc.textFile(inputPath.concat("top_1.txt"))
  var num = 0
  val result = lines.filter(line => (line.trim().length > 0))
    // 取第二个元素，index=1
    .map(_.split(",")(1))
    // 转为int 并作为key
    .map(v => (v.toInt, ""))
    .sortByKey(false)
    // 取出key & top3
    .map(_._1).take(3)
    .foreach(v => {
      num += 1
      println(num + "\t" + v)
    })
}
