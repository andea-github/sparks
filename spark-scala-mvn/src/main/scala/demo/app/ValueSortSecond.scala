package demo.app

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 二次排序
 *
 * 给定文件：每行2列数字
 * 排序规则：第一列降序；如果第一列相同，第二列降序
 * step:
 * 1.实现Ordered和 Serializable接口，自定义排序key
 * 2.生成pairRdd
 * 3.sortByKey
 * 4.return value
 *
 * @author admin 2020-5-25
 */
object ValueSortSecond extends App {
  val inputPath = "file:///".concat("C:/tatas/spark/")
  val conf = new SparkConf().setMaster("local").setAppName("ValueSort")
  val sc = new SparkContext(conf)
  val lines = sc.textFile(inputPath.concat("sort_2.txt"))
  val kv = lines.map(line => {
    val values = line.split("\t")
    (new SecondarySortKey(values(0).toInt, values(1).toInt), line)
  })
  val sorted = kv.sortByKey(false)
  val result = sorted.map(_._2)
  result.collect().foreach(println)
}

class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if (this.first - that.first != 0)
      this.first - that.first
    else
      this.second - that.second
  }
}