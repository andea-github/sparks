package demo.app

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils.MyFileUtil

/**
 * 文件排序：每行内容为一个整数
 *
 * 对文件内容进行排序(升序)，然后输出到新文件中：sort content
 * 每次执行前删除输出文件 if exists
 *
 * @author admin 2020-5-25
 */
object ValueSort extends App {
  MyFileUtil.deleteOutputPath("C:\\Users\\workspace\\work_idea\\sparks\\result")
  val inputPath = "file:///".concat("C:/tatas/spark/")
  val conf = new SparkConf().setMaster("local").setAppName("ValueSort")
  val sc = new SparkContext(conf)
  val lines = sc.textFile(inputPath.concat("max_value.txt"))
  var index = 0
  val result = lines.filter(_.trim().length > 0).map(v => (v.trim.toInt, ""))
    .partitionBy(new HashPartitioner(1)).sortByKey()
    .map(t => {
      index += 1
      (index, t._1)
    })
  result.saveAsTextFile("result")
  println("==============")
}
