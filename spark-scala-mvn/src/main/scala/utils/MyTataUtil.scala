package utils

import org.apache.spark.SparkContext

/**
 * The common test data
 *
 * @author admin 2020-7-13
 */
object MyTataUtil extends App {

  val sc: SparkContext = MySparkUtil.sc("MyTataUtil")

  def getRangeRdd(sc: SparkContext, range: Int, slice: Int) = {
    sc.parallelize(1 to range, slice)
  }

  //  foreachRdd(rangeRdd)

  val row_abc = List("a", "a b", "a b c", "3", "4", "5")
  val arr = new Array[String](3)
  arr(0) = "a"
  arr(1) = "a b"
  arr(2) = "a b c"

}
