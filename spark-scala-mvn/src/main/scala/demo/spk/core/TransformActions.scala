package demo.spk.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.MySparkUtil

/**
 *
 * @author admin 2020-7-31
 */
object TransformActions extends App {

  import utils.MyRddUtil.foreachRdd

  private val sc: SparkContext = MySparkUtil.sc("TransformActions")
  private val rdd1: RDD[(String, Int)] = sc.parallelize(List(("A", 1), ("B", 2), ("C", 3), ("D", 4), ("A", 3)))
  private val rdd2: RDD[(String, Int)] = sc.parallelize(List(("A", 2), ("A", 4), ("B", 2)))
  private val unionRdd: RDD[(String, Int)] = rdd1.union(rdd2)
  //  unionRdd.foreach(println); rdd1 union rdd2
  // 交集
  val intersectionRdd = rdd1 intersection rdd2

  //  差集
  private val subtractRdd: RDD[(String, Int)] = rdd1.subtract(rdd2)

  // join: 笛卡尔积
  private val joinRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
  //  joinRdd.foreach(println)
  foreachRdd(joinRdd)
  foreachRdd(unionRdd)
  //  foreachRdd(intersectionRdd)
  //  foreachRdd(subtractRdd)
  //  foreachRdd(rdd2.subtract(rdd1))
}
