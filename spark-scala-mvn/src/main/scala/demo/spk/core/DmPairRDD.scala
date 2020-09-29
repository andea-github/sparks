package demo.spk.core

import org.apache.spark.SparkContext
import utils.MySparkUtil

/**
 * aggregateByKey
 *
 * @author admin 2020-7-13
 */
object DmPairRDD extends App {

  private val sc: SparkContext = MySparkUtil.sc("local", "DmPairRDD")
  sc.setLogLevel("WARN")
  val rdd = sc.parallelize(List(("a", 1), ("a", 2), ("a", 3), ("b", 4), ("b", 5), ("c", 6)), 3)
  println(rdd.partitions.size)
  // Array[Array[(String, Int)]]
  rdd.glom.collect.foreach(_.foreach(println))
  val res = rdd.aggregateByKey(0, 1)(math.max(_, _), _ + _) // math.max(_, _)
  println()
  res.foreach(println)
  println()
  // Array[(String, Int)]
  res.collect.foreach(println)

}
