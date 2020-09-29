package demo.spk.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.MySparkUtil

/**
 *
 * @author admin 2020-8-10
 */
object ParalleRdd extends App {

  private val sc: SparkContext = MySparkUtil.sc("ParalleRdd")
  private val rdd = sc.parallelize(List(1, 2, 3), 2)
  //  rdd.foreach(println)
  private val rdd2: RDD[Unit] = rdd.map(s => println(s))
  println(rdd2.count())
}

