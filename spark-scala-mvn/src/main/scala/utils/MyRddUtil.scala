package utils

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 *
 * @author admin 2020-8-3
 */
object MyRddUtil {
  /** get 指定分区 */
  def forIndexPartition[T: ClassTag](rdd: RDD[T], index: Int) = {
    rdd.mapPartitionsWithIndex { (idx, iter) => {
      if (idx == index) {
        iter
      } else {
        Iterator.empty
      }
    }
    }.foreach(println)
  }

  /** RDD: foreach */
  def foreachRdd[T: ClassTag](rdd: RDD[T]): Unit = {
    println()
    rdd.foreach(println)
  }
}
