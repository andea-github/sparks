package demo.spk.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.MySparkUtil

/**
 *
 * @author admin 2020-8-3
 */
object OptionMap extends App {
  private val sc: SparkContext = MySparkUtil.sc("OptionMap")

  import utils.MyTataUtil.getRangeRdd

  private val rdd: RDD[Int] = getRangeRdd(sc, 7, 2)
  private val mapRdd: RDD[Int] = rdd.map(_ + 1)
  println(s"========= map ========= partition: ${mapRdd.getNumPartitions} \n ${mapRdd.collect().mkString(" ")}")
  /**
   * mapPartitions是map的一个变种。
   * map的输入函数是应用于RDD中每个元素，
   * mapPartitions的输入函数是应用于每个分区，也就是把每个分区中的内容作为整体来处理的。
   */
  private val mpRdd: RDD[Int] = rdd.mapPartitions(x => {
    var result = List[Int]()
    var cur = 0
    while (x.hasNext) {
      cur += x.next()
    }
    result.::(cur).iterator
  })
  println(s"========= mapPartitions ========= partition: ${mpRdd.getNumPartitions} \n ${mpRdd.collect().mkString(" ")}")

  import utils.MyRddUtil.forIndexPartition

  forIndexPartition(mpRdd, 0)
}
