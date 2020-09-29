package demo.spk.ml

import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import utils.{MySparkUtil, MyTataUtil}

/**
 *
 * @author admin 2020-7-13
 */
object DmFp extends App {
  val support = 0.0
  val confidence = 0.0

  private val sc: SparkContext = MySparkUtil.sc("DmFp")
  //  val item = getDataByFile
  val item = getParallelizeRdd
  run(item)

  private def run(item: RDD[Array[String]]) = {
    val growth: FPGrowth =
      new FPGrowth().setMinSupport(support).setNumPartitions(1)
    val model = growth.run(item)
    model.freqItemsets.collect().foreach(itemset => {
      println(itemset.items.mkString("[", ",", "]") + "," + itemset.freq)
    })
    println(model.itemSupport)
    model.generateAssociationRules(confidence).collect().foreach(rule => {
      println(s"[${rule.antecedent.mkString(",")}] => [${rule.consequent.mkString(",")}], ${rule.confidence}")
    })
  }

  private def getDataByFile = {
    val inputRdd = sc.textFile("file:///C:/datas/test/growth_chars.txt")
    val item: RDD[Array[String]] = inputRdd.map(r => r.split(" "))
    item
  }

  private def getParallelizeRdd = {
    val inputRdd = sc.parallelize(MyTataUtil.row_abc, 2)
    inputRdd.map(r => r.split(" "))
  }
}
