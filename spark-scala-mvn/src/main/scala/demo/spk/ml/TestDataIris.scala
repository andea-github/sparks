package demo.spk.ml

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import utils.{MyFileUtil, MySparkUtil}

/**
 * 鸢尾花卉数据集: https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
 * 是一类多重变量分析的，是常用的分类实验数据集。
 * 数据集包含150个数据集，分为3类，每类50个数据，每个数据包含4个属性。
 * 可通过花萼长度，花萼宽度，花瓣长度，花瓣宽度4个属性预测鸢尾花卉属于（Setosa，Versicolour，Virginica）三个种类中的哪一类。
 *
 * @author admin 2020-5-26
 */
object TestDataIris extends App {

  val sc: SparkContext = MySparkUtil.sc("local", "MllibIris")
  val inputRdd = sc.textFile(MyFileUtil.inputPath.concat("iris.data"))
    .filter(_.size > 0).map(_.split(","))
  inputRdd.cache()
  val observations = inputRdd.map(p => Vectors.dense(p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble))
  /** 摘要统计 Summary statistics */
  val summary = Statistics.colStats(observations)
  println("=== 方差向量 \n\t" + summary.variance)
  println("=== 平均向量 \n\t" + summary.mean)
  println("=== L1范数向量 \n\t" + summary.normL1)
  println("=== L2范数向量 \n\t" + summary.normL2)

  /** 相关性Correlations */
  val seriesX = inputRdd.map(p => p(0).toDouble)
  val seriesY = inputRdd.map(p => p(1).toDouble)
  val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")

}
