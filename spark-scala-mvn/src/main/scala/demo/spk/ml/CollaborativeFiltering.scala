package demo.spk.ml

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{MyFileUtil, MySparkUtil}

case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

/**
 * 协同过滤
 * todo runtime too long
 *
 * @author admin 2020-6-4
 */
object CollaborativeFiltering extends App {
  private val spark: SparkSession = MySparkUtil.spark("local", "CollaborativeFiltering")
  //  spark.conf.set("spark.default.parallelism","8")
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val ratings = spark.sparkContext.textFile(MyFileUtil.inputPath.concat("ml-1m/ratings.dat"), 10)
    .filter(_.size > 0).map(parseRating)
    .toDF()
  // |userId|movieId|rating|timestamp|
  ratings.show(false)
  // 数据集划分训练集和测试集
  val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
  // 模型评估：通过计算模型的均方根误差来对模型进行评估，均方根误差越小，模型越准确
  val evaluator = new RegressionEvaluator().setMetricName("rmse")
    .setLabelCol("rating").setPredictionCol("prediction")
  /**
   * 推荐模型构建
   * - alpha 是一个针对于隐性反馈 ALS 版本的参数，这个参数决定了偏好行为强度的基准（默认为1.0
   * - implicitPrefs 决定了是用显性反馈ALS的版本还是用适用隐性反馈数据集的版本（默认是false，即用显性反馈）
   * - numBlocks 是用于并行化计算的用户和商品的分块个数 (默认为10)
   * - maxIter 是迭代的次数（默认为10）(5,20）
   * - nonnegative 决定是否对最小二乘法使用非负的限制（默认为false）
   * - rank 是模型中隐语义因子的个数（默认为10）(10, 200)
   * - regParam 是ALS的正则化参数（默认为1.0）
   * 可以调整这些参数，不断优化结果，使均方差变小。比如：maxIter越大，regParam越 小，均方差会越小，推荐结果较优
   * runtime: 16:36:46-  48s
   */
  // 使用ALS来建立推荐模型：显性反馈
  private val predictionsExplicit: DataFrame = dmAlsExplicit
  // 使用ALS来建立隐性反馈
  //  private val predictionsImplicit: DataFrame = dmAlsImplicit


  private def dmAlsExplicit = {
    val alsExplicit = new ALS().setMaxIter(5).setRegParam(0.01)
      .setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
      .setNumBlocks(100)
    // 训练模型
    val modelExplicit = alsExplicit.fit(training)
    // 模型预测: 使用训练好的推荐模型对测试集中的用户商品进行预测评分，得到预测评分的数据集
    val predictionsExplicit = modelExplicit.transform(test)
    predictionsExplicit.show()
    println("===============")
    val rmseExplicit = evaluator.evaluate(predictionsExplicit)
    println(s"Explicit:Root-mean-square error = $rmseExplicit")

    predictionsExplicit
  }

  private def dmAlsImplicit = {
    val alsImplicit = new ALS().setMaxIter(5).setRegParam(0.01)
      .setImplicitPrefs(true)
      .setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
    val modelImplicit = alsImplicit.fit(training)
    val predictionsImplicit = modelImplicit.transform(test)
    predictionsImplicit.show()
    println("===============")
    val rmseImplicit = evaluator.evaluate(predictionsImplicit)
    println(s"Implicit:Root-mean-square error = $rmseImplicit")

    predictionsImplicit
  }


  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
}
