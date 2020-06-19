package demo.spk.ml

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{MyFileUtil, MySparkUtil}

case class Iris2(features: Vector, label: String)

/**
 * 分类--逻辑斯蒂回归：统计学习中的经典分类方法，属于对数线性模型。
 * logistic回归的因变量可以是二分类的，也可以是多分类的。
 *
 * 这里主要用鸢尾花的后两个属性（花瓣的长度和宽度）来进行分类
 *
 * @demo iris数据集:包含150个数据集，分为3类，每类50个数据，每个数据包含4个属性，是在数据挖掘、数据分类中非常常用的测试集、训练集
 *       https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
 * @author admin 2020-6-4
 */
object ClassifyLogisticRegression extends App {
  private val spark: SparkSession = MySparkUtil.spark("local", "ClassifyLogisticRegression")
  spark.sparkContext.setLogLevel("WARN")
  var index = 0

  import spark.implicits._

  val inputData = spark.sparkContext.textFile(MyFileUtil.inputPath.concat("iris.data"))
    .filter(_.size > 0).map(_.split(","))
  inputData.cache()
  dmStatistics
  val df: DataFrame = inputRdd2DF
  val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
  val pipeline = buildPipeline
  val model = pipeline.fit(trainingData) // 训练模型
  predictAndEvaluate(model) // 预测评估

  println(s"=====${index += 1}$index ")

  /*构建ML的pipeline & 模型评估*/
  private def buildPipeline = {
    val labelIndexer = new StringIndexer()
      .setInputCol("label").setOutputCol("indexedLabel").fit(df)
    // spark 2.0 需要使用 ml
    val featureIndexer = new VectorIndexer()
      .setInputCol("features").setOutputCol("indexedFeatures").fit(df)
    val lr = new LogisticRegression()
      .setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
      .setRegParam(0.3).setMaxIter(10).setElasticNetParam(0.8)
    // 查看参数设置
    println(s"=====${index += 1}$index LogisticRegression parameters:\n${lr.explainParams()}")
    // 将预测的标签重新转化成字符型
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))
  }

  private def predictAndEvaluate(model: PipelineModel) = {
    val predictions = model.transform(testData)
    predictions.show(false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel").setPredictionCol("prediction")
    // 模型评估
    val accuracy = evaluator.evaluate(predictions)
    println(s"=====${index += 1}$index Success = $accuracy, Error = ${1.0 - accuracy}")

    val lrModel = model.stages(2).asInstanceOf[LogisticRegressionModel]
    println(s"Coefficients: ${lrModel.coefficients}")
    println(s"Intercept: ${lrModel.intercept}")
    println(s"numClasses: ${lrModel.numClasses}")
    println(s"numFeatures: ${lrModel.numFeatures}")
  }

  private def inputRdd2DF = {
    val df = inputData.map(p => Iris2(Vectors.dense(p(2).toDouble, p(3).toDouble), p(4))).toDF()
    df.show(false)
    df.createOrReplaceTempView("iris")
    // ml库中的logistic回归目前只支持2分类, 只取2个特征？
    spark.sql("select * from iris where label != 'Iris-setosa'")
    //    df
  }

  /**
   * 简要分析：数据的基本的统计信息，例如最大值、最小值、均值、方差等
   */
  private def dmStatistics = {
    val rdd: RDD[org.apache.spark.mllib.linalg.Vector] = inputData.map(p => org.apache.spark.mllib.linalg.Vectors.dense(p(2).toDouble, p(3).toDouble))
    println(s"===========${index += 1}$index ${rdd.first()}")
    val summary: MultivariateStatisticalSummary = Statistics.colStats(rdd)
    println(s"=====${index += 1}$index count: ${summary.count}")
    //    println(s"=====${index += 1}$index mean: ${summary.mean}")
    //    println(s"=====${index += 1}$index variance: ${summary.variance}")
    //    println(s"=====${index += 1}$index numNonzeros: ${summary.numNonzeros}")
    //    println(s"=====${index += 1}$index max: ${summary.max}")
    //    println(s"=====${index += 1}$index min: ${summary.min}")
  }


}
