package demo.spk.ml

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{MyFileUtil, MySparkUtil}

case class Iris(features: Vector, label: String)

/**
 * 分类--决策树（decision tree）是一种基本的分类与回归方法
 *
 * 决策树：呈树形结构，其中
 * 每个内部节点表示一个属性上的测试，
 * 每个分支代表一个测试输出，
 * 每个叶节点代表一种类别。
 * 学习时  利用训练数据，根据损失函数最小化的原则建立决策树模型；
 * 预测时  对新的数据，利用决策树模型进行分类。
 *
 * 决策树学习通常包括3个步骤：特征选择 决策树的生成 决策树的剪枝
 * - 1 特征选择的准则是信息增益（或信息增益比、基尼指数等），每次计算每个特征的信息增益，并比较它们的大小，选择信息增益最大（信息增益比最大、基尼指数最小）的特征
 * - 2 选择信息增益最大的特征作为结点的特征，实际运用中一般希望决策树提前停止生长，限定叶节点包含的最低数据量，以防止由于过度生长造成的过拟合问题
 * - 3 剪枝 决策树往往对训练数据的分类很准确，但对未知的测试数据的分类却没有那么准确，即出现过拟合现象。解决这个问题的办法是考虑决策树的复杂度，对已生成的决策树进行简化
 * -   决策树的剪枝往往通过极小化决策树整体的损失函数来实现
 *
 * @author admin 2020-5-31
 */
object ClassifyDecisionTree extends App {
  private val spark: SparkSession = MySparkUtil.spark("local", "DecisionTree")
  spark.sparkContext.setLogLevel("WARN")
  private var index = 0

  private val df: DataFrame = getDfByFile("iris.data")
  println(s"=====${index += 1}$index count: ${df.count()} select * from iris")
  df.show(3, false)
  //  df.map(t => t(1) + ":" + t(0)).collect().take(10).foreach(println)
  // 我们把数据集随机分成训练集和测试集，其中训练集占1%
  val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

  var pipeline: Pipeline = buildPipeline(1)
  predictAndEvaluate(1) // 分类
  pipeline = buildPipeline(0)
  predictAndEvaluate(0) // 回归

  println(s"=====${index += 1}$index ")

  private def predictAndEvaluate(flag: Int) = {
    val model: PipelineModel = pipeline.fit(trainingData) //训练决策树模型
    val predictions = model.transform(testData) //进行预测
    println(s"==============${index += 1}$index 預測結果")
    //    predictions.show(false)
    //  predictions.select("predictedLabel", "label", "features").show()
    if (flag == 1)
      modelEvaluateClassify
    else
      modelEvaluateRegressor

    /*模型评估:评估决策树回归模型*/
    def modelEvaluateRegressor = {
      val evaluator = new RegressionEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
        .setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)
      println(s"======${index += 1}$index Success = ${1.0 - rmse}, squaredError = $rmse")
      val treeRegressionModel = model.stages(2).asInstanceOf[DecisionTreeRegressionModel]
      println(s"======${index += 1}$index Learned Regression tree model:\n" + treeRegressionModel.toDebugString)
    }

    /*模型评估: 评估决策树分类模型*/
    def modelEvaluateClassify = {
      val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
        .setMetricName("accuracy")
      val accuracy = evaluator.evaluate(predictions)
      println(s"======${index += 1}$index Success = $accuracy, Error = " + (1.0 - accuracy))
      // get model by PipelineModel.stages
      val treeModelClassifier = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
      println(s"======${index += 1}$index Learned classification tree model:\n" + treeModelClassifier.toDebugString)
    }


  }

  /*构建ML的pipeline 分别获取标签列和特征列，进行索引，并进行了重命名*/
  private def buildPipeline(flag: Int) = {
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures")
      .setMaxCategories(2)
      .fit(df)
    // 构建决策树分类模型
    val dtClassifier = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
      .setImpurity("gini") // 设置了用gini指数来进行特征选择
    //      .setMaxDepth(2)

    //构建决策树回归模型
    val dtRegressor = new DecisionTreeRegressor().setLabelCol("indexedLabel")
    //      .setMaxDepth(3)

    // 设置一个labelConverter，目的是把预测的类别重新转化成字符型的
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    println(s"======${index += 1}$index column changing ")
    //    df.columns.foreach(col => print(col.concat("\t")))
    //    print(labelIndexer.getOutputCol.concat("\t"))
    //    print(featureIndexer.getOutputCol.concat("\t"))
    //    print(dtClassifier.getRawPredictionCol.concat("\t"))
    //    print(dtClassifier.getProbabilityCol.concat("\t"))
    //    print(dtClassifier.getPredictionCol.concat("\t"))
    //    println(labelConverter.getOutputCol)

    // 构建pipeline，设置stage，然后调用fit()来训练模型
    if (flag >= 1) {
      println(s"======${index += 1}$index DecisionTreeClassifier parameters:\n ${dtClassifier.explainParams()}")
      new Pipeline().setStages(Array(labelIndexer, featureIndexer, dtClassifier, labelConverter))
    }
    else
      new Pipeline().setStages(Array(labelIndexer, featureIndexer, dtRegressor, labelConverter))
  }

  private def getDfByFile(fileName: String) = {
    import spark.implicits._ // 实现RDD到DataFrame的隐式传递
    // toDF 须使用 implicits 隐式装换
    val data = spark.sparkContext.textFile(MyFileUtil.inputPath.concat(fileName))
      .filter(_.size > 0).map(_.split(","))
      .map(p => Iris(Vectors.dense(p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble), p(4).toString()))
      .toDF()
    data.createOrReplaceTempView("iris")

    //    data
    spark.sql("select * from iris")
  }

}
