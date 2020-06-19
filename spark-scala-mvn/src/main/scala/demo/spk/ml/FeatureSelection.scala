package demo.spk.ml

import org.apache.spark.ml.feature.{ChiSqSelector, ChiSqSelectorModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import utils.MySparkUtil

/**
 * 特征选择 -- 卡方选择器(是有监督的)
 * 在特征向量中选择出那些“优秀”的特征，在高维数据分析中十分常用，可以提升学习器的性能。
 *
 * 卡方选择则是统计学上常用的一种有监督特征选择方法，它
 * 通过对特征和真实标签之间进行卡方检验，来判断该特征和真实标签的关联程度，进而确定是否对其进行选择
 *
 * @author admin 2020-5-31
 */
object FeatureSelection extends App {
  private val spark: SparkSession = MySparkUtil.spark("local", "FeatureSelection")
  val df = spark.createDataFrame(Seq(
    (1, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1),
    (2, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0),
    (3, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0)
  )).toDF("id", "features", "label")
  // 设置只选择和标签关联性最强的一个特征
  val selector = new ChiSqSelector().setNumTopFeatures(1)
    .setFeaturesCol("features").setLabelCol("label").setOutputCol("selected-feature")
  private val model: ChiSqSelectorModel = selector.fit(df)
  // 选出最有用的特征列
  val result = model.transform(df)
  result.show()
}
