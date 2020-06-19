package demo.spk.ml

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession
import utils.MySparkUtil

/**
 * 特征抽取
 * CountVectorizer旨在通过计数来将一个文档转换为向量。
 * 当不存在先验字典时，CountVectorizer作为Estimator提取词汇进行训练，并生成一个CountVectorizerModel用于存储相应的词汇向量空间。
 * 该模型产生文档关于词语的稀疏表示，其表示可以传递给其他算法，例如LDA。
 *
 * @author admin 2020-5-29
 */
object FeatureExtractionCountVectorizer extends App {
  private val spark: SparkSession = MySparkUtil.spark("local", "FeatureExtractionCountVectorizer")
  val df = spark.createDataFrame(Seq(
    (0, Array("a", "b", "c")),
    (1, Array("a", "b", "b", "c", "a"))
  )).toDF("id", "words")
  // 设定词汇表的最大量为3，设定词汇表中的词至少要在2个文档中出现过，以过滤那些偶然出现的词汇。
  val cvModel = new CountVectorizer().setInputCol("words").setOutputCol("features")
    .setVocabSize(3).setMinDF(2).fit(df)
  // 训练结束后，可以通过CountVectorizerModel的vocabulary成员获得到模型的词汇表
  cvModel.vocabulary
  println("===================== \n")
  cvModel.transform(df).show(false)

  /*和其他Transformer不同，CountVectorizerModel可以通过指定一个先验词汇表来直接生成*/
  val cvm = new CountVectorizerModel(Array("a", "b", "c")).setInputCol("words").setOutputCol("features")
  cvm.transform(df).select("features").collect().foreach {
    println
  }
}
