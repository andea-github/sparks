package demo.spk.ml

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession
import utils.MySparkUtil

/**
 * 特征抽取
 * eg: 以一组句子开始。
 * 首先使用分解器Tokenizer把句子划分为单个词语。对每一个句子（词袋），我们使用HashingTF将句子转换为特征向量，最后使用IDF重新调整特征向量。
 * 这种转换通常可以提高使用文本特征的性能。
 *
 * @author admin 2020-5-29
 */
object FeatureExtraction extends App {
  private val spark: SparkSession = MySparkUtil.spark("local", "MlFeatureExtraction")
  spark.sparkContext.setLogLevel("WARN")
  val sentenceData = spark.createDataFrame(Seq(
    (0, "I heard about Spark and I love Spark"),
    (0, "I wish Java could use case classes"),
    (1, "Logistic regression models are neat")
  )).toDF("label", "sentence")
  /*对句子进行分词*/
  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
  val wordsData = tokenizer.transform(sentenceData)
  wordsData.show(false)
  /*把句子哈希成特征向量，这里设置哈希表的桶数为2000。 */
  val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)
  // 分词序列被变换成一个稀疏特征向量，其中每个单词都被散列成了一个不同的索引值，特征向量在某一维度上的值即该词汇在文档中出现的次数。
  val featurizedData = hashingTF.transform(wordsData)
  featurizedData.select("rawFeatures").show(false)

  /*最后，使用IDF来对单纯的词频特征向量进行修正，使其更能体现不同词汇对文本的区别能力*/
  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
  val idfModel = idf.fit(featurizedData)
  // Get 每一个单词对应的 TF-IDF 度量值。
  val rescaledData = idfModel.transform(featurizedData)
  rescaledData.select("features", "label").take(3).foreach(println)

  /*通过TF-IDF得到的特征向量，可以被应用到相关的机器学习方法中。*/
}
