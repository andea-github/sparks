package demo.spk.ml

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import utils.MySparkUtil

/**
 * 特征抽取
 * Word2Vec 是一种著名的 词嵌入（Word Embedding） 方法，它可以计算每个单词在其给定语料库环境下的 分布式词向量（Distributed Representation，亦直接被称为词向量）。
 * 词向量表示可以在一定程度上刻画每个单词的语义。如果词的语义相近，它们的词向量在向量空间中也相互接近
 *
 * Word2vec是一个Estimator，它采用一系列代表文档的词语来训练word2vecmodel。* 该模型将每个词语映射到一个固定大小的向量。
 * word2vecmodel使用文档中每个词语的平均数来将文档转换为向量，然后这个向量可以作为预测的特征，来计算文档相似度计算等等。
 *
 * Word2Vec具有两种模型:
 * 1. CBOW ，其思想是通过每个词的上下文窗口词词向量来预测中心词的词向量。
 * 2. Skip-gram，其思想是通过每个中心词来预测其上下文窗口词，并根据预测结果来修正中心词的词向量。
 * 在ml库中，Word2vec 的实现使用的是skip-gram模型。Skip-gram的训练目标是学习词表征向量分布，其优化目标是在给定中心词的词向量的情况下，最大化似然函数
 *
 * @eg 用一组文档，其中一个词语序列代表一个文档。对于每一个文档，我们将其转换为一个特征向量。此特征向量可以被传递到一个学习算法。
 * @author admin 2020-5-29
 */
object FeatureExtractionWord2vec extends App {
  private val spark: SparkSession = MySparkUtil.spark("local", "FeatureExtractionWord2vec")
  val documentDF = spark.createDataFrame(Seq(
    "Hi I heard about Spark".split(" "),
    "I wish Java could use case classes".split(" "),
    "Logistic regression models are neat".split(" ")
  ).map(Tuple1.apply)).toDF("text")
  // 设置相应的超参数: 特征向量的维度为3
  val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result")
    .setVectorSize(3).setMinCount(0)
  // 读入训练数据 生成一个Word2VecModel
  val model = word2Vec.fit(documentDF)
  // 利用Word2VecModel把文档转变成特征向量
  val result = model.transform(documentDF)
  result.select("result").take(3).foreach(println)
}
