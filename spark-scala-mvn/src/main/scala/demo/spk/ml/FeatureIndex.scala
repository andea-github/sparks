package demo.spk.ml

import org.apache.spark.ml.feature
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.MySparkUtil

/**
 * 特征变换–标签和索引的转化
 * 在机器学习处理过程中，为了方便相关算法的实现，经常需要把标签数据（一般是字符串）转化成整数索引，或是在计算结束后将整数索引还原为相应的标签。
 * 常用转换器如：StringIndexer、IndexToString、OneHotEncoder、VectorIndexer
 * 用于特征转换的转换器和其他的机器学习算法一样，也属于ML Pipeline模型的一部分，可以用来构成机器学习流水线
 *
 * @author admin 2020-5-29
 */
object FeatureIndex extends App {
  private val spark: SparkSession = MySparkUtil.spark("local", "")

  /*StringIndexer*/
  val df1 = spark.createDataFrame(Seq(
    (0, "a"),
    (1, "b"),
    (2, "c"),
    (3, "a"),
    (4, "a"),
    (5, "b")
  )).toDF("id", "category")
  val model: StringIndexerModel = getStringIndexerModel("category", "categoryIndex", df1)
  dmStringIndexer(model, df1)
  val df2 = spark.createDataFrame(Seq(
    (0, "a"),
    (1, "b"),
    (2, "c"),
    (3, "a"),
    (4, "a"),
    (5, "d")
  )).toDF("id", "category")
  //  indexDfByModel(model,df2)

  /*IndexToString*/
  val indexed = model.setHandleInvalid("skip").transform(df2)
  dmIndex2String("categoryIndex", "originalCategory", indexed)

  /*OneHotEncoder*/
  dmHotEncoder(spark)

  /* VectorIndexer*/
  val data = Seq(
    Vectors.dense(-1.0, 1.0, 1.0),
    //    Vectors.dense(-2.0, 2.0, 1.0),
    Vectors.dense(-1.0, 3.0, 1.0),
    Vectors.dense(0.0, 5.0, 1.0))
  println(" ===>>>")
  dmVectorIndexer(spark, data)
  println("<<<===")

  /**
   * 之前的StringIndexer是针对单个类别型特征进行转换，倘若
   * 所有特征都已经被组织在一个向量中，又想对其中某些单个分量进行处理时，
   * Spark ML提供了 VectorIndexer 类来解决向量数据集中的类别性特征转换
   *
   * maxCategories 参数可以自动识别哪些特征是类别型的，并且将原始值转换为类别索引。
   * 它基于不同特征值的数量来识别哪些特征需要被类别化，那些取值可能性最多不超过maxCategories的特征需要会被认为是类别型的。
   *
   * @param spark
   * @param data
   */
  private def dmVectorIndexer(spark: SparkSession, data: Seq[Vector]) = {
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val indexer = new VectorIndexer().setInputCol("features").setOutputCol("indexed")
    // 只有种类小于2的特征才被认为是类别型特征，否则被认为是连续型特征：
    indexer.setMaxCategories(2)
    val indexerModel = indexer.fit(df)
    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " + categoricalFeatures.mkString(", "))
    val resIndex = indexerModel.transform(df)
    // 0号特征只有-1，0两种取值，分别被映射成0，1，而2号特征只有1种取值，被映射成0
    resIndex.show()
  }

  /** One-Hot编码适合一些期望类别特征为连续特征的算法，比如说逻辑斯蒂回归等 */
  private def dmHotEncoder(spark: SparkSession) = {
    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      //      (5, "c"),
      (6, "d"),
      (7, "d"),
      (8, "d"),
      (9, "d"),
      (10, "e"),
      (11, "e"),
      (12, "e"),
      (13, "e"),
      (14, "e")
    )).toDF("id", "category")
    val model = new feature.StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df)
    val indexed = model.transform(df)
    val encoder = new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec")
    // if not set 最后一个Category（””）被编码为全0向量
    //encoder.setDropLast(false)
    // 编码后的二进制特征呈稀疏向量形式，与StringIndexer编码的顺序相同
    val encoded = encoder.transform(indexed)
    encoded.show()
  }

  private def dmIndex2String(inputCol: String, outputCol: String, df: DataFrame) = {
    val converter = new IndexToString().setInputCol(inputCol).setOutputCol(outputCol)
    val converted = converter.transform(df)
    converted.select("id", "originalCategory").show(true)
  }

  def indexDfByModel(model: StringIndexerModel, df: DataFrame) = {
    val indexed = model.transform(df)
    //indexed.show()
    // SparkException: Unseen label: d
    val indexed2 = model.setHandleInvalid("skip").transform(df)
    indexed2.show()
  }

  def getStringIndexerModel(inputCol: String, outputCol: String, df: DataFrame) = {
    val indexer = new StringIndexer().setInputCol(inputCol).setOutputCol(outputCol)
    // 对这个DataFrame进行训练，产生StringIndexerModel对象
    indexer.fit(df)
  }

  private def dmStringIndexer(model: StringIndexerModel, df: DataFrame) = {
    // 对DataFrame进行转换操作,按照词频率的高低，把字符标签进行了排序
    val indexed = model.transform(df)
    indexed.show()
    model
  }
}
