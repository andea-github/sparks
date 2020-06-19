package demo.spk.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}
import utils.MySparkUtil

/**
 * 构建机器学习工作流, 预测包含 spark 的句子
 *
 * @author admin 2020-5-28
 */
object MlPipeline extends App {

  private val spark: SparkSession = MySparkUtil.spark("local", "MlPipeline")
  val training = spark.createDataFrame(Seq(
    (0L, "a b c d e spark", 1.0),
    (1L, "b d", 0.0),
    (2L, "spark f g h", 1.0),
    (3L, "hadoop mapReduce", 0.0)
  )).toDF("id", "text", "label")

  val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
  val hashTF = new HashingTF().setNumFeatures(1000)
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("features")
  val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)

  val pipeline = new Pipeline().setStages(Array(tokenizer, hashTF, lr))
  val model = pipeline.fit(training)

  val testDf = spark.createDataFrame(Seq(
    (8L, "b c spark d"),
    (4L, "spark i j k"),
    (5L, "l m n"),
    (6L, "spark a"),
    (7L, "apache hadoop")
  )).toDF("id", "text")
  model.transform(testDf)
    .select("id", "text", "probability", "prediction").collect()
    .foreach({ case Row(id: Long, text: String, prob: Vector, prediction: Double) => println(s"($id, $text) --> prob=$prob, prediction=$prediction") })
}
