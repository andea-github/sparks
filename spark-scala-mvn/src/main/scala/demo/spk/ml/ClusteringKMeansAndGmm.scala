package demo.spk.ml

import org.apache.spark.ml.clustering.{GaussianMixture, KMeans}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import utils.{MyFileUtil, MySparkUtil}

case class model_instance(features: Vector)

/**
 * 聚类算法
 *
 * @author admin 2020-6-4
 */
object ClusteringKMeansAndGmm extends App {
  private val spark: SparkSession = MySparkUtil.spark("local", "ClusteringKMeansAndGmm")

  import spark.implicits._

  val rawData = spark.sparkContext.textFile(MyFileUtil.inputPath.concat("iris.data"))
  val df = rawData.filter(_.size > 0).map(line => {
    model_instance(Vectors.dense(
      line.split(",").filter(p => p.matches("\\d*(\\.?)\\d*"))
        .map(_.toDouble)
    ))
  }).toDF()

  //  dmKMeans
  /* 概率式的聚类方法：高斯混合模型（Gaussian Mixture Model, GMM）*/
  val gm = new GaussianMixture().setK(3) // 聚类数目为3
    .setPredictionCol("Prediction").setProbabilityCol("Probability")
  // maxIter  | 最大迭代次数，默认为100 |
  // seed     | 随机数种子，默认为随机Long值 |
  // Tol      | 对数似然函数收敛阈值，默认为0.01 |
  val model = gm.fit(df)
  val result = model.transform(df)
  result.show(10, false)
  /*查看模型的相关参数，与KMeans方法不同，GMM不直接给出聚类中心，而是给出各个混合成分（多元高斯分布）的参数*/
  for (i <- 0 until model.getK) {
    println("Component %d : weight is %f \n mu vector is %s \n sigma matrix is %s"
      .format(i,
        // weights成员获取到各个混合成分的权重，
        model.weights(i),
        // gaussians成员来获取到各个混合成分的参数（均值向量和协方差矩阵）
        model.gaussians(i).mean, model.gaussians(i).cov
      ))
  }

  /* 划分（Partitioning） 型的聚类方法*/
  private def dmKMeans = {
    val model = new KMeans().setK(3)
      .setFeaturesCol("features").setPredictionCol("prediction")
      .fit(df)
    val results = model.transform(df)
    results.show()
    println(s"===columns.size ${results.first().schema.fields.size}")
    results.collect().foreach(row => {
      println(row(0) + " is predicted as cluster " + row(1))
    })
    model.clusterCenters.foreach(center => println(s"Clustering Center: $center"))
    println(model.computeCost(df))
  }

}
