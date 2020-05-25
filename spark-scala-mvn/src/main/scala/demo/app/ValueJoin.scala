package demo.app

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.MyFileUtil

/**
 * 连接操作
 *
 * 开发测试集  https://grouplens.org/datasets/movielens/
 * 使用数据集ml-1m 连接 ratings.dat 和 movies.dat，获取评分超过4.0的电影列表
 * 数据格式：
 * movies.dat   mid::Title::Genres
 * ratings.dat  userId::mid::rating::timestamp
 *
 * @author admin 2020-5-25
 */
object ValueJoin extends App {
  MyFileUtil.deleteOutputPath("C:\\Users\\workspace\\work_idea\\sparks\\result")
  val inputPath = "file:///".concat("C:/tatas/spark/ml-1m/")
  val conf = new SparkConf().setMaster("local").setAppName("ValueJoin")
  val sc = new SparkContext(conf)

  // (mid,avg_rating)
  val ratingRdd = getRatingRdd()
  // (mid,Title)
  val movieRDD = getMovieRdd
  // (mid,(mid,Title))
  val mKey = movieRDD.keyBy(_._1)
  println("1)========== mKey " + mKey.first())
  // (mid,(mid,avg_rating))
  val rKey = ratingRdd.keyBy(_._1)
  // (mid, ((mid,avg_rating), (mid,Title)))
  val join = rKey.join(mKey)
  println("2)========== join " + join.first())
  // (mid,avg_rating,Title)
  val result = join.filter(f => f._2._1._2 > 4.0)
    .map(f => (f._1, f._2._1._2, f._2._2._2))
  result.saveAsTextFile("result")
  println("================")

  private def getRatingRdd(): RDD[(Int, Double)] = {
    val ratings = sc.textFile(inputPath.concat("ratings.dat"))
    // (mid,rating)
    val rating = ratings.map(line => {
      val fields = line.split("::")
      (fields(1).toInt, fields(2).toDouble)
    })
    rating.groupByKey().map(t => (t._1, t._2.sum / t._2.size))
  }

  private def getMovieRdd = {
    val movies = sc.textFile(inputPath.concat("movies.dat"))
    movies.map(line => {
      val fields = line.split("::")
      (fields(0).toInt, fields(1))
    })
  }

}
