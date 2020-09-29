package utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author admin 2020-5-26
 */
object MySparkUtil {
  val conf = new SparkConf()

  def sc(master: String, appName: String) = {
    conf.setMaster(master).setAppName(appName)
    new SparkContext(conf)
  }

  def sc(appName: String) = {
    conf.setMaster("local").setAppName(appName)
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("WARN")
    sparkContext
  }

  def spark(master: String, appName: String) = {
    SparkSession.builder().master(master).appName(appName).getOrCreate()
  }

}
