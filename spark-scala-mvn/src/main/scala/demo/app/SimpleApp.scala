package demo.app

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 该程序计算 /usr/local/spark/README 文件中包含 "a" 的行数 和包含 "b" 的行数。
 * /usr/local/spark 为 Spark 的安装目录，如果不是该目录请自行修改。
 * 不同于 Spark shell，独立应用程序需要通过 `val sc = new SparkContext(conf)`
 * 初始化 SparkContext，SparkContext 的参数 SparkConf 包含了应用程序的信息。
 */
object SimpleApp {
  def main(args: Array[String]): Unit = {
    print("======================== ")
    //    println(args.apply(0) + ", " + args.apply(1))
    // C:/datas   usr/local/spark
    val logFile = "file:///C:/datas/README.md";
    val conf = new SparkConf().setMaster("local").setAppName("SimpleApp")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(logFile, 2).cache()
    val numAs = lines.filter(line => line.contains("a")).count()
    val numBs = lines.filter(line => line.contains("b")).count()
    print("================== ")
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    val wordCounts = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //打印输出
    wordCounts.foreach(pair => println(pair._1 + ": " + pair._2))

    //文本输出
    //    wordCounts.saveAsTextFile("D://result.txt")
    sc.stop()
  }
}

