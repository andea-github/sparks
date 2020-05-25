package demo.spk

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 *
 * @author admin 2020-5-25
 */
object DemoStreaming extends App {
  val inputPath = "file:///".concat("C:/tatas/spark/")
  val conf = new SparkConf().setMaster("local").setAppName("DemoStreaming")
  //  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(conf, Seconds(10))
  //  StreamFile

  var rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()
  var qStream = ssc.queueStream(rddQueue)
  val value: DStream[(Int, Int)] = qStream.map(r => (r % 10, 1)).reduceByKey(_ + _)
  value.print()

  ssc.start()
  appendRdd(ssc, rddQueue)
  ssc.awaitTermination()


  private def appendRdd(ssc: StreamingContext, queue: mutable.SynchronizedQueue[RDD[Int]]) = {
    for (i <- 1 to 10) {
      queue += ssc.sparkContext.makeRDD(1 to 100, 2)
      Thread.sleep(1000)
    }
  }

  private def StreamFile = {
    // 监听该目录下新增文件
    val lines = ssc.textFileStream(inputPath.concat("logfile"))
    val words = lines.flatMap(_.split(" "))
    val wdCounts = words.map(w => (w, 1)).reduceByKey(_ + _)
    wdCounts.print()
  }
}
