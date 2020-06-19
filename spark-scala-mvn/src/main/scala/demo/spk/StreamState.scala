package demo.spk

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * DStream 有状态转换
 * output local & mysql
 *
 * @author admin 2020-5-26
 */
object StreamState extends App {
  //设置log4j日志级别
  StreamingExamples.setStreamingLogLevels()
  val conf = new SparkConf().setMaster("local[2]").setAppName("StreamState")
  val ssc = new StreamingContext(conf, Seconds(5))
  //设置检查点，检查点具有容错机制
  ssc.checkpoint("file:///usr/local/spark/mycode/streaming/stateful/")
  val lines = ssc.socketTextStream("localhost", 9999)
  val words = lines.flatMap(_.split(" "))
  val wordDstream = words.map(x => (x, 1))
  // 按key做reduce操作，然后对各个批次的数据进行累加
  val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)
  stateDstream.print()
  //stateDstream.saveAsTextFiles("output/dstream.txt")
  ssc.start()
  ssc.awaitTermination()

  //outputMysql(stateDstream)

  private def outputMysql(dstream: DStream[(String, Int)]) = {
    //下面是新增的语句，把DStream保存到MySQL数据库中
    dstream.foreachRDD(rdd => {
      //内部函数
      def func(records: Iterator[(String, Int)]) {
        var conn: Connection = null
        var stmt: PreparedStatement = null
        try {
          val url = "jdbc:mysql://localhost:3306/spark"
          val user = "root"
          val password = "hadoop" //笔者设置的数据库密码是hadoop，请改成你自己的mysql数据库密码
          conn = DriverManager.getConnection(url, user, password)
          records.foreach(p => {
            val sql = "insert into wordcount(word,count) values (?,?)"
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, p._1.trim)
            stmt.setInt(2, p._2.toInt)
            stmt.executeUpdate()
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (stmt != null) {
            stmt.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      }

      val repartitionedRDD = rdd.repartition(3)
      repartitionedRDD.foreachPartition(func)
    })
  }


  //定义状态更新函数
  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }
}
