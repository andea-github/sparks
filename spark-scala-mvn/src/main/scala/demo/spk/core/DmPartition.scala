package demo.spk.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, TaskContext}
import utils.MySparkUtil

/**
 * sc.textFIle("hdfs:///data/", n)
 * 从 HDFS 中读取文件, 所以最终的分区数是由 Hadoop 的 InputFormat 来指定的,可能大于n
 *
 * @author admin 2020-8-20
 */
object DmPartition extends App {

  private val sc: SparkContext = MySparkUtil.sc("DmPartition")
  //sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0), 2)
  private val rdd: RDD[Int] = sc.parallelize(1 to 8, 2)
  printf("partition = %s %n", rdd.getNumPartitions)

  rdd.foreach(n => println(s" ${n}\t${TaskContext.getPartitionId()}"))
  println("repartition 过程可能出现 shuffle 操作")
  private val rdd_4: RDD[Int] = rdd.repartition(4)
  rdd_4.foreach(n => println(s" ${n}\t${TaskContext.getPartitionId()}"))
  println("===========")
  rdd_4.repartition(2).foreach(n => println(s" ${n}\t${TaskContext.getPartitionId()}"))


}
