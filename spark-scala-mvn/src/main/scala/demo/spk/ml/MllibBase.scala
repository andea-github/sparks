package demo.spk.ml

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1.本地向量（Local Vector）
 * 2.标注点（Labeled Point） 是一种带有标签（Label/Response）的本地向量
 * 3.本地矩阵（Local Matrix）
 * 4.分布式矩阵（Distributed Matrix）
 *
 * @author admin 2020-5-26
 */
object MllibBase extends App {
  // 创建一个稠密本地向量
  val dv = Vectors.dense(2.0, 0.0, 8.0)
  println(dv)
  /* 创建一个稀疏本地向量*/
  // 第二个参数数组指定了非零元素的索引，而第三个参数数组则给定了非零元素值
  val sv1: linalg.Vector = Vectors.sparse(3, Array(0, 2), Array(2.0, 8.0))
  // 第二个参数是一个序列，其中每个元素都是一个非零值的元组：(index,elem)
  val sv2 = Vectors.sparse(3, Seq((0, 2.0), (2, 8.0)))
  println(sv1)
  println(sv2)

  //创建一个标签为1.0（分类中可视为正样本）的稠密向量标注点
  val pos = LabeledPoint(1.0, Vectors.dense(2.0, 0.0, 8.0))
  //创建一个标签为0.0（分类中可视为负样本）的稀疏向量标注点
  val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(2.0, 8.0)))

  val conf = new SparkConf().setMaster("local").setAppName("DemoMllib")
  val sc = new SparkContext(conf)
  var inputpath = "file:///".concat("C:/tatas/spark/")
  val examples = MLUtils.loadLibSVMFile(sc, inputpath.concat("sample_libsvm_data.txt"))
  val head: LabeledPoint = examples.collect().head
  //  println(head)

  // 创建一个3行2列的稠密矩阵[ [1.0,2.0], [3.0,4.0], [5.0,6.0] ],数组参数是列先序的！
  val dm = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
  println(dm)
  // 创建一个3行2列的稀疏矩阵[ [9.0,0.0], [0.0,8.0], [0.0,6.0]]
  val sm = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
  println(sm)

  /** 4.1 行矩阵（RowMatrix） */
  val dv1 = Vectors.dense(1.0, 2.0, 3.0)
  val dv2 = Vectors.dense(2.0, 3.0, 4.0)
  // 使用两个本地向量创建一个RDD[Vector]
  val rows = sc.parallelize(Array(dv1, dv2))
  // 通过RDD[Vector]创建一个行矩阵
  val mat = new RowMatrix(rows)
  mat.rows.foreach(println)
  // 获取统计摘要
  val summary = mat.computeColumnSummaryStatistics()
  println("=== 方差向量: " + summary.variance)
  println("=== 平均向量: " + summary.mean)
  println("=== L1范数向量: " + summary.normL1)
  /** 4.2 索引行矩阵（IndexedRowMatrix） */
  val idxr1 = IndexedRow(1, dv1)
  val idxr2 = IndexedRow(2, dv2)
  val idxrows = sc.parallelize(Array(idxr1, idxr2))
  // 通过RDD[IndexedRow]创建一个索引行矩阵
  var idxmat = new IndexedRowMatrix(idxrows)
  idxmat.rows.foreach(println)
  /** 4.3 坐标矩阵（Coordinate Matrix） */
  val ent_1 = MatrixEntry(0, 1, 0.5)
  val ent_2 = MatrixEntry(2, 2, 1.8)
  var entries = sc.parallelize(Array(ent_1, ent_2))
  var coordMat = new CoordinateMatrix(entries)
  coordMat.entries.foreach(println)
  // 转置
  var transMat = coordMat.transpose()
  transMat.entries.foreach(println)
  // 将坐标矩阵转换成一个索引行矩阵
  val indexedRowMatrix = transMat.toIndexedRowMatrix()
  indexedRowMatrix.rows.foreach(println)
  /** 4.4 分块矩阵（Block Matrix） */
  val ent1 = new MatrixEntry(0, 0, 1)
  val ent2 = MatrixEntry(1, 1, 1)
  val ent3 = MatrixEntry(2, 0, -1)
  val ent4 = MatrixEntry(2, 1, 2)
  val ent5 = MatrixEntry(2, 2, 1)
  val ent6 = MatrixEntry(3, 0, 1)
  val ent7 = MatrixEntry(3, 1, 1)
  val ent8 = MatrixEntry(3, 3, 1)

  entries = sc.parallelize(Array(ent1, ent2, ent3, ent4, ent5, ent6, ent7, ent8))
  // 通过RDD[MatrixEntry]创建一个坐标矩阵
  coordMat = new CoordinateMatrix(entries)
  // 将坐标矩阵转换成2x2的分块矩阵并存储，尺寸通过参数传入
  val matA = coordMat.toBlockMatrix(2, 2).cache()
  // 可以用validate()方法判断是否分块成功
  matA.validate()
  println("=== \n" + matA.toLocalMatrix)
  val ata = matA.transpose.multiply(matA)
  println("=== \n" + ata.toLocalMatrix)
  println("===============")


}
