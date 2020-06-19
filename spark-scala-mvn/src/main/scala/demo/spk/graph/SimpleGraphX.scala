package demo.spk.graph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import utils.MySparkUtil

/**
 * 图计算
 *
 * @author admin 2020-6-9
 */
object SimpleGraphX extends App {
  //  var graph: Graph[VertexProperty, String] = null
  private val sc: SparkContext = MySparkUtil.sc("local", "SimpleGraphX")
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


  private val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array(
    (3L, ("rxin", "student")),
    (7L, ("jgonzal", "postdoc")),
    (5L, ("franklin", "prof")),
    (2L, ("istoica", "prof"))
  ))
  private val relationships: RDD[Edge[String]] = sc.parallelize(Array(
    Edge(3L, 7L, "collab"),
    Edge(5L, 3L, "advisor"),
    Edge(2L, 5L, "colleague"),
    Edge(5L, 7L, "pi")
  ))
  // 定义默认的作者,以防与不存在的作者有relationship边
  val defaultUser = ("John Doe", "Missing")
  val graph = Graph(users, relationships, defaultUser)
  println(s"==========${println()} 图结构操作 =============== ")
  println(s"==========${} triplets: ")
  graph.triplets.foreach(println)
  val subGraph = graph.subgraph(vpred = (id, attr) => attr._2 == "prof")
  subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
  subGraph.triplets.foreach(println)
  println()
  val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
  validGraph.vertices.collect.foreach(println(_))
  validGraph.triplets.map(
    triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
  ).collect.foreach(println(_))
  //  dmProps

  private def dmProps = {
    println(s"========== The graph has ${graph.numVertices} vertices and ${graph.numEdges} edges")

    println(s"==========${println()} 属性操作 =============== ")
    // 图的属性
    graph.vertices.filter { case (id, (name, occupation)) => occupation == "student" }.collect.foreach {
      case (id, (name, occupation)) => println(s"$name is $occupation")
    }
    // 边属性
    graph.edges.filter(e => e.attr == "advisor").collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))

    graph.outDegrees.foreach(println)
    println(s"max of outDegrees 出度: ${graph.outDegrees.reduce(max)}")
    graph.inDegrees.foreach(println)
    println(s"max of inDegrees  入度: ${graph.inDegrees.reduce(max)}")
    graph.degrees.foreach(println)
    println(s"max of Degrees  度数: ${graph.degrees.reduce(max)}")

    graph.mapVertices({ case (id, (name, occupation)) => (id, (name, occupation.concat("@finest"))) })
      .vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    graph.mapTriplets(triplet => triplet.srcAttr._2 + triplet.attr + triplet.dstAttr._2).edges.collect.foreach(println(_))

  }

  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }
}

class VertexProperty()

case class UserProperty(val name: String) extends VertexProperty

case class ProductProperty(val name: String, val price: Double) extends VertexProperty