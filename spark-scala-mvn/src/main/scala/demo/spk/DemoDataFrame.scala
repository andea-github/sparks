package demo.spk

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

/**
 * SparkSql
 *
 * create DataFrame
 * by jsonFile 每行一个json对象 {}
 * or by rdd
 *
 * @author admin 2020-5-25
 */
case class Person(name: String, age: Int)

object DemoDataFrame extends App {
  val inputPath = "file:///".concat("C:/tatas/spark/")
  val conf = new SparkConf().setMaster("local").setAppName("DemoDataFrame")
  // .enableHiveSupport()
  val spark = SparkSession.builder().config(conf).getOrCreate()
  var df = spark.read.json(inputPath.concat("people.json"))

  private val sqlContext: SQLContext = spark.sqlContext

  import sqlContext.implicits._

  val inputRdd = spark.sparkContext.textFile(inputPath.concat("people.txt"))
    .map(_.split(","))
  inputRdd.cache()
  /** 1.反射机制推断RDD */
  val rdd = inputRdd.map(field => Person(field(0), field(1).trim.toInt))
  df = rdd.toDF()

  /** 2.自定义RDD模式信息 */
  val schemaString = "name age"
  val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
  val schema = StructType(fields)
  val rdd2 = inputRdd.map(field => Row(field(0), field(1).trim))
  //  df = spark.createDataFrame(rdd2, schema)

  showDf(df)
  /** df 输出为文件 */
  //  df.select("name","age").write.format("csv").save("output/op_people.csv")
  //  df.rdd.saveAsTextFile("output/op_people.txt")
  println("============")

  private def showDf(df: DataFrame) = {
    df.show()
    df.printSchema()
    df.select(df("name"), df("age")).filter(df("age") > 18).show()

    df.createOrReplaceTempView("people")
    val frame: DataFrame = spark.sql("select * from people where age > 20")
    println(frame.first())
    frame.map(row => "Name: %s, Age: %s".format(row(0), row(1))).show()

  }
}

