import java.util.Date
import org.apache.log4j.{Level, Logger}
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

case class stuff(id: Int, width: Double,  height: Double, depth: Double, material: String, color: String)

object question2 {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  Logger.getLogger("org").setLevel(Level.OFF)
	val spark = SparkSession.builder
             							.master("local[*]")
             							.appName("finals")
             							.getOrCreate()
                                                  //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| spark  : org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSessi
                                                  //| on@6754ef00
  import spark.implicits._
  spark.version                                   //> res0: String = 2.1.0
  
  val schema = StructType(StructField("id", IntegerType, true) ::
  												StructField("width", DoubleType, true) ::
  												StructField("height", DoubleType, true) ::
  												StructField("depth", DoubleType, true) ::
  												StructField("material", StringType, true) ::
  												StructField("color", StringType, true) :: Nil)
                                                  //> schema  : org.apache.spark.sql.types.StructType = StructType(StructField(id
                                                  //| ,IntegerType,true), StructField(width,DoubleType,true), StructField(height,
                                                  //| DoubleType,true), StructField(depth,DoubleType,true), StructField(material,
                                                  //| StringType,true), StructField(color,StringType,true))
  
  val BodyDF = spark.read
                  .option("header", true).schema(schema)
                  .csv("./../../workspace/finals/data/bodies.csv")
                                                  //> BodyDF  : org.apache.spark.sql.DataFrame = [id: int, width: double ... 4 mo
                                                  //| re fields]
   BodyDF.printSchema()                           //> root
                                                  //|  |-- id: integer (nullable = true)
                                                  //|  |-- width: double (nullable = true)
                                                  //|  |-- height: double (nullable = true)
                                                  //|  |-- depth: double (nullable = true)
                                                  //|  |-- material: string (nullable = true)
                                                  //|  |-- color: string (nullable = true)
                                                  //| 
   BodyDF.show()                                  //> +---+-----+------+-----+--------+-------+
                                                  //| | id|width|height|depth|material|  color|
                                                  //| +---+-----+------+-----+--------+-------+
                                                  //| |  1| 10.0|  10.0| 10.0|    wood|  brown|
                                                  //| |  2| 20.0|  20.0| 20.0|   glass|  green|
                                                  //| |  3| 30.0|  30.0| 30.0|   metal| yellow|
                                                  //| |  4| 33.0|  30.0| 30.0|   metal|  black|
                                                  //| |  5| 40.0|  30.0| 30.0|   metal|  black|
                                                  //| |  6| 40.0|  30.0| 30.0|   metal| purple|
                                                  //| |  7| 45.0|  30.0| 30.0|   metal| orange|
                                                  //| +---+-----+------+-----+--------+-------+
                                                  //| 
   // case class stuff(id: Int, width: Double,  height: Double, depth: Double, material: String, color: String)
   
   val caseClassDS = BodyDF.as[stuff]             //> caseClassDS  : org.apache.spark.sql.Dataset[stuff] = [id: int, width: doubl
                                                  //| e ... 4 more fields]
   caseClassDS.show()                             //> +---+-----+------+-----+--------+-------+
                                                  //| | id|width|height|depth|material|  color|
                                                  //| +---+-----+------+-----+--------+-------+
                                                  //| |  1| 10.0|  10.0| 10.0|    wood|  brown|
                                                  //| |  2| 20.0|  20.0| 20.0|   glass|  green|
                                                  //| |  3| 30.0|  30.0| 30.0|   metal| yellow|
                                                  //| |  4| 33.0|  30.0| 30.0|   metal|  black|
                                                  //| |  5| 40.0|  30.0| 30.0|   metal|  black|
                                                  //| |  6| 40.0|  30.0| 30.0|   metal| purple|
                                                  //| |  7| 45.0|  30.0| 30.0|   metal| orange|
                                                  //| +---+-----+------+-----+--------+-------+
                                                  //| 
  
}