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

object question2 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(596); 
  println("Welcome to the Scala worksheet");$skip(46); 
  Logger.getLogger("org").setLevel(Level.OFF);$skip(148); 
	val spark = SparkSession.builder
             							.master("local[*]")
             							.appName("finals")
             							.getOrCreate()
  import spark.implicits._;System.out.println("""spark  : org.apache.spark.sql.SparkSession = """ + $show(spark ));$skip(43); val res$0 = 
  spark.version;System.out.println("""res0: String = """ + $show(res$0));$skip(358); 
  
  val schema = StructType(StructField("id", IntegerType, true) ::
  												StructField("width", DoubleType, true) ::
  												StructField("height", DoubleType, true) ::
  												StructField("depth", DoubleType, true) ::
  												StructField("material", StringType, true) ::
  												StructField("color", StringType, true) :: Nil);System.out.println("""schema  : org.apache.spark.sql.types.StructType = """ + $show(schema ));$skip(153); 
  
  val BodyDF = spark.read
                  .option("header", true).schema(schema)
                  .csv("./../../workspace/finals/data/bodies.csv");System.out.println("""BodyDF  : org.apache.spark.sql.DataFrame = """ + $show(BodyDF ));$skip(24); 
   BodyDF.printSchema();$skip(17); 
   BodyDF.show();$skip(154); 
   // case class stuff(id: Int, width: Double,  height: Double, depth: Double, material: String, color: String)
   
   val caseClassDS = BodyDF.as[stuff];System.out.println("""caseClassDS  : org.apache.spark.sql.Dataset[stuff] = """ + $show(caseClassDS ));$skip(22); 
   caseClassDS.show()}
  
}
