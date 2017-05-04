import scala.math._
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

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

// Question 1
object question1 {
	Logger.getLogger("org").setLevel(Level.OFF)
	val spark = SparkSession.builder
             							.master("local[*]")
             							.appName("finals")
             							.getOrCreate()
                                                  //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| spark  : org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSessi
                                                  //| on@610db97e
  import spark.implicits._
  spark.version                                   //> res0: String = 2.1.0
	// Part b
	type Vec = Vector[Double]
	implicit class VectorEnrich(v: Vec){
  // VectorEnrich class copied from code given by Rucker
  def +    ( other : Vec)        = (v zip other)   map { case (x1, x2) => x1 + x2 }
  def -    ( other : Vec)        = (v zip other)   map { case (x1, x2) => x1 - x2 }
   // applying a scalar to the right hand side of a Vector
  def *    (scalar : Double)     =  v map { x => x * scalar}
  def /    (scalar : Double)     =  v map {x => x * 1.0/scalar}
  def -    (scalar : Double)     =  v map { x => x - scalar}
  def +    (scalar : Double)     =  v map { x => x + scalar}
  def dot  ( other : Vec)        =  (v zip other). map { case (x1, x2) => x1 * x2 }.sum
  def norm                       =   sqrt( (v.map(x => x * x)).sum )
  def mean                       =  v.sum/v.size
  
  def center                     = { val m = v.mean
                                     v.map( x => x - m)
                                   }
    }/* end VectorEnrich */
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  val fileMat = Array("[1.0,  6.0]",
                    "[2.0,  8.0]",
                    "[3.0,  10.0]",
                    "[3.0,  10.0]",
                    "[4.0,  12.0]",
                    "[5.0,  14.0]"
                   )                              //> fileMat  : Array[String] = Array([1.0,  6.0], [2.0,  8.0], [3.0,  10.0], [3
                                                  //| .0,  10.0], [4.0,  12.0], [5.0,  14.0])
  
  // Cleaning the data, step one: remove square brackets and spaces
  val inputArray = fileMat.map(x => x.replaceAll(" ", "")
                                     .filterNot(c => c == '[' || c == ']'))
                          .map(e => e.split(","))
                          .map {case Array(x,y) => (x.toDouble,y.toDouble)}
                                                  //> inputArray  : Array[(Double, Double)] = Array((1.0,6.0), (2.0,8.0), (3.0,10
                                                  //| .0), (3.0,10.0), (4.0,12.0), (5.0,14.0))
                                 
  // Cleaning the data, step two: split at , and get the X and Y vectors
  val xVec = inputArray.map(x => x._1).toVector   //> xVec  : Vector[Double] = Vector(1.0, 2.0, 3.0, 3.0, 4.0, 5.0)
  val yVec = inputArray.map(x => x._2).toVector   //> yVec  : Vector[Double] = Vector(6.0, 8.0, 10.0, 10.0, 12.0, 14.0)
  val b_slope = (xVec.center dot yVec.center) / (xVec.center dot xVec.center)
                                                  //> b_slope  : Double = 2.0
  val a_intercept = yVec.mean - b_slope * xVec.mean
                                                  //> a_intercept  : Double = 4.0
  
  val aRDD = spark.sparkContext.makeRDD(inputArray)
                                                  //> aRDD  : org.apache.spark.rdd.RDD[(Double, Double)] = ParallelCollectionRDD[
                                                  //| 0] at makeRDD at question1.scala:69
  aRDD.collect().foreach(println)                 //> [Stage 0:>                                                          (0 + 0
                                                  //| ) / 8]                                                                    
                                                  //|             (1.0,6.0)
                                                  //| (2.0,8.0)
                                                  //| (3.0,10.0)
                                                  //| (3.0,10.0)
                                                  //| (4.0,12.0)
                                                  //| (5.0,14.0)
  
}