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
object q1 {
	// Part b
	type Vec = Vector[Double]
	implicit class VectorEnrich(v: Vec){
  //  TBD def zipOp(vec1 : Vec, vec2 : Vec, operation: String)= {/* a utility function */}
  def +    ( other : Vec)        = (v zip other)   map { case (x1, x2) => x1 + x2 }
  def -    ( other : Vec)        = (v zip other)   map { case (x1, x2) => x1 - x2 }
   // applying a scalar to the right hand side of a Vector
  def *    (scalar : Double)     =  v map { x => x * scalar}
  def /    (scalar : Double)     =  v map {x => x * 1.0/scalar}
  def -    (scalar : Double)     =  v map { x => x - scalar}
  def +    (scalar : Double)     =  v map { x => x + scalar}
  def dot  ( other : Vec)        = (v zip other). map { case (x1, x2) => x1 * x2 }.sum
  def norm                       =   sqrt( (v.map(x => x * x)).sum )
  def mean                       =     v.sum/v.size
  
  def center                     = { val m = v.mean
                                     v.map( x => x - m)
                                   }
    };import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(1769); /* end VectorEnrich */
  println("Welcome to the Scala worksheet");$skip(239); 
  
  val fileMat = Array("[1.0,  6.0]",
                    "[2.0,  8.0]",
                    "[3.0,  10.0]",
                    "[3.0,  10.0]",
                    "[4.0,  12.0]",
                    "[5.0,  14.0]"
                   );System.out.println("""fileMat  : Array[String] = """ + $show(fileMat ));$skip(331); 
  
  // Cleaning the data, step one: remove square brackets and spaces
  val inputArray = fileMat.map(x => x.replaceAll(" ", "")
                                     .filterNot(c => c == '[' || c == ']'))
                          .map(e => e.split(","))
                          .map {case Array(x,y) => (x.toDouble,y.toDouble)};System.out.println("""inputArray  : Array[(Double, Double)] = """ + $show(inputArray ));$skip(155); 
                                 
  // Cleaning the data, step two: split at , and get the X and Y vectors
  val xVec = inputArray.map(x => x._1).toVector;System.out.println("""xVec  : Vector[Double] = """ + $show(xVec ));$skip(48); 
  val yVec = inputArray.map(x => x._2).toVector;System.out.println("""yVec  : Vector[Double] = """ + $show(yVec ));$skip(78); 
  val b_slope = (xVec.center dot yVec.center) / (xVec.center dot xVec.center);System.out.println("""b_slope  : Double = """ + $show(b_slope ));$skip(52); 
  val a_intercept = yVec.mean - b_slope * xVec.mean;System.out.println("""a_intercept  : Double = """ + $show(a_intercept ))}
}
