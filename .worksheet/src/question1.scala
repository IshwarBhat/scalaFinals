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
object question1 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(752); 
	Logger.getLogger("org").setLevel(Level.OFF);$skip(148); 
	val spark = SparkSession.builder
             							.master("local[*]")
             							.appName("finals")
             							.getOrCreate()
  import spark.implicits._;System.out.println("""spark  : org.apache.spark.sql.SparkSession = """ + $show(spark ));$skip(43); val res$0 = 
  spark.version
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
    };System.out.println("""res0: String = """ + $show(res$0));$skip(1033); /* end VectorEnrich */
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
  val a_intercept = yVec.mean - b_slope * xVec.mean;System.out.println("""a_intercept  : Double = """ + $show(a_intercept ));$skip(55); 
  
  val aRDD = spark.sparkContext.makeRDD(inputArray);System.out.println("""aRDD  : org.apache.spark.rdd.RDD[(Double, Double)] = """ + $show(aRDD ));$skip(34); 
  aRDD.collect().foreach(println);$skip(38); 
  val xVecUnitized = xVec/(xVec.norm);System.out.println("""xVecUnitized  : scala.collection.immutable.Vector[Double] = """ + $show(xVecUnitized ));$skip(99); 
  val parsedData = aRDD.map {tup => LabeledPoint(tup._2, Vectors.dense(tup._1/xVec.norm))}.cache();System.out.println("""parsedData  : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint] = """ + $show(parsedData ));$skip(40); 
  parsedData.collect().foreach(println);$skip(55); 
  
  
  var regression = new LinearRegressionWithSGD();System.out.println("""regression  : org.apache.spark.mllib.regression.LinearRegressionWithSGD = """ + $show(regression ));$skip(32); val res$1 = 
  regression.setIntercept(true);System.out.println("""res1: org.apache.spark.mllib.regression.LinearRegressionWithSGD = """ + $show(res$1));$skip(26); 
  val numIterations = 600;System.out.println("""numIterations  : Int = """ + $show(numIterations ));$skip(21); 
  val stepSize = 2.0;System.out.println("""stepSize  : Double = """ + $show(stepSize ));$skip(55); val res$2 = 
  regression.optimizer.setNumIterations(numIterations);System.out.println("""res2: org.apache.spark.mllib.optimization.GradientDescent = """ + $show(res$2));$skip(45); val res$3 = 
  regression.optimizer.setStepSize(stepSize);System.out.println("""res3: org.apache.spark.mllib.optimization.GradientDescent = """ + $show(res$3));$skip(44); 
  
  val model = regression.run(parsedData);System.out.println("""model  : org.apache.spark.mllib.regression.LinearRegressionModel = """ + $show(model ));$skip(16); val res$4 = 
  model.weights;System.out.println("""res4: org.apache.spark.mllib.linalg.Vector = """ + $show(res$4));$skip(17); val res$5 = 
	model.intercept;System.out.println("""res5: Double = """ + $show(res$5));$skip(91); 
  
  val testArray = Array((5.0,14.0), (6.0,16.0), (7.0,18.0), (10.0, 24.0), (15.0, 34.0));System.out.println("""testArray  : Array[(Double, Double)] = """ + $show(testArray ));$skip(54); 
  val testRDD = spark.sparkContext.makeRDD(testArray);System.out.println("""testRDD  : org.apache.spark.rdd.RDD[(Double, Double)] = """ + $show(testRDD ));$skip(106); 
  val parsedTestData = testRDD.map {tup => LabeledPoint(tup._2, Vectors.dense(tup._1/xVec.norm))}.cache();System.out.println("""parsedTestData  : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint] = """ + $show(parsedTestData ));$skip(131); 
  val valuesAndPreds = parsedData.map { point =>
	  val prediction = model.predict(point.features)
	  (point.label, prediction)
	};System.out.println("""valuesAndPreds  : org.apache.spark.rdd.RDD[(Double, Double)] = """ + $show(valuesAndPreds ));$skip(135); 
  val valuesAndPreds2 = parsedTestData.map { point =>
  val prediction = model.predict(point.features)
	  (point.label, prediction)
	};System.out.println("""valuesAndPreds2  : org.apache.spark.rdd.RDD[(Double, Double)] = """ + $show(valuesAndPreds2 ));$skip(72); 
  // test with original data:
  valuesAndPreds.collect.foreach(println);$skip(72); 
  // test with our own data:
  valuesAndPreds2.collect.foreach(println)}
  
}
