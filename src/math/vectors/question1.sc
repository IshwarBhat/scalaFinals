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
                                                  //| on@3fabf088
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
  println("Question 1")       //> Question 1
  
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
  aRDD.collect().foreach(println)                 //>               
                                                  //| (1.0,6.0)
                                                  //| (2.0,8.0)
                                                  //| (3.0,10.0)
                                                  //| (3.0,10.0)
                                                  //| (4.0,12.0)
                                                  //| (5.0,14.0)
  val xVecUnitized = xVec/(xVec.norm)             //> xVecUnitized  : scala.collection.immutable.Vector[Double] = Vector(0.125, 0
                                                  //| .25, 0.375, 0.375, 0.5, 0.625)
  val parsedData = aRDD.map {tup => LabeledPoint(tup._2, Vectors.dense(tup._1/xVec.norm))}.cache()
                                                  //> parsedData  : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.La
                                                  //| beledPoint] = MapPartitionsRDD[1] at map at question1.scala:72
  parsedData.collect().foreach(println)           //> (6.0,[0.125])
                                                  //| (8.0,[0.25])
                                                  //| (10.0,[0.375])
                                                  //| (10.0,[0.375])
                                                  //| (12.0,[0.5])
                                                  //| (14.0,[0.625])
  
  
  var regression = new LinearRegressionWithSGD()  //> regression  : org.apache.spark.mllib.regression.LinearRegressionWithSGD = o
                                                  //| rg.apache.spark.mllib.regression.LinearRegressionWithSGD@7c455e96
  regression.setIntercept(true)                   //> res1: org.apache.spark.mllib.regression.LinearRegressionWithSGD = org.apach
                                                  //| e.spark.mllib.regression.LinearRegressionWithSGD@7c455e96
  val numIterations = 600                         //> numIterations  : Int = 600
  val stepSize = 2.0                              //> stepSize  : Double = 2.0
  regression.optimizer.setNumIterations(numIterations)
                                                  //> res2: org.apache.spark.mllib.optimization.GradientDescent = org.apache.spar
                                                  //| k.mllib.optimization.GradientDescent@985696
  regression.optimizer.setStepSize(stepSize)      //> res3: org.apache.spark.mllib.optimization.GradientDescent = org.apache.spar
                                                  //| k.mllib.optimization.GradientDescent@985696
  
  val model = regression.run(parsedData)          //> 17/05/04 02:02:04 WARN BLAS: Failed to load implementation from: com.github
                                                  //| .fommil.netlib.NativeSystemBLAS
                                                  //| 17/05/04 02:02:04 WARN BLAS: Failed to load implementation from: com.github
                                                  //| .fommil.netlib.NativeRefBLAS
                                                  //| model  : org.apache.spark.mllib.regression.LinearRegressionModel = org.apac
                                                  //| he.spark.mllib.regression.LinearRegressionModel: intercept = 5.467170722815
                                                  //| 118, numFeatures = 1
  model.weights                                   //> res4: org.apache.spark.mllib.linalg.Vector = [12.176614383802315]
	model.intercept                           //> res5: Double = 5.467170722815118
  
  val testArray = Array((5.0,14.0), (6.0,16.0), (7.0,18.0), (10.0, 24.0), (15.0, 34.0))
                                                  //> testArray  : Array[(Double, Double)] = Array((5.0,14.0), (6.0,16.0), (7.0,1
                                                  //| 8.0), (10.0,24.0), (15.0,34.0))
  val testRDD = spark.sparkContext.makeRDD(testArray)
                                                  //> testRDD  : org.apache.spark.rdd.RDD[(Double, Double)] = ParallelCollectionR
                                                  //| DD[989] at makeRDD at question1.scala:88
  val parsedTestData = testRDD.map {
    tup => LabeledPoint(tup._2, Vectors.dense(tup._1/xVec.norm))
  }.cache()
                                                  //> parsedTestData  : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regressio
                                                  //| n.LabeledPoint] = MapPartitionsRDD[990] at map at question1.scala:89
  val valuesAndPreds = parsedData.map { point =>
	  val prediction = model.predict(point.features)
	  (point.label, prediction)
	}                                         //> valuesAndPreds  : org.apache.spark.rdd.RDD[(Double, Double)] = MapPartition
                                                  //| sRDD[991] at map at question1.scala:90
  val valuesAndPreds2 = parsedTestData.map { point =>
  val prediction = model.predict(point.features)
	  (point.label, prediction)
	}                                         //> valuesAndPreds2  : org.apache.spark.rdd.RDD[(Double, Double)] = MapPartitio
                                                  //| nsRDD[992] at map at question1.scala:94
  // test with original data:
  valuesAndPreds.collect.foreach(println)         //> (6.0,6.989247520790408)
                                                  //| (8.0,8.511324318765697)
                                                  //| (10.0,10.033401116740986)
                                                  //| (10.0,10.033401116740986)
                                                  //| (12.0,11.555477914716276)
                                                  //| (14.0,13.077554712691565)
  // test with our own data:
  valuesAndPreds2.collect.foreach(println)        //> (14.0,13.077554712691565)
                                                  //| (16.0,14.599631510666853)
                                                  //| (18.0,16.121708308642145)
                                                  //| (24.0,20.68793870256801)
                                                  //| (34.0,28.29832269244446)
  
}