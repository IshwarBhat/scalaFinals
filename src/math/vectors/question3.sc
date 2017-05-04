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
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// Put case class outside object to avoid troubles of not getting output
case class Review(text: String, label: Double)

object question3 {
  println("Question 3: Sentiment Analysis")       //> Question 3: Sentiment Analysis
  Logger.getLogger("org").setLevel(Level.OFF)
	/*
	val spark = SparkSession.builder
             							.master("local[*]")
             							.appName("finals")
             							.getOrCreate()
  // import spark.implicits._
  spark.version
  */
  val conf = new SparkConf()
                   .setAppName("finals")
                   .setMaster("local[*]")         //> conf  : org.apache.spark.SparkConf = org.apache.spark.SparkConf@31206beb

	val sc = new SparkContext(conf)           //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.proper
                                                  //| ties
                                                  //| sc  : org.apache.spark.SparkContext = org.apache.spark.SparkContext@27eb329
                                                  //| 8
  val lines = sc.textFile("C:/Users/ibhatkat/workspace/finals/data/sentiment labelled sentences/imdb_labelled.txt")
                                                  //> lines  : org.apache.spark.rdd.RDD[String] = C:/Users/ibhatkat/workspace/fin
                                                  //| als/data/sentiment labelled sentences/imdb_labelled.txt MapPartitionsRDD[1]
                                                  //|  at textFile at question3.scala:35
  lines.persist()                                 //> res0: question3.lines.type = C:/Users/ibhatkat/workspace/finals/data/sentim
                                                  //| ent labelled sentences/imdb_labelled.txt MapPartitionsRDD[1] at textFile at
                                                  //|  question3.scala:35
  lines.count()                                   //> res1: Long = 1000
  val columns = lines.map{_.split("\\t")}         //> columns  : org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[2] at
                                                  //|  map at question3.scala:38
  columns.first                                   //> res2: Array[String] = Array("A very, very, very slow-moving, aimless movie 
                                                  //| about a distressed, drifting young man.  ", 0)
  
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
                                                  //> sqlContext  : org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLCon
                                                  //| text@7cb8437d
  import sqlContext.implicits._
  
	val reviews = columns.map{a => Review(a(0),a(1).toDouble)}.toDF()
                                                  //> reviews  : org.apache.spark.sql.DataFrame = [text: string, label: double]
  reviews.printSchema                             //> root
                                                  //|  |-- text: string (nullable = true)
                                                  //|  |-- label: double (nullable = true)
                                                  //| 
  reviews.groupBy("label").count.show             //> +-----+-----+
                                                  //| |label|count|
                                                  //| +-----+-----+
                                                  //| |  0.0|  500|
                                                  //| |  1.0|  500|
                                                  //| +-----+-----+
                                                  //| 
  val Array(trainingData, testData) = reviews.randomSplit(Array(0.8, 0.2))
                                                  //> trainingData  : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [t
                                                  //| ext: string, label: double]
                                                  //| testData  : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [text:
                                                  //|  string, label: double]
	trainingData.count                        //> res3: Long = 799
	testData.count                            //> res4: Long = 201
  
  import org.apache.spark.ml.feature.Tokenizer
	val tokenizer = new Tokenizer()
											  .setInputCol("text")
											  .setOutputCol("words")
                                                  //> tokenizer  : org.apache.spark.ml.feature.Tokenizer = tok_0cadc4c51aa1
   val tokenizedData = tokenizer.transform(trainingData)
                                                  //> tokenizedData  : org.apache.spark.sql.DataFrame = [text: string, label: dou
                                                  //| ble ... 1 more field]
   
   import org.apache.spark.ml.feature.HashingTF
	 val hashingTF = new HashingTF()
	                       .setNumFeatures(1000)
	                       .setInputCol(tokenizer.getOutputCol)
	                       .setOutputCol("features")
                                                  //> hashingTF  : org.apache.spark.ml.feature.HashingTF = hashingTF_a4b1952701f6
                                                  //| 
   val hashedData = hashingTF.transform(tokenizedData)
                                                  //> hashedData  : org.apache.spark.sql.DataFrame = [text: string, label: double
                                                  //|  ... 2 more fields]
   import org.apache.spark.ml.classification.LogisticRegression
	 val lr = new LogisticRegression()
	                .setMaxIter(10)
	                .setRegParam(0.01)        //> lr  : org.apache.spark.ml.classification.LogisticRegression = logreg_70c885
                                                  //| 916880
  
   import org.apache.spark.ml.Pipeline
	 val pipeline = new Pipeline()
	                      .setStages(Array(tokenizer, hashingTF, lr))
                                                  //> pipeline  : org.apache.spark.ml.Pipeline = pipeline_7ab89bf1eb46
   
   val pipeLineModel = pipeline.fit(trainingData) //> 17/05/04 03:42:18 WARN BLAS: Failed to load implementation from: com.github
                 //| .fommil.netlib.NativeSystemBLAS
                 //| 17/05/04 03:42:18 WARN BLAS: Failed to load implementation from: com.github
                 //| .fommil.netlib.NativeRefBLAS
                 //| 17/05/04 03:42:18 INFO LBFGS: Step Size: 1.541
                 //| 17/05/04 03:42:18 INFO LBFGS: Val and Grad Norm: 0.385865 (rel: 0.443) 0.39
                 //| 8530
                 //| 17/05/04 03:42:18 INFO LBFGS: Step Size: 1.000
                 //| 17/05/04 03:42:18 INFO LBFGS: Val and Grad Norm: 0.331483 (rel: 0.141) 0.48
                 //| 9178
                 //| 17/05/04 03:42:18 INFO LBFGS: Step Size: 1.000
                 //| 17/05/04 03:42:18 INFO LBFGS: Val and Grad Norm: 0.258889 (rel: 0.219) 0.23
                 //| 0936
                 //| 17/05/04 03:42:18 INFO LBFGS: Step Size: 1.000
                 //| 17/05/04 03:42:18 INFO LBFGS: Val and Grad Norm: 0.215090 (rel: 0.169) 0.10
                 //| 3048
                 //| 17/05/04 03:42:18 INFO LBFGS: Step Size: 1.000
                 //| 17/05/04 03:42:18 INFO LBFGS: Val and Grad Norm: 0.187097 (rel: 0.130) 0.08
                 //| 73480
                 //| 17/05/04 03:42:18 INFO LBFGS: Step
                 //| Output exceeds cutoff limit.
   val testPredictions = pipeLineModel.transform(testData)
                                                  //> testPredictions  : org.apache.spark.sql.DataFrame = [text: string, label: d
                                                  //| ouble ... 5 more fields]
   val trainingPredictions = pipeLineModel.transform(trainingData)
                                                  //> trainingPredictions  : org.apache.spark.sql.DataFrame = [text: string, labe
                                                  //| l: double ... 5 more fields]
   
   import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
   val evaluator = new BinaryClassificationEvaluator()
                                                  //> evaluator  : org.apache.spark.ml.evaluation.BinaryClassificationEvaluator =
                                                  //|  binEval_bbf52c5f7e74
   
   import org.apache.spark.ml.param.ParamMap
   val evaluatorParamMap = ParamMap(evaluator.metricName -> "areaUnderROC")
                                                  //> evaluatorParamMap  : org.apache.spark.ml.param.ParamMap = {
                                                  //| 	binEval_bbf52c5f7e74-metricName: areaUnderROC
                                                  //| }
   val aucTraining = evaluator.evaluate(trainingPredictions, evaluatorParamMap)
                                                  //> aucTraining  : Double = 0.9999812006517107
   
   val aucTest = evaluator.evaluate(testPredictions, evaluatorParamMap)
                                                  //> aucTest  : Double = 0.7084325396825397
   
   import org.apache.spark.ml.tuning.ParamGridBuilder
   val paramGrid = new ParamGridBuilder()
                         .addGrid(hashingTF.numFeatures, Array(10000, 100000))
                         .addGrid(lr.regParam, Array(0.01, 0.1, 1.0))
                         .addGrid(lr.maxIter, Array(20, 30))
                         .build()                 //> paramGrid  : Array[org.apache.spark.ml.param.ParamMap] = Array({
                //| 	logreg_70c885916880-maxIter: 20,
                //| 	hashingTF_a4b1952701f6-numFeatures: 10000,
                //| 	logreg_70c885916880-regParam: 0.01
                //| }, {
                //| 	logreg_70c885916880-maxIter: 20,
                //| 	hashingTF_a4b1952701f6-numFeatures: 10000,
                //| 	logreg_70c885916880-regParam: 0.1
                //| }, {
                //| 	logreg_70c885916880-maxIter: 20,
                //| 	hashingTF_a4b1952701f6-numFeatures: 10000,
                //| 	logreg_70c885916880-regParam: 1.0
                //| }, {
                //| 	logreg_70c885916880-maxIter: 20,
                //| 	hashingTF_a4b1952701f6-numFeatures: 100000,
                //| 	logreg_70c885916880-regParam: 0.01
                //| }, {
                //| 	logreg_70c885916880-maxIter: 20,
                //| 	hashingTF_a4b1952701f6-numFeatures: 100000,
                //| 	logreg_70c885916880-regParam: 0.1
                //| }, {
                //| 	logreg_70c885916880-maxIter: 20,
                //| 	hashingTF_a4b1952701f6-numFeatures: 100000,
                //| 	logreg_70c885916880-regParam: 1.0
                //| }, {
                //| 	logreg_70c885916880-maxIter: 30,
                //| 	hashingTF_a4b1952701f6-numFeatures: 10000,
                //| 	log
                //| Output exceeds cutoff limit.
   import org.apache.spark.ml.tuning.CrossValidator
   val crossValidator = new CrossValidator()
   													  .setEstimator(pipeline)
   													  .setEstimatorParamMaps(paramGrid)
   													  .setNumFolds(10)
   													  .setEvaluator(evaluator)
                                                  //> crossValidator  : org.apache.spark.ml.tuning.CrossValidator = cv_444ac38090
                                                  //| 0e
   val crossValidatorModel = crossValidator.fit(trainingData)
          //> 17/05/04 03:42:19 INFO LBFGS: Step Size: 0.9467
          //| 17/05/04 03:42:19 INFO LBFGS: Val and Grad Norm: 0.216270 (rel: 0.688) 0.28
          //| 0968
          //| 17/05/04 03:42:19 INFO LBFGS: Step Size: 1.000
          //| 17/05/04 03:42:19 INFO LBFGS: Val and Grad Norm: 0.135876 (rel: 0.372) 0.16
          //| 0260
          //| 17/05/04 03:42:19 INFO LBFGS: Step Size: 1.000
          //| 17/05/04 03:42:19 INFO LBFGS: Val and Grad Norm: 0.0755306 (rel: 0.444) 0.0
          //| 859171
          //| 17/05/04 03:42:19 INFO LBFGS: Step Size: 1.000
          //| 17/05/04 03:42:19 INFO LBFGS: Val and Grad Norm: 0.0557529 (rel: 0.262) 0.0
          //| 246433
          //| 17/05/04 03:42:19 INFO LBFGS: Step Size: 1.000
          //| 17/05/04 03:42:19 INFO LBFGS: Val and Grad Norm: 0.0528117 (rel: 0.0528) 0.
          //| 0124216
          //| 17/05/04 03:42:19 INFO LBFGS: Step Size: 1.000
          //| 17/05/04 03:42:19 INFO LBFGS: Val and Grad Norm: 0.0516282 (rel: 0.0224) 0.
          //| 00755015
          //| 17/05/04 03:42:19 INFO LBFGS: Step Size: 1.000
          //| 17/05/04 03:42:19 INFO LBFGS: Val and Grad Norm: 0.0512541 (
          //| Output exceeds cutoff limit.
   
   val newPredictions = crossValidatorModel.transform(testData)
                                                  //> newPredictions  : org.apache.spark.sql.DataFrame = [text: string, label: do
                                                  //| uble ... 5 more fields]
   val newAucTest = evaluator.evaluate(newPredictions, evaluatorParamMap)
                                                  //> newAucTest  : Double = 0.866468253968254
   
   val bestModel = crossValidatorModel.bestModel  //> bestModel  : org.apache.spark.ml.Model[_] = pipeline_7ab89bf1eb46
  
  
}