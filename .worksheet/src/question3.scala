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

object question3 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(720); 
  println("Question 3: Sentiment Analysis");$skip(46); 
  Logger.getLogger("org").setLevel(Level.OFF);$skip(315); 
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
                   .setMaster("local[*]");System.out.println("""conf  : org.apache.spark.SparkConf = """ + $show(conf ));$skip(34); 

	val sc = new SparkContext(conf);System.out.println("""sc  : org.apache.spark.SparkContext = """ + $show(sc ));$skip(116); 
  val lines = sc.textFile("C:/Users/ibhatkat/workspace/finals/data/sentiment labelled sentences/imdb_labelled.txt");System.out.println("""lines  : org.apache.spark.rdd.RDD[String] = """ + $show(lines ));$skip(18); val res$0 = 
  lines.persist();System.out.println("""res0: question3.lines.type = """ + $show(res$0));$skip(16); val res$1 = 
  lines.count();System.out.println("""res1: Long = """ + $show(res$1));$skip(42); 
  val columns = lines.map{_.split("\\t")};System.out.println("""columns  : org.apache.spark.rdd.RDD[Array[String]] = """ + $show(columns ));$skip(16); val res$2 = 
  columns.first;System.out.println("""res2: Array[String] = """ + $show(res$2));$skip(62); 
  
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._;System.out.println("""sqlContext  : org.apache.spark.sql.SQLContext = """ + $show(sqlContext ));$skip(102); 
  
	val reviews = columns.map{a => Review(a(0),a(1).toDouble)}.toDF();System.out.println("""reviews  : org.apache.spark.sql.DataFrame = """ + $show(reviews ));$skip(22); 
  reviews.printSchema;$skip(38); 
  reviews.groupBy("label").count.show;$skip(75); 
  val Array(trainingData, testData) = reviews.randomSplit(Array(0.8, 0.2));System.out.println("""trainingData  : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = """ + $show(trainingData ));System.out.println("""testData  : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = """ + $show(testData ));$skip(20); val res$3 = 
	trainingData.count;System.out.println("""res3: Long = """ + $show(res$3));$skip(16); val res$4 = 
	testData.count
  
  import org.apache.spark.ml.feature.Tokenizer;System.out.println("""res4: Long = """ + $show(res$4));$skip(153); 
	val tokenizer = new Tokenizer()
											  .setInputCol("text")
											  .setOutputCol("words");System.out.println("""tokenizer  : org.apache.spark.ml.feature.Tokenizer = """ + $show(tokenizer ));$skip(57); 
   val tokenizedData = tokenizer.transform(trainingData)
   
   import org.apache.spark.ml.feature.HashingTF;System.out.println("""tokenizedData  : org.apache.spark.sql.DataFrame = """ + $show(tokenizedData ));$skip(243); 
	 val hashingTF = new HashingTF()
	                       .setNumFeatures(1000)
	                       .setInputCol(tokenizer.getOutputCol)
	                       .setOutputCol("features");System.out.println("""hashingTF  : org.apache.spark.ml.feature.HashingTF = """ + $show(hashingTF ));$skip(55); 
   val hashedData = hashingTF.transform(tokenizedData)
   import org.apache.spark.ml.classification.LogisticRegression;System.out.println("""hashedData  : org.apache.spark.sql.DataFrame = """ + $show(hashedData ));$skip(169); 
	 val lr = new LogisticRegression()
	                .setMaxIter(10)
	                .setRegParam(0.01)
  
   import org.apache.spark.ml.Pipeline;System.out.println("""lr  : org.apache.spark.ml.classification.LogisticRegression = """ + $show(lr ));$skip(141); 
	 val pipeline = new Pipeline()
	                      .setStages(Array(tokenizer, hashingTF, lr));System.out.println("""pipeline  : org.apache.spark.ml.Pipeline = """ + $show(pipeline ));$skip(54); 
   
   val pipeLineModel = pipeline.fit(trainingData);System.out.println("""pipeLineModel  : org.apache.spark.ml.PipelineModel = """ + $show(pipeLineModel ));$skip(59); 
   val testPredictions = pipeLineModel.transform(testData);System.out.println("""testPredictions  : org.apache.spark.sql.DataFrame = """ + $show(testPredictions ));$skip(67); 
   val trainingPredictions = pipeLineModel.transform(trainingData)
   
   import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;System.out.println("""trainingPredictions  : org.apache.spark.sql.DataFrame = """ + $show(trainingPredictions ));$skip(130); 
   val evaluator = new BinaryClassificationEvaluator()
   
   import org.apache.spark.ml.param.ParamMap;System.out.println("""evaluator  : org.apache.spark.ml.evaluation.BinaryClassificationEvaluator = """ + $show(evaluator ));$skip(125); 
   val evaluatorParamMap = ParamMap(evaluator.metricName -> "areaUnderROC");System.out.println("""evaluatorParamMap  : org.apache.spark.ml.param.ParamMap = """ + $show(evaluatorParamMap ));$skip(80); 
   val aucTraining = evaluator.evaluate(trainingPredictions, evaluatorParamMap);System.out.println("""aucTraining  : Double = """ + $show(aucTraining ));$skip(76); 
   
   val aucTest = evaluator.evaluate(testPredictions, evaluatorParamMap)
   
   import org.apache.spark.ml.tuning.ParamGridBuilder;System.out.println("""aucTest  : Double = """ + $show(aucTest ));$skip(344); 
   val paramGrid = new ParamGridBuilder()
                         .addGrid(hashingTF.numFeatures, Array(10000, 100000))
                         .addGrid(lr.regParam, Array(0.01, 0.1, 1.0))
                         .addGrid(lr.maxIter, Array(20, 30))
                         .build()
   import org.apache.spark.ml.tuning.CrossValidator;System.out.println("""paramGrid  : Array[org.apache.spark.ml.param.ParamMap] = """ + $show(paramGrid ));$skip(269); 
   val crossValidator = new CrossValidator()
   													  .setEstimator(pipeline)
   													  .setEstimatorParamMaps(paramGrid)
   													  .setNumFolds(10)
   													  .setEvaluator(evaluator);System.out.println("""crossValidator  : org.apache.spark.ml.tuning.CrossValidator = """ + $show(crossValidator ));$skip(62); 
   val crossValidatorModel = crossValidator.fit(trainingData);System.out.println("""crossValidatorModel  : org.apache.spark.ml.tuning.CrossValidatorModel = """ + $show(crossValidatorModel ));$skip(68); 
   
   val newPredictions = crossValidatorModel.transform(testData);System.out.println("""newPredictions  : org.apache.spark.sql.DataFrame = """ + $show(newPredictions ));$skip(74); 
   val newAucTest = evaluator.evaluate(newPredictions, evaluatorParamMap);System.out.println("""newAucTest  : Double = """ + $show(newAucTest ));$skip(53); 
   
   val bestModel = crossValidatorModel.bestModel;System.out.println("""bestModel  : org.apache.spark.ml.Model[_] = """ + $show(bestModel ))}
  
  
}
