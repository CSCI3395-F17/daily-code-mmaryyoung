package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation._
import org.apache.spark.sql.Row
import org.apache.spark.mllib.stat.correlation
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter

object Classification extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  /*val schema = StructType(Array(
    StructField("age", IntegerType),
    StructField("workclass", StringType),
    StructField("fnlwgt", IntegerType),
    StructField("education", StringType),
    StructField("educationNum", IntegerType),
    StructField("maritalStatus", StringType),
    StructField("occupation", StringType),
    StructField("relationship", StringType),
    StructField("race", StringType),
    StructField("sex", StringType),
    StructField("capitalGain", IntegerType),
    StructField("capitalLoss", IntegerType),
    StructField("hoursPerWeek", IntegerType),
    StructField("nativeCountry", StringType),
    StructField("income", StringType)))*/
    
 // val data = spark.read.schema(schema).option("header",true).option("delimiter", "\t").csv("/data/BigData/admissions/AdmissionAnon.tsv").cache
  val data = spark.read.textFile("/data/BigData/admissions/AdmissionAnon.tsv").cache()
  val dataCol = data.map(s=>s.split("\t")).rdd
  val columnData = (1 to 47).toArray
  val schema = StructType(columnData.map(s=>StructField(s.toString(),DoubleType)))
  
  val dataColD = dataCol.map(s=>s.map(z => try{
    z.toDouble
  } catch {
    case e:NumberFormatException => 0.0
  }))
  
  val rddRow = dataColD.map(s=>Row(s:_*))

  var dataWithSchema = spark.createDataFrame(rddRow, schema).cache
  
  dataWithSchema.select($"47").distinct().show
  
  dataWithSchema.groupBy($"47").count().show()
 
  println("rows " + dataCol.count())
    

  val a1 = (1 to 5).toArray
  val a2 = (8 to 47).toArray
  val comb = (1 to 47).toArray.map(_.toString())
  
  val assembler = new VectorAssembler().
    setInputCols(comb).
    setOutputCol("features")
  val assembledData = assembler.transform(dataWithSchema)
  val Row(coeff1:Matrix) = Correlation.corr(assembledData, "features").head
  
  /*fancy print*/
  /*
  var arrayDim = Array.ofDim[Double](comb.length,comb.length)
  for(i <- 0 until comb.length; j <- 0 until comb.length) {
        arrayDim(i)(j) = coeff1.apply(i, j)
  }
  println("Pearson correlation matrix:\n" )
  arrayDim.foreach(arr => 
    {arr.foreach(x => {print(f"$x%2.2f | ")})
    println()
  })*/
  
  val inputVa = new VectorAssembler().setInputCols(comb.dropRight(1)).setOutputCol("features")
  val inputAssembled = inputVa.transform(dataWithSchema.withColumn("label", floor(col("47"))))
  val binaryAssembled = inputVa.transform(dataWithSchema.withColumn("label", when(col("47") < 2, 1).otherwise(0)))
  //binaryAssembled.show()
  val Array(train, test) =  binaryAssembled.randomSplit(Array(0.8, 0.2)).map(_.cache())
  
  val rf = new RandomForestClassifier() //accuracy a = 0.49, b = 0.861
  val gbt = new GBTClassifier() //binary accuracy b = 0.844
  val mlp = new MultilayerPerceptronClassifier().setLayers(Array(46,2)) //accuracy = 0.006
  val svm = new LinearSVC() //binary accuracy b = 0.846
  val ova = new OneVsRest().setClassifier(gbt) //accuracy = svm = 0.40
  val bayes = new NaiveBayes() //accuracy a = 0.22, b = 0.498
  val model = rf.fit(train)
  
  val predictions = model.transform(test)
  predictions.show()
  val evaluator = new MulticlassClassificationEvaluator
  val bevaluator = new BinaryClassificationEvaluator
  val accuracy = bevaluator.evaluate(predictions)
  println(s"accuracy = $accuracy")

  spark.stop
}
