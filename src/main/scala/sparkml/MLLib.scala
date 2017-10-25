package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.DataFrameStatFunctions

object MLLib extends App {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  val directory = "/data/BigData/brfss/"

  spark.sparkContext.setLogLevel("WARN")
  
  case class ColumnRow(col:Int,name:String,fieldLength:Int)
  
  
  def getColFile(fname:String):Array[ColumnRow] = {
		val source = scala.io.Source.fromFile(fname)
		val lines = source.getLines().drop(1).toArray
		val cols = lines.map(_.split('\t')).map(row => ColumnRow(row(0).toInt,row(1),row(2).toInt))
		source.close()
    cols
  }
  
  val colFile = getColFile(directory+"Columns.txt")
  
  val schemaSeq = for (i <- colFile) yield {
    StructField(i.name,DoubleType)
  }
  
  val schema = StructType(schemaSeq)
  
  val dataRDD = spark.sparkContext.textFile(directory+"LLCP2016.asc")
  
  val rddDelimited = dataRDD.map{row => 
    val splitUp = for (i <- colFile) yield {
      val fieldString = row.substring(i.col, i.col+i.fieldLength).trim()
      try{ fieldString.toDouble}
      catch{case e: NumberFormatException=> 0.0}
    }
    Row.fromSeq(splitUp)
  }
  
  
  var dataframe = spark.createDataFrame(rddDelimited,schema)
  
  // Filtering the dataframe leaving only meaningful rows
  dataframe = dataframe.filter('GENHLTH < 6 && 'PHYSHLTH < 31 && 'MENTHLTH < 31 && 'POORHLTH < 31 && 'EXERANY2 < 3 && 'SLEPTIM1 < 25 && 'CHECKUP1 < 6 && 'CVDINFR4 < 3 && 'CVDCRHD4 < 3 && 'CVDSTRK3 < 3 && 'ASTHMA3 < 3 && 'CHCSCNCR < 3 && 'CHCOCNCR < 3 && 'CHCCOPD1 < 3 && 'HAVARTH3 < 3 && 'ADDEPEV2 < 3 && 'CHCKIDNY < 3 && 'DIABETE3 < 5)

  def findMaxCorr(colName: String):(Double, String)={
      val healthCols = Array("CHECKUP1", "EXERANY2", "SLEPTIM1", "CVDINFR4", "CVDCRHD4", "CVDSTRK3", "ASTHMA3", "CHCSCNCR", "CHCOCNCR", "CHCCOPD1", "HAVARTH3", "ADDEPEV2", "CHCKIDNY", "DIABETE3")
  
      var corrMax = 0.0
      var colMax = ""
      for( c <- healthCols){
          val tmp = dataframe.stat.corr(c, colName)
          if(math.abs(corrMax) < math.abs(tmp)){
              corrMax = tmp
              colMax = c
          }
      }
      return (corrMax, colMax)
  }

  //println("Max Corr for General Health: " + findMaxCorr("GENHLTH")._2) //CHECKUP1
  //println("Max Corr for Physical Health: " + findMaxCorr("PHYSHLTH")._2 //CHECKUP1)
  //println("Max Corr for Metal Health: " + findMaxCorr("MENTHLTH")._2) //CHCKIDNY --VERY INTERESTING...
  println("Max Corr for Poor Health: " + findMaxCorr("POORHLTH")._2)

  def regressionPredict(colName: String){
      val va = new VectorAssembler().setInputCols(Array("CHECKUP1", "EXERANY2", "SLEPTIM1", "CVDINFR4", "CVDCRHD4", "CVDSTRK3", "ASTHMA3", "CHCSCNCR", "CHCOCNCR", "CHCCOPD1", "HAVARTH3", "ADDEPEV2", "CHCKIDNY", "DIABETE3")).setOutputCol("features")
      val withFeatures = va.transform(dataframe.select('CHECKUP1, 'EXERANY2, 'SLEPTIM1, 'CVDINFR4, 'CVDCRHD4, 'CVDSTRK3, 'ASTHMA3, 'CHCSCNCR, 'CHCOCNCR, 'CHCCOPD1, 'HAVARTH3, 'ADDEPEV2, 'CHCKIDNY, 'DIABETE3, dataframe(colName)))
      val lr = new LinearRegression().setLabelCol(colName)
      val model = lr.fit(withFeatures)
      println(model.coefficients + " " + model.intercept)
      val fitData = model.transform(withFeatures)
      fitData.show()
     /* val evaluator = new BinaryClassificationEvaluator
      val accuracy = evaluator.evaluate(fitData)
      println(s"accuracy = $accuracy")
      */
  }

    //regressionPredict("GENHLTH")
    //regressionPredict("PHYSHLTH")
    //regressionPredict("MENTHLTH")
    //regressionPredict("POORHLTH")




/*
  //Gen health regression with almost all health related columns
  val va = new VectorAssembler().setInputCols(Array("CHECKUP1", "EXERANY2", "SLEPTIM1", "CVDINFR4", "CVDCRHD4", "CVDSTRK3", "ASTHMA3", "CHCSCNCR", "CHCOCNCR", "CHCCOPD1", "HAVARTH3", "ADDEPEV2", "CHCKIDNY", "DIABETE3")).setOutputCol("features")
  val withFeaturesGen = va.transform(dataframe.select('CHECKUP1, 'EXERANY2, 'SLEPTIM1, 'CVDINFR4, 'CVDCRHD4, 'CVDSTRK3, 'ASTHMA3, 'CHCSCNCR, 'CHCOCNCR, 'CHCCOPD1, 'HAVARTH3, 'ADDEPEV2, 'CHCKIDNY, 'DIABETE3, 'GENHLTH))
  withFeaturesGen.show()
  val lrGen = new LinearRegression().setLabelCol("GENHLTH")

  val modelGen = lrGen.fit(withFeaturesGen)
  
  //Phys health regression with almost all health related colums
  val withFeaturesPhys = va.transform(dataframe.select('CHECKUP1, 'EXERANY2, 'SLEPTIM1, 'CVDINFR4, 'CVDCRHD4, 'CVDSTRK3, 'ASTHMA3, 'CHCSCNCR, 'CHCOCNCR, 'CHCCOPD1, 'HAVARTH3, 'ADDEPEV2, 'CHCKIDNY, 'DIABETE3, 'PHYSHLTH))
  withFeaturesPhys.show()
  val lrPhys = new LinearRegression().setLabelCol("PHYSHLTH")

  val modelPhys = lrPhys.fit(withFeaturesPhys)

  //Mental health regression with depression column only
  val vaMent = new VectorAssembler().setInputCols(Array("ADDEPEV2")).setOutputCol("features")
  val withFeaturesMent = vaMent.transform(dataframe.select('ADDEPEV2, 'MENTHLTH))
  withFeaturesMent.show()
  val lrMent = new LinearRegression().setLabelCol("MENTHLTH")

  val modelMent = lrMent.fit(withFeaturesMent)


  println(modelMent.coefficients + " " + modelMent.intercept)

  val fitDataMent = modelMent.transform(withFeaturesMent)
  fitDataMent.show()
  */
  
  //dataframe.select('GENHLTH,'PHYSHLTH,'MENTHLTH,'POORHLTH,'EXERANY2,'SLEPTIM1).describe().show()
  
    
  spark.stop()
//  dataframe.show()
  
//  val lines = spark.read.option("header",false).option("delimiter","""(\w)+""").csv(directory+"LLCP2016.asc")
  
//  lines.show()

}
