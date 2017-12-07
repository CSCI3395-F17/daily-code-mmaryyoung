package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.Normalizer
import scalafx.application.JFXApp
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class Song(index: String, song: String, year: String, artist: String, genre: String, var lyrics: Array[String])

object Lyrics extends App {
implicit class OpsNum(val str: String) extends AnyVal {
      def isNumeric():Boolean =  {
            scala.util.Try(str.toDouble).isSuccess
      }
      def isDataLine():Boolean = {
           if (! str.contains(',') ) { return false; } 
           val commaLocation = str.indexOf(',')
           val words = str.split(',')
           return (words.length > 5 && str(commaLocation + 1) != ' ' && str.substring(0, commaLocation).isNumeric() ) 
      }
}
  val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten.distinct)

  val pronounUDF = udf{s: Seq[String] => {
      val wordList = Set("i", "me", "you", "he", "him", "she", "her", "it", "we", "us", "they", "them")
      s.toArray.count(x => wordList.contains(x))
      
  }}
  
  val lengthUDF = udf{s: Seq[String]=>{
      val result = s.toArray.foldLeft((0.0, 0))((z, rec) => {
          if(rec.length < 2) {
              z    
          } else{
              (z._1 + rec.length, z._2+1)
          }
      })
      if(result._2 == 0) 0
      else result._1/result._2
  
  }}

  val modeUDF = udf{s: Seq[String] => {
      s.groupBy(i=>i).mapValues(_.size).toSeq.sortBy(_._2 * (-1)).slice(0, 5) 
  }}

  val spark = SparkSession.builder.appName("Lyrics").master("local[*]").getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext

  sc.setLogLevel("WARN")
  val schema = StructType(Array(
        StructField("index", StringType),
        StructField("song", StringType),
        StructField("year", StringType),
        StructField("artist", StringType),
        StructField("genre", StringType),
        StructField("lyrics", ArrayType(StringType))
    ))
    

  def getColFile(fname:String) = {
		val source = scala.io.Source.fromFile(fname)
		val lines = source.getLines().drop(1).toArray
		val songs = new Array[Row](400000)
        var counter = 0
        lines.foreach(x => {
            val arr = x.split(",")
            val isLine = x.isDataLine
            if(isLine && arr(0).isNumeric() && arr(4) != "" && !arr(4)(0).isDigit) {
                val words = arr.slice(5, arr.length).reduceLeft(_+_).split("[ ,.!?;\"]+").filter(_.nonEmpty).map(_.toLowerCase)
                
                songs(counter) = Row(arr(0), arr(1), arr(2), arr(3), arr(4), words)
                counter+=1
            }
            else if(!isLine){
                val tmp = songs(counter-1)
                songs(counter-1) = Row(tmp(0), tmp(1), tmp(2), tmp(3), tmp(4), tmp.getAs[Array[String]](5)++ x.split("[ ,.!?;\"]+").filter(_.nonEmpty).map(_.toLowerCase)) 
            }
        })
		source.close()
        sc.parallelize(songs.slice(0, counter))
  }
  val colFile = getColFile("/data/BigData/students/jyang/lyrics.csv") 
  
  println("count: " + colFile.count())
 
  val mainData = spark.createDataFrame(colFile, schema)

  //mainData.describe().show()
  
  //pronoun count
  //val pronouns = mainData.select('genre, pronounUDF('lyrics) as "you")
  //pronouns.groupBy('genre).agg(sum("you") as "sum", count("you") as "count").withColumn("ratio", 'sum.cast("int")/'count.cast("int")).show()
  
  //length count
  //val pronouns = mainData.select('genre, lengthUDF('lyrics) as "len")
  //pronouns.groupBy('genre).agg(sum("len"), count("len"), avg("len")).show()
  
  //common words
  mainData.groupBy('genre).agg(flatten(collect_list("lyrics")) as "all_words").withColumn("mode", modeUDF('all_words)).show()
  
  spark.stop()

}
