package sparkml

import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.Row


case class UserRatingData(movieID: Int, userID: Int,rating: Int)

/**
 * Audioscrobbler data from: http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html
 * 
 */
object Recommendation extends App {
  val spark = SparkSession.builder.appName("Recommendor").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")
  
  var movieCounter = 0
  val lines = Source.fromFile("/data/BigData/Netflix/combined_data_1.txt").getLines().flatMap(line =>
    if(line.contains(":")) {
      movieCounter+=1
      println("movie" + movieCounter)
      None
    }
    else if(movieCounter > 1000){
      None
    } 
    else{
      val splits = line.split(",")
      Some( UserRatingData(movieCounter, splits(0).toInt, splits(1).toInt))
    }
  ).toSeq
    
  val movieData = spark.createDataset(lines).cache()
  movieData.describe().show()
  movieData.take(5) foreach println
  
  spark.stop()
}