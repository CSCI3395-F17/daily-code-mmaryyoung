import scalafx.application.JFXApp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object TempSQL extends JFXApp{
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  
  spark.sparkContext.setLogLevel("WARN")
  
  val schema = StructType(Array(
      StructField("day", IntegerType),
      StructField("doy", IntegerType),
      StructField("month", IntegerType),
      StructField("state", IntegerType),
      StructField("year", IntegerType),
      StructField("precip", DoubleType),
      StructField("tave", DoubleType),
      StructField("tmax", DoubleType),
      StructField("tmin", DoubleType)
      
  ))
  val data = spark.read.schema(schema).option("header", true).csv("/users/mlewis/CSCI3395-F17/InClass/SanAntonioTemps.csv")
    
    data.show
    
    data.filter('precip > 1).show
    println(data.filter('tave > 100).count())
    data.groupBy('year).mean("tmax").orderBy($"avg(tmax)").show()
    
    data.describe().show()
    
    data.createOrReplaceTempView("tdata")
    spark.sql("SELECT tmax FROM tdata").show()
    
    spark.stop()
  
}