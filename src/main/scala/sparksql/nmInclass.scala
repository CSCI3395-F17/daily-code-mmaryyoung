package sparksql
import scalafx.application.JFXApp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object nmInclass{
 
  def main(args: Array[String]){
    //val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val spark = SparkSession.builder().master("spark://pandora00:7077").getOrCreate()

    import spark.implicits._
    
    spark.sparkContext.setLogLevel("WARN")
    val schema = StructType(Array(
        StructField("seriesID", StringType),
        StructField("year", IntegerType),
        StructField("period", StringType),
        StructField("value", DoubleType),
        StructField("footnote", StringType)
    ))
    
    val schema2 = StructType(Array(
        StructField("seriesID", StringType),
        StructField("areaTypeCode", StringType)
    ))

    val schemaArea = StructType(Array(
        StructField("areaType", StringType),
        StructField("areaCode", StringType),
        StructField("areaText", StringType)
    ))
    val datac = spark.read.schema(schema).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.concatenatedStateFiles")
    val dataArea = spark.read.schema(schemaArea).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.area")

/*
    val data = spark.read.schema(schema).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.38.NewMexico")
    val data2 = spark.read.schema(schema2).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.series")
    
    
    //1 264
    println(data.select('seriesID).distinct().count())
    //2 85751
    data.filter(substring('seriesID, 19, 2) === "04").orderBy(desc("value")).show()

    //3 7304
    println(data2.filter('areaTypeCode === "G").dropDuplicates("seriesID").count())
    
    //4
    //a
    data.filter(substring('seriesID, 19, 2) === "03").groupBy('period).agg(avg('value)).orderBy('period).show()
    //b 6.82
    data.filter(substring('seriesID, 19, 2) === "03").agg(avg('value)).show()
    
    //individual
    //1
    //c 2.74
    val labor1 = data.filter(substring('seriesID, 19, 2) === "06").groupBy(substring('seriesID, 4, 15) as "areaCode", 'period).agg(avg('value) as "labor")
    val unemploy1 = data.filter(substring('seriesID, 19, 2) === "03").groupBy(substring('seriesID, 4, 15) as "areaCode", 'period).agg(avg('value) as "unemploy")
    val joined1 = labor1.join(unemploy1, labor1.col("areaCode") === unemploy1.col("areaCode") && labor1.col("period") === unemploy1.col("period")).select('labor, 'unemploy)
    val num1 = joined1.select('labor*'unemploy as "weighted").agg(sum("weighted")).first.getDouble(0)
    val num2 = joined1.agg(sum("labor")).first.getDouble(0)
    println(num1)
    
    //2 Rio Grande City and Starr County, both in Feb, 1990 unemploy 54.1
    val texas2 = datac.filter(substring('seriesID, 6, 2) === "48")
    val labor2 = texas2.filter(substring('seriesID, 19, 2) === "06" && 'value > 10000).select(substring('seriesID, 4, 15) as "areaCode").distinct()
    val unemploy2 = texas2.filter(substring('seriesID, 19, 2) === "03").select(substring('seriesID, 4, 15) as "areaCode1", 'value as "unemploy", 'year, 'period)
    val highUnemploy2 = unemploy2.join(labor2, unemploy2.col("areaCode1") === labor2.col("areaCode")).orderBy(desc("unemploy")).limit(2)
    //highUnemploy2.show()
    dataArea.join(highUnemploy2, dataArea.col("areaCode") === highUnemploy2.col("areaCode1")).select('areaText, 'year, 'period).show()
    
    //3 San Luis City, AZ, July 2013, 58.9
    val labor3 = datac.filter(substring('seriesID, 19, 2) === "06" && 'value > 10000).select(substring('seriesId, 4, 15) as "areaCode").distinct()
    val unemploy3 = datac.filter(substring('seriesID, 19, 2) === "03").select(substring('seriesID, 4, 15) as "areaCode1", 'value as "unemploy", 'year, 'period)
    val highUnemploy3 = unemploy3.join(labor3, unemploy3.col("areaCode1") === labor3.col("areaCode")).orderBy(desc("unemploy")).limit(2)
    highUnemploy3.show()
    dataArea.join(highUnemploy3, dataArea.col("areaCode") === highUnemploy3.col("areaCode1")).select('areaText, 'year, 'period).show()
    */
    //4 maine
    datac.select('seriesID).groupBy(substring('seriesID, 6, 2) as "state").count().orderBy(desc("count")).show()
    spark.stop()

  }
}
