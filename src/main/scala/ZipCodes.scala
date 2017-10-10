import scalafx.application.JFXApp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._

import swiftvis2.plotting._

case class ZipData(zip: String, lat: Double, lon: Double, city: String, state: String, county: String)

object ZipCodes extends JFXApp{
  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._
  
  spark.sparkContext.setLogLevel("WARN")
  
  val zips = spark.read.schema(Encoders.product[ZipData].schema).option("header", true).csv("zip_codes_states.csv").as[ZipData]
  
  val counties = zips.groupBy('state, 'county).agg(avg('lat) as "lat", avg('long) as "long").filter('lat.isNotNull && 'lon.isNotNull)
  
  val x = counties.select('lon).collect.map(_.getDouble(0))
  val y = counties.select('lat).collect.map(_.getDouble(0))
  Plot.scatterPlot(x, y, "County Locs", "Lon", "Lat", 3, BlackARGB)
  
}
