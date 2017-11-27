package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.Normalizer
import scalafx.application.JFXApp
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class NOAAData(sid: String, date: java.sql.Date, measure: String, value: Double)
case class Station(sid: String, lat: Double, lon: Double, elev: Double, name: String)

object Clustering extends App {
  val spark = SparkSession.builder.appName("NOAA SQL Data").master("local[*]").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val mainData = spark.read.option("header", true).csv("/data/BigData/bls/qcew/2016.q1-q4.singlefile.csv")
  val areaData = spark.read.option("header", true).csv("/data/BigData/bls/qcew/area_titles.csv")

  val filtered = areaData.filter('area_title.contains("County")).select('area_fips).distinct
  
  val bexar = mainData.filter('area_fips === "48029").count
  println(bexar)
  //Question 2: 9244
  
  val indCodes = mainData.groupBy('industry_code)
  val question3 = indCodes.count.orderBy($"count".desc)
  question3.take(5) foreach println
  //Question 3: [10,76952] (in code, number)
              //[102,54360]
              //[1025,40588]
  val digits = mainData.filter('industry_code > 99999).groupBy('industry_code).agg(sum('total_qtrly_wages) as "total_wage").orderBy($"total_wage".desc)
  digits.take(5) foreach println
  //Question 4: [622110,1.103354426244E12]
                //[611110,9.74563706861E11]
                //[551114,8.93458074528E11]
  
  val electionData = spark.read.option("header", true).csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv")
  val concatenateData = spark.read.option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.concatenatedStateFiles").filter('year === "2015")
  val mapData = spark.read.option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.area").filter('area_type_code === "F")
  println("counties")
  val countyData = mapData.join(concatenateData, substring(concatenateData("series_id"), 4, 15) === mapData("area_code"))
  
  val labor = countyData.filter(substring('series_id, 19, 2) === "06").groupBy('area_code, 'area_text).agg(avg('value) as "labor")
  val unemploy = countyData.filter(substring('series_id, 19, 2) === "03").groupBy('area_code, 'area_text).agg(avg('value) as "unemploy")
  val laCombined = labor.join(unemploy, labor("area_code") === unemploy("area_code")).drop(labor("area_code")).drop(labor("area_text")).drop(unemploy("area_code"))
  val blsData = electionData.join(laCombined, concat(electionData("county_name"), lit(", "), electionData("state_abbr")) === laCombined("area_text")).drop('area_text).drop('area_code).drop('_c0)


  val joinedAll = mainData.join(blsData, mainData("area_fips").cast("Int") === blsData("combined_fips").cast("Int"))

  var columnsToKeep = "total_qtrly_wages taxable_qtrly_wages taxable_qtrly_wages taxable_qtrly_wages oty_qtrly_estabs_chg oty_qtrly_estabs_pct_chg oty_total_qtrly_wages_chg oty_total_qtrly_wages_pct_chg oty_taxable_qtrly_wages_chg oty_taxable_qtrly_wages_pct_chg oty_qtrly_contributions_chg oty_qtrly_contributions_pct_chg oty_avg_wkly_wage_chg oty_avg_wkly_wage_pct_chg labor unemploy".split(" ")
  val typedData = columnsToKeep.foldLeft(joinedAll)((df, colName) => df.withColumn(colName, df(colName).cast(DoubleType).as(colName))).na.drop().groupBy('area_fips).avg()
  println("all")
  columnsToKeep = columnsToKeep.map(x => "avg(" + x + ")")
  val assembler = new VectorAssembler().setInputCols(columnsToKeep).setOutputCol("features")
 
  val dataWithFeatures = assembler.transform(typedData)
  dataWithFeatures.show()

  val normalizer = new Normalizer().setInputCol("features").setOutputCol("normFeatures")
  val normData = normalizer.transform(dataWithFeatures)

  val kmeans = new KMeans().setK(3).setFeaturesCol("normFeatures")
  val model = kmeans.fit(normData)

  val cost = model.computeCost(normData)
  println("total cost = " + cost)
  println("cost distance = " + math.sqrt(cost / normData.count()))

  val predictions = model.transform(normData)
  //predictions.select("features", "prediction").show()

  
  

  
  
  
  
  
  
  /*val stations = spark.read.textFile("../data/NOAA/ghcnd-stations.txt").map { line =>
    val id = line.substring(0, 11)
    val lat = line.substring(12, 20).trim.toDouble
    val lon = line.substring(21, 30).trim.toDouble
    val elev = line.substring(31, 37).trim.toDouble
    val name = line.substring(41, 71).trim
    Station(id, lat, lon, elev, name)
  }.cache()
  
  val stationVA = new VectorAssembler().setInputCols(Array("lat","lon")).setOutputCol("location")
  val stationsWithVect = stationVA.transform(stations) 

  val kMeans = new KMeans().setK(2000).setFeaturesCol("location")
  val stationClusterModel = kMeans.fit(stationsWithVect)
  
  val stationsWithClusters = stationClusterModel.transform(stationsWithVect)
  stationsWithClusters.show
  
  val x = stationsWithClusters.select('lon).as[Double].collect()
  val y = stationsWithClusters.select('lat).as[Double].collect()
  val predict = stationsWithClusters.select('prediction).as[Double].collect()
  val cg = ColorGradient(0.0 -> BlueARGB, 1000.0 -> RedARGB, 2000.0 -> GreenARGB)
  val plot = Plot.scatterPlot(x, y, "Station Clusters", "Longitude", "Latitude", 3, predict.map(cg))
  
  FXRenderer(plot)
  * 
  */
  
  spark.stop()
}
