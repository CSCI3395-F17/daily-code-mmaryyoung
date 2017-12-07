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

object Clustering extends JFXApp {
  val spark = SparkSession.builder.appName("NOAA SQL Data").master("local[*]").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val mainData = spark.read.option("header", true).csv("/data/BigData/bls/qcew/2016.q1-q4.singlefile.csv")
  val areaData = spark.read.option("header", true).csv("/data/BigData/bls/qcew/area_titles.csv")

  val filtered = areaData.filter('area_title.contains("County")).select('area_fips).distinct
  
  /*
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
  */
  val electionData = spark.read.option("header", true).csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv")
  val concatenateData = spark.read.option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.concatenatedStateFiles").filter('year === "2015")
  val mapData = spark.read.option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.area").filter('area_type_code === "F")
  println("counties")
  val countyData = mapData.join(concatenateData, substring(concatenateData("series_id"), 4, 15) === mapData("area_code"))
  
  val labor = countyData.filter(substring('series_id, 19, 2) === "06").groupBy('area_code, 'area_text).agg(avg('value) as "labor")
  val unemploy = countyData.filter(substring('series_id, 19, 2) === "03").groupBy('area_code, 'area_text).agg(avg('value) as "unemploy")
  val laCombined = labor.join(unemploy, labor("area_code") === unemploy("area_code")).drop(labor("area_code")).drop(labor("area_text")).drop(unemploy("area_code"))
  val blsData = electionData.join(laCombined, concat(electionData("county_name"), lit(", "), electionData("state_abbr")) === laCombined("area_text")).drop('area_text).drop('area_code).drop('_c0)


  val joinedAll = mainData.filter('industry_code === "10"&& 'own_code === "2" && 'qtr === "1").join(blsData, mainData("area_fips").cast("Int") === blsData("combined_fips").cast("Int"))

  //var columnsToKeep = "total_qtrly_wages taxable_qtrly_wages oty_qtrly_estabs_chg oty_qtrly_estabs_pct_chg oty_total_qtrly_wages_chg oty_total_qtrly_wages_pct_chg oty_taxable_qtrly_wages_chg oty_taxable_qtrly_wages_pct_chg oty_qtrly_contributions_chg oty_qtrly_contributions_pct_chg oty_avg_wkly_wage_chg oty_avg_wkly_wage_pct_chg labor unemploy".split(" ")
  var columnsToKeep = "labor unemploy".split(" ")

  
  println("before groupby: ")
  var typedData = columnsToKeep.foldLeft(joinedAll)((df, colName) => df.withColumn(colName, df(colName).cast(DoubleType).as(colName))).na.drop()
  val assembler = new VectorAssembler().setInputCols(columnsToKeep).setOutputCol("features")
 
  val dataWithFeatures = assembler.transform(typedData)

  val normalizer = new Normalizer().setInputCol("features").setOutputCol("normFeatures")
  val normData = normalizer.transform(dataWithFeatures)

  val kmeans = new KMeans().setK(2).setFeaturesCol("normFeatures")
  val model = kmeans.fit(normData)

  val cost = model.computeCost(normData)
  println("total cost = " + cost)
  println("cost distance = " + math.sqrt(cost / normData.count()))

  val predictions = model.transform(normData)
  //predictions.select("features", "prediction").show()
  predictions.show()
  //val check = predictions.groupBy('prediction).agg(avg('per_dem), stddev('per_dem), avg('per_gop), stddev('per_gop))
  //check.show()
 /* 
  val total_cluster0 = predictions.filter('prediction === 0).count()
  println("cluster 0 dem: " + 1.0*predictions.filter('prediction === 0 && 'per_dem.cast("Double") > 0.5).count()/total_cluster0)
  println("cluster 0 gop: " + 1.0*predictions.filter('prediction === 0 && 'per_gop.cast("Double") > 0.5).count()/total_cluster0)
  val total_cluster1 = predictions.filter('prediction === 1).count()
  println("cluster 1 dem: " + 1.0*predictions.filter('prediction === 1 && 'per_dem.cast("Double") > 0.5).count()/total_cluster1)
  println("cluster 1 gop: " + 1.0*predictions.filter('prediction === 1 && 'per_gop.cast("Double") > 0.5).count()/total_cluster1)
*/
//  val total_cluster2 = predictions.filter('prediction === 2).count()
//  println("cluster 2 dem: " + 1.0*predictions.filter('prediction === 2 && 'per_dem.cast("Double") > 0.3).count()/total_cluster2)
//  println("cluster 2 gop: " + 1.0*predictions.filter('prediction === 2 && 'per_gop.cast("Double") > 0.3).count()/total_cluster2)
 
  val cg = ColorGradient(1.0 -> GreenARGB, 0.0 -> RedARGB)
 
  val accUDF = udf{(prediction:Int, per_dem: Double, per_gop: Double)=>{
       Math.abs(Math.ceil(0.5 - per_dem) - prediction)
       //if(prediction == 1) Math.ceil(prediction - 0.5 - per_dem) 
       //else Math.ceil(prediction - 0.5 - per_gop)

      }} 
  val countyUDF = udf{s:String => {s.substring(0, s.length-7)}} 
  val zipData = spark.read.option("header", true).csv("/data/BigData/bls/zip_codes_states.csv").groupBy('county, 'state).agg(max("longitude") as "longitude", max("latitude") as "latitude")
  println("zipData count: " + zipData.count())
  val smallPrd = predictions.select('state_abbr, 'county_name, 'per_gop, 'per_dem, 'prediction)
  println("small prediction count: " + smallPrd.count())
  val mapTable = smallPrd.join(zipData, countyUDF(smallPrd("county_name")) === zipData("county") && smallPrd("state_abbr") === zipData("state")).withColumn("accuracy", accUDF('prediction, 'per_dem, 'per_gop))
  mapTable.show()
 
 val lati = mapTable.select('latitude).rdd.map(x => x(0).asInstanceOf[String].toDouble).collect()
 val longi = mapTable.select('longitude).rdd.map(x => x(0).asInstanceOf[String].toDouble).collect()
 val accu = mapTable.select('accuracy).rdd.map(x => x(0).asInstanceOf[Double])collect()
 val plot = Plot.scatterPlot(longi, lati, "Election Prediction", "Longitude", "Latitude", 8, accu.map(cg))
 FXRenderer(plot)

 spark.stop()
}
