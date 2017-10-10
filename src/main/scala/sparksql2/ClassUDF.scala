package sparksql2


import scalafx.application.JFXApp
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.DataFrameStatFunctions


//spark-submit --class ClassSQL --master spark://pandora00:7077 target/scala-2.11/CSCI3395-F17-InClassLewis-assembly-0.1.0-SNAPSHOT.jar

object ClassUDF extends JFXApp{
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  case class electionSchema(id:Int,votes_dem:Double,votes_gop:Double,total_votes:Double,per_dem:Double,per_gop:Double,
 diff:Double,per_point_diff:String,state_abbr:String,county_name:String,combined_fips:Int)

case class zipSchema(zip_code:String,latitude:Double,longitude:Double,city:String,state:String,county:String) 

case class stateSchema(series_id:String,year:Int,period:String,value:Double)

case class areaSchema(area_type_code:String,area_code:String,area_text:String)

 val data = spark.read.schema(Encoders.product[electionSchema].schema).
    option("header", true).
    csv("/users/mreyes1/2016_US_County_Level_Presidential_Results.csv").
    as[electionSchema]
val zipdata = spark.read.schema(Encoders.product[zipSchema].schema).
    option("header", true).
    csv("/data/BigData/bls/zip_codes_states.csv").
    as[zipSchema]
val statesData = spark.read.schema(Encoders.product[stateSchema].schema).
    option("header", true).option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.data.concatenatedStateFiles").
    as[stateSchema]
val areaData = spark.read.schema(Encoders.product[areaSchema].schema).
    option("header", true).option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.area").
    as[areaSchema]



println("\n")
 println("In Class")
println("")

 val electResults= data.filter(s=>s.state_abbr != "AK")
/*
 val totalRepublicanCounty = electResults.filter(s=> s.votes_gop > s.votes_dem).count()
 val totalCounty = electResults.count()
 println("1.) " + totalRepublicanCounty + "/" + totalCounty + " | " + totalRepublicanCounty.toDouble/totalCounty.toDouble)
val repub10 = electResults.filter(s=> ((s.votes_gop.toDouble/(s.votes_dem.toDouble+s.votes_gop.toDouble))-(s.votes_dem.toDouble/(s.votes_dem.toDouble+s.votes_gop.toDouble))) > 0.1).count
val dem10 = electResults.filter(s=> ((s.votes_dem.toDouble/(s.votes_dem.toDouble+s.votes_gop.toDouble))-(s.votes_gop.toDouble/(s.votes_dem.toDouble+s.votes_gop.toDouble))) > 0.1).count
println("2.A) Republican: " + repub10 + "/" + totalCounty + " | " + repub10.toDouble/totalCounty.toDouble)
println("2.B) Democrats: " + dem10 + "/" + totalCounty + " | " + dem10.toDouble/totalCounty.toDouble)

println("3.)")


val totalDemocrats = electResults.map(s=>s.votes_dem).collect
val totalVotes = electResults.map(s=>s.total_votes).collect
val totalPercent = electResults.map(s=>s.votes_dem.toDouble/s.total_votes.toDouble).collect


val cg2 = ColorGradient((0.40, RedARGB), (0.60, BlueARGB))
val plot = Plot.scatterPlot(totalVotes,totalPercent,"2016 Democratic Party","# of Votes","% Democrat",4,totalDemocrats.map(cg2))
FXRenderer(plot,1280,720)




println("4.)")
//zipdata.show()
//electResults.show()
//val electionZip = zipdata.dropDuplicates(Seq("state","county")).join(electResults,($"county_name".contains($"county") && $"state" === $"state_abbr")).filter($"longitude" > -145.00)
val electionZip = zipdata.join(electResults,($"county_name".contains($"county") && $"state" === $"state_abbr")).filter($"longitude" > -145.00)
//println(electionZip.count())
//electionZip.show()
val lon = electionZip.select($"longitude").filter($"longitude".isNotNull).collect.map(s=>s.getDouble(0))
val lat = electionZip.select($"latitude").filter($"latitude".isNotNull).collect.map(s=>s.getDouble(0))
val electRes = electionZip.select($"votes_dem"/$"total_votes").collect.map(s=>s.getDouble(0))
//for(i <- 0 to 15) println(electRes(i))
val cg = ColorGradient((0.40, RedARGB), (0.60, BlueARGB))
val sPlot = Plot.scatterPlot(lon,lat,"2016 Election Results","Longitude","Latitude",3,electRes.map(cg))
FXRenderer(sPlot,1280,720)
*/
println("")
println("Between Class:")
println("")
println("1.)")

   val statesRate = statesData.filter(substring($"series_id",19,2) === "03")
   val bins = (0.0 to 50.0 by 1.0).toArray
 

    def getHist(year: Int, period: String, atc: String):Array[Long] = {
      val areas = areaData.filter('area_type_code === atc);
      val statesRateTime = statesRate.filter('year === year && 'period === period)
      val joinedRate = statesRateTime.join(areas, 'series_id.contains('area_code))
      val hist = joinedRate.select('value).rdd.map(_.getDouble(0)).histogram(bins, true)
      return hist
  }
  val hist1990B = getHist(1990, "M06", "B")
  val hist1991B = getHist(1991, "M03", "B")
  val hist20012B = getHist(2001, "M02", "B")
  val hist200112B = getHist(2001, "M12", "B")
  val hist2007B = getHist(2007, "M11", "B")
  val hist2009B = getHist(2009, "M06", "B")
  
  val hist1990D = getHist(1990, "M06", "D")
  val hist1991D = getHist(1991, "M03", "D")
  val hist20012D = getHist(2001, "M02", "D")
  val hist200112D = getHist(2001, "M12", "D")
  val hist2007D = getHist(2007, "M11", "D")
  val hist2009D = getHist(2009, "M06", "D")
  
  val hist1990F = getHist(1990, "M06", "F")
  val hist1991F = getHist(1991, "M03", "F")
  val hist20012F = getHist(2001, "M02", "F")
  val hist200112F = getHist(2001, "M12", "F")
  val hist2007F = getHist(2007, "M11", "F")
  val hist2009F = getHist(2009, "M06", "F")
  
  val gridPlot = Plot.histogramGrid(bins, Seq(
        Seq((hist1990B:PlotDoubleSeries) -> GreenARGB,(hist1991B:PlotDoubleSeries) -> RedARGB, 
            (hist20012B:PlotDoubleSeries) -> GreenARGB,(hist200112B:PlotDoubleSeries) -> RedARGB, 
            (hist2007B:PlotDoubleSeries) -> GreenARGB,(hist2009B:PlotDoubleSeries) -> RedARGB),
        Seq((hist1990D:PlotDoubleSeries) -> GreenARGB,(hist1991D:PlotDoubleSeries) -> RedARGB, 
            (hist20012D:PlotDoubleSeries) -> GreenARGB,(hist200112D:PlotDoubleSeries) -> RedARGB, 
            (hist2007D:PlotDoubleSeries) -> GreenARGB,(hist2009D:PlotDoubleSeries) -> RedARGB),
        Seq((hist1990F:PlotDoubleSeries) -> GreenARGB,(hist1991F:PlotDoubleSeries) -> RedARGB, 
            (hist20012F:PlotDoubleSeries) -> GreenARGB,(hist200112F:PlotDoubleSeries) -> RedARGB, 
            (hist2007F:PlotDoubleSeries) -> GreenARGB,(hist2009F:PlotDoubleSeries) -> RedARGB)), 
false, false, "Recession Histogram", "Unemployment Rate", "Sample Count")
    FXRenderer(gridPlot, 3000, 900)

    println("2.)")
    println("a.)")
    //0.16672925713741318
    //It means the democratic votes and unemployment have a positive correlation with each other, but not very strong or indicative
 /*   
    val counties = areaData.filter('area_type_code === "F").select('area_code, 'area_text)
    val employmentNov = statesRate.filter('year === 2016 && 'period === "M11").join(counties, 'series_id.contains('area_code)).select('area_code as "ac1", 'value)
    //electResults.show()
    val votesNov = electResults.join(counties, ('area_text.contains('county_name))&&('area_text).contains('state_abbr)).select('area_code as "ac2", 'votes_dem/'total_votes as "dem_vote_per", 'total_votes as "population", 'area_text as "area_text1")
    val employXvotes = votesNov.join(employmentNov, 'ac1 === 'ac2)
    val corr = employXvotes.stat.corr("value", "dem_vote_per")
    
    println(corr)
    
    println("b.)")
    // Counties with a lower population clearly tend to vote for democrats less, while larger counties have a higher percentage of democrat votes. There is not a strong correlation between unemployment rate and
    // percentage of democratic votes, although it seems that most counties vote 50% or less for democrats. 
    val dem_vote_per = employXvotes.select('dem_vote_per).rdd.map(_.getDouble(0)*100.0).collect()
    val unemploy = employXvotes.select('value).rdd.map(_.getDouble(0)).collect()
    val population = employXvotes.select('population).rdd.map(_.getDouble(0)).collect()
    val cg3 = ColorGradient(1000.0 -> BlueARGB,  10000.0 -> GreenARGB, 100000.0 -> RedARGB)
    //val democratPlot = Plot.scatterPlot(dem_vote_per,unemploy,"2016 Democratic Party","% of Democratic Votes","Unemployment",4,population.map(cg3))
    //FXRenderer(democratPlot, 700, 500)

    
   val labor = statesData.filter(substring($"series_id",19,2) === "06" && 'year === 2016 && 'period === "M11").select('series_id, 'value as "labor_force")
   val name2Labor = labor.join(counties, 'series_id.contains('area_code)).select('area_text, 'labor_force)
   val total_votes = electResults.select('state_abbr, 'county_name, 'total_votes)
   val laborXvotes = name2Labor.join(total_votes, 'area_text.contains('state_abbr) && 'area_text.contains('county_name)).select('area_text, 'total_votes/'labor_force as "voter_turnout")
   val laborXemployXvotes = employXvotes.join(laborXvotes, 'area_text1 === 'area_text)
   val unemploy2 = laborXemployXvotes.select('value).rdd.map(_.getDouble(0)).collect()
   val dem_vote_per2 = laborXemployXvotes.select('dem_vote_per).rdd.map(_.getDouble(0)*100.0).collect()
   val turnout2 = laborXemployXvotes.select('voter_turnout).rdd.map(_.getDouble(0)).collect()
   laborXvotes.show()
   val cg4 = ColorGradient(0.5 -> BlueARGB, 0.8 -> GreenARGB, 1.4-> RedARGB)

   val voterTurnoutPlot = Plot.scatterPlot(dem_vote_per2,unemploy2,"Voter Turnout Plot","% of Democratic Votes","Unemployment",4,turnout2.map(cg4))
 //   FXRenderer(voterTurnoutPlot, 700, 500)
*/
println("\n")
  spark.stop()
  
}