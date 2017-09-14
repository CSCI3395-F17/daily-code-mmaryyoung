package sparkrdd

import scalafx.application.JFXApp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting.Plot
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.collection.Seq
import swiftvis2.plotting.ArrayIntToDoubleSeries
import swiftvis2.plotting.ArrayToDoubleSeries
import swiftvis2.plotting.DoubleToDoubleSeries
import swiftvis2.plotting.IntToIntSeries

case class Data(val id:String, val day:String, val month:String, val year:String, val obvsType:String, val obvsValue:Double, val obvsTime: String)
case class Station(val id: String, val state: String, val name: String)
case class Country(val id: String)

object HelloSpark extends JFXApp {

  
  def toData(line:String):Data = {
    val data = line.split(",")
    val id = data(0)
    val year = data(1).slice(0,4)
    val month = data(1).slice(4,6)
    val day = data(1).slice(6,8)
    val obvsType = data(2)
    val obvsValue = data(3).toDouble
    val obvsTime = if(data.last.length() == 4) data.last else ""
    val data2 = new Data(id,day,month, year,obvsType,obvsValue, obvsTime)
    if(obvsType.isEmpty()) println(data2)
    data2
  }
  
  def toStation(line:String):Station = {
    val id = line.slice(0, 11).trim()
    val state = line.slice(38, 40).trim()
    val name = line.drop(41).takeWhile(i => i.isLetter || i == ' ').trim()
    val station = new Station(id, state, name)
    station
  }
  //1
  def numStationsByState(a:org.apache.spark.rdd.RDD[Station], state:String):Long = {
    a.filter(_.state==state).count()
  }
  //2
  def reportedStateStations(a:org.apache.spark.rdd.RDD[Data], b:org.apache.spark.rdd.RDD[Station], state:String):Long = {
    val stateStations = b.filter(_.state==state).map(_.id)
    val yearStations = a.map(_.id).distinct()
    stateStations.intersection(yearStations).count()
  }
  //3
  def findHighestTemp(a:org.apache.spark.rdd.RDD[Data], b:org.apache.spark.rdd.RDD[Station]):Unit = {
    //val s = a.filter(_.obvsType == "TMAX").fold(a.take(1).apply(0))((z, i)=> if(z.obvsValue > i.obvsValue) z else i)
    val s = a.filter(_.obvsType == "TMAX").sortBy(_.obvsValue, false).first()
    val stationName = b.filter(_.id == s.id).first().name
    println(s.obvsValue, s.year, s.month, s.day, s.obvsTime, stationName)
  }
  //4
  def nonReportingStations(a:org.apache.spark.rdd.RDD[Data], b:org.apache.spark.rdd.RDD[Station]):Long = {
    val totalNum = b.count()
    val reported = a.map(_.id).distinct().count()
    totalNum - reported
  }
  
  //1
  def maxRainTexas(a:org.apache.spark.rdd.RDD[Data], b:org.apache.spark.rdd.RDD[Station]):Unit = {
    val ret = a.filter(i => i.id.slice(0,2) == "US" && i.id.slice(3,5) == "TX" && i.obvsType == "PRCP").sortBy(_.obvsValue, false).first()
    val year = ret.year
    val value = ret.obvsValue
    val name = b.filter(_.id == ret.id).first().name
    println(name + " " + value + " " + ret.year + "/" + ret.month + "/" + ret.day + " " + ret.obvsTime)
  }
  
  //2
  def maxRainIndia(a:org.apache.spark.rdd.RDD[Data], b:org.apache.spark.rdd.RDD[Station]):Unit = {
    val ret = a.filter(i => i.id.slice(0,2) == "IN" && i.obvsType == "PRCP").sortBy(_.obvsValue, false).first()
    val year = ret.year
    val value = ret.obvsValue
    val name = b.filter(_.id == ret.id).first().name
    println(name + " " + value + " " + ret.year + "/" + ret.month + "/" + ret.day + " " + ret.obvsTime)
  }
  
  //3
  def sanAntonioStations(b:org.apache.spark.rdd.RDD[Station]):Long = {
    b.filter(_.name.toLowerCase().contains("san antonio")).count()
  }
  
  //4
  def saReportedStations(a:org.apache.spark.rdd.RDD[Data], b:org.apache.spark.rdd.RDD[Station]):Long = {
    val saStations = b.filter(_.name.toLowerCase().contains("san antonio")).map(_.id)
    a.filter(i => i.id.slice(0,2) == "US").map(_.id).intersection(saStations).count()
  }
  
  //5
  def saDailyIncrease(a:org.apache.spark.rdd.RDD[Data], b:org.apache.spark.rdd.RDD[Station]):Double = {
      val saStations = b.filter(_.name.toLowerCase().contains("san antonio")).map(_.id).collect().toSet
      val stationInfo = a.filter(i=>i.obvsType == "TMAX" && saStations(i.id)).sortBy(i=>(i.year + i.month + i.day)).collect()
      stationInfo.foldLeft(0.0->stationInfo(0).obvsValue)((a, data)=> if(a._1 < data.obvsValue - a._2) (data.obvsValue - a._2) -> data.obvsValue else a._1->data.obvsValue)._1
  }
  
  //6
  def corCoef(a:org.apache.spark.rdd.RDD[Data], b:org.apache.spark.rdd.RDD[Station]):Double = {
    val saStations = b.filter(_.name.toLowerCase().contains("san antonio")).map(_.id).collect().toSet
    val stationInfo = a.filter(i=>(i.obvsType == "TMAX" || i.obvsType == "PRCP")&& saStations(i.id)).groupBy(i=>i.id->(i.year + i.month + i.day)).values
    val pairs = stationInfo.map(_.toArray).filter(_.length == 2).collect()
    pairs.map(_.sortBy(_.obvsType))
    val meanX = pairs.foldLeft(0.0)((a,b)=>a+b(0).obvsValue)/pairs.length
    val meanY= pairs.foldLeft(0.0)((a,b)=>a+b(1).obvsValue)/pairs.length
    val numerator = pairs.foldLeft(0.0)((a,b)=>a + (b(0).obvsValue - meanX)*(b(1).obvsValue-meanY))
    val denominators = pairs.foldLeft(0.0->0.0)((a,b)=>(a._1 + math.pow(b(0).obvsValue - meanX, 2)->(a._2 + math.pow(b(1).obvsValue - meanX, 2))))
    numerator/math.sqrt(denominators._1)/math.sqrt(denominators._2)    
  }
  
  //7
  def plotTmp(a:org.apache.spark.rdd.RDD[Data]){
    //buenos aires
    val station1 = a.filter(i => i.id == "AR000875850" && i.obvsType == "TMAX")
    val station1Tmps = station1.map(_.obvsValue).collect()
    val station1Time = station1.map(i=> (i.month.toInt*30 + i.day.toInt)).collect()

    //oslo
    val station2 = a.filter(i => i.id == "NOE00134274" && i.obvsType == "TMAX")
    val station2Tmps = station2.map(_.obvsValue).collect()
    val station2Time = station2.map(i=> (i.month.toInt*30 + i.day.toInt)).collect()
    
    //orlando 
    val station3 = a.filter(i => i.id == "USC00086634" && i.obvsType == "TMAX")
    val station3Tmps = station3.map(_.obvsValue).collect()
    val station3Time = station3.map(i=> (i.month.toInt*30 + i.day.toInt)).collect()
    
    //toronto 
    val station4 = a.filter(i => i.id == "CA006158355" && i.obvsType == "TMAX")
    val station4Tmps = station4.map(_.obvsValue).collect()
    val station4Time = station4.map(i=> (i.month.toInt*30 + i.day.toInt)).collect()
    
    //LAGOA DA CARNAUBA 
    val station5 = a.filter(i => i.id == "BR002554002" && i.obvsType == "TMAX")
    val station5Tmps = station5.map(_.obvsValue).collect()
    val station5Time = station5.map(i=> (i.month.toInt*30 + i.day.toInt)).collect()
    
    val plot = Plot.scatterPlots(Seq((station1Time, station1Tmps, 0xff3ad8a4, 6), (station2Time, station2Tmps, 0xff9dd4f9, 6), (station3Time, station3Tmps, 0xffff9bba, 6), (station4Time, station4Tmps, 0xfffc8662, 6), (station5Time, station5Tmps, 0xffefdb58, 6)), "Problem 7", "Time", "Temperature")
    

    FXRenderer(plot)
  }
  
  
  val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  sc.setLogLevel("WARN")
  
  val lines2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
  val linesStations = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-stations.txt")
  val linesCountries = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-countries.txt")
  
  val data2017 = lines2017.map(toData(_))
  val dataStations = linesStations.map(toStation(_))
  
  //println(numStationsByState(dataStations, "TX")) /*4541*/
  //println(reportedStateStations(data2017, dataStations, "TX")) /*2302*/
  //findHighestTemp(data2017, dataStations) /*649.0,2017/05/22,PEDERSON LAGOON ALASKA*/
  //println(nonReportingStations(data2017, dataStations)) /*66141*/
  //maxRainTexas(data2017, dataStations) /*DAYTON 6350.0 2017/08/27 */
  //maxRainIndia(data2017, dataStations) /*BHUBANESWAR 4361.0 2017/03/09 */
  //println(sanAntonioStations(dataStations)) /*38*/
  //println(saReportedStations(data2017, dataStations)) /*14*/
  //println(saDailyIncrease(data2017, dataStations)) /*348.0*/
  //println(corCoef(data2017, dataStations)) /*-0.027054099777969617*/
  //plotTmp(data2017)
  

  sc.stop()
}