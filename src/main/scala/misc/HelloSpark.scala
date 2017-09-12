package misc

import scalafx.application.JFXApp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class Data(val id:String, val day:Int, val month:Int, val year:Int, val obvsType:String, val obvsValue:Double, val obvsTime: String)
case class Station(val id: String, val state: String, val name: String)
case class Country(val id: String)

object HelloSpark extends JFXApp {

  
  def toData(line:String):Data = {
    val data = line.split(",")
    val id = data(0)
    val year = data(1).slice(0,4).toInt
    val month = data(1).slice(4,6).toInt
    val day = data(1).slice(6,8).toInt
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
    val s = a.filter(_.obvsType == "TMAX").fold(a.take(1).apply(0))((z, i)=> if(z.obvsValue > i.obvsValue) z else i)
    val stationName = b.filter(_.id == s.id).take(1).apply(0).name
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
  /*def saDailyIncrease(a:org.apache.spark.rdd.RDD[Data], b:org.apache.spark.rdd.RDD[Station]):Long = {
    
  }*/
  
  
  val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  val lines2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
  val linesStations = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-stations.txt")
  val linesCountries = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-countries.txt")
  
  val data2017 = lines2017.map(toData(_))
  val dataStations = linesStations.map(toStation(_))
  
  //println(numStationsByState(dataStations, "TX"))
  //println(reportedStateStations(data2017, dataStations, "TX"))
  //findHighestTemp(data2017, dataStations)
  //println(nonReportingStations(data2017, dataStations))
  //maxRainTexas(data2017, dataStations)
  //maxRainIndia(data2017, dataStations)
  //println(sanAntonioStations(dataStations))
  println(saReportedStations(data2017, dataStations))

}