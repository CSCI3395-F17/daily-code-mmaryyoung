package sparkrdd2

import scalafx.application.JFXApp
import org.apache.spark._

object SpecialSparks extends App {
	val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
	val sc = new SparkContext(conf)
  	sc.setLogLevel("WARN")

case class Station(val ID:String, val Latitude:String, val Longitude:String,val Elevation:String,val State:String, val name:String)
case class yearData(val ID:String, val Date:String,val obsType:String, val obsValue:String,val obsTime:Int)
	val lines2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
	val lines1987 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1987.csv")
	val stations = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-stations.txt")
 	 val m2017Cases = lines2017.map{s =>
	    val p = s.split(",")
            yearData(p(0),p(1),p(2),p(3),(4))
  }
 	 val m1987Cases = lines1987.map{s =>
	    val p = s.split(",")
            yearData(p(0),p(1),p(2),p(3),(4))
  }
	val stationArray = stations.map(s => new Station(s.substring(0,11).trim,s.substring(12,20).trim,s.substring(21,30).trim,s.substring(31,37).trim,s.substring(38,40).trim,s.substring(41,71).trim))

/*// In class 1
val highTemps = m2017Cases.filter(s => s.obsType == "TMAX").map(x=>((x.Date,x.ID),x))
val lowTemp = m2017Cases.filter(s => s.obsType ==  "TMIN").map(x=>((x.Date,x.ID),x))
val joined = highTemps.join(lowTemp)
val merge = joined.fold(joined.first)({case ((tup1,(maxD1,minD1)),(tup2,(maxD2,minD2))) => {
	if(maxD1.obsValue.toInt - minD1.obsValue.toInt > maxD2.obsValue.toInt - minD2.obsValue.toInt)
	(tup1,(maxD1,minD1))
	else (tup2,(maxD2,minD2))
}})
val location = stationArray.filter(_.ID == merge._1._2).first().name
println("Problem 1:")
println("station name: " + location + " time: " + merge._1._1)

// In Class 2
val highTemps2 = m2017Cases.filter(s => s.obsType == "TMAX").map(x=>((x.ID),x))
val lowTemp2 = m2017Cases.filter(s => s.obsType ==  "TMIN").map(x=>((x.ID),x))
val joined2 = highTemps2.join(lowTemp2)
val merge2 = joined2.fold(joined2.first)({case ((tup1,(maxD1,minD1)),(tup2,(maxD2,minD2))) => {
	if(maxD1.obsValue.toInt - minD1.obsValue.toInt > maxD2.obsValue.toInt - minD2.obsValue.toInt)
	(tup1,(maxD1,minD1))
	else (tup2,(maxD2,minD2))
}})
val location2 = stationArray.filter(_.ID == merge2._1).first().name
println("Problem 2:")
println("station name: " + location2 )

// In Class 3

val highTemps3 = m2017Cases.filter(s => s.obsType == "TMAX").map(x=>x.obsValue.toDouble)
val lowTemp3 = m2017Cases.filter(s => s.obsType ==  "TMIN").map(x=>x.obsValue.toDouble)
val stdevHigh = highTemps3.stdev
val stdevLow = lowTemp3.stdev

println("Problem 3:")
println("High Temperature: " + stdevHigh)
println("Low Temperature: " + stdevLow)

// In Class 4

val reportedStations1987 = m1987Cases.map(_.ID).distinct
val reportedStations2017 = m2017Cases.map(_.ID).distinct
val allStationsCount = reportedStations1987.intersection(reportedStations2017).count

println("Problem 4:") 
println("Number of stations: " + allStationsCount)
*/
// indiv 1
val stations1 = stationArray.filter(x => x.Latitude.toDouble < 35).map(x => (x.ID,x))
val stations2 = stationArray.filter(x => x.Latitude.toDouble < 42 && x.Latitude.toDouble > 35).map(x => (x.ID,x))

val stations3 = stationArray.filter(x => x.Latitude.toDouble > 42).map(x => (x.ID,x))

val data1 = stations1.join(m2017Cases.map(x=>(x.ID,x)))
val data2 = stations2.join(m2017Cases.map(x=>(x.ID,x)))
val data3 = stations3.join(m2017Cases.map(x=>(x.ID,x)))


println("Individual 1:")
val highs1 = data1.filter(tup => tup._2._2.obsType == "TMAX").map(tup => tup._2._2.obsValue.toDouble)
val std1 = highs1.stdev
println("1.A) under 35 stdev: " + std1)
val highs2 = data2.filter(tup => tup._2._2.obsType == "TMAX").map(tup => tup._2._2.obsValue.toDouble)
val std2 = highs2.stdev
println("1.A) between 35 and 42 stdev: " + std2)
val highs3 = data3.filter(tup => tup._2._2.obsType == "TMAX").map(tup => tup._2._2.obsValue.toDouble)
val std3 = highs3.stdev
println("1.A) above 42 stdev: " + std3)

println("Individual 2: ")
val tmax1 = data1.filter(tup => tup._2._2.obsType == "TMAX").map(tup => (tup._2._1, tup._2._2.Date)->tup._2._2.obsValue.toDouble)
val tmin1 = data1.filter(tup => tup._2._2.obsType == "TMIN").map(tup => (tup._2._1, tup._2._2.Date)->tup._2._2.obsValue.toDouble)



	sc.stop
}