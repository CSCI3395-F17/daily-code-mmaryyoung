  package sparkrdd2
  
  import scalafx.application.JFXApp
  import org.apache.spark._
  import swiftvis2.plotting._
  import swiftvis2.plotting.renderer.FXRenderer
  import org.apache.spark.rdd.RDD
  
  object SpecialSparks extends JFXApp {
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
  
  	def inClass1(){
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
  	}
   def inClass2(){
     val highTemps2 = m2017Cases.filter(s => s.obsType == "TMAX").map(x=>((x.ID),x))
     val lowTemp2 = m2017Cases.filter(s => s.obsType ==  "TMIN").map(x=>((x.ID),x))
     val joined2 = highTemps2.join(lowTemp2)
     val merge2 = joined2.fold(joined2.first)({case ((tup1,(maxD1,minD1)),(tup2,(maxD2,minD2))) => {
       if(maxD1.obsValue.toInt - minD1.obsValue.toInt > maxD2.obsValue.toInt - minD2.obsValue.toInt)
         (tup1,(maxD1,minD1))
         else (tup2,(maxD2,minD2))}})
     val location2 = stationArray.filter(_.ID == merge2._1).first().name
     println("Problem 2:")
     println("station name: " + location2 )
   } 	
   
   def inClass3(){
     val highTemps3 = m2017Cases.filter(s => s.obsType == "TMAX").map(x=>x.obsValue.toDouble)
     val lowTemp3 = m2017Cases.filter(s => s.obsType ==  "TMIN").map(x=>x.obsValue.toDouble)
     val stdevHigh = highTemps3.stdev
     val stdevLow = lowTemp3.stdev
    
     println("Problem 3:")
     println("High Temperature: " + stdevHigh*0.1)
     println("Low Temperature: " + stdevLow*0.1)
   }
   
   def inClass4(){
     val reportedStations1987 = m1987Cases.map(_.ID).distinct
     val reportedStations2017 = m2017Cases.map(_.ID).distinct
     val allStationsCount = reportedStations1987.intersection(reportedStations2017).count
    
     println("Problem 4:") 
     println("Number of stations: " + allStationsCount)
   }
   
   def individual1(){
      val stations1 = stationArray.filter(x => x.Latitude.toDouble < 35).map(x => (x.ID,x))
      val stations2 = stationArray.filter(x => x.Latitude.toDouble < 42 && x.Latitude.toDouble > 35).map(x => (x.ID,x))
      val stations3 = stationArray.filter(x => x.Latitude.toDouble > 42).map(x => (x.ID,x))
      //stations: map(ID, Station)
      val data1 = stations1.join(m2017Cases.map(x=>(x.ID,x)))
      val data2 = stations2.join(m2017Cases.map(x=>(x.ID,x)))
      val data3 = stations3.join(m2017Cases.map(x=>(x.ID,x)))
      //datas: (ID, (Station, data))
      
      println("Individual 1:")
      val highs1 = data1.filter(tup => tup._2._2.obsType == "TMAX").map(tup => tup._2._2.obsValue.toDouble)
      val std1 = highs1.stdev
      println("1.A) under 35 stdev: " + std1*0.1) // 8.337095188830301
      val highs2 = data2.filter(tup => tup._2._2.obsType == "TMAX").map(tup => tup._2._2.obsValue.toDouble)
      val std2 = highs2.stdev
      println("1.A) between 35 and 42 stdev: " + std2*0.1) // 10.790696624500666
      val highs3 = data3.filter(tup => tup._2._2.obsType == "TMAX").map(tup => tup._2._2.obsValue.toDouble)
      val std3 = highs3.stdev
      println("1.A) above 42 stdev: " + std3*0.1) // 12.903498001797848
      
      
      
      
      val tmax1 = data1.filter(tup => tup._2._2.obsType == "TMAX").map(tup => (tup._1, tup._2._2.Date)->tup._2._2.obsValue.toDouble)
      val tmin1 = data1.filter(tup => tup._2._2.obsType == "TMIN").map(tup => (tup._1, tup._2._2.Date)->tup._2._2.obsValue.toDouble)
      //tmaxnmin: ((ID, Date), obsValue)
      val avg1 = tmax1.join(tmin1).mapValues(tup => (tup._1 + tup._2)/2).values
      val stdavg1 = avg1.stdev
      println("1.B) under 35 avg stdev: " + stdavg1*0.1) // 8.013252578458058
      val tmax2 = data2.filter(tup => tup._2._2.obsType == "TMAX").map(tup => (tup._1, tup._2._2.Date)->tup._2._2.obsValue.toDouble)
      val tmin2 = data2.filter(tup => tup._2._2.obsType == "TMIN").map(tup => (tup._1, tup._2._2.Date)->tup._2._2.obsValue.toDouble)
      //tmaxnmin: ((ID, Date), obsValue)
      val avg2 = tmax2.join(tmin2).mapValues(tup => (tup._1 + tup._2)/2).values
      val stdavg2 = avg2.stdev
      println("1.B) between 35 and 42 avg stdev: " + stdavg2*0.1) // 9.908423665720707
      val tmax3 = data3.filter(tup => tup._2._2.obsType == "TMAX").map(tup => (tup._1, tup._2._2.Date)->tup._2._2.obsValue.toDouble)
      val tmin3 = data3.filter(tup => tup._2._2.obsType == "TMIN").map(tup => (tup._1, tup._2._2.Date)->tup._2._2.obsValue.toDouble)
      //tmaxnmin: ((ID, Date), obsValue)
      val avg3 = tmax3.join(tmin3).mapValues(tup => (tup._1 + tup._2)/2).values
      val stdavg3 = avg3.stdev
      println("1.B) above 42 avg stdev: " + stdavg3*0.1) // 11.718242126409598
      val bins = (-30.0 to 100.0 by 2.0).toArray
      val hist1 = highs1.map(_*0.1).histogram(bins, true)
      val plot1 = Plot.histogramPlot(bins, hist1, BlueARGB, false, "under 35", "Temperature(C*10)", "Occurence") 
      FXRenderer(plot1)
      val hist2 = highs2.map(_*0.1).histogram(bins, true)
      val plot2 = Plot.histogramPlot(bins, hist2, BlueARGB, false, "35 to 42", "Temperature(C*10)", "Occurence")
      FXRenderer(plot2)
      val hist3 = highs3.map(_*0.1).histogram(bins, true)
      val plot3 = Plot.histogramPlot(bins, hist3, BlueARGB, false, "above 42", "Temperature(C*10)", "Occurence")
      FXRenderer(plot3)
   }
   
   def individual2(){
     val cg = ColorGradient(-17.78 -> BlueARGB, 10.0 -> GreenARGB, 37.78 -> RedARGB)
     val longitude = stationArray.map(sta => sta.ID -> sta.Longitude.toDouble)
     val latitude = stationArray.map(sta => sta.ID-> sta.Latitude.toDouble)
     val avgs = m2017Cases.filter(_.obsType == "TMAX").groupBy(_.ID).mapValues(va => {
       
       val tup = va.foldLeft(0.0, 0)((a,b)=>(a._1+b.obsValue.toDouble)->(a._2+1))
       tup._1/tup._2
     })
     val logivslati = longitude.join(latitude)
     val locvstemp = logivslati.join(avgs).values.collect()
     val plot = Plot.scatterPlot(locvstemp.map(_._1._1), locvstemp.map(_._1._2), "World Heat Map", "Longitude", "Latitude", 3, locvstemp.map(_._2*0.1).map(cg))
     FXRenderer(plot)
     
   }
   
   def individual3HelperA(year: String):Double = {
     val lines = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/"+year+".csv")
     val mCases = lines.map{s =>
  	    val p = s.split(",")
        yearData(p(0),p(1),p(2),p(3),(4))
     }
     val temps = mCases.filter(dat => dat.obsType == "TMAX" || dat.obsType == "TMIN").map(_.obsValue.toDouble*0.1)
     temps.sum()/temps.count()
   }
   
   def individual3HelperB(year: String):RDD[(String, Double)] = {
     val lines = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/"+year+".csv")
     val mCases = lines.map{s =>
  	    val p = s.split(",")
        yearData(p(0),p(1),p(2),p(3),(4))
     }
     mCases.filter(dat => dat.obsType == "TMAX" || dat.obsType == "TMIN").map(dat => dat.ID -> dat.obsValue.toDouble*0.1)
   }
   
   
   
   def individual3(){
     val lines1897 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1897.csv")
     val m1897Cases = lines1897.map{s =>
  	    val p = s.split(",")
        yearData(p(0),p(1),p(2),p(3),(4))
     }
     val lines2016 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2016.csv")
     val m2016Cases = lines2016.map{s =>
  	    val p = s.split(",")
        yearData(p(0),p(1),p(2),p(3),(4))
     }
     //A
     val temps1897 = m1897Cases.filter(dat => dat.obsType == "TMAX" || dat.obsType == "TMIN").map(_.obsValue.toDouble*0.1)
     val avg1897 = temps1897.sum()/temps1897.count()
     val temps2016 = m2016Cases.filter(dat => dat.obsType == "TMAX" || dat.obsType == "TMIN").map(_.obsValue.toDouble*0.1)
     val avg2016 = temps2016.sum()/temps2016.count()
     println("1897 avg tmp: " + avg1897) // 10.67203098965887
     println("2016 avg tmp: " + avg2016) // 11.694144970760048
    
     //B
     val sharedT1897 = m1897Cases.filter(dat => dat.obsType == "TMAX" || dat.obsType == "TMIN").map(dat => dat.ID -> dat.obsValue.toDouble*0.1)
     val sharedT2016 = m2016Cases.filter(dat => dat.obsType == "TMAX" || dat.obsType == "TMIN").map(dat => dat.ID -> dat.obsValue.toDouble*0.1)
     val sharedStations = sharedT1897.keys.intersection(sharedT2016.keys).collect().toSet
     val tmp1897 = sharedT1897.filter(tup => sharedStations(tup._1)).values
     val tmp2016 = sharedT2016.filter(tup => sharedStations(tup._1)).values
     val sharedAvg1897 = tmp1897.sum()/tmp1897.count()
     val sharedAvg2016 = tmp2016.sum()/tmp2016.count()
     //val intersec = sharedT1897.join(sharedT2016)
     //val sharedAvg1897 = intersec.values.map(_._1).sum()/intersec.count()
     //val sharedAvg2016 = intersec.values.map(_._2).sum()/intersec.count()
     println("shared 1897 avg tmp: " + sharedAvg1897) // 10.590595799285971
     println("shared 2016 avg tmp: " + sharedAvg2016) // 12.086669060116423
 
     //C
     val years = (1897 to 2017 by 10).toArray.map(_.toString())
     years(years.length - 1) = "2016"
     var avgYears1 = List[Double]()
     years.foreach(x => avgYears1 ::=individual3HelperA(x))
     val bins = (1897.0 to 2017.0 by 10.0).toArray
     val plot1 = Plot.scatterPlot(bins, avgYears1.toArray.map(_*1.0) , "3c", "year", "average temperature in C", 10, BlackARGB)
     FXRenderer(plot1)
     
     //D
     val avgYears2 = new Array[Double](years.length)
     val interm = new Array[RDD[(String, Double)]](years.length)
     val stations2 = new Array[RDD[String]](years.length)
     for(i <- 0 until years.length){
       interm(i) = individual3HelperB(years(i))
       stations2(i) = interm(i).map(_._1)       
     }
     val sharedStations2 = stations2.reduceLeft((a,b)=>a.intersection(b)).collect().toSet
     for(i <- 0 until years.length){
       val tmp2 = interm(i).filter(tup=>sharedStations2(tup._1)).map(tup => tup._2)
       avgYears2(i) = tmp2.sum()/tmp2.count()
     }
     val plot2 = Plot.scatterPlot(bins, avgYears2.toArray.map(_*1.0) , "3d", "year", "average temperature in C", 10, BlackARGB)
     FXRenderer(plot2)
     
     
   }
   
   
   
   
   
   individual1()
    
  
  	sc.stop
}