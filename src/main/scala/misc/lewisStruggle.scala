
package misc

case class TempData(day: Int, doy: Int, month: Int, year: Int, precip: Double, tave: Double, tmax: Double, tmin: Double)

object tempAnalysis{
    def main(args: Array[String]): Unit ={

val source = scala.io.Source.fromFile("/users/jyang/SanAntonioTemps.csv")
val lines = source.getLines.drop(2)

val tempData = lines.map{line => 
    val p = line.split(',')
    TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
    }.toArray

tempData.take(5) foreach println

println(tempData.reduceLeft((a,b)=> if(a.tmax > b.tmax) a else b))
println(tempData.foldLeft(tempData(0))((a,b) => if(a.tmax > b.tmax) a else b))
//avg high tmp for rainy days
val (tempSum, tempCnt) = tempData.foldLeft(0.0 -> 0.0){case ((sum, cnt), td) => if(td.precip >= 1)(sum +td.tmax, cnt+1) else (sum, cnt)}
println(tempSum/tempCnt)

val groups:Map[Int, Array[TempData]] = tempData.groupBy(_.month)
val groupList = groups.map{case (month, tds) => 
    val aveTemp = tds.foldLeft(0.0)((agg, td)=> agg + td.tmax)/tds.length
    (month, aveTemp)}
println(groupList)
println("")
val groupPrecip = groups.map{case (month, tds) => 
    val avePrecip = tds.foldLeft(0.0)((agg, td)=> agg + td.precip)/tds.length
    (month, avePrecip)}
println(groupPrecip.toSeq.sortBy(_._2))
println("")

val precipMedian = groups.map{case (month, tds) =>
    val medPrecip = tds.sortBy(_.precip).apply(tds.length/2).precip
    (month, medPrecip)
}


source.close
    }
}
