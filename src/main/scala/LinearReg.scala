import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler

object LinearReg extends App {
      val spark = SparkSession.builder().master("local").getOrCreate()
        import spark.implicits._

          spark.sparkContext.setLogLevel("WARN")

            val schema = StructType(Array(
                  StructField("x1", DoubleType),
                        StructField("x2", DoubleType),
                              StructField("y", DoubleType)
                                    ))
              val data = for(x1 <- 0.0 to 10.0 by 0.1; x2 <- 0.0 to 10.0 by 0.1) yield {
                      Row(x1, x2,2*x1 + 3*x2 + 0.01*(math.random-0.5))
                        }
                          val va = new VectorAssembler().setInputCols(Array("x1", "x2")).
                              setOutputCol("features")
                                val df = spark.createDataFrame(
                                      spark.sparkContext.parallelize(data), schema)
                                  val withFeatures = va.transform(df)
                                    withFeatures.show
                                          
                                            val lr = new LinearRegression().setLabelCol("y")
                                              
                                                val model = lr.fit(withFeatures)
                                                  
                                                    println(model.coefficients+" "+model.intercept)
                                                      
                                                        val fitData = model.transform(withFeatures)
                                                          fitData.show()
                                                            
                                                              spark.stop()
}
