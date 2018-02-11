package sparksql

import org.apache.spark.sql.SparkSession
import scalafx.application.JFXApp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import swiftvis2.spark._

/*
 * NOAA data from ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/  in the by_year directory
 */

object NOAAData extends JFXApp {
  val spark = SparkSession.builder().master("local[*]").appName("NOAA Data").getOrCreate()
  import spark.implicits._
  
  spark.sparkContext.setLogLevel("WARN")
  
  val tschema = StructType(Array(
      StructField("sid",StringType),
      StructField("date",DateType),
      StructField("mtype",StringType),
      StructField("value",DoubleType)
      ))
  val data2017 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("data/2017.csv").cache()
//  data2017.show()
//  data2017.schema.printTreeString()
  
  val sschema = StructType(Array(
      StructField("sid", StringType),
      StructField("lat", DoubleType),
      StructField("lon", DoubleType),
      StructField("name", StringType)
      ))
  val stationRDD = spark.sparkContext.textFile("data/ghcnd-stations.txt").map { line =>
    val id = line.substring(0, 11)
    val lat = line.substring(12, 20).toDouble
    val lon = line.substring(21, 30).toDouble
    val name = line.substring(41, 71)
    Row(id, lat, lon, name)
  }
  val stations = spark.createDataFrame(stationRDD, sschema).cache()
  
  val tmax2017 = data2017.filter($"mtype" === "TMAX").limit(1000000).drop("mtype").withColumnRenamed("value", "tmax")
  val tmin2017 = data2017.filter('mtype === "TMIN").limit(1000000).drop("mtype").withColumnRenamed("value", "tmin")
  val combinedTemps2017 = tmax2017.join(tmin2017, Seq("sid", "date"))
  val dailyTemp2017 = combinedTemps2017.select('sid, 'date, ('tmax + 'tmin)/20*1.8+32 as "tave")
  val stationTemp2017 = dailyTemp2017.groupBy('sid).agg(avg('tave) as "tave")
  val joinedData2017 = stationTemp2017.join(stations, "sid")
  joinedData2017.show()

  {
    implicit val df = joinedData2017
    val cg = ColorGradient(0.0 -> BlueARGB, 50.0 -> GreenARGB, 100.0 -> RedARGB)
    val plot = Plot.scatterPlot('lon, 'lat, title = "Global Temps", xLabel = "Longitude", 
        yLabel = "Latitude", symbolSize = 3, symbolColor = cg('tave))
    FXRenderer(plot, 800, 600)
  }
  
  spark.stop()
}