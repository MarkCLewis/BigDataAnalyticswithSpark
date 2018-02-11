package sparksql

import scalafx.application.JFXApp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import swiftvis2.spark._

case class Series(sid: String, area: String, measure: String, title: String)
case class LAData(id: String, year: Int, period: String, value: Double)
case class ZipData(zipCode: String, lat: Double, lon: Double, city: String, state: String, county: String)
case class ZipCountyData(lat: Double, lon: Double, state: String, county: String)

/*
 * BLS data from https://download.bls.gov/pub/time.series/la/
 * Zipcode data from https://www.gaslampmedia.com/download-zip-code-latitude-longitude-city-state-county-csv/
 */
object BLSTyped extends JFXApp {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  
  spark.sparkContext.setLogLevel("WARN")
  
  val countyData = spark.read.schema(Encoders.product[LAData].schema).option("header", true).
    option("delimiter", "\t").csv("data/la.data.64.County").
    select(trim('id) as "id", 'year, 'period, 'value).as[LAData].
    filter('id.endsWith("03") && 'year === 2016 && 'period === "M10").cache()
    //sample(false, 0.1).cache()
  
  val series = spark.read.textFile("data/la.series").map { line => 
    val p = line.split("\t").map(_.trim)
    Series(p(0), p(2), p(3), p(6))
  }.cache()
  
  val joined1 = countyData.joinWith(series, 'id === 'sid)
  joined1.show()
  println(joined1.first())
  
  val zipData = spark.read.schema(Encoders.product[ZipData].schema).option("header", true).
    csv("data/zip_codes_states.csv").as[ZipData].filter('lat.isNotNull).cache()
  val countyLocs = zipData.groupByKey(zd => zd.county -> zd.state).agg(avg('lat).as[Double],
      avg('lon).as[Double]).map { case ((county, state), lat, lon) => ZipCountyData(lat, lon, state, county) }
  countyLocs.show()
  
  val fullJoined = joined1.joinWith(countyLocs, '_2("title").contains('county) && '_2("title").contains('state))
  fullJoined.show()
  val cg = ColorGradient(0.0 -> BlueARGB, 4.0 -> GreenARGB, 8.0 -> RedARGB)
  val plot = Plot.scatterPlot(doubles(fullJoined)(_._2.lon), doubles(fullJoined)(_._2.lat), 
      title = "Unemployment", xLabel = "Longitude", 
      yLabel = "Latitude", symbolSize = 3, symbolColor = cg(doubles(fullJoined)(_._1._1.value)))
  FXRenderer(plot, 800, 600)
  
  spark.stop()
}