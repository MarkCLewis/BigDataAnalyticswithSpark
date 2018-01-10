package sparkml

import org.apache.spark.sql.SparkSession
import swiftvis2.plotting
import swiftvis2.plotting._
import scalafx.application.JFXApp
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler

/*
 * NOAA data from ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/  in the by_year directory
 */

case class Station(sid: String, lat: Double, lon: Double, elev: Double, name: String)

object NOAAClustering extends JFXApp {
  val spark = SparkSession.builder().master("local[*]").appName("NOAA Data").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")
  
  val stations = spark.read.textFile("data/ghcnd-stations.txt").map { line => 
    val id = line.substring(0, 11)
    val lat = line.substring(12, 20).trim.toDouble
    val lon = line.substring(21, 30).trim.toDouble
    val elev = line.substring(31, 37).trim.toDouble
    val name = line.substring(41, 71)
    Station(id, lat, lon, elev, name)
  }.cache()
  
  val stationsVA = new VectorAssembler().setInputCols(Array("lat","lon")).setOutputCol("location")
  val stationsWithLoc = stationsVA.transform(stations)
//  stationsWithLoc.show()
  
  val kMeans = new KMeans().setK(2000).setFeaturesCol("location")
  val stationClusterModel = kMeans.fit(stationsWithLoc)
  
  val stationsWithClusters = stationClusterModel.transform(stationsWithLoc)
  stationsWithClusters.show()
  
//  println(kMeans.explainParams())
  
  val x = stationsWithClusters.select('lon).as[Double].collect()
  val y = stationsWithClusters.select('lat).as[Double].collect()
  val predict = stationsWithClusters.select('prediction).as[Double].collect()
  val cg = ColorGradient(0.0 -> BlueARGB, 1000.0 -> RedARGB, 2000.0 -> GreenARGB)
  val plot = Plot.scatterPlot(x, y, title = "Stations", xLabel = "Longitude", yLabel = "Latitude", 
      symbolSize = 3, symbolColor = predict.map(cg))
  FXRenderer(plot, 1000, 650)

  spark.stop()
}