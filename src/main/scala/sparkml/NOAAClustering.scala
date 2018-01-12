package sparkml

import org.apache.spark.sql.SparkSession
import swiftvis2.plotting
import swiftvis2.plotting._
import scalafx.application.JFXApp
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.LinearRegression
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/*
 * NOAA data from ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/  in the by_year directory
 */

case class Station(sid: String, lat: Double, lon: Double, elev: Double, name: String)
case class NOAAData(sid: String, date: java.sql.Date, measure: String, value: Double)

object NOAAClustering extends JFXApp {
  Future {
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

    val stationsVA = new VectorAssembler().setInputCols(Array("lat", "lon")).setOutputCol("location")
    val stationsWithLoc = stationsVA.transform(stations)
    //  stationsWithLoc.show()

    val kMeans = new KMeans().setK(2000).setFeaturesCol("location").setPredictionCol("cluster")
    val stationClusterModel = kMeans.fit(stationsWithLoc)

    val stationsWithClusters = stationClusterModel.transform(stationsWithLoc)
    stationsWithClusters.show()

    //  println(kMeans.explainParams())

    val x = stationsWithClusters.select('lon).as[Double].collect()
    val y = stationsWithClusters.select('lat).as[Double].collect()
    val predict = stationsWithClusters.select('cluster).as[Double].collect()
    val cg = ColorGradient(0.0 -> BlueARGB, 1000.0 -> RedARGB, 2000.0 -> GreenARGB)
    val plot = Plot.scatterPlot(x, y, title = "Stations", xLabel = "Longitude", yLabel = "Latitude",
      symbolSize = 3, symbolColor = predict.map(cg))
    FXRenderer(plot, 1000, 650)

    val data2017 = spark.read.schema(Encoders.product[NOAAData].schema).
      option("dateFormat", "yyyyMMdd").csv("data/2017.csv")

    val clusterStations = stationsWithClusters.filter('cluster === 441).select('sid)
    val clusterData = data2017.filter('measure === "TMAX").join(clusterStations, "sid")
    val withDOYinfo = clusterData.withColumn("doy", dayofyear('date)).
      withColumn("doySin", sin('doy / 365 * 2 * math.Pi)).
      withColumn("doyCos", cos('doy / 365 * 2 * math.Pi))
    val linearRegData = new VectorAssembler().setInputCols(Array("doySin", "doyCos")).
      setOutputCol("doyTrig").transform(withDOYinfo).cache()
    val linearReg = new LinearRegression().setFeaturesCol("doyTrig").setLabelCol("value").
      setMaxIter(10).setPredictionCol("pmaxTemp")
    val linearRegModel = linearReg.fit(linearRegData)
    println(linearRegModel.coefficients + " " + linearRegModel.intercept)
    val withLinearFit = linearRegModel.transform(linearRegData)

    // y = a*sin(doy) + b*cos(doy) + c

    val doy = withLinearFit.select('doy).as[Double].collect(): PlotDoubleSeries
    val maxTemp = withLinearFit.select('value).as[Double].collect(): PlotDoubleSeries
    val pmaxTemp = withLinearFit.select('pmaxTemp).as[Double].collect(): PlotDoubleSeries
    val size1 = 3: PlotDoubleSeries
    val size2 = 0: PlotDoubleSeries
    val color = BlackARGB: PlotIntSeries
    val stroke = renderer.Renderer.StrokeData(1, Nil)
    val tempPlot = Plot.scatterPlotsFull(
      Array(
      (doy, maxTemp, color, size1, None, None, None),
      (doy, pmaxTemp, color, size2, Some((0: PlotIntSeries) -> stroke), None, None)),
      title = "High Temps", xLabel = "Day of Year", yLabel = "Temp")
    FXRenderer(tempPlot, 600, 600)

    spark.stop()
  }
}