package sparksql

import scalafx.application.JFXApp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._

case class Series(sid: String, area: String, measure: String, title: String)
case class LAData(id: String, year: Int, period: String, value: Double)

object BLSTyped extends JFXApp {
  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._
  
  spark.sparkContext.setLogLevel("WARN")
  
  val countyData = spark.read.schema(Encoders.product[LAData].schema).option("header", true).
    option("delimiter", "\t").csv("data/la.data.64.County").
    select(trim('id) as "id", 'year, 'period, 'value).as[LAData].
    sample(false, 0.1).cache()
  
  val series = spark.read.textFile("data/la.series").map { line => 
    val p = line.split("\t").map(_.trim)
    Series(p(0), p(2), p(3), p(6))
  }.cache()
  
  val joined1 = countyData.joinWith(series, 'id === 'sid)
  joined1.show()
  println(joined1.first())
  
  spark.stop()
}