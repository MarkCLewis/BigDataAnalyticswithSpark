package sparksql

import org.apache.spark.sql.SparkSession
import scalafx.application.JFXApp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType

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
  val data2017 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("data/2017.csv")
//  data2017.show()
//  data2017.schema.printTreeString()
  
  val tmax2017 = data2017.filter($"mtype" === "TMAX").limit(1000).drop("mtype").withColumnRenamed("value", "tmax")
  val tmin2017 = data2017.filter('mtype === "TMIN").limit(1000).drop("mtype").withColumnRenamed("value", "tmin")
  val combinedTemps2017 = tmax2017.join(tmin2017, Seq("sid", "date"))
  val averageTemp2017 = combinedTemps2017.select('sid, 'date, ('tmax + 'tmin)/2)
  averageTemp2017.show()
  
  spark.stop()
}