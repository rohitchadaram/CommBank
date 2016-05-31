import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by RChadaram on 5/30/2016.
  */
object GenerateWeather {

  def main(args: Array[String]) {

    var sc : SQLContext = null

    val conf = new SparkConf().setAppName("GenerateWeather")
    conf.setMaster("local")

    sc = new SQLContext(new SparkContext(conf))

    val loadinput = new LoadInputData(sc)
    val environmentmodel = new MakeEnvironmentModel()

    var Atmosphere:DataFrame = null
    var Elevation:DataFrame = null
    var Geography:DataFrame = null
    var Oceangraphy:DataFrame = null
    var WeatherReport:DataFrame = null

    Atmosphere = loadinput.atmosphereconditions()
    Elevation = loadinput.topographicconditions()
    Geography = loadinput.Geographicconditions()
    Oceangraphy = loadinput.Oceanographicconditions()

    WeatherReport = environmentmodel.Environmentmodel(Atmosphere,Elevation,Geography,Oceangraphy)

    val Weather_Report = "src/main/resources/Weather_Report.txt"
    val sep = "|"
    val header:Boolean = false

    val WeatherReport_final = environmentmodel.saveDfToCsv(WeatherReport,Weather_Report,sep,header)

  }


}
