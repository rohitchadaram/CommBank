import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by RChadaram on 5/30/2016.
  */
object GenerateWeather {

  /***** This is the Driver Program *****/

  def main(args: Array[String]) {

    var sc : SQLContext = null

    // ***** Creating a SQLContext to run our job *****
    val conf = new SparkConf().setAppName("GenerateWeather")
    conf.setMaster("local")

    sc = new SQLContext(new SparkContext(conf))

    // ****** Initializing our variables ******

    val loadinput = new LoadInputData(sc)
    val environmentmodel = new MakeEnvironmentModel()

    var Atmosphere:DataFrame = null
    var Elevation:DataFrame = null
    var Geography:DataFrame = null
    var Oceangraphy:DataFrame = null
    var WeatherReport:DataFrame = null


    // ***** Populating Data into our variables by calling the methods which load the input data in the resources and return respective Dataframes *****
    Atmosphere = loadinput.atmosphereconditions()
    Elevation = loadinput.topographicconditions()
    Geography = loadinput.Geographicconditions()
    Oceangraphy = loadinput.Oceanographicconditions()


   // ***** Generating the Weather Report for 10 stations across 10 different cities in Australia *****
    WeatherReport = environmentmodel.Environmentmodel(Atmosphere,Elevation,Geography,Oceangraphy)

   //  ***** Generating a Single txt file, as the Dataframe write method creates a directory and part-0000 file instead of a single txt file *****
    val Weather_Report = "src/main/resources/Weather_Report.txt"
    val sep = "|"
    val header:Boolean = false

    val WeatherReport_final = environmentmodel.saveDfToTxt(WeatherReport,Weather_Report,sep,header)

  }


}
