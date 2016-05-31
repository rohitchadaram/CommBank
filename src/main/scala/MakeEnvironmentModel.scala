import java.io.File

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by RChadaram on 5/30/2016.
  */
class MakeEnvironmentModel {

  /******* This Program implements the functionality for the Output Report ******/

  def Environmentmodel(Atmosphere:DataFrame, Elevation:DataFrame, Geography:DataFrame, Oceangraphy:DataFrame) : DataFrame = {

    /* We build a single DataFrame joining all the four dataframes generated from the load method so that we can define
    small Logics to derive the Conditions at the Weather Stations using the temperature,Pressure and the humidity values
     */


     val Geographyelevation = Geography.join(Elevation,Geography("GPS")=== Elevation("GPS"))
       .select(Geography("GPS"),Geography("Station"),Elevation("Altitude"))

     val GeographyelevationAtmosphere = Geographyelevation.join(Atmosphere,Geographyelevation("GPS")=== Atmosphere("GPS"))
       .select(Geographyelevation("GPS"),Geographyelevation("Station"),Geographyelevation("Altitude"),Atmosphere("Temperature"),Atmosphere("Pressure"),Atmosphere("Time"))

     val GeographyelevationAtmosphereOceanogrpahy = GeographyelevationAtmosphere.join(Oceangraphy,GeographyelevationAtmosphere("GPS")=== Oceangraphy("GPS") and GeographyelevationAtmosphere("Time")=== Oceangraphy("Time"))
       .select(GeographyelevationAtmosphere("GPS"),GeographyelevationAtmosphere("Station"),GeographyelevationAtmosphere("Altitude"),GeographyelevationAtmosphere("Temperature"),
         GeographyelevationAtmosphere("Pressure"),GeographyelevationAtmosphere("Time"),Oceangraphy("Humidity"))

    // Deriving the Conditions Columns by checking the Temperature, Humidity and Pressure to define if it is Sunny,Raining or Snowing

     val GenerateConditions = GeographyelevationAtmosphereOceanogrpahy.select(GeographyelevationAtmosphereOceanogrpahy("Station"),GeographyelevationAtmosphereOceanogrpahy("GPS"),GeographyelevationAtmosphereOceanogrpahy("Time"),
     expr("case when (Temperature<=15 and Humidity>=90 and Pressure>=1000) then 'Rainy' else 'Sunny' end as Conditions"),GeographyelevationAtmosphereOceanogrpahy("Temperature"),GeographyelevationAtmosphereOceanogrpahy("Pressure"),
       GeographyelevationAtmosphereOceanogrpahy("Humidity")).orderBy(GeographyelevationAtmosphereOceanogrpahy("Station"))

     val WeatherReport = GenerateConditions.select(GenerateConditions("Station"),GenerateConditions("GPS"),GenerateConditions("Time"),expr("case when (Conditions ='Rainy' and Temperature<=0 and Pressure>=1000) then 'Snow' else Conditions end as Conditions"),
       GenerateConditions("Temperature"),GenerateConditions("Pressure"),GenerateConditions("Humidity"))

     WeatherReport

  }

  /* Rebuilding the Output Weather Report File as the Spark Dataframe output write builds a directory with part-00000 files depending on
     the number of partitions so repartioning it to a single partition and deleting the directory and generating a single output text file
   */

  def saveDfToTxt(df: DataFrame, txtOutput: String,
                  sep: String, header: Boolean ): Unit = {

    val tmpDir = "src/main/resources/WeatherReport"

    df.repartition(1).write.format("com.databricks.spark.csv").option("header", header.toString).option("delimiter", sep).save(tmpDir)

    val dir = new File(tmpDir)

    val tmpTsvFile = tmpDir + File.separatorChar + "part-00000"

    (new File(tmpTsvFile)).renameTo(new File(txtOutput))

    dir.listFiles.foreach( f => f.delete )
    dir.delete

  }


}
