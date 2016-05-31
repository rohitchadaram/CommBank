import java.io.File

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by RChadaram on 5/30/2016.
  */
class MakeEnvironmentModel {

  def Environmentmodel(Atmosphere:DataFrame, Elevation:DataFrame, Geography:DataFrame, Oceangraphy:DataFrame) : DataFrame = {

     val Geographyelevation = Geography.join(Elevation,Geography("GPS")=== Elevation("GPS"))
       .select(Geography("GPS"),Geography("Station"),Elevation("Altitude"))

     val GeographyelevationAtmosphere = Geographyelevation.join(Atmosphere,Geographyelevation("GPS")=== Atmosphere("GPS"))
       .select(Geographyelevation("GPS"),Geographyelevation("Station"),Geographyelevation("Altitude"),Atmosphere("Temperature"),Atmosphere("Pressure"),Atmosphere("Time"))

     val GeographyelevationAtmosphereOceanogrpahy = GeographyelevationAtmosphere.join(Oceangraphy,GeographyelevationAtmosphere("GPS")=== Oceangraphy("GPS") and GeographyelevationAtmosphere("Time")=== Oceangraphy("Time"))
       .select(GeographyelevationAtmosphere("GPS"),GeographyelevationAtmosphere("Station"),GeographyelevationAtmosphere("Altitude"),GeographyelevationAtmosphere("Temperature"),
         GeographyelevationAtmosphere("Pressure"),GeographyelevationAtmosphere("Time"),Oceangraphy("Humidity"))

     val GenerateConditions = GeographyelevationAtmosphereOceanogrpahy.select(GeographyelevationAtmosphereOceanogrpahy("Station"),GeographyelevationAtmosphereOceanogrpahy("GPS"),GeographyelevationAtmosphereOceanogrpahy("Time"),
     expr("case when (Temperature<=15 and Humidity>=90 and Pressure>=1000) then 'Rainy' else 'Sunny' end as Conditions"),GeographyelevationAtmosphereOceanogrpahy("Temperature"),GeographyelevationAtmosphereOceanogrpahy("Pressure"),
       GeographyelevationAtmosphereOceanogrpahy("Humidity")).orderBy(GeographyelevationAtmosphereOceanogrpahy("Station"))

     val WeatherReport = GenerateConditions.select(GenerateConditions("Station"),GenerateConditions("GPS"),GenerateConditions("Time"),expr("case when (Conditions ='Rainy' and Temperature<=0 and Pressure>=1000) then 'Snow' else Conditions end as Conditions"),
       GenerateConditions("Temperature"),GenerateConditions("Pressure"),GenerateConditions("Humidity"))

     WeatherReport

  }


  def saveDfToCsv(df: DataFrame, txtOutput: String,
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
