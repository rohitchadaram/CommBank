import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by RChadaram on 5/30/2016.
  */
class LoadInputData(sqlcontext:SQLContext) {


  def atmosphereconditions() : DataFrame = {

    sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").load("src/main/resources/Atmosphere.csv")

  }

  def topographicconditions() : DataFrame = {

    sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").load("src/main/resources/Elevation.csv")

  }

  def Geographicconditions() : DataFrame = {

    sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").load("src/main/resources/Geography.csv")

  }

  def Oceanographicconditions() : DataFrame = {

    sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").load("src/main/resources/Oceangraphy.csv")

  }


}
