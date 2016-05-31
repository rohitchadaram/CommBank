import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by RChadaram on 5/30/2016.
  */
class LoadInputData(sqlcontext:SQLContext) {

  /***** This Program loads input Data *****/


  // ****** Loading the sample Atmosphere Data ******
  def atmosphereconditions() : DataFrame = {

    println(new Timestamp(System.currentTimeMillis()) + ": ***** Loading sample Atmosphere  Data *****")
    sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").load("src/main/resources/Atmosphere.csv")

  }

  // ****** Loading the sample Elevation Data ******
  def topographicconditions() : DataFrame = {

    println(new Timestamp(System.currentTimeMillis()) + ": ***** Loading sample Elevation Data *****")
    sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").load("src/main/resources/Elevation.csv")

  }

  // ****** Loading the sample Geography Data ******
  def Geographicconditions() : DataFrame = {

    println(new Timestamp(System.currentTimeMillis()) + ": ***** Loading sample Geography Data *****")
    sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").load("src/main/resources/Geography.csv")

  }

  // ****** Loading the sample Oceanography Data ******
  def Oceanographicconditions() : DataFrame = {

    println(new Timestamp(System.currentTimeMillis()) + ": ***** Loading sample Oceanography Data *****")
    sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").load("src/main/resources/Oceangraphy.csv")

  }


}
