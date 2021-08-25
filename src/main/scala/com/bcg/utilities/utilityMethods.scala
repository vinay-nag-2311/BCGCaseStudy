package com.bcg.utilities

import com.bcg.utilities.constants.outputPath
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The utilityMethods object:
 * Contains methods used for file utilities
 * such as reading and writing back dataframes.
 */
object utilityMethods {

  /**
   * File.csv -> Spark Dataframe
   * Reads file into a spark DataFrame
   * according to the path and schema inputs
   */
  def readFileIntoDataframe(
                             spark: SparkSession,
                             schemaInput: StructType,
                             pathInput: String): DataFrame = {

    // Check for default input
    val df = spark.read
        .option("header", "true")
        .schema(schemaInput)
        .option("mode", "DROPMALFORMED")
        .csv(pathInput)
    df
  }

  /**
   * File.csv -> Spark Dataframe
   * Reads file into a spark DataFrame
   * according to the path input
   */
  def readFileIntoDataframe(
                             spark: SparkSession,
                             pathInput: String): DataFrame = {

    // Check for default input
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .csv(pathInput)
    df
  }

  /**
   * Spark Dataframe -> outputPath/partfile.csv
   * Saves a dataframe as a csv file
   * in the given location
   */
  def saveCSV(
             outputDF: DataFrame,
             outputPathString: String
             ): Unit ={
    outputDF.coalesce(1).write
          .mode("overwrite")
          .option("header","true").csv(outputPath + outputPathString)
  }

}
