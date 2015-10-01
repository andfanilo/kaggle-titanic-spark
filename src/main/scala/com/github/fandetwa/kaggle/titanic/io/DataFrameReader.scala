package com.github.fandetwa.kaggle.titanic.io

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Utility object for reading into a DataFrame
 */
object DataFrameReader {

  /**
   * Load CSV file as DataFrame using spark-csv
   * @param sqlContext context
   * @param pathToFile path to file to read
   * @return CSV file as DataFrame with header as column names
   */
  def loadCSV(sqlContext: SQLContext, pathToFile: String): DataFrame = {
    sqlContext.read.format("com.databricks.spark.csv").options(Map(
      "header" -> "true",
      "delimiter" -> ",",
      "quote" -> "\"",
      "inferSchema" -> "true"
    )).load(pathToFile)
  }
}
