package com.github.fandetwa.kaggle.titanic.io

import org.apache.spark.sql.DataFrame

/**
 * Utility object for persisting a DataFrame
 */
object DataFrameWriter {

  /**
   * Save DataFrame inside a csv file
   * @param df DataFrame to persist in csv file
   * @param pathToFile path to persisted csv file
   */
  def saveAsCSV(df: DataFrame, pathToFile: String): Unit = {
    df.write.format("com.databricks.spark.csv").save(pathToFile)
  }
}
