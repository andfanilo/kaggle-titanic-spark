package com.github.fandetwa.kaggle.titanic

import java.io.File

import com.github.fandetwa.kaggle.titanic.io.DataFrameReader
import com.github.fandetwa.kaggle.titanic.spark.SparkRegistrator
import com.github.fandetwa.kaggle.titanic.utils.Logging
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Abstract base app which initialises the application context and dataframes to study.
 *
 * Extend that application to gain access to data directly
 */
abstract class AbstractApp extends Logging {

  val trainingPath = "src/main/resources/data/train.csv"
  val testingPath = "src/main/resources/data/test.csv"

  /**
   * Entry point to application, initialises the application context and dataframes
   * @param args arguments for app
   */
  def main(args: Array[String]): Unit = {
    val (sc, sqlContext) = loadContexts()
    val (trainingDF, testingDF) = loadDataFrames(sqlContext, trainingPath, testingPath)
    execute(sc, sqlContext, trainingDF, testingDF)
    sc.stop()
  }

  /**
   * Override this to add functionality to your application
   */
  def execute(sc: SparkContext, sQLContext: SQLContext, trainingDataFrame: DataFrame, testDataFrame: DataFrame)

  /**
   * Create a SparkContext and associated SQLContext
   * @return tuple (SparkContext, associated SQLContext)
   */
  private def loadContexts(): (SparkContext, SQLContext) = {
    // Hadoop functionnalities of spark needs the winutils.exe file to be present on windows
    loadWinUtils()
    val conf = new SparkConf()
    conf
      .setMaster("local[*]")
      .setAppName("kaggle-titanic-spark")
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      .set("spark.kryo.registrator", classOf[SparkRegistrator].getCanonicalName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    (sc, sqlContext)
  }

  /**
   * Load data input into DataFrames
   * @param sqlContext Spark SQL Context
   * @param trainingPath path to training dataset
   * @param testingPath path to test dataset
   * @return (dataframe of training data, dataframe of test data)
   */
  private def loadDataFrames(sqlContext: SQLContext, trainingPath: String, testingPath: String): (DataFrame, DataFrame) = {
    (DataFrameReader.loadCSV(sqlContext, trainingPath), DataFrameReader.loadCSV(sqlContext, testingPath))
  }

  /**
   * Need to load winutils.exe when developing
   * https://issues.apache.org/jira/browse/SPARK-2356
   */
  private def loadWinUtils(): Unit = {
    val sep = File.separator
    val current = new File(".").getAbsolutePath.replace(s"$sep.", "")
    System.setProperty("hadoop.home.dir", s"$current${sep}src${sep}test${sep}resources$sep")
  }
}
