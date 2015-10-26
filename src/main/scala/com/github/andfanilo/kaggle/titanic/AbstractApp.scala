package com.github.andfanilo.kaggle.titanic

import java.io.File

import com.github.andfanilo.kaggle.titanic.io.DataFrameReader
import com.github.andfanilo.kaggle.titanic.spark.SparkRegistrator
import com.github.andfanilo.kaggle.titanic.utils.Logging
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

  // Since we initialise the App before launching the main, let's start our contexts at the very init
  val (sc, sqlContext) = loadContexts()

  /**
   * Entry point to application, which serves at loading data and stopping Spark context at end of execution
   * @param args arguments for app
   */
  def main(args: Array[String]): Unit = {
    val (trainingDF, testingDF) = loadDataFrames(trainingPath, testingPath)
    execute(trainingDF, testingDF)
    sc.stop()
  }

  /**
   * Override this to add functionality to your application
   */
  def execute(trainingDataFrame: DataFrame, testDataFrame: DataFrame)

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
      .set("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    (sc, sqlContext)
  }

  /**
   * Load data input into DataFrames
   * @param trainingPath path to training dataset
   * @param testingPath path to test dataset
   * @return (dataframe of training data, dataframe of test data)
   */
  private def loadDataFrames(trainingPath: String, testingPath: String): (DataFrame, DataFrame) = {
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
