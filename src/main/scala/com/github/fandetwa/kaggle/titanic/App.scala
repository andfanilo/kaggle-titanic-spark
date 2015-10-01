package com.github.fandetwa.kaggle.titanic

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

object App extends AbstractApp {

  override def execute(sc: SparkContext, sQLContext: SQLContext, trainingDataFrame: DataFrame, testDataFrame: DataFrame) {

    trainingDataFrame.show()
    testDataFrame.show()

    trainingDataFrame.printSchema()
  }
}