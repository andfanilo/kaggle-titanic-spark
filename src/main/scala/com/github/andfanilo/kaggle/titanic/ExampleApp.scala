package com.github.andfanilo.kaggle.titanic

import org.apache.spark.sql.DataFrame

object ExampleApp extends AbstractApp {

  override def execute(trainingDataFrame: DataFrame, testDataFrame: DataFrame) {

    trainingDataFrame.show()
    testDataFrame.show()

    trainingDataFrame.printSchema()
  }
}