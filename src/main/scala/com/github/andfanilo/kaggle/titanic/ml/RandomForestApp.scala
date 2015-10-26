package com.github.andfanilo.kaggle.titanic.ml

import com.github.andfanilo.kaggle.titanic.AbstractApp
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame

object RandomForestApp extends AbstractApp {

  override def execute(trainingDataFrame: DataFrame, testDataFrame: DataFrame) {

    val train = trainingDataFrame.drop("Name").drop("Cabin").drop("Ticket").na.fill(-1.0, Seq("Age"))
    val test = testDataFrame.drop("Name").drop("Cabin").drop("Ticket").na.fill(-1.0, Seq("Age"))
    val sexIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("SexIndexed")
    val embarkedIndexer = new StringIndexer().setInputCol("Embarked").setOutputCol("EmbarkedIndexed")
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("Pclass", "SexIndexed", "EmbarkedIndexed", "Age", "SibSp", "Parch", "Fare"))
      .setOutputCol("features")
    val decisionTree = new DecisionTreeClassifier()
      .setLabelCol("Survived")
      .setFeaturesCol("features")
    val pipeline = new Pipeline()
      .setStages(Array(sexIndexer, embarkedIndexer, vectorAssembler))
    val output = pipeline.fit(train).transform(train)
    output.show()

  }
}