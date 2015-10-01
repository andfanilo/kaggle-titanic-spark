package com.github.fandetwa.kaggle.titanic

import com.github.fandetwa.kaggle.titanic.spark.SparkImplicits.RichDataFrame
import org.apache.spark.sql.DataFrame

/**
 * Recreating the Getting Started With Python tutorial in Spark
 * https://www.kaggle.com/c/titanic/details/getting-started-with-python
 */
object GenderModelApp extends AbstractApp {

  override def execute(df: DataFrame, test: DataFrame) {
    studyGenderSurvival(df, test).show()
  }

  /**
   * Part 1 : study survival rate per gender
   * Return DataFrame GenderModel to save as genderbasedmodel.csv
   */
  private def studyGenderSurvival(df: DataFrame, test: DataFrame): DataFrame = {
    // Study which gender has highest survival rate
    val dfSurvivedPerGender = df.groupBy("Sex", "Survived").count().cache()
    val dfGender = dfSurvivedPerGender.groupBy("Sex").sum("count").withColumnRenamed("sum(count)", "count")

    val womenWhoSurvive = dfSurvivedPerGender.where("Sex=\"female\" AND Survived=1").extractDouble("count")
    val menWhoSurvive = dfSurvivedPerGender.where("Sex=\"male\" AND Survived=1").extractDouble("count")
    val nbWomen = dfGender.where("Sex=\"female\"").extractDouble("count")
    val nbMen = dfGender.where("Sex=\"male\"").extractDouble("count")

    println(s"Proportion of women who survived is ${100 * womenWhoSurvive / nbWomen} %")
    println(s"Proportion of men who survived is ${100 * menWhoSurvive / nbMen} %")

    dfSurvivedPerGender.unpersist()

    // Now that I have my indication that women were much more likely to survive, I am done with the training set.
    sqlContext.udf.register("predict", (s: String) => s match {
      case "male" => 0
      case "female" => 1
      case _ => -1
    })
    test.selectExpr("PassengerId", "predict(Sex) AS Survived")
  }
}