package com.github.andfanilo.kaggle.titanic.spark

import com.github.andfanilo.kaggle.titanic.spark.SparkImplicits._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{DoNotDiscover, FreeSpec, Matchers}

case class RowSchema(name: String, age: Long)

@DoNotDiscover
class SparkImplicitsTest extends FreeSpec with Matchers with SharedSparkContext {

  "Should reduce number of partitions of an rdd" in {
    val rdd = sc.parallelize(1 to 100).repartition(5)

    rdd.partitions.length shouldBe 5

    rdd.reduceToNbPartitions(0).partitions.length shouldBe 5
    rdd.reduceToNbPartitions(3).partitions.length shouldBe 3
    rdd.reduceToNbPartitions(5).partitions.length shouldBe 5
    rdd.reduceToNbPartitions(7).partitions.length shouldBe 5
  }

  "Should extract value in first row in a column as double" in {
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    val df = sc.parallelize(Seq(RowSchema("hi", 32L))).toDF()
    df.extractDouble("age") should be(32)
  }
}
