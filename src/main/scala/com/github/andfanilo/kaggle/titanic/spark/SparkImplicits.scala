package com.github.andfanilo.kaggle.titanic.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

/**
 * All implicit classes for enriching RDD functionality
 */
object SparkImplicits {

  implicit class RichRDD[T: ClassTag](rdd: RDD[T]) {

    /**
     * Repartition RDD to number of partitions
     * @param nbOutputPartitions number of partitions for output rdd
     * @return if nbOutputPartitions =< 0 don't do anything
     *         else return rdd repartitioned with selected number of output partitions
     */
    def reduceToNbPartitions(nbOutputPartitions: Int = -1) = {
      nbOutputPartitions match {
        case x: Int if x <= 0 => rdd
        case x: Int if x < rdd.partitions.length => rdd.coalesce(nbOutputPartitions, shuffle = false)
        case _ => rdd
      }
    }
  }

  implicit class RichDataFrame(df: DataFrame) {

    /**
     * Extract value inside the given column of first row as Double
     * @param column column where to extract value from
     * @return value inside cell in first row at given column
     */
    def extractDouble(column: String) = {
      df.select(column).head().getLong(0).toDouble
    }
  }

}
