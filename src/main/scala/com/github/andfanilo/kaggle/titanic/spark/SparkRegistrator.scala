package com.github.andfanilo.kaggle.titanic.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
 * Custom class registrating the Kryo serialization of specific classes
 */
class SparkRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
  }
}
