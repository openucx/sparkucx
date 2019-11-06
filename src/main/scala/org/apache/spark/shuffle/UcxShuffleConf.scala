/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.util.Utils

/**
 * Plugin configuration properties.
 */
class UcxShuffleConf(conf: SparkConf) extends SparkConf {
  private def getUcxConf(name: String) = s"spark.shuffle.ucx.$name"

  // Memory Pool
  private lazy val PREALLOCATE_BUFFERS =
  ConfigBuilder(getUcxConf("memory.preAllocateBuffers"))
    .doc("Comma separated list of buffer size : buffer count pairs to preallocate in memory pool. E.g. 4k:1000,16k:500")
    .stringConf.createWithDefault("")

  lazy val preallocateBuffersMap: java.util.Map[java.lang.Integer, java.lang.Integer] = {
    conf.get(PREALLOCATE_BUFFERS).split(",").withFilter(s => !s.isEmpty)
      .map(entry => entry.split(":") match {
        case Array(bufferSize, bufferCount) =>
          (int2Integer(Utils.byteStringAsBytes(bufferSize.trim).toInt),
           int2Integer(bufferCount.toInt))
      }).toMap.asJava
  }

  private lazy val MIN_BUFFER_SIZE = ConfigBuilder(getUcxConf("memory.minBufferSize"))
    .doc("Minimal buffer size in memory pool.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(1024)

  lazy val minBufferSize: Long = conf.getSizeAsBytes(MIN_BUFFER_SIZE.key,
    MIN_BUFFER_SIZE.defaultValueString)

  private lazy val MIN_REGISTRATION_SIZE =
    ConfigBuilder(getUcxConf("memory.minAllocationSize"))
    .doc("Minimal memory registration size in memory pool.")
    .bytesConf(ByteUnit.MiB)
    .createWithDefault(4)

  lazy val minRegistrationSize: Int = conf.getSizeAsBytes(MIN_REGISTRATION_SIZE.key,
    MIN_REGISTRATION_SIZE.defaultValueString).toInt

  private lazy val PREREGISTER_MEMORY = ConfigBuilder(getUcxConf("memory.preregister"))
    .doc("Whether to do ucp mem map for allocated memory in memory pool")
    .booleanConf.createWithDefault(true)

  lazy val preregisterMemory: Boolean = conf.getBoolean(PREREGISTER_MEMORY.key, PREREGISTER_MEMORY.defaultValue.get)
}
