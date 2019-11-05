/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.util.Utils


/**
 * Plugin configuration properties.
 */
class UcxShuffleConf(conf: SparkConf) extends SparkConf {
  private def getUcxConf(name: String) = s"spark.shuffle.ucx.$name"

  lazy val blockManagerPort: Int = getInt("spark.blockManager.port", 0)

  lazy val getNumProcesses: Int = getInt("spark.executor.instances", 1)

  lazy val coresPerProcess: Int = getInt("spark.executor.cores",
    Runtime.getRuntime.availableProcessors())

  lazy val driverHost: String = conf.get(getUcxConf("driver.host"),
    conf.get("spark.driver.host", "0.0.0.0"))

  lazy val driverPort: Int = conf.getInt(getUcxConf("driver.port"), 55443)

  // Metadata
  val METADATA_BLOCK_SIZE: ConfigEntry[Long] =
    ConfigBuilder(getUcxConf("rkeySize"))
    .doc("Maximum size of rKeyBuffer")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(150)

  // For metadata we publish index file + data file rkeys
  lazy val metadataBlockSize: Long = 2 * conf.getSizeAsBytes(METADATA_BLOCK_SIZE.key,
    METADATA_BLOCK_SIZE.defaultValueString)

  private lazy val METADATA_RPC_BUFFER_SIZE =
    ConfigBuilder(getUcxConf("rpc.metadata.bufferSize"))
    .doc("Buffer size of worker -> driver metadata message")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(4096)

  lazy val metadataRPCBufferSize: Int = conf.getSizeAsBytes(METADATA_RPC_BUFFER_SIZE.key,
    METADATA_RPC_BUFFER_SIZE.defaultValueString).toInt

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
    .createWithDefault(4096)

  lazy val minBufferSize: Long = conf.getSizeAsBytes(MIN_BUFFER_SIZE.key,
    MIN_BUFFER_SIZE.defaultValueString)

  private lazy val MIN_REGISTRATION_SIZE =
    ConfigBuilder(getUcxConf("memory.minAllocationSize"))
    .doc("Minimal memory registration size in memory pool")
    .bytesConf(ByteUnit.MiB)
    .createWithDefault(4)

  lazy val minRegistrationSize: Int = conf.getSizeAsBytes(MIN_REGISTRATION_SIZE.key,
    MIN_REGISTRATION_SIZE.defaultValueString).toInt
            
  private lazy val MIN_ALLOCATION_SIZE = ConfigBuilder(getUcxConf("memory.minAllocationSize"))
    .doc("Minimal memory allocation size in memory pool")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(4096)

  lazy val minAllocationSize: Long = conf.getSizeAsBytes(MIN_ALLOCATION_SIZE.key, MIN_ALLOCATION_SIZE.defaultValueString)

  private lazy val PREREGISTER_MEMORY = ConfigBuilder(getUcxConf("memory.preregister"))
    .doc("Whether to do ucp mem map for allocated memory in memory pool")
    .booleanConf.createWithDefault(true)

  lazy val preregisterMemory: Boolean = conf.getBoolean(PREREGISTER_MEMORY.key, PREREGISTER_MEMORY.defaultValue.get)
}
