/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.network.util.ByteUnit

class UcxShuffleConf(val conf: SparkConf) extends SparkConf {
  private def getUcxConf(name: String) = s"spark.shuffle.ucx.$name"

  private val PROTOCOL =
    ConfigBuilder(getUcxConf("protocol"))
      .doc("Which protocol to use: rndv (default), one-sided")
      .stringConf
      .checkValue(protocol => protocol == "rndv" || protocol == "one-sided",
        "Invalid protocol. Valid options: rndv / one-sided.")
      .createWithDefault("rndv")

  private val MEMORY_PINNING =
    ConfigBuilder(getUcxConf("memoryPinning"))
      .doc("Whether to pin whole shuffle data in memory")
      .booleanConf
      .createWithDefault(false)

  lazy val WORKER_ADDRESS_SIZE: ConfigEntry[Long] =
    ConfigBuilder(getUcxConf("maxWorkerSize"))
      .doc("Maximum size of worker address in bytes")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(1000)

  private lazy val RPC_MESSAGE_SIZE =
    ConfigBuilder(getUcxConf("rpcMessageSize"))
      .doc("Size of RPC message to send from fetchBlockByBlockId. Must contain ")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(size => size > maxWorkerAddressSize,
        "Rpc message must contain workerAddress")
      .createWithDefault(2000)

  private lazy val WAKEUP_FEATURE =
    ConfigBuilder(getUcxConf("useWakeup"))
      .doc("Whether to use busy polling for workers")
      .booleanConf
      .createWithDefault(false)

  private lazy val RECV_QUEUE_SIZE =
    ConfigBuilder(getUcxConf("recvQueueSize"))
      .doc("Number of recv requests")
      .intConf
      .createWithDefault(5)

  lazy val protocol: String = conf.get(PROTOCOL.key, PROTOCOL.defaultValueString)

  lazy val useOdp: Boolean = conf.getBoolean(getUcxConf("memory.useOdp"), defaultValue = false)

  lazy val pinMemory: Boolean = conf.getBoolean(MEMORY_PINNING.key, MEMORY_PINNING.defaultValue.get)

  lazy val maxWorkerAddressSize: Long = conf.getSizeAsBytes(WORKER_ADDRESS_SIZE.key,
    WORKER_ADDRESS_SIZE.defaultValueString)

  lazy val rpcMessageSize: Long = conf.getSizeAsBytes(RPC_MESSAGE_SIZE.key,
    RPC_MESSAGE_SIZE.defaultValueString)

  lazy val useWakeup: Boolean = conf.getBoolean(WAKEUP_FEATURE.key, WAKEUP_FEATURE.defaultValue.get)

  lazy val recvQueueSize: Int = conf.getInt(RECV_QUEUE_SIZE.key, RECV_QUEUE_SIZE.defaultValue.get)
}
