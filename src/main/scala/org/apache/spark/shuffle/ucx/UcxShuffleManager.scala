/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

import org.apache.spark.rpc.RpcEnv
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager.canUseBatchFetch
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors}
import org.apache.spark.shuffle.ucx.rpc.{UcxDriverRpcEndpoint, UcxExecutorRpcEndpoint}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer
import org.apache.spark.util.RpcUtils
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, TaskContext}


class UcxShuffleManager(conf: SparkConf, isDriver: Boolean) extends SortShuffleManager(conf) {

  val ucxShuffleConf = new UcxShuffleConf(conf)

  lazy val ucxShuffleTransport: UcxShuffleTransport = if (!isDriver) {
    new UcxShuffleTransport(ucxShuffleConf, "init")
  } else {
    null
  }

  @volatile private var initialized: Boolean = false

  override val shuffleBlockResolver =
    new UcxShuffleBlockResolver(ucxShuffleConf, ucxShuffleTransport)

  logInfo("Starting UcxShuffleManager")

  def initTransport(): Unit = this.synchronized {
    if (!initialized) {
      val driverEndpointName = "ucx-shuffle-driver"
      if (isDriver) {
        val rpcEnv = SparkEnv.get.rpcEnv
        val driverEndpoint = new UcxDriverRpcEndpoint(rpcEnv)
        rpcEnv.setupEndpoint(driverEndpointName, driverEndpoint)
      } else {
        val blockManager = SparkEnv.get.blockManager.blockManagerId
        ucxShuffleTransport.executorId = blockManager.executorId
        val rpcEnv = RpcEnv.create("ucx-rpc-env", blockManager.host, blockManager.host,
          blockManager.port, conf, new SecurityManager(conf), 1, clientMode=false)
        logDebug("Initializing ucx transport")
        val address = ucxShuffleTransport.init()
        val executorEndpoint = new UcxExecutorRpcEndpoint(rpcEnv, ucxShuffleTransport)
        val endpoint = rpcEnv.setupEndpoint(
          s"ucx-shuffle-executor-${blockManager.executorId}",
          executorEndpoint)

        val driverEndpoint = RpcUtils.makeDriverRef(driverEndpointName, conf, rpcEnv)
        driverEndpoint.ask[IntroduceAllExecutors](ExecutorAdded(blockManager.executorId,
          endpoint, new SerializableDirectBuffer(address)))
          .andThen{
            case Success(msg) =>
              logInfo(s"Receive reply $msg")
              executorEndpoint.receive(msg)
          }
      }
      initialized = true
    }
  }

  override def getReader[K, C](handle: ShuffleHandle,
                               startPartition: Int,
                               endPartition: Int,
                               context: TaskContext,
                               metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startPartition, endPartition)
    new UcxShuffleReader(ucxShuffleTransport,
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], blocksByAddress, context, metrics,
      shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
  }

  override def getReaderForRange[K, C]( handle: ShuffleHandle,
                                        startMapIndex: Int,
                                        endMapIndex: Int,
                                        startPartition: Int,
                                        endPartition: Int,
                                        context: TaskContext,
                                        metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByRange(
      handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
    new UcxShuffleReader(ucxShuffleTransport,
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], blocksByAddress, context, metrics,
      shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
  }

  override def stop(): Unit = {
    if (ucxShuffleTransport != null) {
      ucxShuffleTransport.close()
    }
    super.stop()
  }
}
