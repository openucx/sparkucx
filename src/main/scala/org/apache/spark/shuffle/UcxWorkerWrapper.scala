/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import java.io.Closeable
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConverters._

import scala.collection.mutable
import scala.util.Try

import org.openucx.jucx.{UcxException, UcxRequest}
import org.openucx.jucx.ucp.{UcpEndpoint, UcpEndpointParams, UcpRemoteKey, UcpWorker}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.UcxNode
import org.apache.spark.storage.{BlockManager, BlockManagerId}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
class UcxWorkerWrapper(val worker: UcpWorker, val conf: UcxShuffleConf, val id: Int)
  extends Closeable with Logging {
  type ShuffleId = Int
  type MapId = Int

  private final val driverSocketAddress = new InetSocketAddress(conf.driverHost, conf.driverPort)
  private final val endpointParams = new UcpEndpointParams().setSocketAddress(driverSocketAddress)
    .setPeerErrorHadnlingMode()
  val driverEndpoint: UcpEndpoint = worker.newEndpoint(endpointParams)

  val blockManager: BlockManager = SparkEnv.get.blockManager

  private final val connections = mutable.Map.empty[BlockManagerId, UcpEndpoint]

  override def close(): Unit = {
    driverEndpoint.close()
    worker.close()
  }

  /**
   * Progress single request until it's not completed.
   */
  def progressRequest(request: UcxRequest): Unit = {
    val startTime = System.currentTimeMillis()
    while (!request.isCompleted) {
      progress()
    }
    logDebug(s"Request completed in ${Utils.getUsedTimeMs(startTime)}")
  }

  /**
   * The only place for worker progress
   */
  private def progress(): Int = {
    worker.progress()
  }

  /**
   * Establish connections to known instances.
   */
  def preconnnect(): Unit = {
    UcxNode.getWorkerAddresses.keySet().asScala.foreach(getConnection)
  }

  def getConnection(blockManagerId: BlockManagerId): UcpEndpoint = {
    val workerAdresses = UcxNode.getWorkerAddresses
    // Block untill there's no worker address for this BlockManagerID
    val startTime = System.currentTimeMillis()
    val timeout = conf.getTimeAsMs("spark.network.timeout")
    if (workerAdresses.get(blockManagerId) == null) {
      workerAdresses.synchronized {
        while (workerAdresses.get(blockManagerId) == null) {
          workerAdresses.wait(timeout)
          if (System.currentTimeMillis() - startTime > timeout) {
            throw new UcxException(s"Didn't get worker address for $blockManagerId during $timeout")
          }
        }
      }
    }

    connections.getOrElseUpdate(blockManagerId, {
      logInfo(s"Worker $id connecting to $blockManagerId")
      val endpointParams = new UcpEndpointParams()
        .setPeerErrorHadnlingMode()
        .setUcpAddress(workerAdresses.get(blockManagerId))
     worker.newEndpoint(endpointParams)
    })
  }
}
