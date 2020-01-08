/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import java.io.Closeable
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.openucx.jucx.{UcxException, UcxRequest}
import org.openucx.jucx.ucp.{UcpEndpoint, UcpEndpointParams, UcpRemoteKey, UcpWorker}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.UcxNode
import org.apache.spark.storage.{BlockManager, BlockManagerId}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils

/**
 * Driver metadata buffer information that holds unpacked RkeyBuffer for this WorkerWrapper
 * and fetched buffer itself.
 */
case class DriverMetada(address: Long, ucpRkey: UcpRemoteKey, var length: Int, var data: ByteBuffer)

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
class UcxWorkerWrapper(val worker: UcpWorker, val conf: UcxShuffleConf, val id: Int)
  extends Closeable with Logging {
  import UcxWorkerWrapper._

  private final val driverSocketAddress = new InetSocketAddress(conf.driverHost, conf.driverPort)
  private final val endpointParams = new UcpEndpointParams().setSocketAddress(driverSocketAddress)
    .setPeerErrorHadnlingMode()
  val driverEndpoint: UcpEndpoint = worker.newEndpoint(endpointParams)

  private final val connections = mutable.Map.empty[BlockManagerId, UcpEndpoint]

  private final val driverMetadata = mutable.Map.empty[ShuffleId, DriverMetada]

  override def close(): Unit = {
    driverMetadata.values.foreach{
      case DriverMetada(address, rkey, length, data) => rkey.close()
    }
    driverMetadata.clear()
    driverEndpoint.close()
    connections.foreach{
      case (_, endpoint) => endpoint.close()
    }
    connections.clear()
    worker.close()
    driverMetadataBuffer.clear()
  }

  /**
   * Blocking progress single request until it's not completed.
   */
  def progressRequest(request: UcxRequest): Unit = {
    val startTime = System.currentTimeMillis()
    while (!request.isCompleted) {
      progress()
    }
    logDebug(s"Request completed in ${Utils.getUsedTimeMs(startTime)}")
  }

  /**
   * Blocking progress while result queue is empty.
   */
  def progressRequests(queue: LinkedBlockingQueue[_]): Unit = {
    while (queue.isEmpty) {
      progress()
    }
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
    val timeout = conf.getTimeAsMs("spark.network.timeout", "100")
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

  /**
   * Unpacks driver metadata RkeyBuffer for this worker.
   * Needed to perform PUT operation to publish map output info.
   */
  def getDriverMetadata(shuffleId: ShuffleId): DriverMetada = {
    driverMetadata.getOrElseUpdate(shuffleId, {
      val handle = SparkEnv.get.shuffleManager.asInstanceOf[UcxShuffleManager]
        .shuffleIdToHandle(shuffleId)
      val (address, length, rkey): (Long, Int, ByteBuffer) =
        handle match {
          case ucxShuffleHandle: UcxBaseShuffleHandle[_, _, _] =>
            (ucxShuffleHandle.metadataBufferOnDriver.getAddress,
              ucxShuffleHandle.numMaps * conf.metadataBlockSize.toInt,
              ucxShuffleHandle.metadataBufferOnDriver.getRkeyBuffer)
          case ucxShuffleHandle: UcxSerializedShuffleHandle[_, _] =>
            val ucxShuffleHandle = handle.asInstanceOf[UcxSerializedShuffleHandle[_, _]]
            (ucxShuffleHandle.metadataBufferOnDriver.getAddress,
              ucxShuffleHandle.numMaps * conf.metadataBlockSize.toInt,
              ucxShuffleHandle.metadataBufferOnDriver.getRkeyBuffer)
          case _ =>
            val ucxShuffleHandle = handle.asInstanceOf[UcxBypassMergeSortShuffleHandle[_, _]]
            (ucxShuffleHandle.metadataBufferOnDriver.getAddress,
              ucxShuffleHandle.numMaps * conf.metadataBlockSize.toInt,
              ucxShuffleHandle.metadataBufferOnDriver.getRkeyBuffer)
        }
      rkey.clear()
      val unpackedRkey = driverEndpoint.unpackRemoteKey(rkey)
      DriverMetada(address, unpackedRkey, length, null)
    })
  }

  /**
   * Fetches using ucp_get metadata buffer from driver, with all needed information
   * for offset and data addresses and keys.
   */
  def fetchDriverMetadataBuffer(shuffleId: ShuffleId): DriverMetada = {
    val handle = SparkEnv.get.shuffleManager.asInstanceOf[UcxShuffleManager]
      .shuffleIdToHandle(shuffleId)

    val metadata = getDriverMetadata(handle.shuffleId)

    UcxWorkerWrapper.driverMetadataBuffer.computeIfAbsent(shuffleId,
      (t: ShuffleId) => {
        val buffer = Platform.allocateDirectBuffer(metadata.length)
        val request = driverEndpoint.getNonBlocking(
          metadata.address, metadata.ucpRkey, buffer, null)
        progressRequest(request)
        buffer
      }
    )

    if (metadata.data == null) {
      metadata.data =  UcxWorkerWrapper.driverMetadataBuffer.get(shuffleId)
    }
    metadata
  }
}

object UcxWorkerWrapper {
  type ShuffleId = Int
  type MapId = Int
  // Driver metadata buffer, to fetch by first worker wrapper.
  val driverMetadataBuffer = new ConcurrentHashMap[ShuffleId, ByteBuffer]()
}
