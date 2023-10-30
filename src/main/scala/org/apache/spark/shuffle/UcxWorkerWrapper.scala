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

import org.openucx.jucx.UcxException
import org.openucx.jucx.ucp.{
  UcpEndpoint,
  UcpEndpointParams,
  UcpRemoteKey,
  UcpRequest,
  UcpWorker
}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.{UcxNode, UnsafeUtils}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.unsafe.Platform

/** Driver metadata buffer information that holds unpacked RkeyBuffer for this WorkerWrapper
  * and fetched buffer itself.
  */
case class DriverMetadata(
    address: Long,
    driverRkey: UcpRemoteKey,
    length: Int,
    var data: ByteBuffer
) {
  // Driver metadata is an array of blocks:
  // | mapId0 | mapId1 | mapId2 | mapId3 | mapId4 | mapId5 |
  // Each block in driver metadata has next layout:
  // |offsetAddress|dataAddress|offsetRkeySize|offsetRkey|dataRkeySize|dataRkey|

  def offsetAddress(mapId: Int): Long = {
    val mapIdBlock = mapId * UcxWorkerWrapper.metadataBlockSize
    data.getLong(mapIdBlock)
  }

  def dataAddress(mapId: Int): Long = {
    val mapIdBlock = mapId * UcxWorkerWrapper.metadataBlockSize
    data.getLong(mapIdBlock + UnsafeUtils.LONG_SIZE)
  }

  def offsetRkey(mapId: Int): ByteBuffer = {
    val mapIdBlock = mapId * UcxWorkerWrapper.metadataBlockSize
    var offsetWithinBlock = mapIdBlock + 2 * UnsafeUtils.LONG_SIZE
    val rkeySize = data.getInt(offsetWithinBlock)
    offsetWithinBlock += UnsafeUtils.INT_SIZE
    val result = data.duplicate()
    result.position(offsetWithinBlock).limit(offsetWithinBlock + rkeySize)
    result.slice()
  }

  def dataRkey(mapId: Int): ByteBuffer = {
    val mapIdBlock = mapId * UcxWorkerWrapper.metadataBlockSize
    var offsetWithinBlock = mapIdBlock + 2 * UnsafeUtils.LONG_SIZE
    val offsetRkeySize = data.getInt(offsetWithinBlock)
    offsetWithinBlock += UnsafeUtils.INT_SIZE + offsetRkeySize
    val dataRkeySize = data.getInt(offsetWithinBlock)
    offsetWithinBlock += UnsafeUtils.INT_SIZE
    val result = data.duplicate()
    result.position(offsetWithinBlock).limit(offsetWithinBlock + dataRkeySize)
    result.slice()
  }
}

/** Worker per thread wrapper, that maintains connection and progress logic.
  */
class UcxWorkerWrapper(
    val worker: UcpWorker,
    val conf: UcxShuffleConf,
    val id: Int
) extends Closeable
    with Logging {
  import UcxWorkerWrapper._

  private final val driverSocketAddress =
    new InetSocketAddress(conf.driverHost, conf.driverPort)
  private final val endpointParams = new UcpEndpointParams()
    .setSocketAddress(driverSocketAddress)
    .setPeerErrorHandlingMode()
  val driverEndpoint: UcpEndpoint = worker.newEndpoint(endpointParams)

  private final val connections = mutable.Map.empty[BlockManagerId, UcpEndpoint]

  private final val driverMetadata =
    mutable.Map.empty[ShuffleId, DriverMetadata]

  override def close(): Unit = {
    driverMetadata.values.foreach {
      case DriverMetadata(address, rkey, length, data) => rkey.close()
    }
    driverMetadata.clear()
    driverEndpoint.close()
    connections.foreach { case (_, endpoint) =>
      endpoint.close()
    }
    connections.clear()
    worker.close()
    driverMetadataBuffer.clear()
  }

  /** Blocking progress single request until it's not completed.
    */
  def waitRequest(request: UcpRequest): Unit = {
    val startTime = System.currentTimeMillis()
    worker.progressRequest(request)
    logDebug(
      s"Request completed in ${System.currentTimeMillis() - startTime} ms"
    )
  }

  /** Blocking progress while result queue is empty.
    */
  def fillQueueWithBlocks(queue: LinkedBlockingQueue[_]): Unit = {
    while (queue.isEmpty) {
      progress()
    }
  }

  /** The only place for worker progress
    */
  private def progress(): Int = {
    worker.progress()
  }

  /** Establish connections to known instances.
    */
  def preconnect(): Unit = {
    UcxNode.getWorkerAddresses.keySet().asScala.foreach(getConnection)
  }

  def getConnection(blockManagerId: BlockManagerId): UcpEndpoint = {
    val workerAddresses = UcxNode.getWorkerAddresses
    // Block untill there's no worker address for this BlockManagerID
    val startTime = System.currentTimeMillis()
    val timeout = conf.getTimeAsMs("spark.network.timeout", "100")
    if (workerAddresses.get(blockManagerId) == null) {
      workerAddresses.synchronized {
        while (workerAddresses.get(blockManagerId) == null) {
          workerAddresses.wait(timeout)
          if (System.currentTimeMillis() - startTime > timeout) {
            throw new UcxException(
              s"Didn't get worker address for $blockManagerId during $timeout"
            )
          }
        }
      }
    }

    connections.getOrElseUpdate(
      blockManagerId, {
        logInfo(s"Worker $id connecting to $blockManagerId")
        val endpointParams = new UcpEndpointParams()
          .setPeerErrorHandlingMode()
          .setUcpAddress(workerAddresses.get(blockManagerId))
        worker.newEndpoint(endpointParams)
      }
    )
  }

  /** Unpacks driver metadata RkeyBuffer for this worker.
    * Needed to perform PUT operation to publish map output info.
    */
  def getDriverMetadata(shuffleId: ShuffleId): DriverMetadata = {
    driverMetadata.getOrElseUpdate(
      shuffleId, {
        val ucxShuffleHandle = SparkEnv.get.shuffleManager
          .asInstanceOf[CommonUcxShuffleManager]
          .shuffleIdToHandle(shuffleId)
        val (address, length, rkey): (Long, Int, ByteBuffer) = (
          ucxShuffleHandle.metadataBufferOnDriver.getAddress,
          ucxShuffleHandle.numMaps * conf.metadataBlockSize.toInt,
          ucxShuffleHandle.metadataBufferOnDriver.getRkeyBuffer
        )

        rkey.clear()
        val unpackedRkey = driverEndpoint.unpackRemoteKey(rkey)
        DriverMetadata(address, unpackedRkey, length, null)
      }
    )
  }

  /** Fetches using ucp_get metadata buffer from driver, with all needed information
    * for offset and data addresses and keys.
    */
  def fetchDriverMetadataBuffer(shuffleId: ShuffleId): DriverMetadata = {
    val handle = SparkEnv.get.shuffleManager
      .asInstanceOf[CommonUcxShuffleManager]
      .shuffleIdToHandle(shuffleId)

    val metadata = getDriverMetadata(handle.shuffleId)

    UcxWorkerWrapper.driverMetadataBuffer.computeIfAbsent(
      shuffleId,
      (t: ShuffleId) => {
        val buffer = Platform.allocateDirectBuffer(metadata.length)
        val request = driverEndpoint.getNonBlocking(
          metadata.address,
          metadata.driverRkey,
          buffer,
          null
        )
        waitRequest(request)
        buffer
      }
    )

    if (metadata.data == null) {
      metadata.data = UcxWorkerWrapper.driverMetadataBuffer.get(shuffleId)
    }
    metadata
  }
}

object UcxWorkerWrapper {
  type ShuffleId = Int
  type MapId = Int
  // Driver metadata buffer, to fetch by first worker wrapper.
  val driverMetadataBuffer = new ConcurrentHashMap[ShuffleId, ByteBuffer]()

  val metadataBlockSize: MapId =
    SparkEnv.get.shuffleManager
      .asInstanceOf[CommonUcxShuffleManager]
      .ucxShuffleConf
      .metadataBlockSize
      .toInt
}
