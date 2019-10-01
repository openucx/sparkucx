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

class UcxWorkerWrapper(val worker: UcpWorker, val conf: UcxShuffleConf,
                       val id: Int) extends Closeable
  with Logging {
  type ShuffleId = Int
  type MapId = Int

  private final val socketAddress = new InetSocketAddress(conf.driverHost, conf.driverPort)
  private final val endpointParams = new UcpEndpointParams().setSocketAddress(socketAddress)
    .setPeerErrorHadnlingMode()
  val driverEndpoint: UcpEndpoint = worker.newEndpoint(endpointParams)

  final val driverMetadataBuffer = mutable.Map.empty[ShuffleId, DriverMetadaBuffer]

  final val offsetRkeyCache = mutable.Map.empty[ShuffleId, Array[UcpRemoteKey]]

  final val dataRkeyCache = mutable.Map.empty[ShuffleId, Array[UcpRemoteKey]]

  val blockManager: BlockManager = SparkEnv.get.blockManager

  private final val connections = mutable.Map.empty[BlockManagerId, UcpEndpoint]

  override def close(): Unit = {
    driverMetadataBuffer.values.foreach(_.ucpRkey.close())
    driverMetadataBuffer.clear()
    offsetRkeyCache.mapValues(_.foreach(_.close()))
    offsetRkeyCache.clear()
    dataRkeyCache.mapValues(_.foreach(_.close()))
    dataRkeyCache.clear()
    driverEndpoint.close()
    worker.close()
  }

  def addDriverMetadata(handle: ShuffleHandle): Unit = {
    driverMetadataBuffer.getOrElseUpdate(handle.shuffleId, {
      val (address, length, rkey): (Long, Int, ByteBuffer) =
        handle match {
          case ucxShuffleHandle: UcxBaseShuffleHandle[_, _, _] =>
            (ucxShuffleHandle.metadataArrayOnDriver.address,
              ucxShuffleHandle.numMaps * conf.metadataBlockSize.toInt,
              ucxShuffleHandle.metadataArrayOnDriver.rKeyBuffer)
          case _ =>
            val ucxShuffleHandle = handle.asInstanceOf[UcxSerializedShuffleHandle[_, _]]
            (ucxShuffleHandle.metadataArrayOnDriver.address,
              ucxShuffleHandle.numMaps * conf.metadataBlockSize.toInt,
              ucxShuffleHandle.metadataArrayOnDriver.rKeyBuffer)
        }
      rkey.clear()
      val unpackedRkey = driverEndpoint.unpackRemoteKey(rkey)
      DriverMetadaBuffer(address, unpackedRkey, length, null)
    })
  }

  def fetchDriverMetadataBuffer(handle: ShuffleHandle): DriverMetadaBuffer = {
    val numBlocks = handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps
    offsetRkeyCache.getOrElseUpdate(handle.shuffleId, Array.ofDim[UcpRemoteKey](numBlocks))
    dataRkeyCache.getOrElseUpdate(handle.shuffleId, Array.ofDim[UcpRemoteKey](numBlocks))
    val driverMetadata = driverMetadataBuffer(handle.shuffleId)

    if (driverMetadata.data == null) {
      logInfo(s"Worker wrapper: $id fetching driver metadata for ${handle.shuffleId}")
      driverMetadata.data = Platform.allocateDirectBuffer(driverMetadata.length)
      val request = driverEndpoint.getNonBlocking(
        driverMetadata.address, driverMetadata.ucpRkey, driverMetadata.data, null)
      progressRequest(request)
    }
    driverMetadata
  }

  def progressRequest(request: UcxRequest): Unit = {
    val startTime = System.currentTimeMillis()
    while (!request.isCompleted) {
      progress()
    }
    logInfo(s"Request completed in ${Utils.getUsedTimeMs(startTime)}")
  }

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

  def preconnnect(): Unit = {
    UcxNode.getWorkerAddresses.keySet().asScala.foreach(getConnection)
  }

  def getConnection(blockManagerId: BlockManagerId): UcpEndpoint = {
    val workerAdresses = UcxNode.getWorkerAddresses
    if (workerAdresses.get(blockManagerId) == null) {
      workerAdresses.synchronized {
        while (workerAdresses.get(blockManagerId) == null) {
          workerAdresses.wait()
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

  def clearConnections(): Unit = Try {
    connections.values.foreach(_.close())
    connections.clear()
  }
}
