/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.{concurrent, mutable}

import org.openucx.jucx.ucp.UcpMemory
import org.apache.spark.SparkConf
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.ucx.UcxNode
import org.apache.spark.shuffle.ucx.rpc.UcxRemoteMemory
import org.apache.spark.unsafe.Platform

/**
 * Common part for all spark versions for UcxShuffleManager logic
 */
abstract class CommonUcxShuffleManager(val conf: SparkConf, isDriver: Boolean) extends SortShuffleManager(conf) {
  type ShuffleId = Int
  type MapId = Int
  val ucxShuffleConf = new UcxShuffleConf(conf)

  var ucxNode: UcxNode = _

  // Shuffle handle is metadata information about the shuffle (num mappers, etc)
  // distributed by Spark task broadcast protocol.
  // UcxShuffleHandle is extension over Spark's shuffle handle to keep driver metadata info.
  val shuffleIdToHandle: concurrent.Map[ShuffleId, UcxShuffleHandle[_, _, _]] =
    new ConcurrentHashMap[ShuffleId, UcxShuffleHandle[_, _, _]]().asScala

  if (isDriver) {
    startUcxNodeIfMissing()
  }

  protected def registerShuffleCommon[K, V, C](baseHandle: BaseShuffleHandle[K,V,C],
                                               shuffleId: ShuffleId,
                                               numMaps: Int): ShuffleHandle = {
    // Register metadata buffer where each map will publish it's index and data file metadata
    val metadataBufferSize = numMaps * ucxShuffleConf.metadataBlockSize
    val metadataBuffer = Platform.allocateDirectBuffer(metadataBufferSize.toInt)

    val metadataMemory = ucxNode.getContext.registerMemory(metadataBuffer)
    shuffleIdToMetadataBuffer.put(shuffleId, metadataMemory)

    val driverMemory = new UcxRemoteMemory(metadataMemory.getAddress,
      metadataMemory.getRemoteKeyBuffer)

    val handle = new UcxShuffleHandle(shuffleId, driverMemory, numMaps, baseHandle)

    shuffleIdToHandle.putIfAbsent(shuffleId, handle)
    handle
  }

  /**
   * Mapping between shuffle and metadata buffer, to deregister it when shuffle not needed.
   */
  protected val shuffleIdToMetadataBuffer: mutable.Map[ShuffleId, UcpMemory] =
    new ConcurrentHashMap[ShuffleId, UcpMemory]().asScala

  /**
   * Atomically starts UcxNode singleton - one for all shuffle threads.
   */
  def startUcxNodeIfMissing(): Unit = synchronized {
    if (ucxNode == null) {
      ucxNode = new UcxNode(ucxShuffleConf, isDriver)
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    shuffleIdToMetadataBuffer.remove(shuffleId).foreach(_.deregister())
    shuffleBlockResolver.asInstanceOf[CommonUcxShuffleBlockResolver].removeShuffle(shuffleId)
    super.unregisterShuffle(shuffleId)
  }

  /**
   * Called on both driver and executors to finally cleanup resources.
   */
  override def stop(): Unit = synchronized {
    logInfo("Stopping shuffle manager")
    shuffleIdToHandle.keys.foreach(unregisterShuffle)
    shuffleIdToHandle.clear()
    if (ucxNode != null) {
      ucxNode.close()
      ucxNode = null
    }
    super.stop()
  }

}

/**
 * Spark shuffle handles extensions, broadcasted by TCP to executors.
 * Added metadataBufferOnDriver field, that contains address and rkey of driver metadata buffer.
 */
class UcxShuffleHandle[K, V, C](override val shuffleId: Int,
                                val metadataBufferOnDriver: UcxRemoteMemory,
                                val numMaps: Int,
                                val baseHandle: BaseShuffleHandle[K,V,C]) extends ShuffleHandle(shuffleId)
