/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.concurrent

import org.openucx.jucx.ucp.UcpMemory
import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle.sort.{BypassMergeSortShuffleHandle, SerializedShuffleHandle, SortShuffleManager, SortShuffleWriter}
import org.apache.spark.shuffle.ucx.UcxNode
import org.apache.spark.shuffle.ucx.rpc.UcxRemoteMemory
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.ShutdownHookManager

/**
 * Main entry point of Ucx shuffle plugin. It extends spark's default SortShufflePlugin
 * and injects needed logic in override methods.
 */
class UcxShuffleManager(val conf: SparkConf, isDriver: Boolean) extends SortShuffleManager(conf) {
  type ShuffleId = Int
  type MapId = Int

  val ucxShuffleConf = new UcxShuffleConf(conf)
  var ucxNode: UcxNode = _

  // Shuffle handle is metadata information about the shuffle (num mappers, etc)
  // distributed by Spark task broadcast protocol.
  // UcxShuffleHandle is extension over Spark's shuffle handle to keep driver metadata info.
  val shuffleIdToHandle: concurrent.Map[ShuffleId, ShuffleHandle] =
    new ConcurrentHashMap[ShuffleId, ShuffleHandle]().asScala

  ShutdownHookManager.addShutdownHook(Int.MaxValue - 1)(stop)

  /**
   * Mapping between shuffle and metadata buffer, to deregister it when shuffle not needed.
   */
  private val shuffleIdToMetadataBuffer =
    new ConcurrentHashMap[ShuffleId, UcpMemory]().asScala

  /**
   * Atomically starts UcxNode singleton - one for all shuffle threads.
   */
  def startUcxNodeIfMissing(): Unit = synchronized {
    if (ucxNode == null) {
      ucxNode = new UcxNode(ucxShuffleConf, isDriver)
    }
  }

  if (isDriver) {
    startUcxNodeIfMissing()
  }

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   * Called on driver and guaranteed by spark that shuffle on executor will start after it.
   */
  override def registerShuffle[K, V, C](shuffleId: Int,
                                        numMaps: Int,
                                        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    assume(isDriver)
    // Register metadata buffer where each map will publish it's index and data file metadata
    val metadataBufferSize = numMaps * ucxShuffleConf.metadataBlockSize
    val metadataBuffer = Platform.allocateDirectBuffer(metadataBufferSize.toInt)

    val metadataMemory = ucxNode.getContext.registerMemory(metadataBuffer)
    shuffleIdToMetadataBuffer.put(shuffleId, metadataMemory)

    val driverMemory = new UcxRemoteMemory(metadataMemory.getAddress,
      metadataMemory.getRemoteKeyBuffer)

    val handle: ShuffleHandle = if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      new UcxBypassMergeSortShuffleHandle[K, V](driverMemory, shuffleId, numMaps,
        dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      new UcxSerializedShuffleHandle[K, V](driverMemory, shuffleId, numMaps,
        dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      new UcxBaseShuffleHandle(driverMemory, shuffleId, numMaps, dependency)
    }

    shuffleIdToHandle.putIfAbsent(shuffleId, handle)
    handle
  }

  /**
   * Mapper callback on executor. Just start UcxNode and use Spark mapper logic.
   */
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int,
                               context: TaskContext): ShuffleWriter[K, V] = {
    startUcxNodeIfMissing()
    shuffleIdToHandle.putIfAbsent(handle.shuffleId, handle)
    super.getWriter(handle, mapId, context)
  }

  override val shuffleBlockResolver: UcxShuffleBlockResolver = new UcxShuffleBlockResolver(this)

  /**
   * Reducer callback on executor.
   */
  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int,
                               endPartition: Int, context: TaskContext): ShuffleReader[K, C] = {
    startUcxNodeIfMissing()
    shuffleIdToHandle.putIfAbsent(handle.shuffleId, handle)
    new UcxShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition,
      endPartition, context)
  }


  override def unregisterShuffle(shuffleId: Int): Boolean = {
    shuffleIdToMetadataBuffer.remove(shuffleId).foreach(_.deregister())
    shuffleBlockResolver.removeShuffle(shuffleId)
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
class UcxBaseShuffleHandle[K, V, C](val metadataBufferOnDriver: UcxRemoteMemory,
                                    shuffleId: Int,
                                    numMaps: Int,
                                    dependency: ShuffleDependency[K, V, C])
  extends BaseShuffleHandle[K, V, C](shuffleId, numMaps, dependency)

class UcxSerializedShuffleHandle[K, V](val metadataBufferOnDriver: UcxRemoteMemory,
                                       shuffleId: Int,
                                       numMaps: Int,
                                       dependency: ShuffleDependency[K, V, V])
  extends SerializedShuffleHandle[K, V](shuffleId, numMaps, dependency)

class UcxBypassMergeSortShuffleHandle[K, V](val metadataBufferOnDriver: UcxRemoteMemory,
                                            shuffleId: Int,
                                            numMaps: Int,
                                            dependency: ShuffleDependency[K, V, V])
  extends BypassMergeSortShuffleHandle(shuffleId, numMaps, dependency)
