/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import java.io.{Closeable, File, RandomAccessFile}
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}

import scala.collection.JavaConverters._

import org.openucx.jucx.UcxUtils
import org.openucx.jucx.ucp.UcpMemory
import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.shuffle.ucx.UnsafeUtils
import org.apache.spark.storage.ShuffleIndexBlockId
import org.apache.spark.util.Utils

/**
 * Mapper entry point for UcxShuffle plugin. Performs memory registration
 * of data and index files and publish addresses to driver metadata buffer.
 */
class UcxShuffleBlockResolver(ucxShuffleManager: UcxShuffleManager)
  extends IndexShuffleBlockResolver(ucxShuffleManager.conf) with Closeable {
  type MapId = Int

  private lazy val memPool = ucxShuffleManager.ucxNode.getMemoryPool

  // Keep track of registered memory regions to release them when shuffle not needed
  private val fileMappings = new ConcurrentHashMap[ShuffleId, CopyOnWriteArrayList[UcpMemory]].asScala
  private val offsetMappings = new ConcurrentHashMap[ShuffleId, CopyOnWriteArrayList[UcpMemory]].asScala

  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    SparkEnv.get.blockManager
      .diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * Mapper commit protocol extension. Register index and data files and publish all needed
   * metadata to driver.
   */
  override def writeIndexFileAndCommit(shuffleId: ShuffleId, mapId: Int,
                                       lengths: Array[Long], dataTmp: File): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    val startTime = System.currentTimeMillis()

    val dataFile = getDataFile(shuffleId, mapId)
    val dataBackFile = new RandomAccessFile(dataFile, "rw")

    if (dataBackFile.length() == 0) {
      dataBackFile.close()
      return
    }

    fileMappings.putIfAbsent(shuffleId, new CopyOnWriteArrayList[UcpMemory]())
    offsetMappings.putIfAbsent(shuffleId, new CopyOnWriteArrayList[UcpMemory]())

    val indexFile = getIndexFile(shuffleId, mapId)
    val indexBackFile = new RandomAccessFile(indexFile, "rw")
    val indexFileChannel = indexBackFile.getChannel
    val dataFileChannel = dataBackFile.getChannel

    // Memory map and register data and index file.
    val dataFileBuffer = UnsafeUtils.mmap(dataFileChannel, 0, dataBackFile.length())
    // TODO: update to jucx-1.8.0 and add an option of ODP registration
    val dataMemory = ucxShuffleManager.ucxNode.getContext.registerMemory(dataFileBuffer)
    fileMappings(shuffleId).add(dataMemory)
    assume(indexBackFile.length() == UcxWorkerWrapper.LONG_SIZE * (lengths.size + 1))

    val offsetBuffer = UnsafeUtils.mmap(indexFileChannel, 0, indexBackFile.length())
    val offsetMemory = ucxShuffleManager.ucxNode.getContext.registerMemory(offsetBuffer)
    offsetMappings(shuffleId).add(offsetMemory)

    dataFileChannel.close()
    dataBackFile.close()
    indexFileChannel.close()
    indexBackFile.close()

    val fileMemoryRkey = dataMemory.getRemoteKeyBuffer
    val offsetRkey = offsetMemory.getRemoteKeyBuffer

    val metadataRegisteredMemory = memPool.get(
      fileMemoryRkey.capacity() + offsetRkey.capacity() + 24)
    val metadataBuffer = metadataRegisteredMemory.getBuffer.slice()

    if (metadataBuffer.remaining() > ucxShuffleManager.ucxShuffleConf.metadataBlockSize) {
      throw new SparkException(s"Metadata block size ${metadataBuffer.remaining()} " +
        s"is greater then configured 2 * ${ucxShuffleManager.ucxShuffleConf.RKEY_SIZE.key}" +
        s"(${ucxShuffleManager.ucxShuffleConf.metadataBlockSize}).")
    }

    metadataBuffer.clear()

    metadataBuffer.putLong(offsetMemory.getAddress)
    metadataBuffer.putLong(dataMemory.getAddress)

    metadataBuffer.putInt(offsetRkey.capacity())
    metadataBuffer.put(offsetRkey)

    metadataBuffer.putInt(fileMemoryRkey.capacity())
    metadataBuffer.put(fileMemoryRkey)

    metadataBuffer.clear()

    val workerWrapper = ucxShuffleManager.ucxNode.getThreadLocalWorker
    val driverMetadata = workerWrapper.getDriverMetadata(shuffleId)
    val driverOffset = driverMetadata.address +
      mapId * ucxShuffleManager.ucxShuffleConf.metadataBlockSize

    val driverEndpoint = workerWrapper.driverEndpoint
    val request = driverEndpoint.putNonBlocking(UcxUtils.getAddress(metadataBuffer),
      metadataBuffer.remaining(), driverOffset, driverMetadata.driverRkey, null)

    workerWrapper.preconnect()
    // Blocking progress needed to make sure last mapper published data to driver before
    // reducer starts.
    workerWrapper.waitRequest(request)
    memPool.put(metadataRegisteredMemory)
    logInfo(s"MapID: $mapId register files + publishing overhead: " +
      s"${Utils.getUsedTimeMs(startTime)}")
  }

  private def unregisterAndUnmap(mem: UcpMemory): Unit = {
    val address = mem.getAddress
    val length = mem.getData.capacity()
    mem.deregister()
    UnsafeUtils.munmap(address, length)
  }

  def removeShuffle(shuffleId: Int): Unit = {
    logInfo(s"Removing shuffle $shuffleId")
    fileMappings.remove(shuffleId).foreach((mappings: CopyOnWriteArrayList[UcpMemory]) =>
      mappings.asScala.par.foreach(unregisterAndUnmap))
    offsetMappings.remove(shuffleId).foreach((mappings: CopyOnWriteArrayList[UcpMemory]) =>
      mappings.asScala.par.foreach(unregisterAndUnmap))
  }

  override def close(): Unit = {
    fileMappings.keys.foreach(removeShuffle)
  }
}
