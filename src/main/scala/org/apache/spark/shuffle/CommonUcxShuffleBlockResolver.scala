/*
 * Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle

import java.io.{File, RandomAccessFile}
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}

import scala.collection.JavaConverters._

import org.openucx.jucx.UcxUtils
import org.openucx.jucx.ucp.{UcpMemMapParams, UcpMemory}
import org.apache.spark.shuffle.ucx.UnsafeUtils
import org.apache.spark.SparkException

/** Mapper entry point for UcxShuffle plugin. Performs memory registration
  * of data and index files and publish addresses to driver metadata buffer.
  */
abstract class CommonUcxShuffleBlockResolver(
    ucxShuffleManager: CommonUcxShuffleManager
) extends IndexShuffleBlockResolver(ucxShuffleManager.conf) {
  private lazy val memPool = ucxShuffleManager.ucxNode.getMemoryPool

  // Keep track of registered memory regions to release them when shuffle not needed
  private val fileMappings =
    new ConcurrentHashMap[ShuffleId, CopyOnWriteArrayList[UcpMemory]].asScala
  private val offsetMappings =
    new ConcurrentHashMap[ShuffleId, CopyOnWriteArrayList[UcpMemory]].asScala

  /** Mapper commit protocol extension. Register index and data files and publish all needed
    * metadata to driver.
    */
  def writeIndexFileAndCommitCommon(
      shuffleId: ShuffleId,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File,
      indexBackFile: RandomAccessFile,
      dataBackFile: RandomAccessFile
  ): Unit = {
    val startTime = System.currentTimeMillis()

    fileMappings.putIfAbsent(shuffleId, new CopyOnWriteArrayList[UcpMemory]())
    offsetMappings.putIfAbsent(shuffleId, new CopyOnWriteArrayList[UcpMemory]())

    val indexFileChannel = indexBackFile.getChannel
    val dataFileChannel = dataBackFile.getChannel

    // Memory map and register data and index file.
    val dataAddress =
      UnsafeUtils.mmap(dataFileChannel, 0, dataBackFile.length())
    val memMapParams = new UcpMemMapParams()
      .setAddress(dataAddress)
      .setLength(dataBackFile.length())
    if (ucxShuffleManager.ucxShuffleConf.useOdp) {
      memMapParams.nonBlocking()
    }
    val dataMemory =
      ucxShuffleManager.ucxNode.getContext.memoryMap(memMapParams)
    fileMappings(shuffleId).add(dataMemory)
    assume(
      indexBackFile.length() == UnsafeUtils.LONG_SIZE * (lengths.length + 1)
    )

    val offsetAddress =
      UnsafeUtils.mmap(indexFileChannel, 0, indexBackFile.length())
    memMapParams.setAddress(offsetAddress).setLength(indexBackFile.length())
    val offsetMemory =
      ucxShuffleManager.ucxNode.getContext.memoryMap(memMapParams)
    offsetMappings(shuffleId).add(offsetMemory)

    dataFileChannel.close()
    dataBackFile.close()
    indexFileChannel.close()
    indexBackFile.close()

    val fileMemoryRkey = dataMemory.getRemoteKeyBuffer
    val offsetRkey = offsetMemory.getRemoteKeyBuffer

    val metadataRegisteredMemory =
      memPool.get(fileMemoryRkey.capacity() + offsetRkey.capacity() + 24)
    val metadataBuffer = metadataRegisteredMemory.getBuffer.slice()

    if (
      metadataBuffer
        .remaining() > ucxShuffleManager.ucxShuffleConf.metadataBlockSize
    ) {
      throw new SparkException(
        s"Metadata block size ${metadataBuffer.remaining() / 2} " +
          s"is greater then configured ${ucxShuffleManager.ucxShuffleConf.RKEY_SIZE.key}" +
          s"(${ucxShuffleManager.ucxShuffleConf.metadataBlockSize})."
      )
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
    val request = driverEndpoint.putNonBlocking(
      UcxUtils.getAddress(metadataBuffer),
      metadataBuffer.remaining(),
      driverOffset,
      driverMetadata.driverRkey,
      null
    )

    workerWrapper.preconnect()
    // Blocking progress needed to make sure last mapper published data to driver before
    // reducer starts.
    workerWrapper.waitRequest(request)
    memPool.put(metadataRegisteredMemory)
    logInfo(
      s"MapID: $mapId register files + publishing overhead: " +
        s"${System.currentTimeMillis() - startTime} ms"
    )
  }

  private def unregisterAndUnmap(mem: UcpMemory): Unit = {
    val address = mem.getAddress
    val length = mem.getLength
    mem.deregister()
    UnsafeUtils.munmap(address, length)
  }

  def removeShuffle(shuffleId: Int): Unit = {
    fileMappings
      .remove(shuffleId)
      .foreach((mappings: CopyOnWriteArrayList[UcpMemory]) =>
        mappings.asScala.par.foreach(unregisterAndUnmap)
      )
    offsetMappings
      .remove(shuffleId)
      .foreach((mappings: CopyOnWriteArrayList[UcpMemory]) =>
        mappings.asScala.par.foreach(unregisterAndUnmap)
      )
  }

  override def stop(): Unit = {
    fileMappings.keys.foreach(removeShuffle)
  }
}
