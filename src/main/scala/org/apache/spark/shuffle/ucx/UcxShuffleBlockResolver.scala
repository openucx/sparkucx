/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.File
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.openucx.jucx.UcxUtils
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.unsafe.Platform


case class UcxShuffleBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends BlockId {

  def this(shuffleBlockId: ShuffleBlockId) = {
    this(shuffleBlockId.shuffleId, shuffleBlockId.mapId, shuffleBlockId.reduceId)
  }

  def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
}

case class BufferBackedBlock(buffer: ByteBuffer) extends Block {
  override def getMemoryBlock: MemoryBlock = MemoryBlock(UcxUtils.getAddress(buffer), buffer.capacity())
}

class UcxShuffleBlockResolver(conf: UcxShuffleConf, transport: UcxShuffleTransport)
  extends IndexShuffleBlockResolver(conf) {

  type MapId = Long

  private val numPartitionsForMapId = new ConcurrentHashMap[MapId, Int]

  override def writeIndexFileAndCommit(shuffleId: ShuffleId, mapId: Long,
                                       lengths: Array[Long], dataTmp: File): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    val dataFile = getDataFile(shuffleId, mapId)
    if (!dataFile.exists()) {
      return
    }
    numPartitionsForMapId.put(mapId, lengths.length)
    val fileChannel = FileChannel.open(dataFile.toPath, StandardOpenOption.READ,
      StandardOpenOption.WRITE)
    val mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0L, dataFile.length())

    val baseAddress = UcxUtils.getAddress(mappedBuffer)
    fileChannel.close()

    // Register whole map output file as dummy block
    transport.register(UcxShuffleBlockId(shuffleId, mapId, BlocksConstants.MAP_FILE),
      BufferBackedBlock(mappedBuffer))

    val offsetSize = 8 * (lengths.length  + 1)
    val indexBuf = Platform.allocateDirectBuffer(offsetSize)

    var offset = 0L
    indexBuf.putLong(offset)
    for (reduceId <- lengths.indices) {
      if (lengths(reduceId) > 0) {
        transport.register(UcxShuffleBlockId(shuffleId, mapId, reduceId), new Block {
          private val memoryBlock = MemoryBlock(baseAddress + offset, lengths(reduceId))
          override def getMemoryBlock: MemoryBlock = memoryBlock
        })
        offset += lengths(reduceId)
        indexBuf.putLong(offset)
      }
    }

    if (transport.ucxShuffleConf.protocol == transport.ucxShuffleConf.PROTOCOL.ONE_SIDED) {
      transport.register(UcxShuffleBlockId(shuffleId, mapId, BlocksConstants.INDEX_FILE), BufferBackedBlock(indexBuf))
    }
  }

  override def removeDataByMap(shuffleId: ShuffleId, mapId: Long): Unit = {
    transport.unregister(UcxShuffleBlockId(shuffleId, mapId, BlocksConstants.MAP_FILE))
    transport.unregister(UcxShuffleBlockId(shuffleId, mapId, BlocksConstants.INDEX_FILE))

    val numRegisteredBlocks = numPartitionsForMapId.get(mapId)
    (0 until numRegisteredBlocks)
      .foreach(reduceId => transport.unregister(UcxShuffleBlockId(shuffleId, mapId, reduceId)))
    super.removeDataByMap(shuffleId, mapId)
  }

  override def stop(): Unit = {
    numPartitionsForMapId.keys.asScala.foreach(mapId => removeDataByMap(0, mapId))
    super.stop()
  }

}

object BlocksConstants {
  val MAP_FILE: Int = -1
  val INDEX_FILE: Int = -2
}
