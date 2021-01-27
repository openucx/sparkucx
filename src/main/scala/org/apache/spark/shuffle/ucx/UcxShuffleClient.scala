/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.util.concurrent.TimeUnit

import org.openucx.jucx.{UcxException, UcxUtils}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId, BlockId => SparkBlockId}

class UcxShuffleClient(transport: UcxShuffleTransport,
                       blocksByAddress: Iterator[(BlockManagerId, Seq[(SparkBlockId, Long, Int)])])
  extends BlockStoreClient with Logging {

  private val accurateThreshold = transport.ucxShuffleConf.conf.getSizeAsBytes(SHUFFLE_ACCURATE_BLOCK_THRESHOLD.key)

  private val blockSizes: Map[SparkBlockId, Long] = blocksByAddress
    .withFilter { case (blockManagerId, _) => blockManagerId != SparkEnv.get.blockManager.blockManagerId }
    .flatMap {
    case (blockManagerId, blocks) =>
      blocks.map {
        case (blockId, length, _) =>
          if (length > accurateThreshold) {
            (blockId, (length * 1.2).toLong)
          } else {
            (blockId, accurateThreshold * 2)
          }
      }
  }.toMap

  override def fetchBlocks(host: String, port: Int, execId: String,
                           blockIds: Array[String], listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    val ucxBlockIds = new Array[BlockId](blockIds.length)
    val memoryBlocks = new Array[MemoryBlock](blockIds.length)
    val callbacks = new Array[OperationCallback](blockIds.length)
    for (i <- blockIds.indices) {
      val blockId = SparkBlockId.apply(blockIds(i)).asInstanceOf[ShuffleBlockId]
      if (!blockSizes.contains(blockId)) {
        throw new UcxException(s"No $blockId found in MapOutput blocks: ${blockSizes.keys.mkString(",")}")
      }
      val resultMemory = transport.memoryPool.get(blockSizes(blockId))
      ucxBlockIds(i) = UcxShuffleBlockId(blockId.shuffleId, blockId.mapId, blockId.reduceId)
      memoryBlocks(i) = MemoryBlock(resultMemory.address, blockSizes(blockId))
      callbacks(i) =  (result: OperationResult) => {
        if (result.getStatus == OperationStatus.SUCCESS) {
          val stats = result.getStats.get
          logInfo(s" Received block ${ucxBlockIds(i)} " +
            s"of size: ${stats.recvSize} " +
            s"in ${TimeUnit.NANOSECONDS.toMillis(stats.getElapsedTimeNs)} ms")
          val buffer = UcxUtils.getByteBufferView(resultMemory.address, result.getStats.get.recvSize.toInt)
          listener.onBlockFetchSuccess(blockIds(i), new NioManagedBuffer(buffer) {
            override def release: ManagedBuffer = {
              transport.memoryPool.put(resultMemory)
              this
            }
          })
        } else {
          logError(s"Error fetching block $blockId of size ${blockSizes(blockId)}:" +
            s" ${result.getError.getMessage}")
          throw new UcxException(result.getError.getMessage)
        }
      }
    }
    transport.fetchBlocksByBlockIds(execId, ucxBlockIds, memoryBlocks, callbacks)
  }

  override def close(): Unit = {

  }
}
