/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import java.io.ObjectInputStream

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import org.openucx.jucx.ucp.{UcpRequest, UcpWorker}
import org.openucx.jucx.{UcxException, UcxUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.MemoryPool
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{FetchBlockByBlockIdRequest, FetchBlocksByBlockIdsRequest, PrefetchBlockIds}
import org.apache.spark.shuffle.ucx.{MemoryBlock, UcxShuffleTransport}
import org.apache.spark.util.Utils

class GlobalWorkerRpcThread(globalWorker: UcpWorker, memPool: MemoryPool,
                            transport: UcxShuffleTransport) extends Thread with Logging {

  setDaemon(true)
  setName("Ucx Shuffle Transport Progress Thread")

  
  override def run(): Unit = {
    val numRecvs = transport.ucxShuffleConf.recvQueueSize
    val msgSize = transport.ucxShuffleConf.rpcMessageSize
    val requests: Array[UcpRequest] = Array.ofDim[UcpRequest](numRecvs)
    val recvMemory = memPool.get(msgSize * numRecvs)
    val memoryBlocks = (0 until numRecvs).map(i =>
      MemoryBlock(recvMemory.address + i * msgSize, msgSize))

    for (i <- 0 until numRecvs) {
      requests(i) = globalWorker.recvTaggedNonBlocking(memoryBlocks(i).address, msgSize,
        UcxRpcMessages.WILDCARD_TAG, UcxRpcMessages.WILDCARD_TAG_MASK, null)
    }

    while (!isInterrupted) {
      if (globalWorker.progress() == 0) {
        globalWorker.waitForEvents()
        globalWorker.progress()
      }
      for (i <- 0 until numRecvs) {
        if (requests(i).isCompleted) {
          val buffer = UcxUtils.getByteBufferView(memoryBlocks(i).address, msgSize.toInt)
          val senderTag = requests(i).getSenderTag
          if (senderTag >= 2L) {
            // Fetch Single block
            val msg = Utils.tryWithResource(new ByteBufferBackedInputStream(buffer)) { bin =>
              val objIn = new ObjectInputStream(bin)
              val obj = objIn.readObject().asInstanceOf[FetchBlockByBlockIdRequest]
              objIn.close()
              obj
            }
            transport.replyFetchBlockRequest(msg.executorId, msg.workerAddress.value, msg.blockId, senderTag)
          } else if (senderTag < 0 ) {
            // Batch fetch request
            val msg = Utils.tryWithResource(new ByteBufferBackedInputStream(buffer)) { bin =>
              val objIn = new ObjectInputStream(bin)
              val obj = objIn.readObject().asInstanceOf[FetchBlocksByBlockIdsRequest]
              objIn.close()
              obj
            }
            for (j <- msg.blockIds.indices) {
              transport.replyFetchBlockRequest(msg.executorId, msg.workerAddress.value, msg.blockIds(j),
                senderTag + j)
            }

          } else if (senderTag == UcxRpcMessages.PREFETCH_TAG) {
            val msg = Utils.tryWithResource(new ByteBufferBackedInputStream(buffer)) { bin =>
              val objIn = new ObjectInputStream(bin)
              val obj = objIn.readObject().asInstanceOf[PrefetchBlockIds]
              objIn.close()
              obj
            }
            transport.handlePrefetchRequest(msg.executorId, msg.workerAddress.value, msg.blockIds)
          }
          requests(i) = globalWorker.recvTaggedNonBlocking(memoryBlocks(i).address, msgSize,
            UcxRpcMessages.WILDCARD_TAG, UcxRpcMessages.WILDCARD_TAG_MASK, null)
        }
      }
    }

    memPool.put(recvMemory)
    for (i <- 0 until numRecvs) {
      if (!requests(i).isCompleted) {
        try {
          globalWorker.cancelRequest(requests(i))
        } catch {
          case _: UcxException =>
        }
      }
    }
  }
}
