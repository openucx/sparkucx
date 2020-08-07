/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import org.openucx.jucx.UcxException
import org.openucx.jucx.ucp.{UcpRequest, UcpWorker}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.MemoryPool
import org.apache.spark.shuffle.ucx.{MemoryBlock, UcxShuffleTransport}

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
        -1L, 0L, null)
    }

    while (!isInterrupted) {
      if (globalWorker.progress() == 0) {
        globalWorker.waitForEvents()
        globalWorker.progress()
      }
      for (i <- 0 until numRecvs) {
        if (requests(i).isCompleted) {
          transport.replyFetchBlockRequest(memoryBlocks(i))
          requests(i) = globalWorker.recvTaggedNonBlocking(memoryBlocks(i).address, msgSize,
            -1L, 0L, null)
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
