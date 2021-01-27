/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import java.io.ObjectInputStream
import java.nio.ByteBuffer

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.{UcxCallback, UcxUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.UcxShuffleTransport
import org.apache.spark.shuffle.ucx.memory.MemoryPool
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{FetchBlockByBlockIdRequest, FetchBlocksByBlockIdsRequest}
import org.apache.spark.util.Utils

class GlobalWorkerRpcThread(globalWorker: UcpWorker, memPool: MemoryPool,
                            transport: UcxShuffleTransport) extends Thread with Logging {

  setDaemon(true)
  setName("Ucx Shuffle Transport Progress Thread")

  private def handleFetchBlockRequest(buffer: ByteBuffer, ep: UcpEndpoint): Unit = {
    val fetchSingleBlock = buffer.get()
    if (fetchSingleBlock == UcxRpcMessages.FETCH_SINGLE_BLOCK_TAG.toByte) {
      val msg = Utils.tryWithResource(new ByteBufferBackedInputStream(buffer)) { bin =>
        val objIn = new ObjectInputStream(bin)
        val obj = objIn.readObject().asInstanceOf[FetchBlockByBlockIdRequest]
        objIn.close()
        obj
      }
      logInfo(s"Requested single block msg: $msg")
      transport.replyFetchBlockRequest(msg.blockId, ep, msg.msgId)
    } else {
      val msg = Utils.tryWithResource(new ByteBufferBackedInputStream(buffer)) { bin =>
        val objIn = new ObjectInputStream(bin)
        val obj = objIn.readObject().asInstanceOf[FetchBlocksByBlockIdsRequest]
        objIn.close()
        obj
      }
      logInfo(s"Requested blocks msg: ${msg.blockIds.mkString(",")}")
      for (i <- msg.blockIds.indices) {
        transport.replyFetchBlockRequest(msg.blockIds(i), ep, msg.startTag + i)
      }
    }

  }

  override def run(): Unit = {
    val processCallback: UcpAmRecvCallback = (headerAddress: Long, headerSize: Long, amData: UcpAmData,
                                              replyEp: UcpEndpoint) => {
      if (headerSize > 0) {
        logInfo(s"Received AM in header")
        val header = UcxUtils.getByteBufferView(headerAddress, headerSize.toInt)
        handleFetchBlockRequest(header, replyEp)
        UcsConstants.STATUS.UCS_OK
      } else {
        if (amData.isDataValid) {
          logInfo(s"Received AM in eager")
          val data = UcxUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
          handleFetchBlockRequest(data, replyEp)
          UcsConstants.STATUS.UCS_OK
        } else {
          val recvData = memPool.get(amData.getLength)
          amData.receive(recvData.address, new UcxCallback {
            override def onSuccess(request: UcpRequest): Unit = {
              logInfo(s"Received AM in rndv")
              val data = UcxUtils.getByteBufferView(recvData.address,
                request.getRecvSize.toInt)
              handleFetchBlockRequest(data, replyEp)
              memPool.put(recvData)
            }
          })
        }
        UcsConstants.STATUS.UCS_OK
      }
    }
    globalWorker.setAmRecvHandler(0, processCallback)
    while (!isInterrupted) {
      if (globalWorker.progress() == 0) {
        if (transport.ucxShuffleConf.useWakeup) {
          globalWorker.waitForEvents()
        }
      }
    }

  }
}
