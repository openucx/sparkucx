/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import java.io.ObjectInputStream
import java.nio.ByteBuffer

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import org.openucx.jucx.UcxUtils
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.UcxShuffleTransport
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.FetchBlocksByBlockIdsRequest
import org.apache.spark.util.Utils

class GlobalWorkerRpcThread(globalWorker: UcpWorker, transport: UcxShuffleTransport)
  extends Thread with Logging {

  setDaemon(true)
  setName("Ucx Shuffle Transport Progress Thread")

  private def handleFetchBlockRequest(buffer: ByteBuffer, ep: UcpEndpoint): Unit = {
    val msg = Utils.tryWithResource(new ByteBufferBackedInputStream(buffer)) { bin =>
      val objIn = new ObjectInputStream(bin)
      val obj = objIn.readObject().asInstanceOf[FetchBlocksByBlockIdsRequest]
      objIn.close()
      obj
    }
    val startTime = System.nanoTime()
    transport.replyFetchBlocksRequest(msg.blockIds, msg.isHostMemory, ep, msg.startTag, msg.singleReply)
    logInfo(s"Sent reply for ${msg.blockIds.length} blocks in ${Utils.getUsedTimeNs(startTime)}")
  }

  override def run(): Unit = {
    val processCallback: UcpAmRecvCallback = (headerAddress: Long, headerSize: Long, amData: UcpAmData,
                                              replyEp: UcpEndpoint) => {
      if (headerSize > 0) {
        logDebug(s"Received AM in header on ${transport.clientConnections.get(replyEp)}")
        val header = UcxUtils.getByteBufferView(headerAddress, headerSize)
        handleFetchBlockRequest(header, replyEp)
        UcsConstants.STATUS.UCS_OK
      } else {
        assert(amData.isDataValid)
        logDebug(s"Received AM in eager on ${transport.clientConnections.get(replyEp)}")
        val data = UcxUtils.getByteBufferView(amData.getDataAddress, amData.getLength)
        handleFetchBlockRequest(data, replyEp)
        UcsConstants.STATUS.UCS_OK
      }
    }
    globalWorker.setAmRecvHandler(0, processCallback)
    globalWorker.setAmRecvHandler(-1, new UcpAmRecvCallback() {
      override def onReceive(headerAddress: Long, headerSize: Long, amData: UcpAmData, replyEp: UcpEndpoint): Int = {
        logTrace(s"Hello")
        UcsConstants.STATUS.UCS_OK
      }
    })
    while (!isInterrupted) {
      if (globalWorker.progress() == 0) {
        if (transport.ucxShuffleConf.useWakeup) {
          globalWorker.waitForEvents()
        }
      }
    }

  }
}
