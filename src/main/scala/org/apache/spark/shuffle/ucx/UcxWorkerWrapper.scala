/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.{Closeable, ObjectOutputStream}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import ai.rapids.cudf.DeviceMemoryBuffer
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.MemoryPool
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.FetchBlocksByBlockIdsRequest
import org.apache.spark.util.{ByteBufferOutputStream, Utils}

/**
 * Success operation result subclass that has operation stats.
 */
class UcxSuccessOperationResult(stats: OperationStats) extends OperationResult {

  override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

  override def getError: TransportError = null

  override def getStats: Option[OperationStats] = Some(stats)
}

class UcxFailureOperationResult(errorMsg: String) extends OperationResult {
  override def getStatus: OperationStatus.Value = OperationStatus.FAILURE

  override def getError: TransportError = new TransportError(errorMsg)

  override def getStats: Option[OperationStats] = None
}

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
class UcxWorkerWrapper(worker: UcpWorker, transport: UcxShuffleTransport, ucxConf: UcxShuffleConf,
                       memoryPool: MemoryPool)
  extends Closeable with Logging {

  private var tag: Int = 1
  private final val connections = mutable.Map.empty[String, UcpEndpoint]
  private val requestData = new ConcurrentHashMap[Int, (MemoryBlock, UcxCallback, UcxRequest)]

  worker.setAmRecvHandler(0, (headerAddress: Long, headerSize: Long, amData: UcpAmData,
                                     replyEp: UcpEndpoint) => {
    val headerBuffer = UcxUtils.getByteBufferView(headerAddress, headerSize)
    val i = headerBuffer.getInt
    val data = requestData.remove(i)
    if (data == null) {
      throw new UcxException(s"No data for tag $i")
    }
    val (resultMemory, callback, ucxRequest) = data
    logDebug(s"Received message for tag $i with headerSize $headerSize. " +
      s" AmData: ${amData} resultMemory(isHost): ${resultMemory.isHostMemory}")
    ucxRequest.getStats.foreach(s => s.asInstanceOf[UcxStats].amHandleTime = System.nanoTime())
    if (headerSize > 4L) {
      val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address, resultMemory.size)
      resultBuffer.put(headerBuffer)
      ucxRequest.completed = true
      if (callback != null) {
        ucxRequest.getStats.get.asInstanceOf[UcxStats].receiveSize = headerSize - 4
        callback.onSuccess(null)
      }
    } else if (amData.isDataValid && resultMemory.isHostMemory) {
      val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address, resultMemory.size)
      resultBuffer.put(UcxUtils.getByteBufferView(amData.getDataAddress, amData.getLength))
      ucxRequest.completed = true
      if (callback != null) {
        ucxRequest.getStats.get.asInstanceOf[UcxStats].receiveSize = amData.getLength
        callback.onSuccess(null)
      }
    } else {
      require(amData.getLength <= resultMemory.size, s"${amData.getLength} < ${resultMemory.size}")
      val request = worker.recvAmDataNonBlocking(amData.getDataHandle, resultMemory.address,
        amData.getLength, callback,
        if (resultMemory.isHostMemory) UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST else
          UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA)
      ucxRequest.getStats.get.asInstanceOf[UcxStats].receiveSize = amData.getLength
      ucxRequest.setRequest(request)
    }
    UcsConstants.STATUS.UCS_OK
  })


  override def close(): Unit = {
    connections.foreach {
      case (_, endpoint) => endpoint.close()
    }
    connections.clear()
    worker.close()
  }

  /**
   * The only place for worker progress
   */
  private[ucx] def progress(): Unit = this.synchronized {
    if ((worker.progress() == 0) && ucxConf.useWakeup) {
      worker.waitForEvents()
      worker.progress()
    }
  }

  private[ucx] def getConnection(executorId: String): UcpEndpoint = synchronized {
    val workerAddresses = if (ucxConf.useSockAddr) {
      transport.executorIdToSockAddress
    } else {
      transport.executorIdToAddress
    }

    if (!workerAddresses.contains(executorId)) {
      // Block until there's no worker address for this BlockManagerID
      val startTime = System.currentTimeMillis()
      val timeout = ucxConf.conf.getTimeAsMs("spark.network.timeout", "100")
      workerAddresses.synchronized {
        while (workerAddresses.get(executorId) == null) {
          logWarning(s"No workerAddress for executor $executorId")
          workerAddresses.wait(timeout)
          if (System.currentTimeMillis() - startTime > timeout) {
            throw new UcxException(s"Didn't get worker address for $executorId during $timeout")
          }
        }
      }
    }

    connections.getOrElseUpdate(executorId, {
      logInfo(s"Worker from thread ${Thread.currentThread().getName} connecting to $executorId")
      val endpointParams = new UcpEndpointParams().setPeerErrorHandlingMode()
        .setErrorHandler(new UcpEndpointErrorHandler() {
          override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
            logError(errorMsg)
          }
        })
      if (ucxConf.useSockAddr) {
        val sockAddr = workerAddresses.get(executorId).asInstanceOf[InetSocketAddress]
        endpointParams.setSocketAddress(sockAddr)
      } else {
        endpointParams.setUcpAddress(workerAddresses.get(executorId).asInstanceOf[ByteBuffer])
      }

      worker.newEndpoint(endpointParams)
    })
  }


  def preConnect(ep: UcpEndpoint): Unit = this.synchronized {
    val hostBuffer = memoryPool.get(4)
    val gpuBuffer = DeviceMemoryBuffer.allocate(1)
    val req = ep.sendAmNonBlocking(-1, hostBuffer.address, hostBuffer.size,
        gpuBuffer.getAddress, gpuBuffer.getLength, UcpConstants.UCP_AM_SEND_FLAG_REPLY, null)
    while (!req.isCompleted) {
      progress()
    }
    gpuBuffer.close()
    memoryPool.put(hostBuffer)
  }

  private[ucx] def fetchBlocksByBlockIds(executorId: String, blockIds: Seq[BlockId],
                                         resultBuffers: Seq[MemoryBlock],
                                         callbacks: Seq[OperationCallback]): Seq[Request] = this.synchronized {

    val startTime = System.nanoTime()
    val ep = getConnection(executorId)

    val message = FetchBlocksByBlockIdsRequest(tag, blockIds, resultBuffers.map(_.isHostMemory).toArray)

    val bos = new ByteBufferOutputStream(1000)
    val out = new ObjectOutputStream(bos)
    try {
      out.writeObject(message)
      out.flush()
      out.close()
    } catch {
        case ex: Exception => throw new UcxException(ex.getMessage)
    }

    val msgSize = bos.getCount()
    val mem = memoryPool.get(msgSize)
    val buffer = UcxUtils.getByteBufferView(mem.address, msgSize)

    buffer.put(bos.toByteBuffer)

    val requests = new Array[UcxRequest](blockIds.size)
    for (i <- blockIds.indices) {
      val stats = new UcxStats()
      val result = new UcxSuccessOperationResult(stats)
      requests(i) = new UcxRequest(null, stats, worker)
      val callback = if (callbacks.isEmpty) null else new UcxCallback() {

        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          logError(s"Failed to receive blockId ${blockIds(i)} on tag: $tag, from executorId: $executorId " +
            s" of size: ${resultBuffers(i).size}: $errorMsg")
          if (callbacks(i) != null) {
            callbacks(i).onComplete(new UcxFailureOperationResult(errorMsg))
          }
        }

        override def onSuccess(request: UcpRequest): Unit = {
          stats.endTime = System.nanoTime()
          logInfo(s"Received block ${blockIds(i)} from $executorId " +
            s"of size: ${stats.receiveSize} in ${Utils.getUsedTimeNs(stats.startTime)}. Time from amHanlde to recv: " +
            s"${Utils.getUsedTimeNs(stats.amHandleTime)}")
          if (callbacks(i) != null) {
            callbacks(i).onComplete(result)
          }
        }
      }
      requestData.put(tag + i, (resultBuffers(i), callback, requests(i)))
    }

    logDebug(s"Sending message to $executorId to fetch ${blockIds.length} blocks on tag $tag")
    var headerAddress = 0L
    var headerSize = 0L
    var dataAddress = 0L
    var dataSize = 0L

    if (msgSize <= worker.getMaxAmHeaderSize) {
      headerAddress = mem.address
      headerSize = msgSize
    } else {
      dataAddress = mem.address
      dataSize = msgSize
    }
    worker.progressRequest(ep.sendAmNonBlocking(0, headerAddress, headerSize, dataAddress, dataSize,
      UcpConstants.UCP_AM_SEND_FLAG_REPLY, new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          logInfo(s"Sending RPC message of size: ${headerSize + dataSize} " +
            s"to $executorId to fetch ${blockIds.length} blocks on starting tag tag $tag took: " +
            s"${Utils.getUsedTimeNs(startTime)}")
          memoryPool.put(mem)
        }
      }))

    for (i <- blockIds.indices) {
      while (requestData.contains(tag + i)) {
        progress()
      }
    }

    logInfo(s"FetchBlocksByBlockIds data took: ${Utils.getUsedTimeNs(startTime)}")
    tag += blockIds.length
    requests
  }

  private[ucx] def fetchBlocksByBlockIds(executorId: String, blockIds: Seq[BlockId],
                                         resultBuffer: MemoryBlock,
                                         callback: OperationCallback): Request = this.synchronized {
    val stats = new UcxStats()
    val ep = getConnection(executorId)


    val message = FetchBlocksByBlockIdsRequest(tag, blockIds, Seq.empty[Boolean], singleReply = true)

    val bos = new ByteBufferOutputStream(1000)
    val out = new ObjectOutputStream(bos)
    try {
      out.writeObject(message)
      out.flush()
      out.close()
    } catch {
      case ex: Exception => throw new UcxException(ex.getMessage)
    }

    val msgSize = bos.getCount()

    val mem = memoryPool.get(msgSize)
    val buffer = UcxUtils.getByteBufferView(mem.address, msgSize)

    buffer.put(bos.toByteBuffer)

    val request = new UcxRequest(null, stats, worker)
    val result = new UcxSuccessOperationResult(stats)
    val requestCallback = new UcxCallback() {

      override def onError(ucsStatus: Int, errorMsg: String): Unit = {
        logError(s"Failed to receive blockId ${blockIds.mkString(",")} on tag: $tag, from executorId: $executorId " +
          s" of size: ${resultBuffer.size}: $errorMsg")
        if (callback != null) {
          callback.onComplete(new UcxFailureOperationResult(errorMsg))
        }
      }

      override def onSuccess(request: UcpRequest): Unit = {
        stats.endTime = System.nanoTime()
        logInfo(s"Received ${blockIds.length} metadata blocks from $executorId " +
          s"of size: ${stats.receiveSize} in ${Utils.getUsedTimeNs(stats.startTime)}")
        if (callback != null) {
          callback.onComplete(result)
        }
      }

    }
    requestData.put(tag, (resultBuffer, requestCallback, request))

    logDebug(s"Sending message to $executorId to fetch ${blockIds.length} blocks on tag $tag")
    var headerAddress = 0L
    var headerSize = 0L
    var dataAddress = 0L
    var dataSize = 0L

    if (msgSize <= worker.getMaxAmHeaderSize) {
      headerAddress = mem.address
      headerSize = msgSize
    } else {
      dataAddress = mem.address
      dataSize = msgSize
    }
    worker.progressRequest(ep.sendAmNonBlocking(0, headerAddress, headerSize, dataAddress, dataSize,
      UcpConstants.UCP_AM_SEND_FLAG_REPLY, new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          memoryPool.put(mem)
        }
      }))

    while (requestData.contains(tag)) {
      progress()
    }

    logInfo(s"FetchBlocksByBlockIds metadata took: ${Utils.getUsedTimeNs(stats.startTime)}")
    tag += 1
    request
  }

}
