/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.{Closeable, ObjectOutputStream}
import java.util.concurrent.ThreadLocalRandom

import scala.collection.mutable

import com.fasterxml.jackson.databind.util.ByteBufferBackedOutputStream
import org.openucx.jucx.ucp.{UcpEndpoint, UcpEndpointParams, UcpRequest, UcpWorker}
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.MemoryPool
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{FetchBlockByBlockIdRequest, FetchBlocksByBlockIdsRequest, PrefetchBlockIds}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer
import org.apache.spark.util.Utils

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

  // To keep connection map on a remote side by id rather then by worker address, which could be big.
  // Would not need when migrate to active messages.
  private val id: String = transport.executorId + s"_${Thread.currentThread().getId}"
  private final val connections = mutable.Map.empty[String, UcpEndpoint]
  private val workerAddress = worker.getAddress

  override def close(): Unit = {
    connections.foreach{
      case (_, endpoint) => endpoint.close()
    }
    connections.clear()
    worker.close()
  }

  /**
   * The only place for worker progress
   */
  private[ucx] def progress(): Unit = {
    if ((worker.progress() == 0) && ucxConf.useWakeup) {
      worker.waitForEvents()
      worker.progress()
    }
  }

  private def getConnection(executorId: String): UcpEndpoint = {
    val workerAdresses = transport.executorIdToAddress

    if (!workerAdresses.contains(executorId)) {
      // Block until there's no worker address for this BlockManagerID
      val startTime = System.currentTimeMillis()
      val timeout = ucxConf.conf.getTimeAsMs("spark.network.timeout", "100")
      workerAdresses.synchronized {
        while (workerAdresses.get(executorId) == null) {
          workerAdresses.wait(timeout)
          if (System.currentTimeMillis() - startTime > timeout) {
            throw new UcxException(s"Didn't get worker address for $executorId during $timeout")
          }
        }
      }
    }

    connections.getOrElseUpdate(executorId, {
      logInfo(s"Worker from thread ${Thread.currentThread().getName} connecting to $executorId")
      val endpointParams = new UcpEndpointParams()
        .setUcpAddress(workerAdresses.get(executorId))
     worker.newEndpoint(endpointParams)
    })
  }

  private[ucx] def prefetchBlocks(executorId: String, blockIds: Seq[BlockId]): Unit = {
    logInfo(s"Sending prefetch ${blockIds.length} blocks to $executorId")

    val mem = memoryPool.get(transport.ucxShuffleConf.rpcMessageSize)
    val buffer = UcxUtils.getByteBufferView(mem.address, transport.ucxShuffleConf.rpcMessageSize.toInt)

    workerAddress.rewind()
    val message = PrefetchBlockIds(id, new SerializableDirectBuffer(workerAddress), blockIds)

    Utils.tryWithResource(new ByteBufferBackedOutputStream(buffer)) { bos =>
      val out = new ObjectOutputStream(bos)
      out.writeObject(message)
      out.flush()
      out.close()
    }

    val ep = getConnection(executorId)

    ep.sendTaggedNonBlocking(mem.address, transport.ucxShuffleConf.rpcMessageSize,
      UcxRpcMessages.PREFETCH_TAG, new UcxCallback() {
      override def onSuccess(request: UcpRequest): Unit = {
        logTrace(s"Sent prefetch ${blockIds.length} blocks to $executorId")
        memoryPool.put(mem)
      }
    })
  }

  private[ucx] def fetchBlocksByBlockIds(executorId: String, blockIds: Seq[BlockId],
                            resultBuffer: Seq[MemoryBlock],
                            callbacks: Seq[OperationCallback]): Seq[Request] = {
    val ep = getConnection(executorId)
    val mem = memoryPool.get(transport.ucxShuffleConf.rpcMessageSize)
    val buffer = UcxUtils.getByteBufferView(mem.address,
      transport.ucxShuffleConf.rpcMessageSize.toInt)

    workerAddress.rewind()
    val message = FetchBlocksByBlockIdsRequest(id, new SerializableDirectBuffer(workerAddress),
      blockIds)

    Utils.tryWithResource(new ByteBufferBackedOutputStream(buffer)) { bos =>
      val out = new ObjectOutputStream(bos)
      out.writeObject(message)
      out.flush()
      out.close()
    }

    val tag = ThreadLocalRandom.current().nextLong(Long.MinValue, 0)
    logInfo(s"Sending message to $executorId to fetch ${blockIds.length} blocks on tag $tag")
    ep.sendTaggedNonBlocking(mem.address, transport.ucxShuffleConf.rpcMessageSize, tag,
      new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          memoryPool.put(mem)
        }
      })

    val requests = new Array[UcxRequest](blockIds.length)
    for (i <- blockIds.indices) {
      val stats = new UcxStats()
      val result = new UcxSuccessOperationResult(stats)
      val request = worker.recvTaggedNonBlocking(resultBuffer(i).address, resultBuffer(i).size,
        tag + i, -1L, new UcxCallback () {

          override def onError(ucsStatus: Int, errorMsg: String): Unit = {
            logError(s"Failed to receive blockId ${blockIds(i)} on tag: $tag, from executorId: $executorId " +
              s" of size: ${resultBuffer.size}: $errorMsg")
            if (callbacks(i) != null ) {
              callbacks(i).onComplete(new UcxFailureOperationResult(errorMsg))
            }
          }

          override def onSuccess(request: UcpRequest): Unit = {
            stats.endTime = System.nanoTime()
            stats.receiveSize = request.getRecvSize
            if (callbacks(i) != null) {
              callbacks(i).onComplete(result)
            }
          }
        })
      requests(i) = new UcxRequest(request, stats)
    }
    requests
  }

  private[ucx] def fetchBlockByBlockId(executorId: String, blockId: BlockId,
                          resultBuffer: MemoryBlock, cb: OperationCallback): UcxRequest = {
    val stats = new UcxStats()
    val ep = getConnection(executorId)
    val mem = memoryPool.get(transport.ucxShuffleConf.rpcMessageSize)
    val buffer = UcxUtils.getByteBufferView(mem.address,
      transport.ucxShuffleConf.rpcMessageSize.toInt)

    val tag = ThreadLocalRandom.current().nextLong(2, Long.MaxValue)
    workerAddress.rewind()

    val message = FetchBlockByBlockIdRequest(id, new SerializableDirectBuffer(workerAddress), blockId)

    Utils.tryWithResource(new ByteBufferBackedOutputStream(buffer)) { bos =>
      val out = new ObjectOutputStream(bos)
      out.writeObject(message)
      out.flush()
      out.close()
    }


    logTrace(s"Sending message to $executorId to fetch $blockId on tag $tag," +
      s"resultBuffer $resultBuffer")
    ep.sendTaggedNonBlocking(mem.address, transport.ucxShuffleConf.rpcMessageSize, tag,
      new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          memoryPool.put(mem)
        }
    })

    val result = new UcxSuccessOperationResult(stats)
    val request = worker.recvTaggedNonBlocking(resultBuffer.address, resultBuffer.size,
      tag, -1L, new UcxCallback () {

      override def onError(ucsStatus: Int, errorMsg: String): Unit = {
        logError(s"Failed to receive blockId $blockId on tag: $tag, from executorId: $executorId " +
          s" of size: ${resultBuffer.size}: $errorMsg")
        if (cb != null ) {
          cb.onComplete(new UcxFailureOperationResult(errorMsg))
        }
      }

      override def onSuccess(request: UcpRequest): Unit = {
        stats.endTime = System.nanoTime()
        stats.receiveSize = request.getRecvSize
        if (cb != null) {
          cb.onComplete(result)
        }
      }
    })
    new UcxRequest(request, stats)
  }

}
