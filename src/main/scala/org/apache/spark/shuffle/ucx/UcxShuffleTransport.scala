/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

import org.openucx.jucx.ucp._
import org.openucx.jucx.{UcxCallback, UcxException}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.{MemoryPool, UcxHostBounceBuffersPool}
import org.apache.spark.shuffle.ucx.rpc.GlobalWorkerRpcThread


/**
 * Special type of [[ Block ]] interface backed by UcpMemory,
 * it may not be actually pinned if used with [[ UcxShuffleConf.useOdp ]] flag.
 */
case class UcxPinnedBlock(block: Block, ucpMemory: UcpMemory, prefetched: Boolean = false)
  extends Block {
  override def getMemoryBlock: MemoryBlock = block.getMemoryBlock
}

class UcxStats extends OperationStats {
  private[ucx] val startTime = System.nanoTime()
  private[ucx] var endTime: Long = 0L
  private[ucx] var receiveSize: Long = 0L

  /**
   * Time it took from operation submit to callback call.
   * This depends on [[ ShuffleTransport.progress() ]] calls,
   * and does not indicate actual data transfer time.
   */
  override def getElapsedTimeNs: Long = endTime - startTime

  /**
   * Indicates number of valid bytes in receive memory when using
   * [[ ShuffleTransport.fetchBlockByBlockId()]]
   */
  override def recvSize: Long = receiveSize
}

class UcxRequest(request: UcpRequest, stats: OperationStats) extends Request {

  override def isCompleted: Boolean = request.isCompleted

  override def cancel(): Unit = request.close()

  override def getStats: Option[OperationStats] = Some(stats)
}

/**
 * UCX implementation of [[ ShuffleTransport ]] API
 */
class UcxShuffleTransport(var ucxShuffleConf: UcxShuffleConf = null, val executorId: String)
  extends ShuffleTransport with Logging {

  // UCX entities
  private var ucxContext: UcpContext = _
  private var globalWorker: UcpWorker = _
  private val ucpWorkerParams = new UcpWorkerParams()

  // TODO: reimplement as workerPool, since spark may create/destroy threads dynamically
  private var threadLocalWorker: ThreadLocal[UcxWorkerWrapper] = _
  private val allocatedWorkers = ConcurrentHashMap.newKeySet[UcxWorkerWrapper]
  private var progressThread: Thread = _

  private val registeredBlocks = new ConcurrentHashMap[BlockId, Block]
  private val memMapParams = new UcpMemMapParams()

  // Mapping between executorId and it's address
  private[ucx] val executorIdToAddress = new ConcurrentHashMap[String, ByteBuffer]()
  private[ucx] val clientConnections = mutable.Map.empty[String, UcpEndpoint]

  // Need host ucx bounce buffer memory pool to send fetchBlockByBlockId request
  var memoryPool: MemoryPool = _

  /**
   * Initialize transport resources. This function should get called after ensuring that SparkConf
   * has the correct configurations since it will use the spark configuration to configure itself.
   */
  override def init(): ByteBuffer = {
    if (ucxShuffleConf == null) {
      ucxShuffleConf = new UcxShuffleConf(SparkEnv.get.conf)
    }

    if (ucxShuffleConf.useOdp) {
      memMapParams.nonBlocking()
    }

    val params = new UcpParams().requestTagFeature().requestWakeupFeature()
    if (ucxShuffleConf.protocol == "one-sided") {
      params.requestRmaFeature()
    }
    ucxContext = new UcpContext(params)
    globalWorker = ucxContext.newWorker(new UcpWorkerParams().requestWakeupTagRecv()
      .requestWakeupTagSend())

    val result = globalWorker.getAddress
    require(result.capacity <= ucxShuffleConf.maxWorkerAddressSize,
      s"${ucxShuffleConf.WORKER_ADDRESS_SIZE.key} < ${result.capacity}")

    memoryPool = new UcxHostBounceBuffersPool(ucxShuffleConf, ucxContext)
    progressThread = new GlobalWorkerRpcThread(globalWorker, memoryPool, this)

    threadLocalWorker = ThreadLocal.withInitial(() => {
      val localWorker = ucxContext.newWorker(ucpWorkerParams)
      val workerWrapper = new UcxWorkerWrapper(localWorker, this, ucxShuffleConf, memoryPool)
      allocatedWorkers.add(workerWrapper)
      workerWrapper
    })

    progressThread.start()
    result
  }

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    progressThread.interrupt()
    globalWorker.signal()
    try {
      progressThread.join()
    } catch {
      case _:InterruptedException =>
      case e:Throwable => logWarning(e.getLocalizedMessage)
    }

    memoryPool.close()
    clientConnections.values.foreach(ep => ep.close())
    registeredBlocks.forEachKey(1, blockId => unregister(blockId))
    allocatedWorkers.forEach(_.close())
    globalWorker.close()
    ucxContext.close()
  }

  /**
   * Add executor's worker address. For standalone testing purpose and for implementations that makes
   * connection establishment outside of UcxShuffleManager.
   */
  def addExecutor(executorId: String, workerAddress: ByteBuffer): Unit = {
    executorIdToAddress.put(executorId, workerAddress)
  }

  private[ucx] def handlePrefetchRequest(workerId: String, workerAddress: ByteBuffer,
                                         blockIds: Seq[BlockId]) {

    logDebug(s"Prefetching blocks: ${blockIds.mkString(",")}")
    clientConnections.getOrElseUpdate(workerId,
      globalWorker.newEndpoint(new UcpEndpointParams().setUcpAddress(workerAddress))
    )

    blockIds.foreach(blockId => {
      val block = registeredBlocks.get(blockId)
      if (!block.isInstanceOf[UcxPinnedBlock]) {
        registeredBlocks.put(blockId, UcxPinnedBlock(block, pinMemory(block), prefetched = true))
      }
    })
  }

  /**
   * On a sender side process request of fetchBlockByBlockId
   */
  private[ucx] def replyFetchBlockRequest(workerId: String, workerAddress: ByteBuffer,
                                          blockId: BlockId, tag: Long): Unit = {

    val ep = clientConnections.getOrElseUpdate(workerId,
      globalWorker.newEndpoint(new UcpEndpointParams().setUcpAddress(workerAddress))
    )

    val block = registeredBlocks.get(blockId)
    if (block == null) {
      throw new UcxException(s"Block $blockId not registered")
    }
    val lock = block.lock.readLock()
    lock.lock()
    val blockMemory = block.getMemoryBlock

    logDebug(s"Sending $blockId of size ${blockMemory.size} to tag: $tag")
    ep.sendTaggedNonBlocking(blockMemory.address, blockMemory.size, tag, new UcxCallback {
      override def onSuccess(request: UcpRequest): Unit = {
        if (block.isInstanceOf[UcxPinnedBlock]) {
          val pinnedBlock = block.asInstanceOf[UcxPinnedBlock]
          if (pinnedBlock.prefetched) {
            registeredBlocks.put(blockId, pinnedBlock.block)
            pinnedBlock.ucpMemory.deregister()
          }
        }
        lock.unlock()
      }

      override def onError(ucsStatus: Int, errorMsg: String): Unit = {
        logError(s"Failed to send $blockId: $errorMsg")
        lock.unlock()
      }
    })
  }

  private def pinMemory(block: Block): UcpMemory = {
    val blockMemory = block.getMemoryBlock
    ucxContext.memoryMap(
      memMapParams.setAddress(blockMemory.address).setLength(blockMemory.size))
  }

  /**
   * Registers blocks using blockId on SERVER side.
   */
  override def register(blockId: BlockId, block: Block): Unit = {
    val registeredBock: Block = if (ucxShuffleConf.pinMemory) {
      UcxPinnedBlock(block, pinMemory(block))
    } else {
      block
    }
    registeredBlocks.put(blockId, registeredBock)
  }

  /**
   * Change location of underlying blockId in memory
   */
  override def mutate(blockId: BlockId, block: Block, callback: OperationCallback): Unit = {
    Future {
      unregister(blockId)
      register(blockId, block)
    } andThen {
      case Failure(t) => if (callback != null) {
        callback.onComplete(new UcxFailureOperationResult(t.getMessage))
      }
      case Success(_) => if (callback != null) {
        callback.onComplete(new UcxSuccessOperationResult(new UcxStats))
      }
    }
  }

  /**
   * Indicate that this blockId is not needed any more by an application
   */
  override def unregister(blockId: BlockId): Unit = {
    val block = registeredBlocks.remove(blockId)
    if (block != null) {
      block.lock.writeLock().lock()
      block match {
        case b:UcxPinnedBlock => b.ucpMemory.deregister()
        case _ =>
      }
      block.lock.writeLock().unlock()
    } else {
      logTrace(s"No block registered for $blockId")
    }
  }

  /**
   * Fetch remote blocks by blockIds.
   */
  override def fetchBlockByBlockId(executorId: String, blockId: BlockId,
                                   resultBuffer: MemoryBlock,
                                   cb: OperationCallback): UcxRequest = {
    threadLocalWorker.get().fetchBlockByBlockId(executorId, blockId, resultBuffer, cb)
  }

  /**
   * Progress outstanding operations. This routine is blocking. It's important to call this routine
   * within same thread that submitted requests.
   */
  override def progress(): Unit = threadLocalWorker.get().progress()

  /**
   * Remove executor from communications.
   */
  override def removeExecutor(executorId: String): Unit = {
    executorIdToAddress.remove(executorId)
  }

  /**
   * Hint for a transport that these blocks would needed soon.
   */
  override def prefetchBlocks(executorId: String, blockIds: Seq[BlockId]): Unit = {
    threadLocalWorker.get().prefetchBlocks(executorId, blockIds)
  }

  /**
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  override def fetchBlocksByBlockIds(executorId: String, blockIds: Seq[BlockId],
                                     resultBuffer: Seq[MemoryBlock],
                                     callbacks: Seq[OperationCallback]): Seq[Request] = {
    threadLocalWorker.get().fetchBlocksByBlockIds(executorId, blockIds, resultBuffer, callbacks)
  }
}
