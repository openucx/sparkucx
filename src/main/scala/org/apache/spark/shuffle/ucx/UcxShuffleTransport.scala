/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.locks.Lock

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.{UcxGpuBounceBuffersPool, UcxHostBounceBuffersPool}
import org.apache.spark.shuffle.ucx.rpc.GlobalWorkerRpcThread
import org.apache.spark.shuffle.ucx.utils.{SerializationUtils, UcxHelperUtils}
import org.apache.spark.util.Utils


/**
 * Special type of [[ Block ]] interface backed by UcpMemory.
 */
case class UcxPinnedBlock(block: Block, ucpMemory: UcpMemory, prefetched: Boolean = false)
  extends Block {
  override def getMemoryBlock: MemoryBlock = block.getMemoryBlock
}

class UcxStats extends OperationStats {
  private[ucx] val startTime = System.nanoTime()
  private[ucx] var amHandleTime = 0L
  private[ucx] var endTime: Long = 0L
  private[ucx] var receiveSize: Long = 0L

  /**
   * Time it took from operation submit to callback call.
   * This depends on [[ ShuffleTransport.progress() ]] calls,
   * and does not indicate actual data transfer time.
   */
  override def getElapsedTimeNs: Long = endTime - startTime

  /**
   * Indicates number of valid bytes in receive memory
   */
  override def recvSize: Long = receiveSize
}

class UcxRequest(private var request: UcpRequest, stats: OperationStats, private val worker: UcpWorker)
  extends Request {

  private[ucx] var completed = false

  override def isCompleted: Boolean = completed || ((request != null) && request.isCompleted)

  override def cancel(): Unit = if (request != null) worker.cancelRequest(request)

  override def getStats: Option[OperationStats] = Some(stats)

  private[ucx] def setRequest(request: UcpRequest): Unit = {
    this.request = request
  }
}

/**
 * UCX implementation of [[ ShuffleTransport ]] API
 */
class UcxShuffleTransport(var ucxShuffleConf: UcxShuffleConf = null, var executorId: String)
  extends ShuffleTransport with Logging {

  // UCX entities
  private[ucx] var ucxContext: UcpContext = _
  private var globalWorker: UcpWorker = _
  private val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()

  // TODO: reimplement as workerPool, since spark may create/destroy threads dynamically
  private var threadLocalWorker: ThreadLocal[UcxWorkerWrapper] = _
  private val allocatedWorkers = ConcurrentHashMap.newKeySet[UcxWorkerWrapper]
  private var progressThread: Thread = _

  private val registeredBlocks = new ConcurrentHashMap[BlockId, Block]
  private val memMapParams = new UcpMemMapParams()

  // Mapping between executorId and it's address
  private[ucx] val executorIdToAddress = new ConcurrentHashMap[String, ByteBuffer]()
  private[ucx] val executorIdToSockAddress = new ConcurrentHashMap[String, InetSocketAddress]()
  private[ucx] val clientConnections = mutable.HashMap.empty[UcpEndpoint, (UcpEndpoint, InetSocketAddress)]

  // Need host ucx bounce buffer memory pool to send fetchBlockByBlockId request
  var hostMemoryPool: UcxHostBounceBuffersPool = _
  var deviceMemoryPool: UcxGpuBounceBuffersPool = _

  @volatile private var initialized: Boolean = false

  private var workerAddress: ByteBuffer = _
  private var listener: Option[UcpListener] = None

  /**
   * Initialize transport resources. This function should get called after ensuring that SparkConf
   * has the correct configurations since it will use the spark configuration to configure itself.
   */
  override def init(): ByteBuffer = this.synchronized {
    if (!initialized) {
      if (ucxShuffleConf == null) {
        ucxShuffleConf = new UcxShuffleConf(SparkEnv.get.conf)
      }

      if (ucxShuffleConf.useOdp) {
        memMapParams.nonBlocking()
      }

      val params = new UcpParams().requestTagFeature().requestAmFeature()

      if (ucxShuffleConf.useWakeup) {
        params.requestWakeupFeature()
      }

      if (ucxShuffleConf.protocol == ucxShuffleConf.PROTOCOL.ONE_SIDED) {
        params.requestRmaFeature()
      }
      ucxContext = new UcpContext(params)

      val workerParams = new UcpWorkerParams()

      if (ucxShuffleConf.useWakeup) {
        workerParams.requestWakeupTagRecv().requestWakeupTagSend()
      }
      globalWorker = ucxContext.newWorker(workerParams)

      workerAddress = if (ucxShuffleConf.useSockAddr) {
        listener = Some(UcxHelperUtils.startListenerOnRandomPort(globalWorker, ucxShuffleConf.conf, clientConnections))
        val buffer = SerializationUtils.serializeInetAddress(listener.get.getAddress)
        buffer
      } else {
        val workerAddress = globalWorker.getAddress
        require(workerAddress.capacity <= ucxShuffleConf.maxWorkerAddressSize,
          s"${ucxShuffleConf.WORKER_ADDRESS_SIZE.key} < ${workerAddress.capacity}")
        workerAddress
      }

      hostMemoryPool = new UcxHostBounceBuffersPool(ucxShuffleConf, ucxContext)
      deviceMemoryPool = new UcxGpuBounceBuffersPool(ucxShuffleConf, ucxContext)
      progressThread = new GlobalWorkerRpcThread(globalWorker, this)

      val numThreads = ucxShuffleConf.getInt("spark.executor.cores", 4)
      val allocateWorker = () => {
        val localWorker = ucxContext.newWorker(ucpWorkerParams)
        val workerWrapper = new UcxWorkerWrapper(localWorker, this, ucxShuffleConf, hostMemoryPool)
        allocatedWorkers.add(workerWrapper)
        logInfo(s"Pre-connections on a new worker to ${executorIdToAddress.size()} " +
          s"executors")
        executorIdToAddress.keys.asScala.foreach(e => {
          workerWrapper.getConnection(e)
        })
        workerWrapper
      }
      val preAllocatedWorkers = (0 until numThreads).map(_ => allocateWorker()).toList.asJava
      val workersPool = new ConcurrentLinkedQueue[UcxWorkerWrapper](preAllocatedWorkers)
      threadLocalWorker = ThreadLocal.withInitial(() => {
        if (workersPool.isEmpty) {
          logWarning(s"Allocating new worker")
          allocateWorker()
        } else {
          workersPool.poll()
        }
      })

      progressThread.start()
      initialized = true
    }
    workerAddress
  }

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    if (initialized) {
      progressThread.interrupt()
      if (ucxShuffleConf.useWakeup) {
        globalWorker.signal()
      }
      try {
        progressThread.join()
        hostMemoryPool.close()
        deviceMemoryPool.close()
        clientConnections.keys.foreach(ep => ep.close())
        registeredBlocks.forEachKey(100, blockId => unregister(blockId))
        allocatedWorkers.forEach(_.close())
        listener.foreach(_.close())
        globalWorker.close()
        ucxContext.close()
      } catch {
        case _:InterruptedException =>
        case e:Throwable => logWarning(e.getLocalizedMessage)
      }
    }
  }

  /**
   * Add executor's worker address. For standalone testing purpose and for implementations that makes
   * connection establishment outside of UcxShuffleManager.
   */
  def addExecutor(executorId: String, workerAddress: ByteBuffer): Unit = {
    if (ucxShuffleConf.useSockAddr) {
      executorIdToSockAddress.put(executorId, SerializationUtils.deserializeInetAddress(workerAddress))
    } else {
      executorIdToAddress.put(executorId, workerAddress)
    }
    logInfo(s"Pre connection on ${allocatedWorkers.size()} workers")
    allocatedWorkers.forEach(w => {
      val ep = w.getConnection(executorId)
      w.preConnect(ep)
    })
  }

  /**
   * On a sender side process request of fetchBlockByBlockId
   */
  private[ucx] def replyFetchBlocksRequest(blockIds: Seq[BlockId], isRecvToHostMemory: Seq[Boolean],
                                           ep: UcpEndpoint, startTag: Int, singleReply: Boolean): Unit = {
    if (singleReply) {
      var blockAddress = 0L
      var blockSize = 0L
      var responseBlock: MemoryBlock = null
      var headerMem: MemoryBlock = null
      var responseBlockBuff: ByteBuffer = null

      val totalSize = ucxShuffleConf.maxMetadataSize * blockIds.length
      if (totalSize + 4 < globalWorker.getMaxAmHeaderSize) {
        headerMem = hostMemoryPool.get(totalSize + 4L)
        responseBlockBuff = UcxUtils.getByteBufferView(headerMem.address, headerMem.size)
        responseBlockBuff.putInt(startTag)
        logInfo(s"Sending ${blockIds.mkString(",")} in header")
      } else {
        headerMem = hostMemoryPool.get(4L)
        val headerBuf = UcxUtils.getByteBufferView(headerMem.address, headerMem.size)
        headerBuf.putInt(startTag)
        responseBlock = hostMemoryPool.get(totalSize)
        responseBlockBuff = UcxUtils.getByteBufferView(responseBlock.address, responseBlock.size)
        blockAddress = responseBlock.address
        blockSize = responseBlock.size
        logInfo(s"Sending ${blockIds.length} blocks in data")
      }
      val locks = new Array[Lock](blockIds.size)
      for (i <- blockIds.indices) {
        val block = registeredBlocks.get(blockIds(i))
        locks(i) = block.lock.readLock()
        locks(i).lock()
        val blockMemory = block.getMemoryBlock
        require(blockMemory.isHostMemory && blockMemory.size <= ucxShuffleConf.maxMetadataSize)
        val blockBuffer = UcxUtils.getByteBufferView(blockMemory.address, blockMemory.size)
        responseBlockBuff.putInt(blockMemory.size.toInt)
        responseBlockBuff.put(blockBuffer)
      }
      ep.sendAmNonBlocking(0, headerMem.address, headerMem.size, blockAddress, blockSize, 0L,
        new UcxCallback {
          private val startTime = System.nanoTime()
          override def onSuccess(request: UcpRequest): Unit = {
            logInfo(s"Sent ${blockIds.length} blocks of size $totalSize" +
              s" in ${Utils.getUsedTimeNs(startTime)}")
            locks.foreach(_.unlock())
            hostMemoryPool.put(headerMem)
            if (responseBlock != null) {
              hostMemoryPool.put(responseBlock)
            }
          }

          override def onError(ucsStatus: Int, errorMsg: String): Unit = {
            logError(s"Failed to send ${blockIds.mkString(",")}: $errorMsg")
            locks.foreach(_.unlock())
            hostMemoryPool.put(headerMem)
            if (responseBlock != null) {
              hostMemoryPool.put(responseBlock)
            }
          }
        }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

    } else {
      for (i <- blockIds.indices) {
        val blockId = blockIds(i)
        val block = registeredBlocks.get(blockId)

        if (block == null) {
          throw new UcxException(s"Block $blockId is not registered")
        }

        val lock = block.lock.readLock()
        lock.lock()

        val blockMemory = block.getMemoryBlock

        var blockAddress = 0L
        var blockSize = 0L
        var headerMem: MemoryBlock = null

        if (blockMemory.isHostMemory && // The block itself in host memory
          (blockMemory.size + 4 < globalWorker.getMaxAmHeaderSize) && // and can fit to header
          isRecvToHostMemory(i) ) {
          headerMem = hostMemoryPool.get(4L + blockMemory.size)
          val buf = UcxUtils.getByteBufferView(headerMem.address, headerMem.size)
          buf.putInt(startTag + i)
          val blockBuf = UcxUtils.getByteBufferView(blockMemory.address, blockMemory.size)
          buf.put(blockBuf)
        } else {
          headerMem = hostMemoryPool.get(4L)
          val buf = UcxUtils.getByteBufferView(headerMem.address, headerMem.size)
          buf.putInt(startTag + i)
          blockAddress = blockMemory.address
          blockSize = blockMemory.size
        }

        ep.sendAmNonBlocking(0, headerMem.address, headerMem.size, blockAddress, blockSize, 0L,
          new UcxCallback {
            private val startTime = System.nanoTime()
            override def onSuccess(request: UcpRequest): Unit = {
              logInfo(s"Sent $blockId of size ${blockMemory.size} " +
                s"memType: ${if (blockMemory.isHostMemory) "host" else "gpu"}" +
                s" in ${Utils.getUsedTimeNs(startTime)}")
              lock.unlock()
              hostMemoryPool.put(headerMem)
            }

            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              logError(s"Failed to send $blockId-$blockMemory: $errorMsg")
              lock.unlock()
              hostMemoryPool.put(headerMem)
            }
          },
          if (blockMemory.isHostMemory) UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST
          else UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA)
      }
    }

  }

  private def pinMemory(block: Block): UcpMemory = {
    val startTime = System.nanoTime()
    val blockMemory = block.getMemoryBlock
    val memType = if (blockMemory.isHostMemory) {
      UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST
    } else {
      UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA
    }
    val result = ucxContext.memoryMap(
      memMapParams.setAddress(blockMemory.address).setLength(blockMemory.size)
        .setMemoryType(memType))
    logInfo(s"Pinning memory of size: ${blockMemory.size} took: ${Utils.getUsedTimeNs(startTime)}")
    result
  }

  /**
   * Registers blocks using blockId on SERVER side.
   */
  override def register(blockId: BlockId, block: Block): Unit = {
    logTrace(s"Registering $blockId of size: ${block.getMemoryBlock.size}")
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
    unregister(blockId)
    register(blockId, block)
    callback.onComplete(new UcxSuccessOperationResult(new UcxStats))
  }

  /**
   * Indicate that this blockId is not needed any more by an application
   */
  override def unregister(blockId: BlockId): Unit = {
    logInfo(s"Unregistering $blockId")
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
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  override def fetchBlocksByBlockIds(executorId: String, blockIds: Seq[BlockId],
                                     resultBuffer: Seq[MemoryBlock],
                                     callbacks: Seq[OperationCallback]): Seq[Request] = {
    threadLocalWorker.get().fetchBlocksByBlockIds(executorId, blockIds, resultBuffer, callbacks)
  }

  override def fetchBlocksByBlockIds(executorId: String, blockIds: Seq[BlockId], resultBuffer: MemoryBlock,
                                     callback: OperationCallback): Request = {
    threadLocalWorker.get().fetchBlocksByBlockIds(executorId, blockIds, resultBuffer, callback)
  }
}
