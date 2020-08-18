/*
* Copyright (c) 2020, NVIDIA CORPORATION. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.nio.ByteBuffer

/**
 * Class that represents some block in memory with it's address, size.
 *
 * @param isHostMemory host or GPU memory
 */
case class MemoryBlock(address: Long, size: Long, isHostMemory: Boolean = true)

/**
 * Opaque object to describe remote memory block (address, rkey, etc.).
 */
trait Cookie

/**
 * Base class to indicate some blockId. It should be hashable and could be constructed on both ends.
 * E.g. ShuffleBlockId(shuffleId: Int, mapId: Long, reduceId: Int)
 */
trait BlockId

trait Block {
  // Transport will call this method when it would need an actual block memory.
  def getMemoryBlock: MemoryBlock
}


object OperationStatus extends Enumeration {
  val SUCCESS, FAILURE = Value
}

/**
 * Operation statistic, like completionTime, transport used, protocol used, etc.
 */
trait OperationStats {
  def getElapsedTimeNs: Long
}

class TransportError(errorMsg: String) extends Exception(errorMsg)

trait OperationResult {
  def recvSize: Long
  def getStatus: OperationStatus.Value
  def getError: TransportError
  def getStats: OperationStats
}

/**
 * Async operation callbacks
 */
trait OperationCallback {
  def onComplete(result: OperationResult)
}

/**
 * Transport flow example:
 * val transport = new UcxShuffleTransport()
 * transport.init()
 *
 * Mapper/writer:
 * transport.register(metadataBlockId, metadataBlock) // we don't care for metadata cookies
 *
 * Reducer:
 * transport.fetchBlockByBlockId(blockId, resultBounceBuffer)
 *
 * transport.unregister(blockId)
 * transport.close()
 */
trait ShuffleTransport {

  /**
   * Initialize transport resources. This function should get called after ensuring that SparkConf
   * has the correct configurations since it will use the spark configuration to configure itself.
   * @return worker address of current process, to use in [[ addExecutor()]]
   */
  def init(): ByteBuffer

  /**
   * Close all transport resources
   */
  def close()

  /**
   * Add executor's worker address. For standalone testing purpose and for implementations that makes
   * connection establishment outside of UcxShuffleManager.
   */
  def addExecutor(executorId: String, workerAddress: ByteBuffer)

  /**
   * Registers blocks using blockId on SERVER side.
   */
  def register(blockId: BlockId, block: Block): Cookie

  /**
   * Change location of underlying blockId in memory
   */
  def mutate(blockId: BlockId, newBlock: Block, callback: OperationCallback)

  /**
   * Indicate that this blockId is not needed any more by an application
   */
  def unregister(blockId: BlockId)

  /**
   * Fetch remote blocks by blockIds.
   */
  def fetchBlockByBlockId(executorId: String, blockId: BlockId,
                          resultBuffer: MemoryBlock, cb: OperationCallback)

  /**
   * Fetch remote blocks by cookies.
   */
  def fetchBlocksByCookies(executorId: String, cookies: Seq[Cookie],
                           resultBuffer: MemoryBlock, cb: OperationCallback)

  /**
   * Progress outstanding operations. This routine is blocking. It's important to call this routine
   * within same thread that submitted requests.
   */
  def progress()
}
