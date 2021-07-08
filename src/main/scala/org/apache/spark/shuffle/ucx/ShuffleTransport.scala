/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.nio.ByteBuffer
import java.util.concurrent.locks.StampedLock

/**
 * Class that represents some block in memory with it's address, size.
 *
 * @param isHostMemory host or GPU memory
 */
case class MemoryBlock(address: Long, size: Long, isHostMemory: Boolean = true)

/**
 * Base class to indicate some blockId. It should be hashable and could be constructed on both ends.
 * E.g. ShuffleBlockId(shuffleId: Int, mapId: Long, reduceId: Int)
 */
trait BlockId

private[ucx] sealed trait BlockLock {
  // Private transport lock to know when there are outstanding operations to block memory.
  private[ucx] val lock = new StampedLock().asReadWriteLock()
}

/**
 * Some block in memory, that transport registers and that would requested on a remote side.
 */
trait Block extends BlockLock {
  // Transport will call this method when it would need an actual block memory.
  def getMemoryBlock: MemoryBlock
}

object OperationStatus extends Enumeration {
  val SUCCESS, CANCELED, FAILURE = Value
}

/**
 * Operation statistic, like completionTime, transport used, protocol used, etc.
 */
trait OperationStats {
  /**
   * Time it took from operation submit to callback call.
   * This depends on [[ ShuffleTransport.progress() ]] calls,
   * and does not indicate actual data transfer time.
   */
  def getElapsedTimeNs: Long

  /**
   * Indicates number of valid bytes in receive memory when using
   * [[ ShuffleTransport.fetchBlocksByBlockIds()]]
   */
  def recvSize: Long
}

class TransportError(errorMsg: String) extends Exception(errorMsg)

trait OperationResult {
  def getStatus: OperationStatus.Value
  def getError: TransportError
  def getStats: Option[OperationStats]
}

/**
 * Request object that returns by [[ ShuffleTransport.fetchBlocksByBlockIds() ]] routine.
 */
trait Request {
  def isCompleted: Boolean
  def cancel()
  def getStats: Option[OperationStats]
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
 * transport.register(blockId, block)
 *
 * Reducer:
 * transport.fetchBlockByBlockId(blockId, resultBounceBuffer)
 * transport.progress()
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
   * Remove executor from communications.
   */
  def removeExecutor(executorId: String)

  /**
   * Registers blocks using blockId on SERVER side.
   */
  def register(blockId: BlockId, block: Block)

  /**
   * Change location of underlying blockId in memory
   */
  def mutate(blockId: BlockId, newBlock: Block, callback: OperationCallback)

  /**
   * Indicate that this blockId is not needed any more by an application.
   * Note: this is a blocking call. On return it's safe to free blocks memory.
   */
  def unregister(blockId: BlockId)

  /**
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  def fetchBlocksByBlockIds(executorId: String, blockIds: Seq[BlockId],
                            resultBuffer: Seq[MemoryBlock],
                            callbacks: Seq[OperationCallback]): Seq[Request]

  def fetchBlocksByBlockIds(executorId: String, blockIds: Seq[BlockId],
                            resultBuffer: MemoryBlock,
                            callbacks: OperationCallback): Request

  /**
   * Progress outstanding operations. This routine is blocking (though may poll for event).
   * It's required to call this routine within same thread that submitted [[ fetchBlocksByBlockIds ]].
   *
   * Return from this method guarantees that at least some operation was progressed.
   * But not guaranteed that at least one [[ fetchBlocksByBlockIds ]] completed!
   */
  def progress()
}
