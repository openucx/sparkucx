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
trait Cookie {
  // Write this cookie to some address in memory
  def writeToMemory(memory: MemoryBlock)

  // Size of this cookie in bytes
  def size: Int

  // Reference
  def getBlockId: BlockId
}

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
trait OperationStats

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
 * val cookies = transport.register(dataBlockIds, blocks)
 * val metadataBlockIds = dataBlockIds.map(dataBlockId => new MetadataBlockId(dataBlockId))
 * cookies.foreach(cookie => cookie.writeToMemory(metadataBlocks))
 * transport.register(metadataBlockIds, metadataBlocks) // we don't care for metadata cookies
 *
 *
 * Reducer:
 * 1. First need to fetch metadata for blockIds:
 * transport.fetchBlocksByBlockIds(remoteExecutor, metadataBlockIds, resultBuffer, callback)
 * 2. Deserialize cookies from result buffer:
 * val cookies = blockIds.map(_ => transport.getCookieFromMemory(resultBuffer))
 * 3. Fetch blocks by cookies:
 * transport.fetchBlocksByCookies(remoteExecutor, cookies, resultBuffer, callback)
 * 4. Progress communications:
 * while(noMoreBlocks) { transport.progress() }
 *
 *
 * transport.unregister(blockIds)
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
  def register(blockIds: Seq[BlockId], blocks: Seq[Block]): Seq[Cookie]

  /**
   * Change location of underlying blockId in memory
   */
  def mutate(blockId: BlockId, block: Block, callback: OperationCallback)

  /**
   * Indicate that this blockId is not needed any more by an application
   */
  def unregister(blockIds: Seq[BlockId])

  /**
   * Fetch remote blocks by blockIds.
   */
  def fetchBlocksByBlockIds(executorId: String, blockIds: Seq[BlockId],
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
