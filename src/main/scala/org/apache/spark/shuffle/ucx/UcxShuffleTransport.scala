/*
* Copyright (c) 2020, NVIDIA CORPORATION. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

/**
 * Class that represents some block in memory with it's address, size.
 * @param isHostMemory - host or GPU memory
 */
case class UcxMemoryBlock(address: Long, size: Long, isHostMemory: Boolean = true)

/**
 * Opaque object to describe remote memory block (address, rkey, etc.).
 */
trait Cookie {
  // Write this cookie to some address in memory
  def writeToMemory(memory: UcxMemoryBlock)

  // Size of this cookie in bytes
  def size: Int
}

/**
 * Base class to indicate some blockId. It should be hashable and could be constructed on both ends.
 * E.g. ShuffleBlockId(shuffleId: Int, mapId: Long, reduceId: Int)
 */
trait BlockId

/**
 * Special case for metadata blocks, that always would be fetched using
 * [[ UcxShuffleTransport.fetchBlocksByBlockIds() ]]. It has a method to get an actual blockId,
 * it refers to,so when requested this MetadataBlockId, transport could know that soon blockId
 * would be fetched, and may do some optimizations (pinning, pre-fetching, etc).
 */
trait MetadataBlockId extends BlockId {
  def getBlockId: BlockId
}

trait Block {
  // Transport will call this method when it would need an actual block memory.
  def getMemoryBlock: UcxMemoryBlock

  // Called when the transport is done with the Memory, so we can unmap it for example
  // the transport is not to use this Memory anymore
  def doneWithMemory(mem: UcxMemoryBlock)
}

/**
 * Async operation callbacks
 */
trait OperationCallback {
  def onSuccess()
  def onFailure(throwable: Throwable)
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
 * transport.fetchBlocksByCookies(remoteExecutor, dataBlockIds, cookies, resultBuffer, callback)
 * 4. Progress communications:
 * while(noMoreBlocks) { transport.progress() }
 *
 *
 * transport.unregister(blockIds)
 * transport.close()
 */
trait UcxShuffleTransport {

  /**
   * Initialize transport resources
   */
  def init()

  /**
   * Close all transport resources
   */
  def close()

  /**
   * Deserialize cookie from memory
   */
  def cookieFromMemory(memoryBlock: UcxMemoryBlock): Cookie

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
   * Fetch remote blocks by blockIds
   */
  def fetchBlocksByBlockIds(remoteHost: String, blockIds: Seq[BlockId],
                            resultBuffer: UcxMemoryBlock, cb: OperationCallback)

  def fetchBlocksByBlockIds(remoteHost: String, blockIds: Seq[BlockId],
                            resultBuffers: Seq[UcxMemoryBlock], cb: OperationCallback)

  /**
   * Fetch remote blocks by cookies
   */
  def fetchBlocksByCookies(remoteHost: String, blockIds: Seq[BlockId], cookies: Seq[Cookie],
                           resultBuffer: UcxMemoryBlock, cb: OperationCallback)

  def fetchBlocksByCookies(remoteHost: String, blockIds: Seq[BlockId], cookies: Seq[Cookie],
                           resultBuffers: Seq[UcxMemoryBlock], cb: OperationCallback)

  /**
   * Progress outstanding operations. This routine is blocking. It's important to call this routine
   * within same thread that submitted requests.
   */
  def progress()
}
