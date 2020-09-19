/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.memory

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedDeque}

import org.openucx.jucx.ucp.{UcpContext, UcpMemMapParams, UcpMemory}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.{MemoryBlock, UcxShuffleConf}

/**
 * Pre-registered host bounce buffers.
 * TODO: support reclamation
 */
class UcxHostBounceBufferMemoryBlock(private[ucx] val memory: UcpMemory, private[ucx] val refCount: AtomicInteger,
                                     override val address: Long, override val size: Long)
  extends MemoryBlock(address, size)

class UcxHostBounceBuffersPool(ucxShuffleConf: UcxShuffleConf, ucxContext: UcpContext)
  extends MemoryPool with Logging {

  private val allocatorMap = new ConcurrentHashMap[Long, AllocatorStack]()

  ucxShuffleConf.preallocateBuffersMap.foreach {
    case (size, numBuffers) =>
      val roundedSize = roundUpToTheNextPowerOf2(size)
      logDebug(s"Pre allocating $numBuffers buffers of size $roundedSize")
      val allocatorStack = allocatorMap.computeIfAbsent(roundedSize, s => AllocatorStack(s))
      allocatorStack.preallocate(numBuffers)
  }

  private case class AllocatorStack(length: Long) extends Closeable {
    private val stack = new ConcurrentLinkedDeque[UcxHostBounceBufferMemoryBlock]
    private val numAllocs = new AtomicInteger(0)

    private[UcxHostBounceBuffersPool] def get: UcxHostBounceBufferMemoryBlock = {
      var result = stack.pollFirst()
      if (result == null) {
        numAllocs.incrementAndGet()
        if (length < ucxShuffleConf.minRegistrationSize) {
          preallocate((ucxShuffleConf.minRegistrationSize / length).toInt)
          result = stack.pollFirst()
        } else {
          val memory = ucxContext.memoryMap(new UcpMemMapParams().allocate().setLength(length))
          result = new UcxHostBounceBufferMemoryBlock(memory, new AtomicInteger(1),
            memory.getAddress, length)
        }
      }
      result
    }

    private[UcxHostBounceBuffersPool] def put(block: UcxHostBounceBufferMemoryBlock): Unit = {
      stack.add(block)
    }

    private[ucx] def preallocate(numBuffers: Int): Unit = {
      val memory = ucxContext.memoryMap(
        new UcpMemMapParams().allocate().setLength(length * numBuffers))
      val refCount = new AtomicInteger(numBuffers)
      var offset = 0L
      (0 until numBuffers).foreach(_ => {
        stack.add(new UcxHostBounceBufferMemoryBlock(memory, refCount, memory.getAddress + offset, length))
        offset += length
      })
    }

    override def close(): Unit = {
      var numBuffers = 0
      stack.forEach(block => {
        block.refCount.decrementAndGet()
        if (block.memory.getNativeId != null) {
          block.memory.deregister()
        }
        numBuffers += 1
      })
      logInfo(s"Closing $numBuffers buffers of size $length." +
        s"Number of allocations: ${numAllocs.get()}")
      stack.clear()
    }
  }

  private def roundUpToTheNextPowerOf2(size: Long): Long = {
    // Round up length to the nearest power of two
    var length = size
    length -= 1
    length |= length >> 1
    length |= length >> 2
    length |= length >> 4
    length |= length >> 8
    length |= length >> 16
    length += 1
    length
  }

  override def get(size: Long): MemoryBlock = {
    val roundedSize = roundUpToTheNextPowerOf2(size)
    val allocatorStack = allocatorMap.computeIfAbsent(roundedSize, s => AllocatorStack(s))
    allocatorStack.get
  }

  override def put(mem: MemoryBlock): Unit = {
    val allocatorStack = allocatorMap.computeIfAbsent(roundUpToTheNextPowerOf2(mem.size),
      s => AllocatorStack(s))
    allocatorStack.put(mem.asInstanceOf[UcxHostBounceBufferMemoryBlock])
  }

  override def close(): Unit = {
    allocatorMap.values.forEach(allocator => allocator.close())
    allocatorMap.clear()
  }

}
