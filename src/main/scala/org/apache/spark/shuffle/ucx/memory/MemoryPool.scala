/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.memory

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedDeque}

import org.openucx.jucx.ucp.{UcpContext, UcpMemMapParams, UcpMemory}
import org.openucx.jucx.ucs.UcsConstants
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.{MemoryBlock, UcxShuffleConf}
import org.apache.spark.util.Utils

class UcxBounceBufferMemoryBlock(private[ucx] val memory: UcpMemory, private[ucx] val refCount: AtomicInteger,
                                 override val address: Long, override val size: Long)
  extends MemoryBlock(address, size, memory.getMemType == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)


/**
 * Base class to implement memory pool
 */
case class MemoryPool(ucxShuffleConf: UcxShuffleConf, ucxContext: UcpContext, memoryType: Int)
  extends Closeable with Logging {

  protected def roundUpToTheNextPowerOf2(size: Long): Long = {
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

  protected val allocatorMap = new ConcurrentHashMap[Long, AllocatorStack]()

  protected case class AllocatorStack(length: Long, memType: Int) extends Closeable {
    logInfo(s"Allocator stack of memType: $memType and size $length")
    private val stack = new ConcurrentLinkedDeque[UcxBounceBufferMemoryBlock]
    private val numAllocs = new AtomicInteger(0)
    private val memMapParams = new UcpMemMapParams().allocate().setMemoryType(memType).setLength(length)

    private[memory] def get: UcxBounceBufferMemoryBlock = {
      var result = stack.pollFirst()
      if (result == null) {
        numAllocs.incrementAndGet()
        if (length < ucxShuffleConf.minRegistrationSize) {
          preallocate((ucxShuffleConf.minRegistrationSize / length).toInt)
          result = stack.pollFirst()
        } else {
          logInfo(s"Allocating buffer of size $length")
          val memory = ucxContext.memoryMap(memMapParams)
          result = new UcxBounceBufferMemoryBlock(memory, new AtomicInteger(1),
            memory.getAddress, length)
        }
      }
      result
    }

    private[memory] def put(block: UcxBounceBufferMemoryBlock): Unit = {
      stack.add(block)
    }

    private[memory] def preallocate(numBuffers: Int): Unit = {
      logInfo(s"PreAllocating $numBuffers of size $length, " +
        s"totalSize: ${Utils.bytesToString(length * numBuffers) }")
      val memory = ucxContext.memoryMap(
        new UcpMemMapParams().allocate().setMemoryType(memType).setLength(length * numBuffers))
      val refCount = new AtomicInteger(numBuffers)
      var offset = 0L
      (0 until numBuffers).foreach(_ => {
        stack.add(new UcxBounceBufferMemoryBlock(memory, refCount, memory.getAddress + offset, length))
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

  override def close(): Unit = {
    allocatorMap.values.forEach(allocator => allocator.close())
    allocatorMap.clear()
  }

  def get(size: Long): MemoryBlock = {
    val roundedSize = roundUpToTheNextPowerOf2(size)
    val allocatorStack = allocatorMap.computeIfAbsent(roundedSize,
      s => AllocatorStack(s, memoryType))
    val result = allocatorStack.get
    new UcxBounceBufferMemoryBlock(result.memory, result.refCount, result.address, size)
  }

  def put(mem: MemoryBlock): Unit = {
    mem match {
      case m: UcxBounceBufferMemoryBlock =>
        val allocatorStack = allocatorMap.get(roundUpToTheNextPowerOf2(mem.size))
        allocatorStack.put(m)
      case _ =>
    }
  }

  def preAllocate(size: Long, numBuffers: Int): Unit = {
    val roundedSize = roundUpToTheNextPowerOf2(size)
    val allocatorStack = allocatorMap.computeIfAbsent(roundedSize,
      s => AllocatorStack(s, memoryType))
    allocatorStack.preallocate(numBuffers)
  }
}

class UcxHostBounceBuffersPool(ucxShuffleConf: UcxShuffleConf, ucxContext: UcpContext)
  extends MemoryPool(ucxShuffleConf, ucxContext, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST) {
  ucxShuffleConf.preallocateBuffersMap.foreach{
    case (size, numBuffers) => preAllocate(size, numBuffers)
  }
}

class UcxGpuBounceBuffersPool(ucxShuffleConf: UcxShuffleConf, ucxContext: UcpContext)
  extends MemoryPool(ucxShuffleConf, ucxContext, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA)
