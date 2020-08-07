/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.memory

import java.io.Closeable
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedDeque}

import org.openucx.jucx.ucp.{UcpContext, UcpMemMapParams, UcpMemory}
import org.apache.spark.shuffle.ucx.{MemoryBlock, UcxShuffleConf}

/**
 * Pre-registered host bounce buffers.
 * TODO: support pre-allocation and reclamation
 */
class UcxHostBounceBufferMemoryBlock(val memory: UcpMemory, override val address: Long,
                                     override val size: Long)
  extends MemoryBlock(address, size)

class UcxHostBounceBuffersPool(conf: UcxShuffleConf, ucxContext: UcpContext)
  extends MemoryPool {

  private case class AllocatorStack(length: Long) extends Closeable {
    private val stack = new ConcurrentLinkedDeque[UcpMemory]

    def get: UcpMemory = {
      var result = stack.pollFirst()
      if (result == null) {
        result = ucxContext.memoryMap(new UcpMemMapParams().allocate().setLength(length))
      }
      result
    }

    def put(ucpMemory: UcpMemory) {
      stack.add(ucpMemory)
    }

    override def close(): Unit = {
      stack.forEach(_.deregister())
      stack.clear()
    }
  }

  private val allocatorMap = new ConcurrentHashMap[Long, AllocatorStack]()

  private def roundUpToTheNextPowerOf2(size: Long): Long  = {
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
    val ucpMemory = allocatorStack.get
    new UcxHostBounceBufferMemoryBlock(ucpMemory, ucpMemory.getAddress, size)
  }

  override def put(mem: MemoryBlock): Unit = {
    val allocatorStack = allocatorMap.computeIfAbsent(roundUpToTheNextPowerOf2(mem.size),
      s => AllocatorStack(s))
    allocatorStack.put(mem.asInstanceOf[UcxHostBounceBufferMemoryBlock].memory)
  }

  override def close(): Unit = {
    allocatorMap.values.forEach(allocator => allocator.close())
    allocatorMap.clear()
  }
}
