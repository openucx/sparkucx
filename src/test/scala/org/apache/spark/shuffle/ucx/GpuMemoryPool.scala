/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import ai.rapids.cudf.DeviceMemoryBuffer
import org.apache.spark.shuffle.ucx.memory.MemoryPool


/**
 * Test GPU mempool to run with [[ UcxShuffleTransportPerfTool ]]
 */
class GpuMemoryPool extends MemoryPool {

  class GpuMemoryBlock(val deviceBuffer: DeviceMemoryBuffer,
                       override val address: Long, override val size: Long)
    extends MemoryBlock(address, size, isHostMemory = false)

  override def get(size: Long): MemoryBlock = {
    val deviceBuffer = DeviceMemoryBuffer.allocate(size)
    new GpuMemoryBlock(deviceBuffer, deviceBuffer.getAddress, size)
  }

  override def put(mem: MemoryBlock): Unit = {
    mem.asInstanceOf[GpuMemoryBlock].deviceBuffer.close()
  }

  override def close(): Unit = ???
}
