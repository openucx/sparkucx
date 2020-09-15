/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.memory

import java.io.Closeable

import org.apache.spark.shuffle.ucx.MemoryBlock

/**
 * Base class to implement memory pool
 */
abstract class MemoryPool extends Closeable {
  def get(size: Long): MemoryBlock

  def put(mem: MemoryBlock): Unit
}
