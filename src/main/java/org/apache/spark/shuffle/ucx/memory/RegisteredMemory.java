package org.apache.spark.shuffle.ucx.memory;

import org.openucx.jucx.ucp.UcpMemory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Structure to use 1 memory region for multiple ByteBuffers.
 * Keeps track on reference count to memory region.
 */
public class RegisteredMemory {
  private static final Logger logger = LoggerFactory.getLogger(RegisteredMemory.class);

  private final AtomicInteger refcount;
  private final UcpMemory memory;
  private final ByteBuffer buffer;

  RegisteredMemory(AtomicInteger refcount, UcpMemory memory, ByteBuffer buffer) {
    this.refcount = refcount;
    this.memory = memory;
    this.buffer = buffer;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  AtomicInteger getRefCount() {
    return refcount;
  }

  void deregisterNativeMemory() {
    if (refcount.get() != 0) {
      logger.warn("De-registering memory that has active references.");
    }
    if (memory != null && memory.getNativeId() != null) {
      memory.deregister();
    }
  }
}
