/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.reducer;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;

import org.apache.spark.shuffle.ucx.memory.RegisteredMemory;
import org.apache.spark.storage.ShuffleBlockId;
import org.apache.spark.util.Utils;
import org.openucx.jucx.ucp.UcpRequest;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Final callback when all blocks fetched.
 * Notifies Spark's shuffleFetchIterator on block fetch completion.
 */
public class OnBlocksFetchCallback extends ReducerCallback {
  protected RegisteredMemory blocksMemory;
  protected int[] sizes;

  public OnBlocksFetchCallback(ReducerCallback callback, RegisteredMemory blocksMemory, int[] sizes) {
    super(callback);
    this.blocksMemory = blocksMemory;
    this.sizes = sizes;
  }

  @Override
  public void onSuccess(UcpRequest request) {
    logger.info("Endpoint {} fetched {} blocks of total size {} in {}", endpoint.getNativeId(), blockIds.length,
      Utils.bytesToString(Arrays.stream(sizes).sum()), Utils.getUsedTimeMs(startTime));
    int position = 0;
    AtomicInteger refCount = new AtomicInteger(blockIds.length);
    for (int i = 0; i < blockIds.length; i++) {
      ShuffleBlockId block = blockIds[i];
      // Blocks are fetched to contiguous buffer.
      // |----block1---||---block2---||---block3---|
      // Slice each block to avoid buffer copy.
      blocksMemory.getBuffer().position(position).limit(position + sizes[i]);
      ByteBuffer blockBuffer = blocksMemory.getBuffer().slice();
      position += sizes[i];
      // Pass block to Spark's ShuffleFetchIterator.
      listener.onBlockFetchSuccess(block.name(), new NioManagedBuffer(blockBuffer) {
        @Override
        public ManagedBuffer release() {
          if (refCount.decrementAndGet() == 0) {
            mempool.put(blocksMemory);
          }
          return this;
        }
      });
    }
  }
}
